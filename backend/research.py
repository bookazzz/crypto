"""Chronological research runner for the 12 fixed filtered-figure variants.

The historical feature cache is shared by every variant. Each scenario then
replays its own full portfolio over the same minute tape and expenses.
"""
import argparse
import asyncio
import contextlib
from collections import defaultdict, deque
from dataclasses import asdict
from datetime import datetime, timezone
import json
from pathlib import Path
import statistics
import sys
import heapq
import itertools
import shutil
import os

from dotenv import load_dotenv

from .bybit import Bybit
from .engine import Engine
from .models import Candle, Instrument, Settings, Signal
from .patterns import atr, detect, NAMES
from .portfolio_backtest import Cache, DAY, DEFAULT_DIR, MINUTE, TIMEFRAMES, aggregate_minute, csv_file, write_json, progress
from .storage import Store
from .strategy import (CONTEXT, VARIANTS, VERSION, FIGURE_TF, ENTRY_TF,
                       DEFAULT_MIN_BREAKOUT_NOTIONAL_USDT_PER_5M,
                       Variant, clean_rr, gate,
                       line_at, prepare_signal, trend_filter, volume_filter, choose_variant,
                       classify_market_setup, confirm_breakout_5m)

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'data'/'research'
SPLITS=('development','validation','holdout')


def features_cache_path(directory):
    try:
        minimum_5m=float(os.getenv('BREAKOUT_MIN_NOTIONAL_USDT_PER_5M',
                                   DEFAULT_MIN_BREAKOUT_NOTIONAL_USDT_PER_5M))
    except (TypeError,ValueError):
        minimum_5m=DEFAULT_MIN_BREAKOUT_NOTIONAL_USDT_PER_5M
    return Path(directory)/f'features-{VERSION}-notional{minimum_5m:g}.json'


def periods(start, end):
    total=end-start
    a=start+total//2
    b=a+total//4
    return {'development':(start,a),'validation':(a,b),'holdout':(b,end)}


def research_periods(start, end):
    fixed_start=int(datetime(2025,8,5,tzinfo=timezone.utc).timestamp()*1000)
    fixed_end=int(datetime(2026,8,5,tzinfo=timezone.utc).timestamp()*1000)
    if (start,end)==(fixed_start,fixed_end):
        feb=int(datetime(2026,2,5,tzinfo=timezone.utc).timestamp()*1000)
        may=int(datetime(2026,5,5,tzinfo=timezone.utc).timestamp()*1000)
        return {'development':(start,feb),'validation':(feb,may),'holdout':(may,end)}
    return periods(start,end)


def metrics(engine):
    trades=engine.trades
    gross_profit=sum(t['net'] for t in trades if t['net']>0)
    gross_loss=-sum(t['net'] for t in trades if t['net']<0)
    def scenario_metrics(rows):
        profit=sum(t['net'] for t in rows if t['net']>0)
        loss=-sum(t['net'] for t in rows if t['net']<0)
        return {'closed':len(rows),'wins':sum(t['net']>0 for t in rows),
            'win_rate':sum(t['net']>0 for t in rows)/len(rows) if rows else 0.,
            'net':sum(t['net'] for t in rows),
            'profit_factor':profit/loss if loss else (999. if profit else 0.)}
    by_scenario={name:scenario_metrics([t for t in trades if t.get('market_scenario')==name])
                 for name in ('continuation','reversal')}
    return {'closed':len(trades),'wins':sum(t['net']>0 for t in trades),
        'win_rate':sum(t['net']>0 for t in trades)/len(trades) if trades else 0.,
        'net':sum(t['net'] for t in trades),
        'profit_factor':gross_profit/gross_loss if gross_loss else (999. if gross_profit else 0.),
        'max_drawdown':engine.max_drawdown,'open':len(engine.positions),
        'balance':engine.cash,'equity':engine.equity(),
        'funding':sum(t['funding'] for t in trades)+sum(p['funding'] for p in engine.positions),
        'open_positions':engine.positions,'by_market_scenario':by_scenario}


def enrich_signal_report(directory=OUT):
    """Join 15m figure breakouts with their setup class and holdout outcomes."""
    directory=Path(directory)
    report_path=directory/'summary.json';features_path=features_cache_path(directory)
    if not features_path.exists():
        features_path=directory/'features.json'
    if not report_path.exists() or not features_path.exists():
        return None
    report=json.loads(report_path.read_text(encoding='utf-8'))
    features=json.loads(features_path.read_text(encoding='utf-8'))['signals']
    start=report['periods']['holdout']['start'];end=report['periods']['holdout']['end']
    signals_path=directory/'signals.json'
    executed={}
    if signals_path.exists():
        executed={row['id']:row for row in json.loads(signals_path.read_text(encoding='utf-8'))}
    trades_path=directory/'trades.json'
    trades={}
    if trades_path.exists():
        trades={row['id']:row for row in json.loads(trades_path.read_text(encoding='utf-8'))}
    holdout_tested=any(v.get('holdout') is not None for v in report.get('variants',[]))
    pattern_counts={name:{'pattern':name,'name':NAMES[name],'signals':0,'setup_rejected':0,
                          'confirmation_rejected':0,'volume_rejected':0} for name in NAMES}
    scenario_counts={name:{'scenario':name,'signals':0,'eligible':0,'trades':0,'wins':0,'net':0.}
                     for name in ('continuation','reversal','unconfirmed')}
    rows=[]
    for raw in features:
        if not start<=raw['time']<end or raw.get('timeframe')!=FIGURE_TF:
            continue
        row=raw|{'status':'found','reason':''}
        pattern_counts[row['pattern']]['signals']+=1
        filters=row.get('filter_results',{})
        market=filters.get('market_context',{})
        scenario=market.get('scenario','unconfirmed')
        scenario_counts.setdefault(scenario,{'scenario':scenario,'signals':0,'eligible':0,'trades':0,'wins':0,'net':0.})
        bucket=scenario_counts[scenario]
        bucket['signals']+=1
        if not market.get('eligible'):
            row['status']='rejected';row['reason']='Hourly context and 15m structure did not confirm continuation or reversal'
            pattern_counts[row['pattern']]['setup_rejected']+=1
        elif filters.get('confirmation_pass') is not True:
            row['status']='rejected';row['reason']='No closed 5m candle beyond the figure boundary'
            pattern_counts[row['pattern']]['confirmation_rejected']+=1
        elif filters.get('volume_pass') is not True:
            row['status']='rejected'
            volume=filters.get('volume',{})
            if volume.get('notional_pass') is False:
                row['reason']=(f"Breakout turnover {volume['breakout_turnover_usdt']:.0f} USDT "
                    f"is below the {volume['minimum_notional_usdt']:.0f} USDT minimum")
            else:
                row['reason']='5m breakout turnover is below 1.5x its recent median'
            pattern_counts[row['pattern']]['volume_rejected']+=1
        else:
            bucket['eligible']+=1
            if row['id'] in executed:
                row.update(executed[row['id']])
            elif holdout_tested:
                row['status']='rejected';row['reason']='Trade was not opened or was blocked by portfolio limits'
            else:
                row['status']='not_evaluated';row['reason']='No variant passed validation'
            trade=trades.get(row['id'])
            if trade:
                bucket['trades']+=1
                bucket['wins']+=int(float(trade.get('net',0) or 0)>0)
                bucket['net']+=float(trade.get('net',0) or 0)
        rows.append(row)
    report['holdout_signals']={'confirmed':len(rows),
        'setup_rejected':sum(p['setup_rejected'] for p in pattern_counts.values()),
        'confirmation_rejected':sum(p['confirmation_rejected'] for p in pattern_counts.values()),
        'volume_rejected':sum(p['volume_rejected'] for p in pattern_counts.values()),
        'by_pattern':list(pattern_counts.values()),'by_market_scenario':list(scenario_counts.values())}
    write_json(report_path,report)
    write_json(directory/'signals.json',rows)
    csv_file(directory/'signals.csv',rows,['id','symbol','timeframe','pattern','side','time','start','status','reason','entry','stop','target','qty','rr','expected_net','expected_fees','funding_reserve','strategy_version','strategy_variant','entry_method','filter_results'])
    csv_file(directory/'signals_by_pattern.csv',pattern_counts.values(),
             ['pattern','name','signals','setup_rejected','confirmation_rejected','volume_rejected'])
    return report

def _median(values):
    return statistics.median(values) if values else 0.


def warm_from_cache(cache, symbol, tf, start):
    """Rebuild only past bars needed to prime a chronological split."""
    width=tf*MINUTE
    cutoff=start-300*width
    rows=cache.db.execute('''
        SELECT b.bucket, o.o, x.h, x.l, c.c, x.v, x.turnover FROM (
            SELECT (time / ?)*? AS bucket, MIN(time) AS first_time, MAX(time) AS last_time
            FROM minutes WHERE symbol=? AND time>=? AND time<? GROUP BY bucket
        ) b JOIN minutes o ON o.symbol=? AND o.time=b.first_time
          JOIN minutes c ON c.symbol=? AND c.time=b.last_time
          JOIN (SELECT (time / ?)*? AS bucket, MAX(h) AS h, MIN(l) AS l, SUM(v) AS v,
                       SUM(COALESCE(turnover,c*v)) AS turnover
                FROM minutes WHERE symbol=? AND time>=? AND time<? GROUP BY bucket) x ON x.bucket=b.bucket
        ORDER BY b.bucket DESC LIMIT 250
    ''',(width,width,symbol,cutoff,start,symbol,symbol,width,width,symbol,cutoff,start)).fetchall()
    return [Candle(*row) for row in reversed(rows) if row[0]+width<=start]


def scan_features(cache, manifest, period, warmup, output_path):
    """Build confirmed geometry and closed-candle filters once, with no lookahead."""
    start,end=period
    if output_path.exists():
        raw=json.loads(output_path.read_text(encoding='utf-8'))
        return [Signal(**row) for row in raw['signals']],raw['coverage']
    symbols=manifest['symbols']
    tfs=(*TIMEFRAMES,1440)
    buffers={(s,tf):deque(warmup.get((s,tf),[]),maxlen=1000) for s in symbols for tf in tfs}
    cursors={s:cache.cursor(s,start,end) for s in symbols}
    heap=[]
    for symbol in symbols:
        row=next(cursors[symbol],None)
        if row: heapq.heappush(heap,(row[0],symbol,row))
    builders={}
    signals=[]
    coverage={}
    first_by_symbol={}
    for s in symbols:
        first,last,count=cache.db.execute('SELECT MIN(time),MAX(time),COUNT(*) FROM minutes WHERE symbol=? AND time>=? AND time<?',(s,start,end)).fetchone()
        if not count:
            coverage[s]={'days':0.,'full_period':False,'reason':'нет свечей'}
            continue
        # Short listings are reported and start trading from their first bar;
        # gaps after listing invalidate the symbol for this research period.
        contiguous=count==(last-first)//MINUTE+1
        coverage[s]={'first':first,'last':last,'days':count/1440,
                     'full_period':first<=start and last>=end-MINUTE,
                     'complete_since_listing':contiguous}
        first_by_symbol[s]=first
    # Merge symbols by the exact global minute timeline.
    last_progress_day=-1
    while heap:
        timestamp=heap[0][0]
        minute_bars={}
        while heap and heap[0][0]==timestamp:
            _,symbol,row=heapq.heappop(heap)
            minute_bars[symbol]=Candle(*row)
            nxt=next(cursors[symbol],None)
            if nxt: heapq.heappush(heap,(nxt[0],symbol,nxt))
        for symbol,candle in minute_bars.items():
            if not coverage[symbol].get('complete_since_listing') or timestamp<first_by_symbol[symbol]:
                continue
            completed=[]
            for tf in tfs:
                complete=aggregate_minute(builders,symbol,tf,candle)
                if complete:
                    buffers[(symbol,tf)].append(complete)
                    completed.append(tf)
            # Make same-timestamp 5m, 15m and 1h closes visible before any
            # cross-timeframe classification, without reading future bars.
            for tf in completed:
                if tf==1440:
                    continue
                candles=list(buffers[(symbol,tf)])
                found=detect(symbol,str(tf),candles)
                if not found:
                    continue
                signal_time=found[0].time
                hourly=[c for c in buffers[(symbol,60)] if c.time+60*MINUTE<=signal_time]
                fifteen=[c for c in buffers[(symbol,15)] if c.time+15*MINUTE<=signal_time]
                five=[c for c in buffers[(symbol,5)] if c.time+5*MINUTE<=signal_time]
                for signal in found:
                    market=classify_market_setup(hourly,fifteen,signal.side)
                    confirmation_ok,confirmation=confirm_breakout_5m(five,signal)
                    volume_ok,volume=volume_filter(five,ENTRY_TF)
                    signal.strategy_version=VERSION
                    signal.filter_results={'market_context':market,'confirmation_5m':confirmation,
                        'entry_atr':float(confirmation.get('atr_5m') or signal.atr/3),
                        'volume':volume,'trend_pass':market['eligible'],
                        'confirmation_pass':confirmation_ok,'volume_pass':volume_ok,
                        'auto_eligible':(signal.timeframe==FIGURE_TF and market['eligible']
                                         and confirmation_ok and volume_ok)}
                    signals.append(signal)
        day=(timestamp-start)//DAY
        if day>last_progress_day:
            last_progress_day=day
            progress(OUT,stage='features',message=f'Подтверждённые признаки: {day}/{(end-start)/DAY:.0f} дней')
    write_json(output_path,{'signals':[s.dict() for s in signals],'coverage':coverage})
    return signals,coverage


def prepare_events(signals, start, end):
    events=defaultdict(list)
    for signal in signals:
        if not start<=signal.time<end:
            continue
        filters=signal.filter_results
        if (signal.timeframe==FIGURE_TF and filters.get('auto_eligible')
                and filters.get('volume_pass')):
            # Each portfolio/variant must start from the immutable feature cache.
            events[signal.time].append(Signal(**signal.dict()))
    return events


async def simulate(cache, bybit, manifest, warmup, split, variant, slippage, feature_signals):
    start,end=split
    store=Store(':memory:')
    settings=Settings(**manifest['settings'])
    settings.slippage=slippage
    settings.min_rr=variant.net_rr
    engine=Engine(store,settings)
    engine.instruments={s:Instrument(**x) for s,x in manifest['instruments'].items()}
    engine.connected=True
    engine.fees={s:{'rate':.00055,'time':start,'source':'bybit_vip0_standard'} for s in manifest['symbols']}
    latest={s:0. for s in manifest['symbols']}
    for s in manifest['symbols']:
        prior=cache.db.execute('SELECT rate FROM funding WHERE symbol=? AND time<? ORDER BY time DESC LIMIT 1',(s,start)).fetchone()
        if prior: latest[s]=prior[0]
    by_time=prepare_events(feature_signals,start,end)
    events=[]
    event_seq=itertools.count()
    def push_event(time,priority,key,kind,payload):
        heapq.heappush(events,(time,priority,key,next(event_seq),kind,payload))
    for time,candidates in by_time.items():
        for signal in candidates: push_event(time,2,signal.id,'candidate',signal)
    for symbol in manifest['symbols']:
        for time,rate in cache.funding_rows(symbol,start,end):
            push_event(time,0,symbol,'funding',(symbol,rate))
    retests={}
    curve=[]
    def minute(symbol,time):
        row=cache.db.execute('SELECT time,o,h,l,c,v,turnover FROM minutes WHERE symbol=? AND time=?',(symbol,time)).fetchone()
        return Candle(*row) if row else None
    def timeframe_bar(symbol,tf,close_time):
        width=tf*MINUTE;left=close_time-width
        row=cache.db.execute('''SELECT COUNT(*),MIN(time),MAX(time),
            (SELECT o FROM minutes WHERE symbol=? AND time=(SELECT MIN(time) FROM minutes WHERE symbol=? AND time>=? AND time<?)),
            MAX(h),MIN(l),
            (SELECT c FROM minutes WHERE symbol=? AND time=(SELECT MAX(time) FROM minutes WHERE symbol=? AND time>=? AND time<?)),SUM(v),
            SUM(COALESCE(turnover,c*v))
            FROM minutes WHERE symbol=? AND time>=? AND time<?''',
            (symbol,symbol,left,close_time,symbol,symbol,left,close_time,symbol,left,close_time)).fetchone()
        if not row or row[0]!=tf or row[1]!=left or row[2]!=close_time-MINUTE:
            return None
        return Candle(left,row[3],row[4],row[5],row[6],row[7],row[8])
    def entry_atr_5m(symbol,close_time):
        width=5*MINUTE
        history=[timeframe_bar(symbol,5,close_time-offset*width) for offset in range(14,-1,-1)]
        return atr([bar for bar in history if bar is not None])
    engine.daily(start)
    while events:
        timestamp=events[0][0]
        batch=[]
        while events and events[0][0]==timestamp:
            batch.append(heapq.heappop(events))
        funding_batch=[e[5] for e in batch if e[4]=='funding']
        bar_batch=[e[5] for e in batch if e[4]=='position_bar']
        retest_batch=[e[5] for e in batch if e[4]=='retest']
        candidate_batch=[e[5] for e in batch if e[4]=='candidate']
        due=[]
        symbols_at_time={s for s,_ in funding_batch}|{s for s,_ in bar_batch}
        symbols_at_time|={s.symbol for s in candidate_batch}|{x['signal'].symbol for x in retest_batch}
        candles={s:c for s in symbols_at_time if (c:=minute(s,timestamp)) is not None}
        quotes={s:{'price':c.open,'time':timestamp} for s,c in candles.items()}
        engine.daily(timestamp)
        for symbol,rate in funding_batch:
            latest[symbol]=rate
            if any(p['symbol']==symbol for p in engine.positions):
                mark=cache.mark(symbol,timestamp)
                if mark is None:
                    mark_candle=candles.get(symbol) or minute(symbol,timestamp)
                    if mark_candle is None:
                        marks=await bybit.candles(symbol,'1',timestamp,timestamp,True)
                        if not marks: raise RuntimeError(f'{symbol}: отсутствует mark price для funding {timestamp}')
                        mark=marks[0].open
                    else: mark=mark_candle.open
                    cache.save_mark(symbol,timestamp,mark)
                engine.funding_payment(symbol,rate,timestamp,mark)
        for symbol,candle in candles.items():
            engine.tick(symbol,candle.open,timestamp)
        for item in retest_batch:
            signal=item['signal'];tf=int(ENTRY_TF);closed=timeframe_bar(signal.symbol,tf,timestamp)
            n=item['bars']+1;sign=1 if signal.side=='long' else -1
            if closed is None:
                retests.pop(signal.id,None);continue
            entry_atr=entry_atr_5m(signal.symbol,timestamp) or float(signal.filter_results.get('entry_atr',signal.atr/3))
            boundary=line_at(signal,closed.time)
            stop_hit=closed.low<=signal.stop if sign>0 else closed.high>=signal.stop
            if stop_hit or sign*(closed.close-boundary)<-.25*entry_atr or n>3:
                retests.pop(signal.id,None);continue
            touched=closed.low<=boundary+.25*entry_atr and closed.high>=boundary-.25*entry_atr
            confirmed=sign*(closed.close-boundary)>=.1*entry_atr
            item['extreme']=min(item['extreme'],closed.low) if sign>0 else max(item['extreme'],closed.high)
            item['bars']=n
            if touched and confirmed:
                signal.boundary=boundary;signal.stop=item['extreme']-sign*.2*entry_atr
                signal.filter_results['entry_atr']=entry_atr
                signal.entry_method='retest'
                if sign*(closed.close-signal.stop)>=.8*entry_atr: due.append(signal)
                retests.pop(signal.id,None)
            elif n<3:
                next_time=timestamp+int(ENTRY_TF)*MINUTE
                push_event(next_time,1,signal.id,'retest',item)
            else:
                retests.pop(signal.id,None)
        for signal in candidate_batch:
            if variant.entry=='immediate': due.append(signal)
            else:
                pending={'signal':Signal(**signal.dict()),'bars':0,'extreme':signal.boundary}
                retests[signal.id]=pending
                push_event(timestamp+int(ENTRY_TF)*MINUTE,1,signal.id,'retest',pending)
        executable=[]
        for signal in due:
            signal.strategy_version=VERSION;signal.strategy_variant=variant.id
            signal.entry_method=variant.entry
            signal.filter_results.update({'required_net_rr':variant.net_rr,
                'max_hold_ms':(variant.time_exit or 0)*int(ENTRY_TF)*MINUTE})
            quote=quotes.get(signal.symbol)
            if not quote: continue
            tick=engine.instruments[signal.symbol].tick_size
            ok,reason,details=prepare_signal(signal,variant,quote['price'],.00055,slippage,latest.get(signal.symbol,0.),tick)
            signal.filter_results.update(details)
            if ok: executable.append(signal)
            else:
                signal.status='rejected';signal.reason=reason;engine.persist([signal])
        if executable: engine.process(executable,quotes,timestamp,latest)
        # Intraminute conservative stops/targets run after entries at this open.
        for symbol,candle in candles.items():
            if any(p['symbol']==symbol for p in engine.positions):
                engine.bar(symbol,candle,timestamp+MINUTE-1,False)
        # Only active positions need minute-by-minute market events.
        active={p['symbol'] for p in engine.positions}
        for symbol in active:
            next_bar=minute(symbol,timestamp+MINUTE)
            if next_bar:
                push_event(next_bar.time,1,symbol,'position_bar',(symbol,next_bar))
        if timestamp//DAY>(curve[-1]['time']//DAY if curve else start//DAY):
            curve.append({'time':timestamp,'equity':engine.equity(),'cash':engine.cash,'drawdown':engine.max_drawdown})
    engine.daily(end-1)
    curve.append({'time':end,'equity':engine.equity(),'cash':engine.cash,'drawdown':engine.max_drawdown})
    result=metrics(engine)
    result['signals']=store.signals(1000000)
    store.close()
    return result,engine.trades,curve


async def run(directory=OUT):
    directory=Path(directory);directory.mkdir(parents=True,exist_ok=True)
    summary_path=directory/'summary.json'
    if summary_path.exists():
        previous=json.loads(summary_path.read_text(encoding='utf-8'))
        if previous.get('version')!=VERSION:
            archive=directory/'previous-invalid-v1'
            archive.mkdir(exist_ok=True)
            for name in ('summary.json','variants.csv','signals.csv','signals.json','signals_by_pattern.csv',
                         'trades.csv','trades.json','equity.csv','variants-progress.json'):
                source=directory/name
                if source.is_file() and not (archive/name).exists(): shutil.copy2(source,archive/name)
            write_json(summary_path,{'version':VERSION,'approval':{'status':'replay_required','eligible':False,
                'message':'Предыдущий прогон был искажён изменением общих объектов сигналов; результаты архивированы.'},
                'previous_report':str(archive.relative_to(ROOT))})
    load_dotenv(ROOT/'.env')
    legacy=DEFAULT_DIR/'manifest.json'
    if not legacy.exists():
        raise RuntimeError('Сначала нужен зафиксированный список 20 пар из предыдущего прогона')
    prior=json.loads(legacy.read_text(encoding='utf-8'))
    start=int(datetime(2025,8,5,tzinfo=timezone.utc).timestamp()*1000)
    end=int(datetime(2026,8,5,tzinfo=timezone.utc).timestamp()*1000)
    manifest=prior|{'captured_at':prior['captured_at'],'start':start,'end':end,'days':365,
                    'research_version':VERSION,'periods':research_periods(start,end),
                    'timeframes':[str(x) for x in TIMEFRAMES],
                    'symbols_source':'зафиксированный top-20 из data/portfolio_60d/manifest.json'}
    write_json(directory/'manifest.json',manifest)
    bybit=Bybit();cache=Cache(directory/'market.sqlite3')
    progress(directory,stage='download',message='Загрузка минутной истории и funding за 12 месяцев')
    try:
        # Reuse daily chunk logic but preserve shorter listings in coverage.
        segments=[]
        edge=start
        while edge<end:
            nxt=min(end,(edge//DAY+1)*DAY)
            for symbol in manifest['symbols']:
                if not cache.has_segment(symbol,edge,nxt): segments.append((symbol,edge,nxt))
            edge=nxt
        sem=asyncio.Semaphore(3)
        done=0
        async def pull(symbol,a,b):
            nonlocal done
            async with sem:
                cache.save_segment(symbol,a,b,await bybit.history(symbol,'1',a,b-1))
                done+=1
                if done%20==0 or done==len(segments):
                    progress(directory,stage='download',message=f'Загружено {done}/{len(segments)} дневных сегментов')
        for i in range(0,len(segments),30): await asyncio.gather(*(pull(*x) for x in segments[i:i+30]))
        for symbol in manifest['symbols']:
            if not cache.funding_loaded(symbol):
                cache.save_funding(symbol,await bybit.funding_history(symbol,start-DAY,end))
        warm={}
        for symbol in manifest['symbols']:
            for tf in (*TIMEFRAMES,1440):
                end_bar=(start//(tf*MINUTE))*(tf*MINUTE)-tf*MINUTE
                warm[(symbol,tf)]=[c for c in await bybit.candles(symbol,'D' if tf==1440 else str(tf),
                    end_bar-300*tf*MINUTE,end_bar) if c.time+tf*MINUTE<=start][-250:]
        feature_path=features_cache_path(directory)
        features,coverage=scan_features(cache,manifest,(start,end),warm,feature_path)
        write_json(directory/'coverage.json',coverage)
        split=research_periods(start,end)
        checkpoint_path=directory/f'variants-progress-{VERSION}.json'
        completed={}
        if checkpoint_path.exists():
            with contextlib.suppress(OSError,ValueError):
                completed={v['id']:v for v in json.loads(checkpoint_path.read_text(encoding='utf-8'))}
        # Older checkpoints stored complete signal payloads inside aggregate
        # metrics. Keep only metrics so the resumed comparison stays compact.
        for row in completed.values():
            for split_name in ('development','validation'):
                if isinstance(row.get(split_name),dict): row[split_name].pop('signals',None)
        if completed: write_json(checkpoint_path,list(completed.values()))
        variants=[]
        selected=None
        # All variants are scored on validation; final holdout is only run once
        # for the selected candidate below.
        for variant in VARIANTS:
            row=completed.get(variant.id) or {'id':variant.id,**variant.dict(),'development':None,'validation':None,'holdout':None,'approval':'не проверен'}
            if row['development'] is None:
                row['development'],_,_=await simulate(cache,bybit,manifest,warm,split['development'],variant,.0005,features)
                row['development'].pop('signals',None)
                write_json(checkpoint_path,[*completed.values(),row]);completed[variant.id]=row
            if row['validation'] is None:
                row['validation'],_,_=await simulate(cache,bybit,manifest,warm,split['validation'],variant,.0005,features)
                row['validation'].pop('signals',None)
                write_json(checkpoint_path,[*completed.values(),row]);completed[variant.id]=row
            variants.append(row)
            write_json(directory/'progress.json',{'stage':'validation','message':f'Проверено вариантов: {len(variants)}/12'})
        choice=choose_variant(variants)
        if choice:
            chosen=next(v for v in VARIANTS if v.id==choice['id'])
            final,trade_rows,curve=await simulate(cache,bybit,manifest,warm,split['holdout'],chosen,.0005,features)
            final_signals=final.pop('signals',[])
            doubled,_,_=await simulate(cache,bybit,manifest,warm,split['holdout'],chosen,.001,features)
            doubled.pop('signals',None)
            allowed,reason=gate(final,doubled['net'])
            diagnostic_message='Повтор holdout использует уже просмотренный исторический период; независимым подтверждением прибыльности не является.'
            choice['holdout']={k:v for k,v in final.items() if k!='signals'}
            choice['approval']=reason if not allowed else diagnostic_message
            choice['holdout_diagnostic_only']=True
            choice['stress_test_net']=doubled['net']
            # A previously inspected holdout can never open the live-entry gate.
            selected=None
            reason=diagnostic_message
            write_json(directory/'signals.json',final_signals)
            csv_file(directory/'signals.csv',final_signals,['id','symbol','timeframe','pattern','side','time','start','status','reason','entry','stop','target','qty','rr','expected_net','expected_fees','funding_reserve','strategy_version','strategy_variant','entry_method','filter_results'])
            write_json(directory/'trades.json',trade_rows)
            csv_file(directory/'trades.csv',trade_rows,['id','symbol','timeframe','pattern','side','market_scenario','opened','closed','entry','exit','stop','target','qty','gross','entry_fee','exit_fee','funding','net','reason','recovered'])
            csv_file(directory/'equity.csv',curve,['time','equity','cash','drawdown'])
        else:
            enough=any(v['validation']['closed']>=100 for v in variants)
            reason=('недостаточно данных: на валидации меньше 100 закрытых сделок у всех вариантов'
                    if not enough else 'проверка не пройдена: на валидации нет положительных вариантов с просадкой до 10%')
        report={'version':VERSION,'manifest':manifest,'periods':{k:{'start':v[0],'end':v[1]} for k,v in split.items()},
                'costs':{'taker_fee_per_side':.00055,'slippage_per_side':.0005,
                         'stress_slippage_per_side':.001,'initial_balance':1000,
                         'risk_per_trade':.005,'max_positions':2,'daily_loss_limit':.02,
                         'funding':'historical Bybit funding at settlement timestamps'},
                'coverage':coverage,'variants':variants,'selected':selected,
                'approval':{'status':'passed' if selected else 'diagnostic_only' if any(v.get('holdout_diagnostic_only') for v in variants) else 'insufficient_data' if reason.startswith('недостаточно данных') else 'failed','eligible':bool(selected),
                            'message':reason if not selected else 'Условия окончательной проверки выполнены'},
                'audit':{'purpose':'diagnostic_replay_after_simulation_isolation_fix',
                    'holdout_independent':False,'automatic_entry_eligible':False,
                    'reason':'The historical holdout period was already inspected before this corrected replay.'},
                'message':'Пары ограничены зафиксированным снимком top-20 на момент предыдущего исследования.'}
        write_json(directory/'summary.json',report)
        enrich_signal_report(directory)
        if not (directory/'signals.csv').exists():
            csv_file(directory/'signals.csv',({'id':s.id,'symbol':s.symbol,'timeframe':s.timeframe,
                'pattern':s.pattern,'side':s.side,'time':s.time,'start':s.start,'status':'found',
                'reason':'','filter_results':json.dumps(s.filter_results,ensure_ascii=False)} for s in features),
                ['id','symbol','timeframe','pattern','side','time','start','status','reason','filter_results'])
        if not (directory/'trades.csv').exists():
            csv_file(directory/'trades.csv',[],['id','symbol','timeframe','pattern','side','market_scenario','opened','closed','entry','exit','stop','target','qty','gross','entry_fee','exit_fee','funding','net','reason','recovered'])
        if not (directory/'equity.csv').exists():
            csv_file(directory/'equity.csv',[],['time','equity','cash','drawdown'])
        flat_variants=[]
        for v in variants:
            flat={'id':v['id'],'entry':v['entry'],'net_rr':v['net_rr'],'time_exit':v['time_exit'],'approval':v['approval']}
            for split_name in SPLITS:
                stats=v.get(split_name) or {}
                for field in ('closed','win_rate','net','profit_factor','max_drawdown','open','funding'):
                    flat[f'{split_name}_{field}']=stats.get(field)
                for scenario in ('continuation','reversal'):
                    scenario_stats=stats.get('by_market_scenario',{}).get(scenario,{})
                    for field in ('closed','win_rate','net','profit_factor'):
                        flat[f'{split_name}_{scenario}_{field}']=scenario_stats.get(field)
            flat_variants.append(flat)
        csv_file(directory/'variants.csv',flat_variants,['id','entry','net_rr','time_exit','approval',
            *[f'{name}_{field}' for name in SPLITS for field in ('closed','win_rate','net','profit_factor','max_drawdown','open','funding')],
            *[f'{name}_{scenario}_{field}' for name in SPLITS
              for scenario in ('continuation','reversal')
              for field in ('closed','win_rate','net','profit_factor')]])
        progress(directory,stage='complete',message=report['approval']['message'])
        return report
    finally:
        cache.db.close();await bybit.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--directory',default=str(OUT))
    args=parser.parse_args()
    try: asyncio.run(run(args.directory))
    except Exception as exc:
        OUT.mkdir(parents=True,exist_ok=True)
        write_json(OUT/'progress.json',{'stage':'error','message':str(exc)})
        print(f'Ошибка исследования: {exc}',file=sys.stderr);raise
