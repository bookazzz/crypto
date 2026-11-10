"""Resumable, read-only Bybit download and one-capital historical paper replay.

Run with ``python -m backend.portfolio_backtest``. The cache, replay state and
reports are separate from the live scanner's database.
"""
import argparse
import asyncio
from collections import deque, defaultdict
import csv
from dataclasses import asdict
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import sqlite3

from dotenv import load_dotenv
from .bybit import Bybit
from .engine import Engine
from .models import Candle, Instrument, Settings, now_ms
from .patterns import NAMES, detect
from .storage import Store

MINUTE = 60_000
DAY = 1_440 * MINUTE
TIMEFRAMES = (5, 15, 60, 240)
FEE = .00055
DEFAULT_DIR = Path(__file__).resolve().parents[1] / 'data' / 'portfolio_60d'


def write_json(path, value):
    path = Path(path)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')
    os.replace(tmp, path)


def progress(directory, **fields):
    path = directory / 'progress.json'
    current = json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
    current.update(fields)
    write_json(path, current)
    print(f"{current.get('stage', '')}: {current.get('message', '')}", flush=True)


class Cache:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS minutes(symbol TEXT, time INTEGER, o REAL, h REAL, l REAL,
                c REAL, v REAL, turnover REAL, PRIMARY KEY(symbol,time));
            CREATE TABLE IF NOT EXISTS segments(symbol TEXT, start INTEGER, end INTEGER,
                count INTEGER, PRIMARY KEY(symbol,start,end));
            CREATE TABLE IF NOT EXISTS funding(symbol TEXT, time INTEGER, rate REAL,
                PRIMARY KEY(symbol,time));
            CREATE TABLE IF NOT EXISTS funding_loaded(symbol TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS marks(symbol TEXT, time INTEGER, price REAL,
                PRIMARY KEY(symbol,time));
        ''')
        columns={row[1] for row in self.db.execute('PRAGMA table_info(minutes)')}
        if 'turnover' not in columns:
            self.db.execute('ALTER TABLE minutes ADD COLUMN turnover REAL')

    def has_segment(self, symbol, start, end):
        exists=self.db.execute('SELECT 1 FROM segments WHERE symbol=? AND start=? AND end=?',
                               (symbol,start,end)).fetchone() is not None
        missing_turnover=self.db.execute('SELECT 1 FROM minutes WHERE symbol=? AND time>=? AND time<? AND turnover IS NULL LIMIT 1',
                                         (symbol,start,end)).fetchone() is not None
        return exists and not missing_turnover

    def save_segment(self, symbol, start, end, candles):
        with self.db:
            self.db.executemany('INSERT OR REPLACE INTO minutes VALUES(?,?,?,?,?,?,?,?)',
                [(symbol,c.time,c.open,c.high,c.low,c.close,c.volume,c.turnover) for c in candles])
            self.db.execute('INSERT OR REPLACE INTO segments VALUES(?,?,?,?)',
                            (symbol,start,end,len(candles)))

    def save_funding(self, symbol, rows):
        with self.db:
            self.db.executemany('INSERT OR REPLACE INTO funding VALUES(?,?,?)',
                                [(symbol,t,r) for t,r in rows])
            self.db.execute('INSERT OR REPLACE INTO funding_loaded VALUES(?)',(symbol,))

    def funding_loaded(self, symbol):
        return self.db.execute('SELECT 1 FROM funding_loaded WHERE symbol=?',(symbol,)).fetchone() is not None

    def coverage(self, symbol, start, end):
        row = self.db.execute('SELECT MIN(time),MAX(time),COUNT(*) FROM minutes WHERE symbol=? AND time>=? AND time<?',
                              (symbol,start,end)).fetchone()
        first,last,count = row
        if not count or last<end-MINUTE or count != (last-first)//MINUTE+1:
            raise RuntimeError(f'{symbol}: пропуски минутных свечей после начала торгов; сделки не рассчитываются')
        return {'first':first,'last':last,'minutes':count,
                'days':count/1440,'full_60_days':bool(count and first<=start and last>=end-MINUTE)}

    def cursor(self, symbol, start, end):
        return self.db.execute('SELECT time,o,h,l,c,v,turnover FROM minutes WHERE symbol=? AND time>=? AND time<? ORDER BY time',
                               (symbol,start,end))

    def funding_rows(self, symbol, start, end):
        return self.db.execute('SELECT time,rate FROM funding WHERE symbol=? AND time>=? AND time<? ORDER BY time',
                               (symbol,start,end)).fetchall()

    def mark(self, symbol, time):
        row=self.db.execute('SELECT price FROM marks WHERE symbol=? AND time=?',(symbol,time)).fetchone()
        return row[0] if row else None

    def save_mark(self, symbol, time, price):
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO marks VALUES(?,?,?)',(symbol,time,price))


async def snapshot(bybit, directory, days=60, symbols_count=20):
    path=directory/'manifest.json'
    if path.exists():
        return json.loads(path.read_text(encoding='utf-8'))
    instruments,symbols,tickers=await bybit.universe(symbols_count)
    end=now_ms()//MINUTE*MINUTE
    start=end-days*DAY
    manifest={'captured_at':now_ms(),'start':start,'end':end,'days':days,
              'symbols':symbols,'timeframes':[str(x) for x in TIMEFRAMES],
              'turnover24h':{s:float(tickers[s]['turnover24h']) for s in symbols},
              'instruments':{s:asdict(instruments[s]) for s in symbols},
              'fee_rate':FEE,'settings':asdict(Settings())}
    write_json(path,manifest)
    return manifest


async def download(bybit, cache, manifest, directory):
    start,end=manifest['start'],manifest['end']
    segments=[]
    for symbol in manifest['symbols']:
        edge=start
        while edge<end:
            next_edge=min(end,(edge//DAY+1)*DAY)
            if not cache.has_segment(symbol,edge,next_edge):
                segments.append((symbol,edge,next_edge))
            edge=next_edge
    total=len(segments)
    progress(directory,stage='download',message=f'Свечи: осталось {total} сегментов',segments_remaining=total)
    done=0
    # Limit both API pressure and pending task memory. The Bybit adapter also
    # restricts active HTTP requests to three.
    sem=asyncio.Semaphore(3)
    async def one(item):
        nonlocal done
        symbol,a,b=item
        async with sem:
            rows=await bybit.history(symbol,'1',a,b-1)
            cache.save_segment(symbol,a,b,rows)
            done+=1
            if done%20==0 or done==total:
                progress(directory,stage='download',message=f'Свечи: {done}/{total} сегментов',
                         segments_remaining=total-done)
    # Batches prevent creating thousands of live tasks.
    for i in range(0,total,30):
        await asyncio.gather(*(one(x) for x in segments[i:i+30]))
    for symbol in manifest['symbols']:
        if not cache.funding_loaded(symbol):
            cache.save_funding(symbol,await bybit.funding_history(symbol,start-DAY,end))
        elif not cache.db.execute('SELECT 1 FROM funding WHERE symbol=? AND time<? LIMIT 1',
                                  (symbol,start)).fetchone():
            # A resume from an older cache also needs the last pre-period rate
            # for the first funding-reserve calculation.
            cache.save_funding(symbol,await bybit.funding_history(symbol,start-DAY,start-1))
    coverage={s:cache.coverage(s,start,end) for s in manifest['symbols']}
    write_json(directory/'coverage.json',coverage)
    progress(directory,stage='downloaded',message='Загрузка и проверка свечей завершены')
    return coverage


async def warmup(bybit, manifest, directory):
    path=directory/'warmup.json'
    if path.exists():
        raw=json.loads(path.read_text(encoding='utf-8'))
        return {(s,int(tf)):[Candle(**c) for c in rows] for s,tfs in raw.items() for tf,rows in tfs.items()}
    start=manifest['start']
    result={}
    for symbol in manifest['symbols']:
        for tf in TIMEFRAMES:
            tfms=tf*MINUTE
            last=(start//tfms)*tfms-tfms
            rows=await bybit.candles(symbol,str(tf),last-300*tfms,last)
            result[(symbol,tf)]=[c for c in rows if c.time+tfms<=start][-250:]
        progress(directory,stage='warmup',message=f'Прогрев: {len(result)//4}/{len(manifest["symbols"])} пар')
    write_json(path,{s:{str(tf):[asdict(c) for c in result[(s,tf)]] for tf in TIMEFRAMES}
                     for s in manifest['symbols']})
    return result


def save_checkpoint(store, engine, checkpoint_path, next_time, buffers, latest_funding):
    engine.persist()
    payload={'next_time':next_time,
             'buffers':{s:{str(tf):[asdict(c) for c in buffers[(s,tf)]] for tf in TIMEFRAMES}
                        for s in engine.instruments},
             'latest_funding':latest_funding}
    with store.db:
        store.db.execute('CREATE TABLE IF NOT EXISTS checkpoint(id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT)')
        store.db.execute('INSERT OR REPLACE INTO checkpoint VALUES(1,?)',(json.dumps(payload),))
    tmp=checkpoint_path.with_suffix('.tmp')
    if tmp.exists():
        tmp.unlink()
    target=sqlite3.connect(tmp)
    store.db.backup(target)
    target.close()
    os.replace(tmp,checkpoint_path)


def load_checkpoint(directory):
    path=directory/'checkpoint.sqlite3'
    active=directory/'replay.sqlite3'
    if path.exists():
        shutil.copyfile(path,active)
    elif active.exists():
        active.unlink()
    store=Store(str(active))
    row=store.db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='checkpoint'").fetchone()
    state=json.loads(store.db.execute('SELECT payload FROM checkpoint WHERE id=1').fetchone()[0]) if row else None
    return store,state


def aggregate_minute(builder, symbol, tf, candle):
    key=(symbol,tf)
    start=candle.time//(tf*MINUTE)*(tf*MINUTE)
    pending=builder.get(key)
    if pending is None or pending[0]!=start:
        pending=[start,0,candle.open,candle.high,candle.low,candle.close,0.,0.]
        builder[key]=pending
    pending[1]+=1
    pending[3]=max(pending[3],candle.high)
    pending[4]=min(pending[4],candle.low)
    pending[5]=candle.close
    pending[6]+=candle.volume
    pending[7]+=(candle.turnover if candle.turnover is not None else candle.close*candle.volume)
    if pending[1]==tf and candle.time+MINUTE==start+tf*MINUTE:
        del builder[key]
        return Candle(start,pending[2],pending[3],pending[4],pending[5],pending[6],pending[7])
    return None


async def replay(bybit, cache, manifest, warm, directory):
    start,end=manifest['start'],manifest['end']
    store,checkpoint=load_checkpoint(directory)
    engine=Engine(store,Settings(**manifest['settings']))
    engine.instruments={s:Instrument(**raw) for s,raw in manifest['instruments'].items()}
    engine.connected=True
    symbols=manifest['symbols']
    if checkpoint:
        buffers={(s,int(tf)):deque((Candle(**c) for c in rows),maxlen=250)
                 for s,tfs in checkpoint['buffers'].items() for tf,rows in tfs.items()}
        latest_funding=checkpoint['latest_funding']
        current=checkpoint['next_time']
    else:
        buffers={(s,tf):deque(warm[(s,tf)],maxlen=250) for s in symbols for tf in TIMEFRAMES}
        latest_funding={s:0. for s in symbols}
        current=start
    # Cursor-per-symbol uses O(symbols * 250 * timeframes) RAM, not all 1.7m candles.
    cursors={s:cache.cursor(s,current,end) for s in symbols}
    next_rows={s:next(cursors[s],None) for s in symbols}
    funding={s:deque(cache.funding_rows(s,start-DAY,end)) for s in symbols}
    for symbol in symbols:
        while funding[symbol] and funding[symbol][0][0]<current:
            _,latest_funding[symbol]=funding[symbol].popleft()
    builder={}
    last_checkpoint=current
    progress(directory,stage='replay',message=f'Прогон с {datetime.fromtimestamp(current/1000,timezone.utc).isoformat()}')
    for timestamp in range(current,end,MINUTE):
        minutes={}
        for symbol in symbols:
            row=next_rows[symbol]
            if row and row[0]==timestamp:
                minutes[symbol]=Candle(*row)
                next_rows[symbol]=next(cursors[symbol],None)
        # Funding at the boundary applies to positions held before this minute.
        for symbol in symbols:
            while funding[symbol] and funding[symbol][0][0]<=timestamp:
                ft,rate=funding[symbol].popleft()
                if ft==timestamp and any(p['symbol']==symbol for p in engine.positions):
                    mark=cache.mark(symbol,ft)
                    if mark is None:
                        rows=await bybit.candles(symbol,'1',ft,ft,True)
                        if not rows:
                            raise RuntimeError(f'{symbol}: нет mark price для funding {ft}')
                        mark=rows[0].open
                        cache.save_mark(symbol,ft,mark)
                    engine.funding_payment(symbol,rate,ft,mark)
                latest_funding[symbol]=rate
        # Close completed timeframe bars strictly before the new minute opens.
        pending=[]
        for symbol in symbols:
            for tf in TIMEFRAMES:
                if timestamp%(tf*MINUTE)==0 and buffers[(symbol,tf)]:
                    latest=buffers[(symbol,tf)][-1]
                    if latest.time>=start and latest.time+tf*MINUTE==timestamp and symbol in minutes:
                        pending.extend(detect(symbol,str(tf),list(buffers[(symbol,tf)])))
        quotes={}
        for symbol,c in minutes.items():
            engine.tick(symbol,c.open,timestamp)
            quotes[symbol]={'price':c.open,'time':timestamp}
            if timestamp-engine.fees.get(symbol,{}).get('time',-DAY-1)>DAY//2:
                engine.set_fee(symbol,FEE,timestamp,'bybit_vip0_standard')
        if pending:
            engine.process(pending,quotes,timestamp,latest_funding)
        for symbol,c in minutes.items():
            engine.bar(symbol,c,timestamp+MINUTE-1,False)
            for tf in TIMEFRAMES:
                complete=aggregate_minute(builder,symbol,tf,c)
                if complete:
                    buffers[(symbol,tf)].append(complete)
        if timestamp//DAY != (timestamp+MINUTE)//DAY or timestamp+MINUTE==end:
            save_checkpoint(store,engine,directory/'checkpoint.sqlite3',timestamp+MINUTE,buffers,latest_funding)
            progress(directory,stage='replay',message=f'Обработано {(timestamp+MINUTE-start)/DAY:.1f}/{manifest["days"]} дней',
                     replay_time=timestamp+MINUTE)
            last_checkpoint=timestamp+MINUTE
    if last_checkpoint<end:
        save_checkpoint(store,engine,directory/'checkpoint.sqlite3',end,buffers,latest_funding)
    return store,engine


def csv_file(path, rows, fields):
    with open(path,'w',encoding='utf-8-sig',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)


def report(store,engine,manifest,coverage,directory):
    signals=[json.loads(r[0]) for r in store.db.execute('SELECT payload FROM signals ORDER BY time,id')]
    trades=engine.trades
    by_id={t['id']:t for t in trades}
    groups={p:{'pattern':p,'name':NAMES[p],'signals':0,'opened':0,'rejected':0,
               'closed':0,'wins':0,'win_rate':0.,'net':0.,'open':0} for p in NAMES}
    by_tf={str(tf):{'signals':0,'opened':0,'rejected':0,'closed':0,'wins':0,'net':0.}
           for tf in TIMEFRAMES}
    by_pair={s:{'signals':0,'opened':0,'rejected':0,'closed':0,'wins':0,'net':0.}
             for s in manifest['symbols']}
    reasons=defaultdict(int)
    for s in signals:
        trade=by_id.get(s['id'])
        opened=s['status']!='rejected'
        for g in (groups[s['pattern']],by_tf[s['timeframe']],by_pair[s['symbol']]):
            g['signals']+=1
            g['opened' if opened else 'rejected']+=1
            if trade:
                g['closed']+=1
                g['wins']+=int(trade['net']>0)
                g['net']+=trade['net']
        if not opened:
            reasons[s['reason'] or 'Неизвестно']+=1
        elif not trade:
            groups[s['pattern']]['open']+=1
    for g in (*groups.values(),*by_tf.values(),*by_pair.values()):
        g['win_rate']=g['wins']/g['closed'] if g['closed'] else 0.
    open_ids={p['id'] for p in engine.positions}
    assert sum(g['signals'] for g in groups.values())==len(signals)
    assert sum(g['opened'] for g in groups.values())==len(trades)+len(open_ids)
    assert sum(g['rejected'] for g in groups.values())+sum(g['opened'] for g in groups.values())==len(signals)
    assert sum(g['closed'] for g in groups.values())==len(trades)
    for dimension in (by_tf,by_pair):
        for field in ('signals','opened','rejected','closed','wins'):
            assert sum(g[field] for g in dimension.values())==sum(g[field] for g in groups.values())
        assert abs(sum(g['net'] for g in dimension.values())-sum(t['net'] for t in trades))<1e-7
    total={'signals':len(signals),'opened':len(trades)+len(open_ids),
           'rejected':len(signals)-len(trades)-len(open_ids),
           'closed':len(trades),'wins':sum(t['net']>0 for t in trades),
           'win_rate':sum(t['net']>0 for t in trades)/len(trades) if trades else 0,
           'realized_net':sum(t['net'] for t in trades),
           'net_including_open':engine.equity()-engine.settings.initial_balance,
           'balance':engine.cash,'equity':engine.equity(),
           'funding':sum(t['funding'] for t in trades)+sum(p['funding'] for p in engine.positions),
           'max_drawdown':engine.max_drawdown,'open_positions':engine.positions}
    result={'manifest':manifest,'coverage':coverage,'total':total,'patterns':list(groups.values()),
            'timeframes':by_tf,'pairs':by_pair,'rejection_reasons':dict(sorted(reasons.items(),key=lambda x:-x[1])),
            'execution_model':'Следующая минутная свеча после закрытия фигуры; стоп первым при двух касаниях; taker 0,055% на сторону, проскальзывание 0,05% на сторону, фактический funding по mark price'}
    write_json(directory/'summary.json',result)
    write_json(directory/'trades.json',trades)
    csv_file(directory/'patterns.csv',groups.values(),['pattern','name','signals','opened','rejected','closed','wins','win_rate','net','open'])
    csv_file(directory/'timeframes.csv',({'timeframe':k,**v} for k,v in by_tf.items()),
             ['timeframe','signals','opened','rejected','closed','wins','win_rate','net'])
    csv_file(directory/'pairs.csv',({'symbol':k,**v,'covered_days':coverage[k]['days']} for k,v in by_pair.items()),
             ['symbol','signals','opened','rejected','closed','wins','win_rate','net','covered_days'])
    csv_file(directory/'signals.csv',signals,['id','symbol','timeframe','pattern','side','time','start','status','reason',
             'entry','stop','target','qty','rr','expected_net','expected_fees','funding_reserve'])
    csv_file(directory/'trades.csv',trades,['id','symbol','timeframe','pattern','side','opened','closed','entry','exit',
             'stop','target','qty','gross','entry_fee','exit_fee','funding','net','reason','recovered'])
    return result


async def run(directory=DEFAULT_DIR,days=60,symbols_count=20):
    directory=Path(directory)
    directory.mkdir(parents=True,exist_ok=True)
    load_dotenv(Path(__file__).resolve().parents[1]/'.env')
    bybit=Bybit()
    cache=Cache(directory/'market.sqlite3')
    store=None
    try:
        manifest=await snapshot(bybit,directory,days,symbols_count)
        if manifest['days']!=days or len(manifest['symbols'])!=symbols_count:
            raise ValueError('Существующий кэш относится к другому периоду или числу пар')
        coverage=await download(bybit,cache,manifest,directory)
        warm=await warmup(bybit,manifest,directory)
        store,engine=await replay(bybit,cache,manifest,warm,directory)
        result=report(store,engine,manifest,coverage,directory)
        progress(directory,stage='complete',message='Отчёты JSON и CSV готовы',
                 replay_time=manifest['end'])
        return result
    except Exception as exc:
        progress(directory,stage='error',message=str(exc))
        raise
    finally:
        if store:
            store.close()
        cache.db.close()
        await bybit.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--directory',type=Path,default=DEFAULT_DIR)
    parser.add_argument('--days',type=int,default=60)
    parser.add_argument('--symbols',type=int,default=20)
    args=parser.parse_args()
    if args.days<1 or not 1<=args.symbols<=20:
        parser.error('days >= 1; symbols 1..20')
    result=asyncio.run(run(args.directory,args.days,args.symbols))
    print(json.dumps(result['total'],ensure_ascii=False,indent=2))
