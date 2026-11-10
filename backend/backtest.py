"""Historical replay uses the same detectors, fee calculation and position engine."""
import argparse
import asyncio
import json
from dataclasses import asdict
from .models import Settings, Candle, now_ms
from .storage import Store
from .engine import Engine
from .patterns import detect
from .bybit import Bybit


def replay(symbol,tf,candles,minutes,instrument,fee_rate,funding=(),settings=None):
    store=Store(':memory:')
    engine=Engine(store,settings)
    engine.instruments[symbol]=instrument
    engine.connected=True
    close_events={c.time+int(tf)*60000:c for c in candles}
    fund_events={time:(rate,mark) for time,rate,mark in funding}
    history=[c for c in candles if minutes and c.time+int(tf)*60000<minutes[0].time]
    pending=[]
    curve=[]
    for minute in minutes:
        timestamp=minute.time
        engine.set_fee(symbol,fee_rate,timestamp)
        # The decision at this open sees the higher-timeframe bar that just closed.
        if timestamp in close_events:
            history.append(close_events[timestamp])
            pending=detect(symbol,tf,history)
        for time in sorted(t for t in fund_events if timestamp<=t<timestamp+60000):
            rate,mark=fund_events[time]
            engine.funding_payment(symbol,rate,time,mark)
        engine.tick(symbol,minute.open,timestamp)
        if pending:
            known=[(t,r) for t,r,_ in funding if t<=timestamp]
            funding_rate=known[-1][1] if known else 0
            engine.process(pending,{symbol:{'price':minute.open,'time':timestamp}},timestamp,{symbol:funding_rate})
            pending=[]
        engine.bar(symbol,minute,timestamp+59999,True)
        curve.append({'time':timestamp,'equity':engine.equity()})
    trades=engine.trades
    report={'symbol':symbol,'timeframe':tf,'fee_rate':fee_rate,'trades_count':len(trades),
        'win_rate':sum(t['net']>0 for t in trades)/len(trades) if trades else 0,
        'net':engine.equity()-engine.settings.initial_balance,'realized_net':sum(t['net'] for t in trades),
        'max_drawdown':engine.max_drawdown,'open_positions':engine.positions,'trades':trades,
        'signals':store.signals(10000),'curve':curve,
        'execution_model':'Следующая минутная свеча; стоп первым при двух касаниях; фиксированная комиссия; funding по mark price'}
    store.close()
    return report


async def download_replay(bybit,symbol,tf,days,fee_rate,settings):
    instruments,_,_=await bybit.universe(20)
    if symbol not in instruments:
        raise ValueError('Неизвестный USDT-перпетуал')
    end=now_ms()//60000*60000-1
    start=(end-days*86400000)//(int(tf)*60000)*(int(tf)*60000)
    warmup=start-250*int(tf)*60000
    candles=await bybit.history(symbol,tf,warmup,end)
    minutes=await bybit.history(symbol,'1',start,end)
    # Warmup detects shapes but never opens historical positions before start.
    funding_rows=await bybit.funding_history(symbol,start,end)
    funding=[]
    for time,rate in funding_rows:
        marks=await bybit.candles(symbol,'1',time,time,True)
        if not marks:
            raise RuntimeError('Нет mark price для исторического funding')
        funding.append((time,rate,marks[0].open))
    return await asyncio.to_thread(replay,symbol,tf,candles,
        [c for c in minutes if c.time>=start],instruments[symbol],fee_rate,funding,settings)


async def cli(args):
    from dotenv import load_dotenv
    load_dotenv()
    bybit=Bybit()
    try:
        report=await download_replay(bybit,args.symbol,args.timeframe,args.days,args.fee,Settings())
        from pathlib import Path
        Path(args.output).parent.mkdir(parents=True,exist_ok=True)
        Path(args.output).write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
        print(json.dumps({k:v for k,v in report.items() if k not in ('trades','signals','curve','open_positions')},ensure_ascii=False,indent=2))
    finally:
        await bybit.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--symbol',default='BTCUSDT')
    parser.add_argument('--timeframe',choices=['5','15','60','240'],default='5')
    parser.add_argument('--days',type=int,default=7)
    parser.add_argument('--fee',type=float,required=True,help='Taker fee fraction, e.g. 0.0006')
    parser.add_argument('--output',default='data/backtest.json')
    args=parser.parse_args()
    if not 1<=args.days<=30 or not 0<=args.fee<.1:
        parser.error('days must be 1..30 and fee 0..0.1')
    asyncio.run(cli(args))
