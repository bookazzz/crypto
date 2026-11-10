import asyncio
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

from backend.detection_queue import DetectionJob
from backend.engine import Engine
from backend.models import Candle, Signal
from backend.service import Scanner, _snapshot_candles
from backend.storage import Store
from tests.test_engine import T, make_engine, open_position


class FakeBybit:
    fee_mode='standard'
    offset=0

    async def close(self):
        pass


def test_failed_history_stream_does_not_block_other_stream(monkeypatch):
    scanner=Scanner(make_engine(),FakeBybit())
    scanner.split_mode=True
    completed=asyncio.Event()
    retry_gate=asyncio.Event()
    real_sleep=asyncio.sleep
    async def controlled_sleep(delay):
        if delay>=.1:
            await retry_gate.wait()
        else:
            await real_sleep(delay)
    monkeypatch.setattr('backend.service.asyncio.sleep',controlled_sleep)
    attempts=[]
    async def detect(job,*,archival=False):
        attempts.append(job.symbol)
        if job.symbol=='FAILUSDT':
            raise RuntimeError('persistent historical failure')
        completed.set()
    scanner.finish_candle_detection=detect
    failed=DetectionJob(1,1,'FAILUSDT','5',T,(),True,0.)
    healthy=DetectionJob(1,2,'GOODUSDT','5',T,(),True,0.)
    async def scenario():
        scanner.history_detection_inflight.update((failed.stream,healthy.stream))
        assert await scanner.split_detection_queue.put_history(failed)
        assert await scanner.split_detection_queue.put_history(healthy)
        worker=asyncio.create_task(scanner.split_detection_worker('history'))
        scanner.detection_workers=[worker]
        try:
            await asyncio.wait_for(completed.wait(),.75)
            async def wait_for_cursor():
                while scanner.engine.store.scan_cursor('GOODUSDT','5')!=T:
                    await asyncio.sleep(.01)
            await asyncio.wait_for(wait_for_cursor(),.75)
            assert scanner.engine.store.scan_cursor('FAILUSDT','5') is None
            assert attempts[:2]==['FAILUSDT','GOODUSDT']
        finally:
            retry_gate.set()
            await scanner.stop()
    try:asyncio.run(scenario())
    finally:scanner.engine.store.close()


def test_stop_crossed_during_position_recovery_is_not_lost(monkeypatch):
    class WaitingBybit(FakeBybit):
        def __init__(self):
            self.history_started=asyncio.Event()
            self.release_history=asyncio.Event()
        async def history(self,symbol,tf,start,end):
            self.history_started.set()
            await self.release_history.wait()
            # The REST snapshot ends before the live stop-touch tick.
            return [Candle(T,100,101,99,100,1)]
        async def funding_history(self,symbol,start,end):
            return []

    engine=make_engine()
    open_position(engine)
    stop=engine.positions[0]['stop']
    bybit=WaitingBybit()
    scanner=Scanner(engine,bybit)
    scanner.split_mode=True
    scanner.symbols=['BTCUSDT']
    scanner.positions_ready=False
    clock=[T+30_000]
    monkeypatch.setattr('backend.service.now_ms',lambda:clock[0])
    async def tick(price,timestamp):
        clock[0]=timestamp
        await scanner.message({'topic':'tickers.BTCUSDT','ts':timestamp,
                               'data':{'lastPrice':str(price)}})
    async def scenario():
        task=asyncio.create_task(scanner.recover_positions_with_live_ticks())
        try:
            await asyncio.wait_for(bybit.history_started.wait(),1)
            await tick(stop-1,T+31_000)
            await tick(100,T+32_000)
            assert len(engine.positions)==1
            bybit.release_history.set()
            await asyncio.wait_for(task,2)
            assert scanner.positions_ready
            assert not engine.positions
            assert len(engine.trades)==1 and engine.trades[0]['reason']=='stop'
        finally:
            bybit.release_history.set()
            if not task.done():task.cancel()
            await asyncio.gather(task,return_exceptions=True)
            await scanner.stop()
    try:asyncio.run(scenario())
    finally:engine.store.close()


def test_historical_setup_continues_after_restart_without_touching_live_cache(tmp_path,monkeypatch):
    path=tmp_path/'history.sqlite3'
    step=300_000
    first=Candle(T,99.9,100.2,99.4,99.9,1)
    breakout=Candle(T+step,100,100.5,99.8,100.4,1)
    live={'id':'live-control','symbol':'BTCUSDT','timeframe':'5',
        'pattern':'ascending_triangle','side':'long','status':'wait_breakout',
        'detected_at':T,'event_at':T,'updated_at':T,
        'last_closed_candle':T,'delivery':{'mode':'current','recovered':False},
        'snapshot':{'start':T-900_000,'detected_on_bar':T-600_000,
            'boundary':100.,'stop':98.,'target':110.,'atr':1.,'lines':[],'pivots':[]}}
    monkeypatch.setattr('backend.service.now_ms',lambda:T+900_000)
    def detect(symbol,tf,candles,*args):
        if candles[-1].time==T:
            setup=Signal(symbol,tf,'ascending_triangle','long',T+step,T-900_000,
                100.,98.,110.,1.,lines=[],pivots=[])
            return [],[setup]
        return [],[]
    monkeypatch.setattr('backend.service._detect_combined_worker',detect)

    def scanner_for(store):
        scanner=Scanner(Engine(store),FakeBybit())
        scanner.split_mode=True
        scanner.symbols=['BTCUSDT']
        scanner.detection_executor.shutdown(wait=True,cancel_futures=True)
        scanner.detection_executor=ThreadPoolExecutor(max_workers=1)
        current=store.setup('live-control')
        scanner.active_setup_cache=[current]
        scanner.active_setup_cache_at=T+900_000
        scanner.active_setup_streams={('BTCUSDT','5'):[current]}
        return scanner

    async def scan(scanner,candle,candles):
        job=DetectionJob(1,candle.time,'BTCUSDT','5',candle.time,
                         _snapshot_candles(candles),True,0.,source='storage')
        scanner.history_detection_inflight.add(job.stream)
        assert await scanner.split_detection_queue.put_history(job)
        worker=asyncio.create_task(scanner.split_detection_worker('history'))
        scanner.detection_workers=[worker]
        try:
            await asyncio.wait_for(scanner.split_detection_queue.join_history(),2)
        finally:
            worker.cancel()
            await asyncio.gather(worker,return_exceptions=True)
            scanner.detection_workers=[]

    async def before_restart():
        store=Store(str(path))
        store.save_setup(live)
        store.set_cursor('BTCUSDT','5',breakout.time)
        scanner=scanner_for(store)
        try:
            before=deepcopy(scanner.active_setup_cache)
            await scan(scanner,first,[first])
            states=await store.ahistory_setups('BTCUSDT','5')
            assert len(states)==1 and states[0]['status']=='wait_breakout'
            assert store.setup(states[0]['id']) is None
            assert store.scan_cursor('BTCUSDT','5')==first.time
            assert scanner.active_setup_cache==before
            return states[0]['id']
        finally:
            await scanner.stop()
            store.close()

    async def after_restart(historical_id):
        store=Store(str(path))
        scanner=scanner_for(store)
        try:
            before=deepcopy(scanner.active_setup_cache)
            assert store.scan_cursor('BTCUSDT','5')==first.time
            await scan(scanner,breakout,[first,breakout])
            states=await store.ahistory_setups('BTCUSDT','5')
            assert len(states)==1 and states[0]['status']=='breakout_confirmed'
            assert states[0]['id']==historical_id
            assert states[0]['confirmed_at']==breakout.time+step
            assert any(row.get('from')=='wait_breakout' and row.get('to')=='breakout_confirmed'
                       for row in states[0]['transitions'])
            published=store.setup(historical_id)
            assert published and published['status']=='breakout_confirmed'
            assert published['confirmed_at']==breakout.time+step
            assert any(row.get('from')=='wait_breakout' and row.get('to')=='breakout_confirmed'
                       for row in published['transitions'])
            assert store.scan_cursor('BTCUSDT','5')==breakout.time
            assert scanner.active_setup_cache==before
            assert store.setup('live-control')['status']=='wait_breakout'
        finally:
            await scanner.stop()
            store.close()

    historical_id=asyncio.run(before_restart())
    asyncio.run(after_restart(historical_id))
