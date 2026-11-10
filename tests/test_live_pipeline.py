import asyncio
import hashlib
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

from backend.engine import Engine
from backend.models import Candle, Signal, Instrument
from backend.service import Scanner
from backend.storage import Store


NOW = 1_800_000_000_000 // 14_400_000 * 14_400_000


class FakeBybit:
    fee_mode = 'standard'
    offset = 0


def _scanner():
    scanner = Scanner(Engine(Store(':memory:')), FakeBybit())
    scanner.split_mode = True
    scanner.symbols = ['BTCUSDT']
    scanner.engine.strategy_approved = False
    scanner.detection_executor.shutdown(wait=True, cancel_futures=True)
    scanner.detection_executor = ThreadPoolExecutor(max_workers=1)
    return scanner


def _window(interval=300_000):
    return [Candle(NOW-(252-index)*interval,100,101,99,100,1)
            for index in range(251)]


def test_live_receipt_survives_queue_and_is_published_after_detection(monkeypatch):
    scanner = _scanner()
    scanner.live_buffers['BTCUSDT','5'] = _window()
    monkeypatch.setattr('backend.service.now_ms',lambda:NOW+100)
    def detect(symbol,tf,candles,*args):
        return [Signal(symbol,tf,'rectangle','long',candles[-1].time+300_000,
                       candles[0].time,100,98,110,1,id='live-signal')],[]
    monkeypatch.setattr('backend.service._detect_combined_worker',detect)
    async def scenario():
        worker=asyncio.create_task(scanner.split_detection_worker('current'))
        try:
            candle=Candle(NOW-300_000,100,101,99,100,1)
            await scanner.observe_live_candle('BTCUSDT','5',candle,NOW+10,
                asyncio.get_running_loop().time(),'websocket')
            await asyncio.wait_for(scanner.split_detection_queue.join_current(),2)
            row=scanner.engine.store.signal('live-signal')
            assert not row['published_at']
            scanner.stamp_publications({'signals':[row]})
            await scanner.flush_publications()
            row=scanner.engine.store.signal('live-signal')
            delivery=row['filter_results']['delivery']
            assert row['received_at']==NOW+10
            assert row['received_at']<=row['detected_at']<=row['published_at']
            assert delivery['timing_version']==2 and delivery['source']=='websocket'
            assert delivery['mode']=='current' and not delivery['recovered']
            assert delivery['published_at']==row['published_at']
            assert len(scanner.live_candle_transition_latency_ms)==1
            assert len(scanner.live_publish_latency_ms)==1
            assert scanner.live_publish_latency_ms[0]>=scanner.live_candle_transition_latency_ms[0]
            assert scanner.engine.store.live_scan_cursor('BTCUSDT','5')==candle.time
            assert scanner.engine.store.scan_cursor('BTCUSDT','5') is None
        finally:
            worker.cancel()
            await asyncio.gather(worker,return_exceptions=True)
    try:asyncio.run(scenario())
    finally:
        scanner.detection_executor.shutdown(wait=True,cancel_futures=True)
        scanner.engine.store.close()


def test_archived_signal_does_not_hide_current_observation(monkeypatch):
    scanner = _scanner()
    scanner.live_buffers['BTCUSDT','5'] = _window()
    monkeypatch.setattr('backend.service.now_ms',lambda:NOW+100)
    old=Signal('BTCUSDT','5','rectangle','long',NOW-600_000,NOW-900_000,
               100,98,110,1,id='same-shape')
    old.filter_results={'delivery':{'mode':'history','recovered':True}}
    def detect(symbol,tf,candles,*args):
        return [Signal(symbol,tf,'rectangle','long',candles[-1].time+300_000,
                       NOW-900_000,100,98,110,1,id='same-shape')],[]
    monkeypatch.setattr('backend.service._detect_combined_worker',detect)
    async def scenario():
        await scanner.engine.store.asave_historical_signals([old.dict()])
        worker=asyncio.create_task(scanner.split_detection_worker('current'))
        try:
            candle=Candle(NOW-300_000,100,101,99,100,1)
            await scanner.observe_live_candle('BTCUSDT','5',candle,NOW+10,
                asyncio.get_running_loop().time(),'rest')
            await asyncio.wait_for(scanner.split_detection_queue.join_current(),2)
            live_id=hashlib.sha256(b'same-shape:current').hexdigest()[:24]
            assert scanner.engine.store.signal('same-shape')['filter_results']['delivery']['mode']=='history'
            assert scanner.engine.store.signal(live_id)['filter_results']['delivery']['mode']=='current'
            assert [row['id'] for row in scanner.signal_feed(signal_scope='current')]==[live_id]
        finally:
            worker.cancel()
            await asyncio.gather(worker,return_exceptions=True)
    try:asyncio.run(scenario())
    finally:
        scanner.detection_executor.shutdown(wait=True,cancel_futures=True)
        scanner.engine.store.close()


def test_same_historical_event_is_promoted_without_duplicate(monkeypatch):
    scanner=_scanner();scanner.live_buffers['BTCUSDT','5']=_window()
    monkeypatch.setattr('backend.service.now_ms',lambda:NOW+100)
    archived=Signal('BTCUSDT','5','rectangle','long',NOW,NOW-900_000,
                    100,98,110,1,id='same-event')
    archived.published_at=NOW-500
    archived.filter_results={'delivery':{'mode':'history','recovered':True,
                                         'timing_version':2,'published_at':NOW-500}}
    def detect(symbol,tf,candles,*args):
        return [Signal(symbol,tf,'rectangle','long',NOW,NOW-900_000,
                       100,98,110,1,id='same-event')],[]
    monkeypatch.setattr('backend.service._detect_combined_worker',detect)
    async def scenario():
        await scanner.engine.store.asave_historical_signals([archived.dict()])
        worker=asyncio.create_task(scanner.split_detection_worker('current'))
        try:
            await scanner.observe_live_candle('BTCUSDT','5',Candle(NOW-300_000,100,101,99,100,1),
                                              NOW+10,asyncio.get_running_loop().time(),'websocket')
            await asyncio.wait_for(scanner.split_detection_queue.join_current(),2)
            rows=scanner.engine.store.signals(10)
            assert len(rows)==1 and rows[0]['id']=='same-event'
            assert rows[0]['filter_results']['delivery']['mode']=='current'
            assert not rows[0]['published_at']
        finally:
            worker.cancel();await asyncio.gather(worker,return_exceptions=True)
    try:asyncio.run(scenario())
    finally:
        scanner.detection_executor.shutdown(wait=True,cancel_futures=True)
        scanner.engine.store.close()


def test_historical_candle_cannot_cancel_newer_live_setup():
    scanner=_scanner()
    setup={'id':'new','symbol':'BTCUSDT','timeframe':'5','status':'wait_breakout',
           'side':'long','pattern':'rectangle','detected_at':NOW,'event_at':NOW,'updated_at':NOW,
           'delivery':{'mode':'current'},'last_closed_candle':NOW-300_000,
           'snapshot':{'detected_on_bar':NOW-600_000,'start':NOW-900_000,
                       'boundary':100,'stop':98,'target':110,'atr':1,'lines':[]}}
    scanner.active_setup_cache=[setup]
    scanner.active_setup_streams={('BTCUSDT','5'):[setup]}
    try:
        old=Candle(NOW-900_000,100,101,90,100)
        assert scanner.advance_setups('BTCUSDT','5',old,[old],persist=False)==[]
        assert setup['status']=='wait_breakout'
    finally:
        scanner.detection_executor.shutdown(wait=True,cancel_futures=True)
        scanner.engine.store.close()


def test_repeated_live_frames_are_coalesced_before_storage(monkeypatch):
    scanner=_scanner()
    entered=asyncio.Event();release=asyncio.Event();seen=[]
    async def slow(symbol,tf,candle,*args):
        seen.append(candle.time)
        if len(seen)==1:
            entered.set()
            await release.wait()
    scanner.observe_live_candle=slow
    async def scenario():
        scanner.schedule_live_observation('BTCUSDT','5',Candle(1,100,101,99,100),1,1.,'websocket')
        await entered.wait()
        for timestamp in range(2,1002):
            scanner.schedule_live_observation('BTCUSDT','5',Candle(timestamp,100,101,99,100),
                                              timestamp,float(timestamp),'websocket')
        assert len(scanner.live_observation_tasks)==1
        assert len(scanner.live_observation_pending)==1
        assert scanner.history_dispatch_event.is_set()
        release.set()
        await asyncio.gather(*list(scanner.live_observation_tasks))
        assert seen==[1,1001]
    try:asyncio.run(scenario())
    finally:
        scanner.detection_executor.shutdown(wait=True,cancel_futures=True)
        scanner.engine.store.close()


def test_watchdog_rotates_failed_historical_streams(monkeypatch):
    from backend.bybit import BybitNetworkError
    scanner=_scanner()
    scanner.symbols=['AAAUSDT','BBBUSDT','CCCUSDT']
    scanner.engine.settings.timeframes=['5']
    scanner.last_refresh=NOW
    scanner.history_bootstrap_complete=True
    clock=[NOW+3_000]
    monkeypatch.setattr('backend.service.now_ms',lambda:clock[0])
    monkeypatch.setattr(scanner,'status',lambda:{'scanner_diagnostics':{'stale_streams':[]}})
    attempts=[]
    async def recover(rows):
        attempts.extend(row['symbol'] for row in rows)
        if 'CCCUSDT' in attempts:raise asyncio.CancelledError
        return 0,[(row['symbol'],row['timeframe'],BybitNetworkError('offline')) for row in rows]
    scanner.recover_closed_candles=recover
    async def tick(_):clock[0]+=1_000
    monkeypatch.setattr('backend.service.asyncio.sleep',tick)
    try:
        try:asyncio.run(scanner.run_candle_watchdog())
        except asyncio.CancelledError:pass
        assert attempts[:2]==['AAAUSDT','BBBUSDT']
        assert 'CCCUSDT' in attempts
    finally:
        scanner.detection_executor.shutdown(wait=True,cancel_futures=True)
        scanner.engine.store.close()


def test_short_complete_history_is_ready_but_internal_gap_is_not(monkeypatch):
    class ShortHistory(FakeBybit):
        def __init__(self,gap):self.gap=gap
        async def candles_with_timing(self,symbol,tf,start=None,end=None,mark=False,*,limit=1000):
            if end is not None:return [],NOW+10,1.
            rows=[Candle(NOW-(30-index)*300_000,100,101,99,100,1) for index in range(30)]
            if self.gap:rows.pop(10)
            return rows,NOW+10,1.
    monkeypatch.setattr('backend.service.now_ms',lambda:NOW+100)
    async def scenario(gap):
        scanner=_scanner();scanner.bybit=ShortHistory(gap)
        try:
            await scanner.hydrate_live_stream('BTCUSDT','5')
            assert scanner.live_window_ready(('BTCUSDT','5')) is not gap
            assert scanner.split_detection_queue.current_qsize()==(0 if gap else 1)
        finally:
            scanner.detection_executor.shutdown(wait=True,cancel_futures=True)
            scanner.engine.store.close()
    asyncio.run(scenario(False))
    asyncio.run(scenario(True))


def test_current_commit_is_atomic_and_does_not_move_history_cursor():
    store=Store(':memory:')
    store.set_cursor('BTCUSDT','5',NOW-600_000)
    candle=Candle(NOW-300_000,100,101,99,100,1)
    async def scenario():
        try:
            await store.acommit_current_candle('BTCUSDT','5',candle,[{'id':'invalid'}])
        except (KeyError,ValueError):pass
        else:raise AssertionError('Invalid setup must roll back the candle')
        assert store.candles_range('BTCUSDT','5',start=candle.time,end=candle.time+1)==[]
        await store.acommit_current_candle('BTCUSDT','5',candle,[])
        assert store.cursor('BTCUSDT','5')==NOW-600_000
        assert store.candles_range('BTCUSDT','5',start=candle.time,end=candle.time+1)[0].time==candle.time
    try:asyncio.run(scenario())
    finally:store.close()


def test_market_websocket_connects_while_current_history_is_loading():
    class WaitingBybit(FakeBybit):
        ws_url='wss://stream.bybit.com/v5/public/linear'
        def __init__(self):
            self.release=asyncio.Event();self.connected=asyncio.Event();self.loading=asyncio.Event()
        async def universe(self,n):
            return ({'BTCUSDT':Instrument('BTCUSDT',.001,.001,5,.01)},['BTCUSDT'],
                    {'BTCUSDT':{'lastPrice':'100'}})
        async def candles(self,*args,**kwargs):
            self.loading.set();await self.release.wait();return []
        async def spot_matches(self,symbols):return {}
        async def close(self):pass
        @asynccontextmanager
        async def connect_ws(self,*args,**kwargs):
            self.connected.set()
            class Socket:
                async def recv(self):await asyncio.Future()
            yield Socket()
    scanner=_scanner();scanner.bybit=WaitingBybit()
    scanner.subscription_topics=lambda:[]
    async def scenario():
        task=asyncio.create_task(scanner.run())
        try:
            await asyncio.wait_for(scanner.bybit.loading.wait(),2)
            await asyncio.wait_for(scanner.bybit.connected.wait(),2)
            assert not scanner.bybit.release.is_set()
        finally:
            task.cancel();await asyncio.gather(task,return_exceptions=True)
            scanner.bybit.release.set()
            await scanner.stop()
    try:asyncio.run(scenario())
    finally:scanner.engine.store.close()
