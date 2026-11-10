import asyncio
from dataclasses import asdict
import json
import pytest
from backend.engine import Engine
from backend.storage import Store
from backend.service import Scanner, _snapshot_candles
from backend.models import Candle, Instrument, Signal, now_ms
from tests.test_engine import make_engine, open_position, T
from tests.test_patterns import channel


class FakeBybit:
    def __init__(self):
        self.bars=[]
        self.funding=[]
    async def history(self,symbol,tf,start,end):
        return self.bars
    async def funding_history(self,symbol,start,end):
        return [(t,r) for t,r in self.funding if start<=t<=end]
    async def candles(self,symbol,tf,start=None,end=None,mark=False):
        return [Candle(start,100,101,99,100)]


def test_detection_snapshots_keep_quote_turnover_for_volume_filter():
    candles=[Candle(i,100,101,99,100,10,1000) for i in range(20)]
    candles.append(Candle(20,100,101,99,100,15,1500))
    snapshots=_snapshot_candles(candles)

    from backend.strategy import volume_filter
    passed,details=volume_filter(snapshots)

    assert passed is True
    assert details['breakout_turnover_usdt']==1500
    assert snapshots[-1].turnover==1500


def run(coro):
    return asyncio.run(coro)


def test_account_fee_mode_discards_persisted_standard_rates():
    e=make_engine()
    e.fees['BTCUSDT']['source']='bybit_vip0_standard'
    bybit=FakeBybit()
    bybit.fee_mode='account'
    Scanner(e,bybit)
    assert e.fee_mode=='account'
    assert e.fees=={}


def test_scalp_market_context_reports_conflicts_without_blocking_available_plan(monkeypatch):
    from unittest.mock import patch

    now=1_800_000_000_000
    bybit=FakeBybit();bybit.fee_mode='standard';bybit.offset=0
    engine=make_engine();scanner=Scanner(engine,bybit)
    setup={'id':'short-setup','symbol':'BTCUSDT','timeframe':'15','pattern':'rectangle',
        'side':'short','status':'crossing','detected_at':now-60_000,'event_at':now-60_000,
        'snapshot':{'start':now-900_000,'detected_on_bar':now-300_000,'boundary':100,
            'stop':101,'target':98,'height':2,'atr':1,'lines':[],'pivots':[]}}
    scanner.buffers[('BTCUSDT','15')]=[Candle(now-900_000,100,101,99,100)]
    scanner.buffers[('BTCUSDT','5')]=[
        Candle(now-900_000,100,101,99,100),
        Candle(now-600_000,100,102,100,101),
        Candle(now-300_000,101,103,101,102)]
    scanner.quotes['BTCUSDT']={'price':99,'time':now}
    scanner.books['BTCUSDT']={'time':now,'bids':{98.99:10},'asks':{99.01:10}}
    engine.fees['BTCUSDT']={'rate':.0001,'time':now,'source':'standard'}
    scanner.tickers['BTCUSDT']={'fundingRate':'0','nextFundingTime':now+3_600_000,
        '_funding_time':now}
    monkeypatch.setattr('backend.service.now_ms',lambda:now)
    monkeypatch.setattr('backend.service.trend_filter',lambda rows,side:(side=='long',{}))
    with patch('backend.service.assess_scalp_plan',return_value={
        'status':'available','side':'short','as_of':now,'preliminary':False,
        'reason':'ready','confirmation_reason':'confirmed'}):
        plan=scanner.scalp_plan(setup,now)
    assert plan['status']=='available'
    assert plan['market_context']['trend_15m']=='up'
    assert plan['market_context']['momentum_5m']=='up'
    assert len(plan['warnings']) >= 2

    setup['side']='long'
    aligned=scanner._scalp_market_context(setup,now)
    assert aligned['trend_15m']=='up'
    assert not any('Тренд 15м вверх' in warning for warning in aligned['warnings'])


def test_signal_context_keeps_market_invalidation_separate_from_entry_assessment(monkeypatch):
    engine=make_engine();scanner=Scanner(engine,FakeBybit())
    timestamp=now_ms()
    monkeypatch.setattr('backend.service.now_ms',lambda:timestamp)
    signal=Signal('NEARUSDT','15','symmetric_triangle','short',timestamp,
        timestamp-900_000,100,102,90,1,id='near-short',filter_results={
            'trend_pass':False,'volume_pass':True,'confirmation_pass':True,
            'market_context':{'scenario':'unconfirmed','eligible':False,
                'hourly_bias':'down','structure_15m':{'ready':True,'direction':'mixed'}}})
    engine.persist([signal])
    scanner.tickers['NEARUSDT']={'lastPrice':'100.2','_quote_time':timestamp,
        '_ticker_received_at':timestamp}
    scanner.quotes['NEARUSDT']={'price':100.2,'time':timestamp}
    scanner.buffers['NEARUSDT','15']=[Candle(timestamp-900_000,100,101,99,100)]
    scanner.assess_signal_entry=lambda *_args,**_kwargs:{'status':'unavailable',
        'reason':'Текущая цена исполнения недоступна','price_min':None,'price_max':None,'net_rr':None}

    result=scanner.decision_context(signal.id)['current']
    assert result['current_entry_state']=='scenario_invalidated'
    assert result['entry_assessment']['status']=='unavailable'
    assert all(plan['status']=='scenario_invalidated' for plan in result['entry_plans'])


def test_hourly_refresh_reuses_persisted_candles_and_only_fetches_missed_bars(monkeypatch):
    class RefreshBybit(FakeBybit):
        fee_mode='standard'
        def __init__(self):
            super().__init__();self.candle_calls=[]
        async def universe(self,n):
            return ({'BTCUSDT':Instrument('BTCUSDT',.001,.001,5,.1)},
                    ['BTCUSDT'],{'BTCUSDT':{'lastPrice':'100'}})
        async def spot_matches(self,symbols):return {'BTCUSDT':'BTCUSDT'}
        async def candles(self,symbol,tf,start=None,end=None,mark=False):
            self.candle_calls.append((tf,start))
            return [Candle(start,100,101,99,100)]
    e=make_engine();bybit=RefreshBybit();scanner=Scanner(e,bybit)
    monkeypatch.setattr('backend.service.now_ms',lambda:T+15*60_000)
    for tf in [*e.settings.timeframes,'D']:
        e.store.put_candles('BTCUSDT',tf,[Candle(T,100,101,99,100)])
    run(scanner.refresh())
    assert bybit.candle_calls==[('5',T+5*60_000)]
    assert all(('BTCUSDT',tf) in scanner.buffers for tf in [*e.settings.timeframes,'D'])
    assert scanner.phase=='loading' and scanner.last_refresh>0


def test_signal_context_scope_keeps_old_pairs_and_selected_signal():
    e=make_engine();scanner=Scanner(e,FakeBybit())
    old=Signal('OLDUSDT','15','ascending_triangle','long',0,0,100,98,110,1,id='old-signal')
    recent=[Signal(f'PAIR{i}USDT','5','ascending_triangle','long',i+1,0,100,98,110,1,
                   id=f'signal-{i}') for i in range(200)]
    e.persist([old,*recent])
    scanner.symbols=['BTCUSDT']
    assert 'OLDUSDT' not in scanner.signal_context_scope()
    client=object()
    scanner.watch_context(client,old.id)
    scope=scanner.signal_context_scope()
    assert scope['OLDUSDT']=={'15','60','5','D'}
    scanner.context_scope=scope
    topics=scanner.subscription_topics()
    assert {'tickers.OLDUSDT','kline.15.OLDUSDT','kline.60.OLDUSDT',
            'kline.5.OLDUSDT','kline.D.OLDUSDT'}<=topics
    assert 'orderbook.200.OLDUSDT' not in topics
    assert 'publicTrade.OLDUSDT' in scanner.ancillary_topics()
    assert 'orderbook.200.OLDUSDT' in scanner.ancillary_topics()
    assert 'publicTrade.PAIR0USDT' not in topics
    assert 'OLDUSDT' not in scanner.symbols


def test_selected_early_setup_hydrates_five_minute_candles_and_book():
    e=make_engine();scanner=Scanner(e,FakeBybit())
    setup={'id':'old-setup','symbol':'OTHERUSDT','timeframe':'15','side':'long',
        'status':'wait_breakout','detected_at':T,'updated_at':T,'event_at':T,
        'reason':'','snapshot':{'boundary':100,'stop':98,'target':105,'atr':1,
            'lines':[],'pivots':[]}}
    e.store.save_setup(setup)
    client=object()
    scanner.watch_setup(client,'old-setup')
    assert scanner.signal_context_scope()['OTHERUSDT']=={'15','60','5','D'}
    assert {'orderbook.200.OTHERUSDT','publicTrade.OTHERUSDT'}<=scanner.ancillary_topics()


def test_rest_requested_setup_context_keeps_market_data_subscriptions_temporarily():
    e=make_engine();scanner=Scanner(e,FakeBybit())
    setup={'id':'rest-setup','symbol':'OTHERUSDT','timeframe':'15','side':'long',
        'status':'wait_breakout','detected_at':T,'updated_at':T,'event_at':T,
        'snapshot':{'boundary':100,'stop':98,'target':105,'atr':1,'lines':[],'pivots':[]}}
    e.store.save_setup(setup)

    assert scanner.request_setup_context('rest-setup')['id']=='rest-setup'
    assert scanner.signal_context_scope()['OTHERUSDT']=={'15','60','5','D'}
    assert {'orderbook.200.OTHERUSDT','publicTrade.OTHERUSDT'}<=scanner.ancillary_topics()
    assert scanner.request_setup_context('missing') is None


def test_read_side_setup_context_never_synchronously_persists_market_changes(monkeypatch):
    e=make_engine();scanner=Scanner(e,FakeBybit());now=now_ms()
    setup={'id':'read-only','symbol':'BTCUSDT','timeframe':'5','pattern':'ascending_triangle',
        'side':'long','status':'wait_breakout','detected_at':now-300_000,
        'updated_at':now-300_000,'event_at':now-300_000,'reason':'','snapshot':{
            'start':now-3_000_000,'detected_on_bar':now-300_000,'boundary':100.,
            'stop':98.,'target':110.,'atr':2.,'height':10.,'lines':[{
                'label':'support','points':[{'time':now-300_000,'value':100.},
                    {'time':now,'value':100.}]}],'pivots':[]}}
    e.store.save_setup(setup)
    scanner.quotes['BTCUSDT']={'price':100.2,'time':now}

    def forbidden(*args,**kwargs):
        raise AssertionError('read-only context must not synchronously write SQLite')
    monkeypatch.setattr(scanner,'save_setup',forbidden)

    scanner.snapshot_payload(limit=10,setup_limit=10)
    scanner.setup_context('read-only')
    assert e.store.setup('read-only')['status']=='wait_breakout'


def test_broadcast_loop_recovers_after_a_snapshot_timeout(monkeypatch):
    scanner=Scanner(Engine(Store(':memory:')),FakeBybit())
    class Client:
        async def send_json(self,payload):pass
    scanner.clients.add(Client())
    attempts=0
    def snapshot_payload(limit=50,setup_limit=50,view='full',signal_scope='all'):
        nonlocal attempts
        attempts+=1
        if attempts==1:raise TimeoutError('temporary storage delay')
        return {'published_at':now_ms(),'state':{},'signals':[],'setups':[],'trades':[]}
    monkeypatch.setattr(scanner,'snapshot_payload',snapshot_payload)

    async def scenario():
        task=asyncio.create_task(scanner.broadcast_loop())
        scanner.broadcast_event.set()
        for _ in range(30):
            if attempts>=2:break
            await asyncio.sleep(.05)
        await asyncio.sleep(.05)
        assert not task.done()
        assert attempts>=2
        assert scanner.broadcast_error==''
        task.cancel()
        with pytest.raises(asyncio.CancelledError):await task
    run(scenario())


def test_load_context_symbols_refreshes_quote_and_required_candles(monkeypatch):
    class ContextBybit(FakeBybit):
        fee_mode='standard'
        def __init__(self):
            super().__init__();self.candle_calls=[]
        async def ticker(self,symbol):return {'symbol':symbol,'lastPrice':'123'}
        async def candles(self,symbol,tf,start=None,end=None,mark=False):
            self.candle_calls.append((symbol,tf))
            return [Candle(T,100,101,99,100)]
        async def spot_matches(self,symbols):return {}
    e=make_engine();bybit=ContextBybit();scanner=Scanner(e,bybit)
    monkeypatch.setattr('backend.service.now_ms',lambda:T+86_400_000)
    run(scanner.load_context_symbols({'OLDUSDT':{'5','60','D'}}))
    assert scanner.tickers['OLDUSDT']['_quote_time']==T+86_400_000
    assert scanner.quotes['OLDUSDT']['price']==123
    assert set(bybit.candle_calls)=={('OLDUSDT','5'),('OLDUSDT','60'),('OLDUSDT','D')}
    assert all(('OLDUSDT',tf) in scanner.buffers for tf in ('5','60','D'))


def test_stale_context_quote_rest_refresh_is_deduplicated():
    class QuoteBybit(FakeBybit):
        calls=0
        async def ticker(self,symbol):
            self.calls+=1
            await asyncio.sleep(0)
            return {'symbol':symbol,'lastPrice':'125'}
    bybit=QuoteBybit();scanner=Scanner(make_engine(),bybit)
    async def check():
        await asyncio.gather(scanner.refresh_context_quote('OLDUSDT'),
                             scanner.refresh_context_quote('OLDUSDT'))
    run(check())
    assert bybit.calls==1
    assert scanner.quotes['OLDUSDT']['price']==125


def test_selected_signal_requests_context_refresh():
    class QuoteBybit(FakeBybit):
        async def tickers(self):return [{'symbol':'OLDUSDT','lastPrice':'125'}]
    signal=Signal('OLDUSDT','5','ascending_triangle','long',0,0,100,98,110,1)
    scanner=Scanner(make_engine(),QuoteBybit())
    scanner.engine.persist([signal])
    scanner.tickers[signal.symbol]={'lastPrice':'100','_quote_time':0}
    client=object()
    scanner.watch_context(client,signal.id)
    async def check():
        result=await scanner.refresh_decision_context(signal.id)
        assert result is not None
        await scanner.context_quote_tasks[signal.symbol]
    run(check())
    assert 'OLDUSDT' in scanner.signal_context_scope()
    assert scanner.context_refresh_event.is_set()
    assert scanner.quotes['OLDUSDT']['price']==125


def test_recovery_funding_then_conservative_stop(monkeypatch):
    e=make_engine()
    open_position(e)
    bybit=FakeBybit()
    bybit.bars=[Candle(T+60000,100,120,90,100)]
    bybit.funding=[(T+60000,.001)]
    monkeypatch.setattr('backend.service.now_ms',lambda:T+120000)
    scanner=Scanner(e,bybit)
    run(scanner.recover())
    assert not e.positions
    assert e.trades[0]['funding']<0
    assert e.trades[0]['reason']=='stop'
    assert e.trades[0]['recovered']
    assert e.cash-1000==pytest.approx(e.trades[0]['net'])


def test_missing_funding_blocks_recovery(monkeypatch):
    e=make_engine()
    open_position(e)
    e.positions[0]['next_funding']=T+60000
    fake=FakeBybit()
    fake.bars=[Candle(T+60000,100,120,90,100)]
    monkeypatch.setattr('backend.service.now_ms',lambda:T+120000)
    with pytest.raises(RuntimeError,match='funding'):
        run(Scanner(e,fake).recover())
    assert len(e.positions)==1


def test_restart_after_funding_commit_before_schedule_update(monkeypatch):
    e=make_engine()
    open_position(e)
    p=e.positions[0]
    p['next_funding']=T+60000
    e.funding_payment('BTCUSDT',.001,T+60000,100)
    paid=e.cash
    fake=FakeBybit()
    fake.bars=[Candle(T+60000,100,101,99,100)]
    monkeypatch.setattr('backend.service.now_ms',lambda:T+120000)
    scanner=Scanner(e,fake)
    run(scanner.recover())
    assert e.cash==pytest.approx(paid)
    run(scanner.settle_due_funding())
    assert p['next_funding']>T+120000


def test_setup_keeps_frozen_geometry_and_does_not_confirm_intrabar_crossing():
    e=make_engine();scanner=Scanner(e,FakeBybit());scanner.symbols=['BTCUSDT']
    buf=channel('ascending_triangle')[:-1]
    scanner.buffers[('BTCUSDT','15')]=buf
    run(scanner.discover_setups('BTCUSDT','15',buf))
    setup=e.store.setups()[0];original=setup['snapshot']['lines']
    boundary=setup['snapshot']['boundary'];atr=setup['snapshot']['atr'];stop=setup['snapshot']['stop']
    scanner.setup_price_state(setup,boundary+atr*.05,buf[-1].time+1000)
    assert e.store.setup(setup['id'])['status']=='crossing'
    saved_at=e.store.setup(setup['id'])['updated_at']
    scanner.setup_price_state(setup,boundary+atr*.06,buf[-1].time+2000)
    assert e.store.setup(setup['id'])['updated_at']==saved_at
    close=boundary-atr*.2
    candle=Candle(buf[-1].time+900000,close,boundary+atr*.1,stop+atr*.05,close,1)
    scanner.advance_setups('BTCUSDT','15',candle,buf+[candle])
    updated=e.store.setup(setup['id'])
    assert updated['status']=='near_boundary'
    assert updated['snapshot']['lines']==original
    assert not e.positions
    breakout=Candle(candle.time+900000,boundary+atr*.2,boundary+atr*.25,boundary+atr*.15,boundary+atr*.2,1)
    scanner.advance_setups('BTCUSDT','15',breakout,buf+[candle,breakout])
    assert e.store.setup(setup['id'])['status']=='breakout_confirmed'
    assert not e.positions


def test_stream_ignores_open_and_duplicate_candles_and_restores_gap():
    e=make_engine()
    scanner=Scanner(e,FakeBybit())
    scanner.symbols=['BTCUSDT']
    scanner.buffers['BTCUSDT','15']=[Candle(T,100,101,99,100)]
    def message(start,confirm):
        return {'topic':'kline.15.BTCUSDT','data':[{'start':start,'open':'100','high':'101','low':'99','close':'100','volume':'1','confirm':confirm}]}
    run(scanner.message(message(T+900000,False)))
    assert len(scanner.buffers['BTCUSDT','15'])==1
    run(scanner.message(message(T,True)))
    assert len(scanner.buffers['BTCUSDT','15'])==1
    run(scanner.message(message(T+900000,True)))
    assert len(scanner.buffers['BTCUSDT','15'])==2
    run(scanner.message(message(T+2700000,True)))
    assert [c.time for c in scanner.buffers['BTCUSDT','15']]==[
        T,T+900000,T+1800000,T+2700000]


def test_live_candle_gap_uses_rest_history_before_scanning_later_bar():
    bybit=FakeBybit()
    bybit.bars=[Candle(T+300000,100,102,99,101,10,1000)]
    scanner=Scanner(make_engine(),bybit)
    scanner.symbols=['BTCUSDT']
    scanner.buffers['BTCUSDT','5']=[Candle(T,100,101,99,100,10,1000)]
    later={'topic':'kline.5.BTCUSDT','data':[{'start':T+600000,
        'open':'101','high':'103','low':'100','close':'102','volume':'12',
        'turnover':'1200','confirm':True}]}

    run(scanner.message(later))

    assert scanner.engine.store.cursor('BTCUSDT','5')==T+600000
    assert [c.time for c in scanner.buffers['BTCUSDT','5']]==[T,T+300000,T+600000]


def test_chart_unwatched_during_gap_recovery_does_not_report_false_error():
    scanner=Scanner(make_engine(),FakeBybit())
    scanner.watched_charts['browser']={'symbol':'CAPUSDT','timeframe':'5'}
    scanner.engine.store.set_cursor('CAPUSDT','5',T)
    async def history(symbol,tf,start,end):
        scanner.watched_charts.clear()
        return [Candle(T+300000,100,101,99,100)]
    scanner.bybit.history=history
    later={'topic':'kline.5.CAPUSDT','data':[{'start':T+600000,
        'open':'100','high':'101','low':'99','close':'100','volume':'1','confirm':True}]}
    run(scanner.kline_message(later,'5','CAPUSDT',defer_detection=True))
    assert scanner.engine.store.cursor('CAPUSDT','5')==T
    assert scanner.error==''


def test_chart_unwatched_mid_backfill_keeps_only_committed_cursor():
    scanner=Scanner(make_engine(),FakeBybit())
    scanner.watched_charts['browser']={'symbol':'CAPUSDT','timeframe':'5'}
    store=scanner.engine.store
    store.set_cursor('CAPUSDT','5',T)
    scanner.bybit.bars=[Candle(T+300000,100,101,99,100),
                        Candle(T+600000,100,101,99,100)]
    original=store.acommit_candle_transition
    async def commit(*args,**kwargs):
        result=await original(*args,**kwargs)
        scanner.watched_charts.clear()
        return result
    store.acommit_candle_transition=commit
    later={'topic':'kline.5.CAPUSDT','data':[{'start':T+900000,
        'open':'100','high':'101','low':'99','close':'100','volume':'1','confirm':True}]}
    run(scanner.kline_message(later,'5','CAPUSDT',defer_detection=True))
    assert store.cursor('CAPUSDT','5')==T+300000
    assert scanner.error==''


def test_invalid_rest_candle_does_not_advance_gap_cursor():
    scanner=Scanner(make_engine(),FakeBybit())
    scanner.symbols=['BTCUSDT']
    scanner.engine.store.set_cursor('BTCUSDT','5',T)
    scanner.bybit.bars=[Candle(T+300000,100,99,98,100)]
    later={'topic':'kline.5.BTCUSDT','data':[{'start':T+600000,
        'open':'100','high':'101','low':'99','close':'100','volume':'1','confirm':True}]}
    with pytest.raises(RuntimeError,match='не обработана восстановленная свеча'):
        run(scanner.kline_message(later,'5','BTCUSDT',defer_detection=True))
    assert scanner.engine.store.cursor('BTCUSDT','5')==T


def test_gap_recovery_scans_only_candles_at_or_before_each_close():
    scanner=Scanner(make_engine(),FakeBybit())
    scanner.symbols=['BTCUSDT']
    store=scanner.engine.store
    store.set_cursor('BTCUSDT','5',T)
    store.put_candles('BTCUSDT','5',[Candle(T-300000,100,101,99,100),Candle(T,100,101,99,100)])
    scanner.buffers['BTCUSDT','5']=[Candle(T+600000,100,101,99,100),
                                     Candle(T+900000,100,101,99,100)]
    scanner.bybit.bars=[Candle(T+300000,100,101,99,100)]
    snapshots=[]
    async def capture(symbol,tf,candles,candle_time,**kwargs):
        snapshots.append((candle_time,[bar.time for bar in candles]))
    scanner.enqueue_detection_job=capture
    later={'topic':'kline.5.BTCUSDT','data':[{'start':T+600000,
        'open':'100','high':'101','low':'99','close':'100','volume':'1','confirm':True}]}
    run(scanner.kline_message(later,'5','BTCUSDT',defer_detection=True))
    assert store.cursor('BTCUSDT','5')==T+600000
    assert [time for time,_ in snapshots]==[T+300000,T+600000]
    assert all(max(times)==time for time,times in snapshots)
    assert snapshots[0][1][:2]==[T-300000,T]


def test_long_backfill_keeps_unscanned_candles_despite_newer_cached_history():
    store=Store(':memory:')
    interval=300000
    store.set_cursor('BTCUSDT','5',T)
    run(store.aset_scan_cursor('BTCUSDT','5',T))
    future=[Candle(T+(n+100)*interval,100,101,99,100) for n in range(1500)]
    store.put_candles('BTCUSDT','5',future)
    recovered=Candle(T+interval,100,101,99,100)
    run(store.acommit_candle_transition('BTCUSDT','5',recovered,[],recovered.time,scan_floor=T))
    rows=store.candles_range('BTCUSDT','5',start=recovered.time,end=recovered.time+interval)
    assert [row.time for row in rows]==[recovered.time]
    assert store.cursor('BTCUSDT','5')==recovered.time
    assert store.scan_cursor('BTCUSDT','5')==T
    store.close()


def test_ticker_delta_and_old_ticks():
    e=make_engine()
    scanner=Scanner(e,FakeBybit())
    run(scanner.message({'topic':'tickers.BTCUSDT','ts':T,'data':{'lastPrice':'100','fundingRate':'.001','nextFundingTime':str(T+1000)}}))
    run(scanner.message({'topic':'tickers.BTCUSDT','ts':T+1,'data':{'lastPrice':'101'}}))
    run(scanner.message({'topic':'tickers.BTCUSDT','ts':T-1,'data':{'lastPrice':'90'}}))
    assert scanner.quotes['BTCUSDT']['price']==101
    assert scanner.tickers['BTCUSDT']['fundingRate']=='.001'


def test_old_ticker_cannot_overwrite_fresh_rest_quote_or_tick_data():
    scanner=Scanner(make_engine(),FakeBybit())
    scanner.quotes['BTCUSDT']={'price':105.,'time':T+20}
    scanner.tickers['BTCUSDT']={'lastPrice':'105','_quote_time':T+20}
    run(scanner.message({'topic':'tickers.BTCUSDT','ts':T+10,
                         'data':{'lastPrice':'90','fundingRate':'.9'}}))
    assert scanner.quotes['BTCUSDT']=={'price':105.,'time':T+20}
    assert scanner.tickers['BTCUSDT']['lastPrice']=='105'
    # A message without lastPrice may update funding metadata, but never quote age.
    run(scanner.message({'topic':'tickers.BTCUSDT','ts':T+21,
                         'data':{'fundingRate':'.001'}}))
    assert scanner.quotes['BTCUSDT']=={'price':105.,'time':T+20}
    assert scanner.tickers['BTCUSDT']['_quote_time']==T+20


def test_ticker_path_keeps_each_quote_but_throttles_scalp_database_work(monkeypatch):
    scanner=Scanner(make_engine(),FakeBybit())
    scanner.context_scope={'BTCUSDT':{'5'}}
    tracked=[]
    async def track(symbol,price,timestamp):tracked.append(timestamp)
    monkeypatch.setattr(scanner,'track_scalp_setups',track)
    for offset,price in ((10_000,100),(10_500,101),(11_200,102)):
        run(scanner.message({'topic':'tickers.BTCUSDT','ts':T+offset,
            'data':{'lastPrice':str(price)}}))
    assert scanner.quotes['BTCUSDT']=={'price':102.0,'time':T+11_200}
    assert tracked==[T+10_000,T+11_200]


def test_market_and_ancillary_topics_are_separated():
    scanner=Scanner(make_engine(),FakeBybit())
    scanner.symbols=['BTCUSDT']
    market=scanner.subscription_topics()
    ancillary=scanner.ancillary_topics()
    assert 'tickers.BTCUSDT' in market
    assert 'kline.5.BTCUSDT' in market
    assert 'orderbook.200.BTCUSDT' not in market
    assert 'publicTrade.BTCUSDT' not in market
    assert 'orderbook.200.BTCUSDT' not in ancillary
    assert 'publicTrade.BTCUSDT' in ancillary
    assert 'allLiquidation.BTCUSDT' in ancillary


def test_subscription_ack_without_request_id_is_matched_to_its_socket():
    class FakeSocket:
        def __init__(self):self.sent=[]
        async def send(self,message):self.sent.append(message)

    scanner=Scanner(make_engine(),FakeBybit())
    socket=FakeSocket()
    topics=[f'tickers.SYMBOL{i:03}USDT' for i in range(240)]

    async def subscribe_and_ack():
        task=asyncio.create_task(scanner.request_subscription(
            socket,'subscribe',topics))
        while not socket.sent:
            await asyncio.sleep(0)
        assert len(json.loads(socket.sent[0])['args'])==len(topics)
        assert scanner.resolve_subscription_ack(
            {'op':'subscribe','success':True,'ret_msg':''},socket)
        await task

    run(subscribe_and_ack())
    assert socket not in scanner.subscription_ack_waiters_by_socket


def test_breakout_setup_lookup_uses_pair_timeframe_and_status():
    store=make_engine().store
    def save(setup_id,symbol,timeframe,status):
        store.save_setup({'id':setup_id,'symbol':symbol,'timeframe':timeframe,
            'detected_at':1,'updated_at':1,'status':status,'snapshot':{}})
    save('match','BTCUSDT','5','breakout_confirmed')
    save('other-timeframe','BTCUSDT','15','breakout_confirmed')
    save('other-status','BTCUSDT','5','wait_breakout')
    assert [row['id'] for row in store.breakout_setups('BTCUSDT','5')]==['match']


def test_scalp_setups_excludes_historical_confirmed_patterns_but_keeps_active():
    store=make_engine().store
    for setup_id,status,event_at in [
        ('active-old','wait_breakout',T-86_400_000),
        ('confirmed-recent','breakout_confirmed',T-60_000),
        ('confirmed-old','breakout_confirmed',T-600_000),
    ]:
        store.save_setup({'id':setup_id,'symbol':'BTCUSDT','timeframe':'5',
            'detected_at':event_at,'event_at':event_at,'status':status,'snapshot':{}})
    rows=store.scalp_setups('BTCUSDT',T-300_000)
    assert {row['id'] for row in rows}=={'active-old','confirmed-recent'}


def test_scanner_diagnostics_count_closed_bars_even_without_a_breakout():
    scanner=Scanner(make_engine(),FakeBybit())
    scanner.symbols=['BTCUSDT']
    close=(now_ms()//300000)*300000
    bar=Candle(close-300000,100,101,99,100,10)
    assert run(scanner.filter_candidates('BTCUSDT','5',[bar]))==[]
    diagnostics=scanner.status()['scanner_diagnostics']
    assert diagnostics['checked_closed_candles_last_hour']==1
    assert diagnostics['raw_breakouts_last_hour']==0
    assert diagnostics['new_breakouts_last_hour']==0
    assert diagnostics['last_closed_candle_scan']==close
    assert diagnostics['streams_expected']==4


def test_received_closed_candle_is_not_reported_stale_while_worker_is_queued(monkeypatch):
    scanner=Scanner(make_engine(),FakeBybit())
    scanner.symbols=['BTCUSDT']
    expected=T+300_000
    scanner.last_received_close[('BTCUSDT','5')]=expected
    monkeypatch.setattr('backend.service.now_ms',lambda:expected+31_000)
    stale=scanner.status()['scanner_diagnostics']['stale_streams']
    assert not any(row['symbol']=='BTCUSDT' and row['timeframe']=='5' for row in stale)


def test_websocket_close_is_marked_received_only_after_worker_scans_it():
    scanner=Scanner(make_engine(),FakeBybit())
    scanner.symbols=['BTCUSDT']
    async def check():
        release=asyncio.Event()
        original=scanner.kline_message
        async def gated(*args,**kwargs):
            await release.wait()
            await original(*args,**kwargs)
        scanner.kline_message=gated
        worker=asyncio.create_task(scanner.candle_worker())
        scanner.candle_workers=[worker]
        try:
            await scanner.receive_kline({'topic':'kline.5.BTCUSDT','data':[{
                'start':T,'open':'100','high':'101','low':'99','close':'100',
                'volume':'1','confirm':True}]})
            assert ('BTCUSDT','5') not in scanner.last_received_close
            release.set()
            await asyncio.wait_for(scanner.candle_queue.join(),1)
            assert scanner.last_received_close['BTCUSDT','5']==T+5*60_000
            assert scanner.last_closed_scan['BTCUSDT','5']==T+5*60_000
        finally:
            worker.cancel()
            with pytest.raises(asyncio.CancelledError):
                await worker
    run(check())


def test_missing_closed_candle_is_restored_and_scanned(monkeypatch):
    scanner=Scanner(make_engine(),FakeBybit())
    scanner.symbols=['BTCUSDT']
    scanner.buffers['BTCUSDT','5']=[Candle(T,100,101,99,100,1)]
    monkeypatch.setattr('backend.service.now_ms',lambda:T+12*60_000)
    recovered,errors=run(scanner.recover_closed_candles([
        {'symbol':'BTCUSDT','timeframe':'5'}]))
    assert recovered==1 and errors==[]
    assert scanner.last_closed_scan['BTCUSDT','5']==T+10*60_000
    assert scanner.scan_events and scanner.scan_events[-1]['time']==T+12*60_000


def test_cancelled_recovery_is_not_reported_as_stream_error():
    scanner=Scanner(make_engine(),FakeBybit())
    async def cancelled(*args,**kwargs):raise asyncio.CancelledError
    scanner.bybit.candles=cancelled
    with pytest.raises(asyncio.CancelledError):
        run(scanner.recover_closed_candles([{'symbol':'BTCUSDT','timeframe':'5'}]))
    assert scanner.error==''


def test_candle_watchdog_scans_over_rest_when_websocket_is_down(monkeypatch):
    scanner=Scanner(make_engine(),FakeBybit())
    scanner.last_refresh=T
    scanner.phase='reconnecting'
    scanner.last_message=T
    monkeypatch.setattr('backend.service.now_ms',lambda:T+60_000)
    monkeypatch.setattr(scanner,'status',lambda:{'scanner_diagnostics':{
        'stale_streams':[{'symbol':'BTCUSDT','timeframe':'5'}]}})
    recovered=[]
    async def restore(stale):
        recovered.extend(stale)
        return 2,[]
    monkeypatch.setattr(scanner,'recover_closed_candles',restore)
    sleeps=0
    async def stop_after_one_iteration(_):
        nonlocal sleeps
        sleeps+=1
        if sleeps>1:raise asyncio.CancelledError
    monkeypatch.setattr('backend.service.asyncio.sleep',stop_after_one_iteration)
    with pytest.raises(asyncio.CancelledError):
        run(scanner.run_candle_watchdog())
    assert recovered==[{'symbol':'BTCUSDT','timeframe':'5'}]
    assert scanner.phase=='rest_fallback'
    assert scanner.error.startswith('WebSocket недоступен')
    assert scanner.engine.connected is False


@pytest.mark.parametrize('message',['REST recovery failed for 1 streams: BTCUSDT/5',
                                    'Восстановление свечей BTCUSDT/5: ошибка'])
def test_candle_watchdog_clears_resolved_rest_recovery_error(monkeypatch,message):
    scanner=Scanner(make_engine(),FakeBybit())
    scanner.last_refresh=T
    scanner.error=message
    monkeypatch.setattr(scanner,'status',lambda:{'scanner_diagnostics':{'stale_streams':[]}})
    sleeps=0
    async def stop_after_one_iteration(_):
        nonlocal sleeps
        sleeps+=1
        if sleeps>1:raise asyncio.CancelledError
    monkeypatch.setattr('backend.service.asyncio.sleep',stop_after_one_iteration)
    with pytest.raises(asyncio.CancelledError):
        run(scanner.run_candle_watchdog())
    assert scanner.error==''


def test_watchdog_waits_for_initial_market_load_even_with_open_chart(monkeypatch):
    scanner=Scanner(make_engine(),FakeBybit())
    scanner.watched_charts['browser']={'symbol':'BTCUSDT','timeframe':'5'}
    calls=[]
    async def restore(stale):
        calls.extend(stale)
        return 0,[]
    monkeypatch.setattr(scanner,'recover_closed_candles',restore)
    sleeps=0
    async def once(_):
        nonlocal sleeps
        sleeps+=1
        if sleeps>1:raise asyncio.CancelledError
    monkeypatch.setattr('backend.service.asyncio.sleep',once)
    with pytest.raises(asyncio.CancelledError):
        run(scanner.run_candle_watchdog())
    assert calls==[]


def test_watchdog_retries_network_failure_with_backoff_and_recovers(monkeypatch):
    from backend.bybit import BybitNetworkError
    scanner=Scanner(make_engine(),FakeBybit())
    scanner.last_refresh=T
    clock=[T+60_000]
    monkeypatch.setattr('backend.service.now_ms',lambda:clock[0])
    monkeypatch.setattr(scanner,'status',lambda:{'scanner_diagnostics':{
        'stale_streams':[{'symbol':'BTCUSDT','timeframe':'5'}]}})
    attempts=[]
    async def restore(stale):
        attempts.append(clock[0])
        if len(attempts)<=4:
            return 0,[('BTCUSDT','5',BybitNetworkError('Bybit: сеть недоступна'))]
        return 1,[]
    monkeypatch.setattr(scanner,'recover_closed_candles',restore)
    sleeps=[0]
    async def once(_):
        sleeps[0]+=1
        if sleeps[0]>1:raise asyncio.CancelledError
    monkeypatch.setattr('backend.service.asyncio.sleep',once)
    for delay in (5_000,10_000,20_000,30_000):
        sleeps[0]=0
        with pytest.raises(asyncio.CancelledError):run(scanner.run_candle_watchdog())
        assert scanner.candle_recovery_status=='waiting_network'
        assert scanner.candle_recovery_pending==1
        assert scanner.next_candle_recovery==clock[0]+delay
        assert scanner.error==''
        clock[0]+=delay
    sleeps[0]=0
    with pytest.raises(asyncio.CancelledError):run(scanner.run_candle_watchdog())
    assert scanner.candle_recovery_status=='idle'
    assert scanner.candle_recovery_pending==0
    assert scanner.next_candle_recovery==0
    assert scanner.candle_recovery_backoff==5_000


def test_watchdog_keeps_data_errors_visible(monkeypatch):
    scanner=Scanner(make_engine(),FakeBybit())
    scanner.last_refresh=T
    monkeypatch.setattr(scanner,'status',lambda:{'scanner_diagnostics':{
        'stale_streams':[{'symbol':'BTCUSDT','timeframe':'5'}]}})
    async def restore(stale):return 0,[('BTCUSDT','5',ValueError('bad data'))]
    monkeypatch.setattr(scanner,'recover_closed_candles',restore)
    sleeps=0
    async def once(_):
        nonlocal sleeps
        sleeps+=1
        if sleeps>1:raise asyncio.CancelledError
    monkeypatch.setattr('backend.service.asyncio.sleep',once)
    with pytest.raises(asyncio.CancelledError):run(scanner.run_candle_watchdog())
    assert scanner.candle_recovery_status=='error'
    assert 'BTCUSDT/5' in scanner.error and 'ValueError' in scanner.error


def test_scanner_waits_for_network_then_connects(monkeypatch):
    from contextlib import asynccontextmanager
    from backend.bybit import BybitNetworkError
    scanner=Scanner(make_engine(),FakeBybit())
    scanner.bybit.ws_url='wss://stream.bybit.com/v5/public/linear'
    attempts=0
    connected=asyncio.Event()
    async def refresh():
        nonlocal attempts
        attempts+=1
        if attempts==1:raise BybitNetworkError('Bybit: сеть недоступна')
        scanner.error='WebSocket недоступен; сканирование закрытых свечей идёт через REST Bybit'
        scanner.last_refresh=T
    async def recover():pass
    class Socket:
        async def recv(self):await asyncio.Future()
    @asynccontextmanager
    async def connect(*args,**kwargs):
        connected.set()
        yield Socket()
    async def heartbeat(socket):await asyncio.Future()
    monkeypatch.setattr(scanner,'refresh',refresh)
    monkeypatch.setattr(scanner,'recover',recover)
    monkeypatch.setattr(scanner.bybit,'connect_ws',connect,raising=False)
    monkeypatch.setattr(scanner,'bybit_heartbeat',heartbeat)
    monkeypatch.setattr(scanner,'subscription_topics',lambda:[])
    async def check():
        task=asyncio.create_task(scanner.run())
        try:
            for _ in range(100):
                if scanner.phase=='waiting_network':break
                await asyncio.sleep(.01)
            assert scanner.phase=='waiting_network' and scanner.error==''
            await asyncio.wait_for(connected.wait(),3)
            assert scanner.phase=='live' and attempts==2
            assert scanner.error==''
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):await task
    run(check())


@pytest.mark.parametrize('stage',['request','retry'])
def test_stop_waits_for_network_task_before_closing_client(stage):
    scanner=Scanner(make_engine(),FakeBybit())
    events=[]
    async def network_task():
        try:
            events.append(stage)
            await asyncio.Future()
        finally:events.append('cancelled')
    async def close():events.append('client closed')
    scanner.bybit.close=close
    async def check():
        task=asyncio.create_task(network_task())
        scanner.tasks=[task]
        await asyncio.sleep(0)
        await scanner.stop()
        assert task.done()
    run(check())
    assert events==[stage,'cancelled','client closed']


def test_public_orderbook_snapshot_delta_and_aggressive_trade_flow():
    scanner=Scanner(make_engine(),FakeBybit())
    run(scanner.message({'topic':'orderbook.50.BTCUSDT','type':'snapshot','ts':T,'data':{
        'u':10,'b':[['99','2'],['98','3']],'a':[['101','4'],['102','5']]}}))
    run(scanner.message({'topic':'orderbook.50.BTCUSDT','type':'delta','ts':T+1,'data':{
        'u':11,'b':[['99','0'],['97','6']],'a':[['101','7']]}}))
    book=scanner.books['BTCUSDT']
    assert 97. in book['bids']
    assert 99. not in book['bids'] and book['bids'][97.]==6 and book['asks'][101.]==7
    run(scanner.message({'topic':'publicTrade.BTCUSDT','data':[
        {'T':T+2,'s':'BTCUSDT','S':'Buy','p':'100','v':'2','i':'trade-a','seq':9},
        {'T':T+3,'s':'BTCUSDT','S':'Sell','p':'101','v':'1','i':'trade-b','seq':9}]}))
    run(scanner.message({'topic':'publicTrade.BTCUSDT','data':[
        {'T':T+2,'s':'BTCUSDT','S':'Buy','p':'100','v':'2','i':'trade-a','seq':20},
        {'T':T+4,'s':'BTCUSDT','S':'Buy','p':'100','v':'1','i':'trade-c','seq':9}]}))
    bucket=scanner.live_flow['BTCUSDT',(T+2)//60000*60000]
    assert bucket['buy']==300 and bucket['sell']==101


def test_orderbook_u1_replaces_old_book_even_if_message_type_is_delta():
    scanner=Scanner(make_engine(),FakeBybit())
    scanner.books['BTCUSDT']={'bids':{99.:1},'asks':{101.:1},'time':T,'now':T}
    run(scanner.message({'topic':'orderbook.50.BTCUSDT','type':'delta','ts':T+1,'data':{
        'u':1,'b':[['98','2']],'a':[['102','3']]}}))
    assert scanner.books['BTCUSDT']['bids']=={98.:2.}
    assert scanner.books['BTCUSDT']['asks']=={102.:3.}


def test_ancillary_reconnect_requires_new_orderbook_snapshot():
    from backend.market_comparison import apply_orderbook
    scanner=Scanner(make_engine(),FakeBybit())
    run(scanner.message({'topic':'orderbook.200.BTCUSDT','type':'snapshot','ts':T,'data':{
        'u':10,'seq':100,'b':[['99','2']],'a':[['101','2']]}}))
    assert scanner.books['BTCUSDT']['valid']
    scanner.reset_ancillary_stream()
    assert 'BTCUSDT' not in scanner.books
    delta={'type':'delta','ts':T+1,'data':{'u':11,'seq':150,'b':[['98','1']],'a':[]}}
    assert not apply_orderbook(scanner.books,'BTCUSDT',delta,T+1)
    snapshot={'type':'snapshot','ts':T+2,'data':{'u':12,'seq':151,
        'b':[['99','2']],'a':[['101','2']]}}
    assert apply_orderbook(scanner.books,'BTCUSDT',snapshot,T+2)


def test_invalid_partial_candle_is_not_published_as_live_data():
    scanner=Scanner(make_engine(),FakeBybit())
    scanner.symbols=['BTCUSDT']
    run(scanner.receive_kline({'topic':'kline.5.BTCUSDT','data':[{
        'start':T,'open':'100','high':'99','low':'99.5','close':'100',
        'volume':'1','confirm':False}]}))
    assert ('BTCUSDT','5') not in scanner.live_candles
    assert 'Некорректная свеча OHLC' in scanner.error


def test_all_liquidation_flow_is_aggregated_in_usdt_by_position_closed():
    scanner=Scanner(make_engine(),FakeBybit())
    run(scanner.message({'topic':'allLiquidation.BTCUSDT','data':[
        {'s':'BTCUSDT','T':T,'S':'Sell','p':'100','v':'2'},
        {'s':'BTCUSDT','T':T+1,'S':'Buy','p':'101','v':'3'}]}))
    bucket=scanner.live_liquidations['BTCUSDT',T//60_000*60_000]
    assert bucket['long_usdt']==200
    assert bucket['short_usdt']==303
    assert bucket['count']==2


def test_market_flow_archive_writes_closed_minute_once(monkeypatch):
    scanner=Scanner(make_engine(),FakeBybit())
    now=(T//60_000+3)*60_000+10_000
    previous=now//60_000*60_000-60_000
    scanner.symbols=['BTCUSDT']
    scanner.ancillary_connected=True
    scanner.ancillary_flow_started_at=previous-10_000
    scanner.live_flow={
        ('BTCUSDT',previous):{'buy':120.,'sell':80.,'updated':previous+58_000},
        ('BTCUSDT',now//60_000*60_000):{'buy':15.,'sell':5.,'updated':now},
    }
    writes=[]
    scanner._write_market_batches=lambda minutes,spots:writes.append((minutes,spots))
    monkeypatch.setattr('backend.service.now_ms',lambda:now)

    run(scanner.persist_market_minutes())
    assert len(writes)==1
    assert len(writes[0][0])==1
    row=writes[0][0][0]
    assert (row['symbol'],row['time'])==('BTCUSDT',previous)
    assert (row['buy'],row['sell'],row['coverage_ms'],row['quality'])==(120.,80.,60_000,'valid')
    assert scanner.flow_saved['BTCUSDT',previous]==previous+58_000

    run(scanner.persist_market_minutes())
    assert len(writes)==1


def test_live_entries_remain_blocked_until_research_passes(monkeypatch):
    from tests.test_patterns import swings
    c=swings('head_shoulders')
    # Shift fixture into a realistic wall-clock epoch.
    for bar in c:bar.time+=T
    close=c[-1].time+900000
    e=make_engine()
    e.set_fee('BTCUSDT',.0006,close)
    scanner=Scanner(e,FakeBybit())
    scanner.symbols=['BTCUSDT']
    scanner.buffers['BTCUSDT','15']=c[:-1]
    row=asdict(c[-1])|{'start':c[-1].time,'confirm':True}
    run(scanner.message({'topic':'kline.15.BTCUSDT','data':[row]}))
    assert scanner.pending
    e.connected=True
    run(scanner.message({'topic':'tickers.BTCUSDT','ts':close+2000,'data':{'lastPrice':str(c[-1].close)}}))
    monkeypatch.setattr('backend.service.now_ms',lambda:close+2000)
    run(scanner.flush())
    signals=e.store.signals()
    assert signals
    assert not e.positions
    assert all(s['status']=='found' for s in signals)
    assert all(s['reason'].startswith('Пробой найден.') for s in signals)
    assert all(s['filter_results']['auto_entry_allowed'] is False for s in signals)
