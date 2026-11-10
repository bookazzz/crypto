import asyncio
from concurrent.futures import ProcessPoolExecutor
import json
import sqlite3
import pytest

from backend.engine import Engine
from backend.entry import assess_entry
from backend.models import Candle, Instrument, now_ms
from backend.publishing import VersionedPublisher
from backend.service import Scanner, _detect_in_worker
from backend.storage import Store
from backend.market_quality import quality, valid_ohlc
from backend.process_lock import ScannerProcessLock
from backend.detection_queue import DetectionJob, PrioritizedStreamQueue


class QuietBybit:
    offset = 0

    async def close(self):
        pass


def run(coro):
    return asyncio.run(coro)


def setup_row(setup_id, *, profile='expanded', detected_on=0):
    return {'id':setup_id,'symbol':'BTCUSDT','timeframe':'5',
        'pattern':'ascending_triangle','side':'long','status':'wait_breakout',
        'detected_at':detected_on,'updated_at':detected_on,'event_at':detected_on,
        'confirmation_profile':profile,'reason':'','snapshot':{
            'start':detected_on-10*300_000,'detected_on_bar':detected_on,
            'boundary':100.,'stop':98.,'target':110.,'atr':2.,
            'lines':[{'label':'support','points':[{'time':detected_on,'value':100.},
                       {'time':detected_on+300_000,'value':100.}]}],
            'pivots':[]}}


def test_setup_tick_retraction_and_recross_keep_first_event_order():
    store=Store(':memory:')
    engine=Engine(store)
    now=now_ms()
    engine.instruments={'BTCUSDT':Instrument('BTCUSDT',.001,.001,5,.1)}
    engine.fees={'BTCUSDT':{'rate':.00055,'time':now,'source':'bybit_vip0_standard'}}
    scanner=Scanner(engine,QuietBybit())
    setup=setup_row('early',detected_on=now-300_000)
    store.save_setup(setup)

    async def quote(time,price):
        await scanner.message({'topic':'tickers.BTCUSDT','ts':time,
            'data':{'lastPrice':str(price),'fundingRate':'0','nextFundingTime':str(time+60_000)}},
            fast=True)

    run(quote(now,100.2))
    first=store.setup('early')
    assert first['status']=='crossing'
    first_event=first['event_at']
    assert first['current']['entry_assessment']['status']=='available'

    run(quote(now+1,99.5))
    retracted=store.setup('early')
    assert retracted['preview_state']=='retracted'
    assert retracted['current']['entry_assessment']['status']=='waiting'
    run(quote(now+2,100.2))
    repeated=store.setup('early')
    assert repeated['status']=='crossing'
    assert repeated['event_at']==first_event
    assert repeated['id']=='early'
    store.close()


def test_early_setup_context_includes_support_lines_for_its_chart():
    store=Store(':memory:')
    engine=Engine(store)
    scanner=Scanner(engine,QuietBybit())
    now=now_ms()
    previous_day=(now//86_400_000-1)*86_400_000
    scanner.buffers['BTCUSDT','D']=[Candle(previous_day,100,105,95,100,10)]
    scanner.quotes['BTCUSDT']={'price':100.,'time':now}
    store.save_setup(setup_row('support-context',detected_on=now-300_000))

    context=scanner.setup_context('support-context')

    supports=context['current']['chart_levels']
    assert any(level['kind']=='support' and level['price']==95 for level in supports)
    store.close()


def test_confirmation_uses_profile_threshold_and_exchange_close_time():
    store=Store(':memory:')
    engine=Engine(store)
    scanner=Scanner(engine,QuietBybit())
    start=now_ms()//300_000*300_000-300_000
    standard=setup_row('standard',profile='standard',detected_on=start-300_000)
    expanded=setup_row('expanded',profile='expanded',detected_on=start-300_000)
    store.save_setups([standard,expanded])
    scanner.active_setup_cache=None
    bar=Candle(start,100,100.3,99.5,100.15,10)

    scanner.advance_setups('BTCUSDT','5',bar,[bar])

    assert store.setup('standard')['status']=='near_boundary'
    confirmed=store.setup('expanded')
    assert confirmed['status']=='breakout_confirmed'
    assert confirmed['confirmed_at']==start+300_000
    assert confirmed['event_at']==start+300_000
    store.close()


def test_early_retraction_waits_but_confirmed_return_invalidates():
    common={'side':'long','quote_time':1_000_000,'now':1_000_000,
        'boundary':100.,'stop':98.,'target':110.,'atr':1.,'fee':.00055,
        'fee_time':1_000_000,'fee_source':'standard','fee_mode':'standard',
        'slippage':.0005,'funding_rate':0.,'funding_time':1_000_000,
        'tick_size':.1,'min_rr':2.}
    early=assess_entry(**(common|{'price':99.5,'requires_breakout':True}))
    confirmed=assess_entry(**(common|{'price':99.5,'requires_breakout':False}))
    assert early['status']=='waiting'
    assert confirmed['status']=='scenario_invalidated'


def test_entry_zone_requires_minimum_net_rr_before_price_can_be_late():
    common={'side':'long','quote_time':1_000_000,'now':1_000_000,
        'boundary':100.,'stop':98.,'atr':1.,'fee':.00055,
        'fee_time':1_000_000,'fee_source':'standard','fee_mode':'standard',
        'slippage':.0005,'funding_rate':0.,'funding_time':1_000_000,
        'tick_size':.1,'min_rr':2.,'requires_breakout':True}
    impossible=assess_entry(**(common|{'price':100.2,'target':102.}))
    missed=assess_entry(**(common|{'price':105.,'target':110.}))
    assert impossible['status']=='insufficient_rr'
    assert impossible['price_min'] is None and impossible['price_max'] is None
    assert missed['status']=='late'


def test_detection_queue_prioritizes_live_streams_and_preserves_stream_order():
    queue=PrioritizedStreamQueue(maxsize=5)
    async def scenario():
        def job(priority,sequence,symbol):
            return DetectionJob(priority,sequence,symbol,'5',sequence,(),
                priority>0,0.)
        await queue.put(job(1,1,'BTCUSDT'))
        await queue.put(job(0,2,'ETHUSDT'))
        await queue.put(job(0,3,'BTCUSDT'))
        first=await queue.get()
        assert first.symbol=='ETHUSDT'
        await queue.complete(first)
        second=await queue.get()
        assert (second.symbol,second.sequence)==('BTCUSDT',1)
        await queue.complete(second)
        third=await queue.get()
        assert (third.symbol,third.sequence)==('BTCUSDT',3)
        await queue.complete(third)
        await queue.join()
    run(scenario())


def test_detection_queue_keeps_a_simultaneous_160_stream_burst():
    queue=PrioritizedStreamQueue(maxsize=640)
    async def scenario():
        jobs=[]
        sequence=0
        for index in range(40):
            for timeframe in ('5','15','60','240'):
                jobs.append(DetectionJob(index%3,sequence,f'SYN{index:03}USDT',
                    timeframe,sequence,(),index%3>0,0.))
                sequence+=1
        await asyncio.gather(*(queue.put(job) for job in jobs))
        received=[]
        while len(received)<len(jobs):
            job=await queue.get()
            received.append(job)
            await queue.complete(job)
        await queue.join()
        assert len(received)==160
        assert len({(job.symbol,job.timeframe,job.candle_time) for job in received})==160
        assert sum(not job.recovered for job in received)==56
    run(scenario())


def test_closed_candle_cursor_is_durable_and_duplicate_safe():
    store=Store(':memory:')
    engine=Engine(store)
    scanner=Scanner(engine,QuietBybit())
    key=object()
    scanner.watched_charts[key]={'symbol':'BTCUSDT','timeframe':'5'}
    first=Candle(1_800_000_000_000,100,101,99,100,1)
    closed=Candle(first.time+300_000,100,102,99,101,2)
    scanner.buffers['BTCUSDT','5']=[first]
    store.set_cursor('BTCUSDT','5',first.time)
    message={'data':[{'start':closed.time,'open':closed.open,'high':closed.high,
        'low':closed.low,'close':closed.close,'volume':closed.volume,'confirm':True}]}

    run(scanner.kline_message(message,'5','BTCUSDT'))
    assert store.cursor('BTCUSDT','5')==closed.time
    assert scanner.last_received_close['BTCUSDT','5']==closed.time+300_000
    run(scanner.kline_message(message,'5','BTCUSDT',recovered=True))
    assert store.cursor('BTCUSDT','5')==closed.time
    assert len(store.candles('BTCUSDT','5'))==1
    store.close()


def test_live_candle_commits_known_setup_before_deferred_detection():
    store=Store(':memory:')
    engine=Engine(store)
    scanner=Scanner(engine,QuietBybit())
    scanner.symbols=['BTCUSDT']
    start=now_ms()//300_000*300_000
    prior=Candle(start-300_000,100,100.1,99.9,100,1)
    closed=Candle(start,100,100.4,100,100.3,2)
    scanner.buffers['BTCUSDT','5']=[prior]
    store.set_cursor('BTCUSDT','5',prior.time)
    store.save_setup(setup_row('fast-confirmation',detected_on=prior.time))
    scanner.active_setup_cache=None
    gate=asyncio.Event()

    async def delayed(*args,**kwargs):
        await gate.wait()
        return []

    scanner.filter_candidates=delayed
    scanner.discover_setups=delayed

    async def scenario():
        worker=asyncio.create_task(scanner.candle_worker())
        scanner.candle_workers=[worker]
        detection_worker=asyncio.create_task(scanner.detection_worker())
        scanner.detection_workers=[detection_worker]
        message={'topic':'kline.5.BTCUSDT','data':[{
            'start':closed.time,'open':closed.open,'high':closed.high,
            'low':closed.low,'close':closed.close,'volume':closed.volume,'confirm':True}]}
        try:
            await asyncio.wait_for(scanner.enqueue_candle(message,wait=True),timeout=1)
            persisted=store.setup('fast-confirmation')
            assert store.cursor('BTCUSDT','5')==closed.time
            assert persisted['status']=='breakout_confirmed'
            assert scanner.broadcast_event.is_set()
            assert scanner.detection_queue.unfinished==1
        finally:
            gate.set()
            await scanner.detection_queue.join()
            assert store.scan_cursor('BTCUSDT','5')==closed.time
            worker.cancel()
            try:
                await worker
            except asyncio.CancelledError:
                pass
            detection_worker.cancel()
            try:
                await detection_worker
            except asyncio.CancelledError:
                pass
            scanner.candle_workers=[]
            scanner.detection_workers=[]

    run(scenario())
    store.close()


def test_signal_order_cursor_and_status_filter_precede_limit():
    store=Store(':memory:')
    rows=[{'id':'a','time':100,'event_at':100,'status':'found'},
          {'id':'b','time':100,'event_at':100,'status':'rejected'},
          {'id':'c','time':100,'event_at':100,'status':'found'}]
    store.save({},rows)

    assert [row['id'] for row in store.signals(2)]==['c','b']
    assert [row['id'] for row in store.signals(1,statuses=['found'])]==['c']
    assert [row['id'] for row in store.signals(5,100,'b')]==['a']
    store.close()


def test_existing_database_is_backed_up_before_schema_migration(tmp_path):
    path=tmp_path/'legacy.sqlite3'
    with sqlite3.connect(path) as db:
        db.executescript('''
            CREATE TABLE state (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);
            CREATE TABLE signals (id TEXT PRIMARY KEY, time INTEGER NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE setups (id TEXT PRIMARY KEY, symbol TEXT NOT NULL, timeframe TEXT NOT NULL,
                detected_at INTEGER NOT NULL, status TEXT NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE scanner_cursors (symbol TEXT NOT NULL,timeframe TEXT NOT NULL,
                candle_time INTEGER NOT NULL,PRIMARY KEY(symbol,timeframe));
            PRAGMA user_version=1;
        ''')
        db.execute('INSERT INTO state VALUES(1,?)',(json.dumps({'balance':1000}),))
        db.execute('INSERT INTO signals VALUES(?,?,?)',('legacy',100,json.dumps({'id':'legacy','time':100})))
        db.execute('INSERT INTO scanner_cursors VALUES(?,?,?)',('BTCUSDT','5',300000))

    store=Store(str(path))
    try:
        backup=store.migration_backup
        assert backup
        with sqlite3.connect(backup) as saved:
            assert saved.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
            assert saved.execute('PRAGMA user_version').fetchone()[0]==1
            assert 'event_at' not in {row[1] for row in saved.execute('PRAGMA table_info(signals)')}
            assert saved.execute('SELECT count(*) FROM signals').fetchone()[0]==1
        assert store.db.execute('PRAGMA user_version').fetchone()[0]==7
        assert store.signal('legacy')['event_at']==100
        assert store.scan_cursor('BTCUSDT','5')==300000
    finally:
        store.close()


def test_corrupt_setup_payload_is_skipped_and_replaced(tmp_path):
    path=tmp_path/'legacy-setups.sqlite3'
    with sqlite3.connect(path) as db:
        db.execute('''CREATE TABLE setups (
            id TEXT PRIMARY KEY, symbol TEXT NOT NULL, timeframe TEXT NOT NULL,
            detected_at INTEGER NOT NULL, status TEXT NOT NULL, payload TEXT,
            event_at INTEGER NOT NULL DEFAULT 0)''')
        db.execute('PRAGMA user_version=5')

    store=Store(str(path))
    try:
        store.db.executemany('INSERT INTO setups VALUES(?,?,?,?,?,?,?)',[
            ('null-payload','BTCUSDT','5',100,'wait_breakout',None,100),
            ('bad-json','ETHUSDT','5',101,'near_boundary','{broken',101)])
        store.db.commit()

        assert store.active_setups()==[]
        assert store.setups()==[]

        replacement=setup_row('null-payload',detected_on=200)
        assert store.save_setup(replacement)=='created'
        assert store.setup('null-payload')['id']=='null-payload'
    finally:
        store.close()


def test_safe_error_does_not_label_data_errors_as_connection_failures():
    assert Scanner.safe_error(TypeError('bad payload'))=='Ошибка обработки данных (TypeError)'


def test_quality_metadata_distinguishes_warmup_partial_stale_and_invalid():
    from backend.models import Candle
    now=1_800_000_000_000
    assert quality('ticker',now-100,now-80,now,max_age_ms=1000)['status']=='valid'
    assert quality('trades',now,now,now,coverage_ms=0,window_ms=60_000)['status']=='warming_up'
    assert quality('trades',now,now,now,coverage_ms=10_000,window_ms=60_000)['status']=='partial'
    assert quality('ticker',now-2000,now-1900,now,max_age_ms=1000)['status']=='stale'
    assert valid_ohlc(Candle(0,100,99,98,100,1))[0] is False
    assert valid_ohlc(Candle(0,100,101,99,100,1))[0] is True


def test_scanner_process_lock_rejects_second_owner_and_releases(tmp_path):
    database=tmp_path/'scanner.sqlite3'
    first=ScannerProcessLock(str(database)).acquire()
    try:
        with pytest.raises(RuntimeError,match='Another scanner'):
            ScannerProcessLock(str(database)).acquire()
    finally:
        first.release()
    second=ScannerProcessLock(str(database)).acquire()
    second.release()


def test_versioned_publisher_emits_ordered_deltas_and_removals():
    publisher=VersionedPublisher()
    initial=publisher.initial({'type':'snapshot','state':{},'signals':[
        {'id':'a','event_at':1}],'setups':[],'trades':[],'published_at':1})
    assert initial['revision']==0
    first=publisher.publish({'type':'snapshot','state':{},'signals':[
        {'id':'a','event_at':1}],'setups':[],'trades':[],'published_at':2})
    assert first['type']=='snapshot' and first['revision']==1
    delta=publisher.publish({'type':'snapshot','state':{},'signals':[
        {'id':'b','event_at':2}],'setups':[],'trades':[],'published_at':3})
    assert delta['type']=='delta' and delta['revision']==2
    assert delta['signals_upsert']==[{'id':'b','event_at':2}]
    assert delta['signals_remove']==['a']


def test_pattern_worker_survives_windows_process_spawn():
    candles=[Candle(i*300_000,100+i*.01,101+i*.01,99+i*.01,100+i*.01,10)
             for i in range(1000)]
    with ProcessPoolExecutor(max_workers=1) as pool:
        result=pool.submit(_detect_in_worker,'BTCUSDT','5',tuple(candles),
            'setup','early',True).result(timeout=30)
    assert isinstance(result,list)
