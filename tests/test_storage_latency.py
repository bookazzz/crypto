import asyncio
import sqlite3
import threading

import pytest

from backend.models import Candle
from backend.storage import Store
from backend.write_queue import SQLiteWriteQueue


def test_version_six_backup_seeds_live_cursor_without_advancing_history(tmp_path):
    path=tmp_path/'state.sqlite3'
    initial=Store(str(path))
    initial.close()
    with sqlite3.connect(path) as db:
        db.executescript('''
            DROP TABLE live_scan_cursors;
            DROP INDEX setups_symbol_tf_event_id;
            DROP INDEX setups_active_detected;
            INSERT INTO scanner_cursors VALUES('BTCUSDT','5',600000);
            INSERT INTO scan_cursors VALUES('BTCUSDT','5',300000);
            PRAGMA user_version=5;
        ''')
    store=Store(str(path))
    try:
        assert store.migration_backup
        with sqlite3.connect(store.migration_backup) as backup:
            assert backup.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
            assert backup.execute('PRAGMA user_version').fetchone()[0]==5
            assert backup.execute("SELECT count(*) FROM sqlite_master WHERE name='live_scan_cursors'").fetchone()[0]==0
        assert store.db.execute('PRAGMA user_version').fetchone()[0]==7
        assert store.live_scan_cursor('BTCUSDT','5')==300000
        asyncio.run(store.aset_live_scan_cursor('BTCUSDT','5',900000))
        asyncio.run(store.aset_live_scan_cursor('BTCUSDT','5',600000))
        assert store.live_scan_cursor('BTCUSDT','5')==900000
        assert store.cursor('BTCUSDT','5')==600000
        assert store.scan_cursor('BTCUSDT','5')==300000
        indexes={row[1] for row in store.db.execute('PRAGMA index_list(setups)')}
        assert {'setups_symbol_tf_event_id','setups_active_detected'}<=indexes
    finally:store.close()
    reopened=Store(str(path))
    try:
        assert reopened.migration_backup is None
        assert reopened.live_scan_cursor('BTCUSDT','5')==900000
        assert reopened.scan_cursor('BTCUSDT','5')==300000
    finally:reopened.close()


def test_live_candles_and_historical_cursor_remain_independent_after_restart(tmp_path):
    path=tmp_path/'history.sqlite3'
    store=Store(str(path))
    start=1_800_000_000_000
    step=300_000
    try:
        store.set_cursor('BTCUSDT','5',start)
        asyncio.run(store.aset_scan_cursor('BTCUSDT','5',start))
        # The window is ahead of the historical scan, as after a long outage.
        bars=[Candle(start+i*step,100,101,99,100) for i in range(1,1601)]
        asyncio.run(store.aput_candles('BTCUSDT','5',bars))
        asyncio.run(store.aset_live_scan_cursor('BTCUSDT','5',bars[-1].time))
        assert store.cursor('BTCUSDT','5')==start
        assert store.scan_cursor('BTCUSDT','5')==start
        assert store.db.execute('SELECT count(*) FROM candles').fetchone()[0]==1600
    finally:store.close()
    reopened=Store(str(path))
    try:
        assert reopened.live_scan_cursor('BTCUSDT','5')==bars[-1].time
        assert reopened.scan_cursor('BTCUSDT','5')==start
        restored=Candle(start+step,100,101,99,100)
        asyncio.run(reopened.acommit_candle_transition('BTCUSDT','5',restored,[],restored.time,
                                                        scan_floor=start,priority=1))
        assert reopened.cursor('BTCUSDT','5')==restored.time
        assert reopened.scan_cursor('BTCUSDT','5')==start
        assert [x.time for x in reopened.candles_range('BTCUSDT','5',start=restored.time,
                                                        end=restored.time+step)]==[restored.time]
    finally:reopened.close()


def test_historical_results_do_not_overwrite_live_state_or_active_figures():
    store=Store(':memory:')
    live_signal={'id':'same','time':300000,'event_at':300000,'status':'found'}
    live_setup={'id':'same-setup','symbol':'BTCUSDT','timeframe':'5','detected_at':1,
                'event_at':1,'status':'wait_breakout','snapshot':{},'transitions':[]}
    archive_signal={'id':'old','time':100000,'event_at':100000,'status':'found'}
    archive_setup={'id':'old-setup','symbol':'BTCUSDT','timeframe':'5','detected_at':1,
                   'event_at':1,'status':'expired','snapshot':{},'transitions':[]}
    try:
        asyncio.run(store.asave({'balance':123},[live_signal]))
        asyncio.run(store.asave_setups([live_setup]))
        inserted=asyncio.run(store.asave_historical_signals([
            live_signal|{'time':1,'status':'rejected'},archive_signal]))
        assert inserted==['old']
        inserted=asyncio.run(store.asave_historical_setups([
            live_setup|{'status':'expired'},archive_setup]))
        assert inserted==['old-setup']
        assert store.load()=={'balance':123}
        assert store.signal('same')==live_signal
        assert store.setup('same-setup')['status']=='wait_breakout'
        assert [row['id'] for row in store.active_setups()]==['same-setup']
        with pytest.raises(ValueError):
            asyncio.run(store.asave_historical_setups([live_setup]))
    finally:store.close()


def test_delayed_setup_updates_cannot_rewind_or_reactivate_terminal_state():
    store=Store(':memory:')
    base={'id':'setup','symbol':'BTCUSDT','timeframe':'5','detected_at':1,'event_at':1,
          'status':'near_boundary','snapshot':{},'transitions':[],
          'last_closed_candle':300,'updated_at':300}
    try:
        assert store.save_setups([base])==['created']
        stale=base|{'status':'expired','last_closed_candle':200,'updated_at':200}
        assert store.save_setups([stale])==[False]
        assert store.setup('setup')['status']=='near_boundary'
        terminal=base|{'status':'target_reached','last_closed_candle':400,'updated_at':400}
        assert store.save_setups([terminal])==['status']
        assert store.save_setups([base|{'last_closed_candle':500}])==[False]
        assert store.setup('setup')['status']=='target_reached'
        assert store.setup('setup')['last_closed_candle']==400
    finally:store.close()


def test_delivery_batch_merges_fresh_payload_without_overwriting_history():
    store=Store(':memory:')
    signal={'id':'s','time':1,'event_at':1,'status':'found','filter_results':{
        'delivery':{'mode':'current','source':'websocket','received_at':10},'other':True}}
    setup={'id':'u','symbol':'BTCUSDT','timeframe':'5','detected_at':20,'event_at':1,
           'status':'wait_breakout','snapshot':{'boundary':100},'transitions':[],
           'delivery':{'mode':'current','source':'websocket','received_at':10}}
    async def check():
        await store.asave({'balance':123},[signal])
        await store.asave_setups([setup])
        await store.aupdate_signal_snapshot('s',{'current':'kept'})
        changed=await store.aupdate_signal_deliveries([
            {'id':'missing','published_at':30},
            {'id':'s','received_at':10,'detected_at':20,'published_at':30,
             'delivery':{'timing_version':2,'published_at':30}}])
        assert changed==['s']
        changed=await store.aupdate_setup_deliveries([
            {'id':'u','received_at':10,'detected_at':20,'published_at':30,
             'delivery':{'timing_version':2,'published_at':30}}])
        assert changed==['u']
        loaded=store.signal('s')
        assert loaded['filter_results']['decision_snapshot']=={'current':'kept'}
        assert loaded['filter_results']['other'] is True
        assert loaded['filter_results']['delivery']=={'mode':'current','source':'websocket',
            'received_at':10,'timing_version':2,'published_at':30}
        loaded_setup=store.setup('u')
        assert loaded_setup['status']=='wait_breakout' and loaded_setup['snapshot']=={'boundary':100}
        assert loaded_setup['delivery']['mode']=='current' and loaded_setup['published_at']==30
        assert await store.aupdate_signal_deliveries([{'id':'s','published_at':99,
            'delivery':{'mode':'history'}}],priority=1)==[]
        assert await store.aupdate_setup_deliveries([{'id':'u','published_at':99,
            'delivery':{'mode':'history'}}],priority=1)==[]
        assert store.signal('s')['published_at']==30
        assert store.setup('u')['published_at']==30
    try:asyncio.run(check())
    finally:store.close()


def test_invalid_delivery_rolls_back_all_signal_updates(tmp_path):
    store=Store(str(tmp_path/'signals.sqlite3'))
    rows=[{'id':'a','time':1,'status':'found'},{'id':'b','time':2,'status':'found'}]
    async def check():
        await store.asave({},rows)
        with pytest.raises(ValueError):
            await store.aupdate_signal_deliveries([
                {'id':'a','published_at':30,'delivery':{'source':'rest'}},
                {'id':'b','published_at':30,'delivery':'invalid'}])
        assert 'published_at' not in store.signal('a')
        assert 'published_at' not in store.signal('b')
    try:asyncio.run(check())
    finally:store.close()


def test_restart_does_not_seed_absent_history_cursor_from_newer_candle_cursor(tmp_path):
    path=tmp_path/'state.sqlite3'
    store=Store(str(path))
    store.set_cursor('BTCUSDT','5',900000)
    store.close()
    reopened=Store(str(path))
    try:
        assert reopened.cursor('BTCUSDT','5')==900000
        assert reopened.scan_cursor('BTCUSDT','5') is None
        assert reopened.live_scan_cursor('BTCUSDT','5') is None
    finally:reopened.close()


@pytest.mark.parametrize('memory',[True,False])
def test_async_reads_return_consistent_results_for_memory_and_file(tmp_path,memory):
    store=Store(':memory:' if memory else str(tmp_path/'reads.sqlite3'))
    active={'id':'a','symbol':'BTCUSDT','timeframe':'5','detected_at':1,
            'event_at':1,'status':'wait_breakout','snapshot':{},'transitions':[]}
    archived=active|{'id':'b','detected_at':2,'event_at':2,'status':'expired'}
    async def check():
        await store.asave({},[{'id':'signal','time':1,'status':'found'},
                              {'id':'pending','time':2,'status':'pending_retest'}])
        await store.asave_setups([active,archived])
        await store.aset_cursor('BTCUSDT','5',100)
        await store.aset_scan_cursor('BTCUSDT','5',50)
        await store.aset_live_scan_cursor('BTCUSDT','5',200)
        existing,signals,found,current,recent,cursor,scan,live=await asyncio.gather(
            store.aexisting_signal_ids(['signal','pending','absent']),
            store.afetch_signals(['signal','pending','absent']),
            store.afetch_setups(['a','b','absent']),store.aactive_setups('BTCUSDT','5'),
            store.arecent_setups('BTCUSDT','5'),store.aget_cursor('BTCUSDT','5'),
            store.aget_scan_cursor('BTCUSDT','5'),store.aget_live_scan_cursor('BTCUSDT','5'))
        assert existing=={'signal'}
        assert set(signals)=={'signal','pending'} and signals['pending']['status']=='pending_retest'
        assert set(found)=={'a','b'}
        assert [row['id'] for row in current]==['a']
        assert [row['id'] for row in recent]==['b','a']
        assert (cursor,scan,live)==(100,50,200)
    try:asyncio.run(check())
    finally:store.close()


def test_signal_time_bound_and_cursor_filter_before_limit(tmp_path):
    store=Store(str(tmp_path/'signals.sqlite3'))
    signals=[{'id':'a','time':100,'event_at':100,'status':'found'},
             {'id':'b','time':200,'event_at':200,'status':'found'},
             {'id':'c','time':300,'event_at':300,'status':'found'},
             {'id':'d','time':300,'event_at':300,'status':'rejected'}]
    async def check():
        await store.asave({},signals)
        current=await store.asignals(limit=2,after_time=200,statuses=['found'])
        assert [row['id'] for row in current]==['c','b']
        page=await store.asignals(limit=10,before_time=300,before_id='c',after_time=200)
        assert [row['id'] for row in page]==['b']
        assert [row['id'] for row in store.signals(after_time=300)]==['d','c']
    try:asyncio.run(check())
    finally:store.close()


def test_candle_range_releases_read_connection_before_database_close(tmp_path):
    path=tmp_path/'candles.sqlite3'
    store=Store(str(path))
    try:
        store.put_candles('BTCUSDT','5',[Candle(300000,100,101,99,100)])
        for _ in range(3):
            assert [row.time for row in store.candles_range('BTCUSDT','5')]==[300000]
    finally:store.close()
    # On Windows this also verifies no read connection still holds the file.
    moved=tmp_path/'moved.sqlite3'
    path.rename(moved)
    assert moved.is_file()


@pytest.mark.parametrize('order',['history_first','current_first','concurrent'])
def test_same_setup_event_preserves_live_observation_regardless_of_arrival_order(tmp_path,order):
    store=Store(str(tmp_path/'setups.sqlite3'))
    base={'id':'shared','symbol':'BTCUSDT','timeframe':'5','event_at':300000,
          'detected_at':300100,'status':'expired','snapshot':{'boundary':100},'transitions':[],
          'recovered':True,'delivery':{'mode':'history','timing_version':2}}
    current=base|{'status':'near_boundary','detected_at':300200,'received_at':300150,
                  'published_at':300300,'recovered':False,'snapshot':{'boundary':101},
                  'delivery':{'mode':'current','timing_version':2,'source':'websocket'}}
    async def check():
        if order=='history_first':
            assert await store.asave_historical_setups([base])==['shared']
            assert await store.asave_current_setups([current])==['status']
        elif order=='current_first':
            assert await store.asave_current_setups([current])==['created']
            assert await store.asave_historical_setups([base])==[]
        else:
            await asyncio.gather(store.asave_historical_setups([base]),
                                 store.asave_current_setups([current]))
        saved=store.setup('shared')
        assert saved['status']=='near_boundary'
        assert saved['snapshot']=={'boundary':101}
        assert saved['detected_at']==300200 and saved['received_at']==300150
        assert saved['published_at']==300300 and saved['delivery']['mode']=='current'
        assert [row['id'] for row in store.active_setups()]==['shared']
        assert len(store.setups())==1
    try:asyncio.run(check())
    finally:store.close()


def test_current_setup_does_not_promote_different_history_event_or_rewind_live():
    store=Store(':memory:')
    history={'id':'shared','symbol':'BTCUSDT','timeframe':'5','event_at':300000,
             'detected_at':300100,'status':'expired','snapshot':{'boundary':100},'transitions':[],
             'recovered':True,'delivery':{'mode':'history'}}
    other_event=history|{'event_at':600000,'status':'wait_breakout','recovered':False,
                         'snapshot':{'boundary':101},'delivery':{'mode':'current'}}
    terminal=other_event|{'id':'live','event_at':300000,'status':'cancelled',
                          'last_closed_candle':600000}
    async def check():
        await store.asave_historical_setups([history])
        assert await store.asave_current_setups([other_event])==[False]
        assert store.setup('shared')['delivery']['mode']=='history'
        assert await store.asave_current_setups([terminal])==['created']
        stale=terminal|{'status':'wait_breakout','last_closed_candle':900000}
        assert await store.asave_current_setups([stale])==[False]
        assert store.setup('live')['status']=='cancelled'
    try:asyncio.run(check())
    finally:store.close()


def test_legacy_recovered_same_event_can_be_promoted_to_current():
    store=Store(':memory:')
    history={'id':'old','symbol':'BTCUSDT','timeframe':'5','event_at':300000,
             'detected_at':300100,'status':'expired','snapshot':{},'transitions':[],
             'recovered':True}
    current=history|{'detected_at':300200,'status':'wait_breakout',
                     'recovered':False,'delivery':{'mode':'current'}}
    async def check():
        await store.asave_historical_setups([history])
        assert await store.asave_current_setups([current])==['status']
        assert store.setup('old')['delivery']['mode']=='current'
    try:asyncio.run(check())
    finally:store.close()


def test_same_event_promotion_does_not_rewind_processed_historical_candle():
    store=Store(':memory:')
    history={'id':'old','symbol':'BTCUSDT','timeframe':'5','event_at':300000,
             'detected_at':300100,'status':'expired','snapshot':{},'transitions':[],
             'last_closed_candle':600000,'recovered':True,'delivery':{'mode':'history'}}
    current=history|{'detected_at':300200,'status':'wait_breakout','last_closed_candle':300000,
                     'recovered':False,'delivery':{'mode':'current'}}
    async def check():
        await store.asave_historical_setups([history])
        assert await store.asave_current_setups([current])==[False]
        assert store.setup('old')['last_closed_candle']==600000
        assert store.setup('old')['delivery']['mode']=='history'
    try:asyncio.run(check())
    finally:store.close()


def test_historical_setup_states_survive_restart_without_mutating_active_setup(tmp_path):
    path=tmp_path/'history.sqlite3'
    store=Store(str(path))
    history={'id':'shared','symbol':'BTCUSDT','timeframe':'5','event_at':300000,
             'detected_at':300100,'status':'wait_breakout','snapshot':{'boundary':100},
             'transitions':[],'recovered':True,'delivery':{'mode':'history'}}
    live=history|{'status':'near_boundary','snapshot':{'boundary':101},'recovered':False,
                  'delivery':{'mode':'current'}}
    async def initial():
        assert await store.asave_current_setups([live])==['created']
        assert await store.asave_history_setup_states([history])==['shared']
    try:asyncio.run(initial())
    finally:store.close()
    reopened=Store(str(path))
    async def check():
        states=await reopened.ahistory_setups('BTCUSDT','5')
        assert len(states)==1 and states[0]['snapshot']=={'boundary':100}
        assert reopened.setup('shared')['snapshot']=={'boundary':101}
        state=states[0]|{'status':'target_reached','last_closed_candle':600000,
                         'updated_at':900000,'reason':'target'}
        assert await reopened.asave_history_setup_states([state])==['shared']
        stale=state|{'status':'wait_breakout','last_closed_candle':300000}
        assert await reopened.asave_history_setup_states([stale])==[]
        assert (await reopened.ahistory_setups('BTCUSDT','5'))[0]['status']=='target_reached'
        assert await reopened.apublish_historical_setups([state])==[]
        assert reopened.setup('shared')['status']=='near_boundary'
        assert (await reopened.ahistory_setups('ETHUSDT','5'))==[]
    try:asyncio.run(check())
    finally:reopened.close()


def test_only_terminal_history_is_published_and_live_rows_are_protected():
    store=Store(':memory:')
    base={'id':'archive','symbol':'BTCUSDT','timeframe':'5','event_at':300000,
          'detected_at':300100,'status':'expired','snapshot':{'boundary':100},'transitions':[],
          'last_closed_candle':600000,'recovered':True,'delivery':{'mode':'history'}}
    live=base|{'id':'live','status':'near_boundary','recovered':False,
               'delivery':{'mode':'current'}}
    async def check():
        await store.asave_historical_setups([base])
        await store.asave_current_setups([live])
        updated=base|{'status':'target_reached','last_closed_candle':900000,
                      'updated_at':1200000,'reason':'target'}
        assert await store.apublish_historical_setups([updated])==['archive']
        assert store.setup('archive')['status']=='target_reached'
        assert store.setup('archive')['snapshot']=={'boundary':100}
        older=updated|{'status':'cancelled','last_closed_candle':300000}
        assert await store.apublish_historical_setups([older])==[]
        assert await store.apublish_historical_setups([updated|{'id':'live'}])==[]
        assert store.setup('live')['status']=='near_boundary'
        fresh=updated|{'id':'new'}
        assert await store.apublish_historical_setups([fresh])==['new']
        assert store.setup('new')['status']=='target_reached'
        with pytest.raises(ValueError):
            await store.apublish_historical_setups([base|{'status':'wait_breakout'}])
        with pytest.raises(ValueError):
            await store.apublish_historical_setups([base|{'delivery':{'mode':'current'}}])
    try:asyncio.run(check())
    finally:store.close()


def test_writer_prioritizes_current_work_and_drains_before_close(tmp_path):
    path=tmp_path/'writer.sqlite3'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE writes (value TEXT)')
    writer=SQLiteWriteQueue(str(path),batch_window=.01,max_batch_size=4)
    entered=threading.Event();release=threading.Event()
    def blocking(db):
        entered.set()
        if not release.wait(5):raise RuntimeError('timed out')
        db.execute('INSERT INTO writes VALUES(?)',('first',))
    def op(value):
        return lambda db:db.execute('INSERT INTO writes VALUES(?)',(value,)).rowcount
    first=writer.submit(blocking)
    try:
        assert entered.wait(5)
        old_a=writer.submit(op('old-a'),priority=1)
        old_b=writer.submit(op('old-b'),priority=1)
        live_a=writer.submit(op('live-a'))
        live_b=writer.submit(op('live-b'))
        release.set()
        for future in (first,old_a,old_b,live_a,live_b):future.result(timeout=5)
        writer.close()
        with sqlite3.connect(path) as db:
            assert [row[0] for row in db.execute('SELECT value FROM writes ORDER BY rowid')]==[
                'first','live-a','live-b','old-a','old-b']
        with pytest.raises(RuntimeError):writer.submit(op('too late'))
    finally:
        release.set()
        writer.close()


def test_writer_preserves_other_operations_when_one_fails(tmp_path):
    path=tmp_path/'writer.sqlite3'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE writes (value TEXT UNIQUE)')
    writer=SQLiteWriteQueue(str(path),batch_window=.02)
    try:
        first=writer.submit(lambda db:db.execute('INSERT INTO writes VALUES(?)',('same',)).rowcount)
        duplicate=writer.submit(lambda db:db.execute('INSERT INTO writes VALUES(?)',('same',)).rowcount)
        third=writer.submit(lambda db:db.execute('INSERT INTO writes VALUES(?)',('next',)).rowcount)
        assert first.result(timeout=5)==1
        with pytest.raises(sqlite3.IntegrityError):duplicate.result(timeout=5)
        assert third.result(timeout=5)==1
    finally:writer.close()
    with sqlite3.connect(path) as db:
        assert [row[0] for row in db.execute('SELECT value FROM writes ORDER BY rowid')]==['same','next']


def test_cancelling_awaiter_does_not_abort_durable_write_or_writer(tmp_path):
    path=tmp_path/'writer.sqlite3'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE writes (value TEXT)')
    writer=SQLiteWriteQueue(str(path))
    entered=threading.Event();release=threading.Event()
    def block(db):
        entered.set()
        if not release.wait(5):raise RuntimeError('timed out')
    try:
        first=writer.submit(block)
        assert entered.wait(5)
        cancelled=writer.submit(lambda db:db.execute('INSERT INTO writes VALUES(?)',('committed',)))
        assert cancelled.cancel()
        release.set()
        first.result(timeout=5)
        later=writer.submit(lambda db:db.execute('INSERT INTO writes VALUES(?)',('later',)).rowcount)
        assert later.result(timeout=5)==1
    finally:
        release.set()
        writer.close()
    with sqlite3.connect(path) as db:
        assert [r[0] for r in db.execute('SELECT value FROM writes ORDER BY rowid')]==['committed','later']


def test_writer_close_drains_pending_low_priority_batches(tmp_path):
    path=tmp_path/'writer.sqlite3'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE writes (value INTEGER)')
    writer=SQLiteWriteQueue(str(path),max_batch_size=4)
    entered=threading.Event();release=threading.Event()
    def block(db):
        entered.set()
        if not release.wait(5):raise RuntimeError('timed out')
    try:
        writer.submit(block)
        assert entered.wait(5)
        futures=[writer.submit(lambda db,n=n:db.execute('INSERT INTO writes VALUES(?)',(n,)).rowcount,
                               priority=1) for n in range(19)]
        closed=threading.Event();errors=[]
        def close_writer():
            try:writer.close()
            except Exception as exc:errors.append(exc)
            finally:closed.set()
        closer=threading.Thread(target=close_writer)
        closer.start()
        release.set()
        assert closed.wait(5)
        closer.join()
        assert not errors
        assert all(f.result(timeout=1)==1 for f in futures)
    finally:
        release.set()
        writer.close()
    with sqlite3.connect(path) as db:
        assert [row[0] for row in db.execute('SELECT value FROM writes ORDER BY rowid')]==list(range(19))
