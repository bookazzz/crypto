from fastapi.testclient import TestClient
from backend.app import create_app
from backend.engine import Engine
from backend.storage import Store
from backend.service import Scanner
from backend.models import Signal, Candle


class FakeBybit:
    fee_mode='standard'


def test_api_controls_validation_and_no_secrets():
    scanner=Scanner(Engine(Store(':memory:')),FakeBybit())
    with TestClient(create_app(scanner)) as client:
        state=client.get('/api/state').json()
        assert state['mode']=='paper' and state['balance']==1000
        assert 'secret' not in str(state).lower()
        assert client.post('/api/pause').json()['paused']
        assert not client.post('/api/resume').json()['paused']
        assert client.put('/api/settings',json={'risk_fraction':.03}).status_code==422
        assert client.put('/api/settings',json={'risk_fraction':.01}).status_code==200
        assert client.get('/api/settings').json()['risk_fraction']==.01
        assert client.get('/api/signals').json()==[]
        outcome_report=client.get('/api/forecast-outcomes').json()
        assert outcome_report['version']=='forecast-outcomes-v1'
        assert outcome_report['candidate_count']==0
        assert set(outcome_report['ablation']['horizons'])=={'5','15','30'}
        assert len(client.get('/api/patterns').json())==16
        assert client.get('/api/candles/BTCUSDT/5').status_code==200
        assert client.get('/api/candles/BTCUSDT/30').status_code==400
        assert set(client.get('/api/portfolio-backtest').json())=={'progress','report'}
        assert client.get('/api/portfolio-backtest/file/manifest.json').status_code==404
        assert client.post('/api/backtest',json={'symbol':'BTCUSDT','timeframe':'5','days':7}).status_code==400
        assert client.get('/api/state',headers={'Origin':'https://evil.example'}).status_code==403
        assert client.get('/api/state',headers={'Host':'evil.example'}).status_code==403
        assert client.post('/api/backtest',json={'symbol':'BTCUSDT','timeframe':'15','days':7}).status_code==400


def test_candle_range_query_keeps_legacy_response_and_supports_after():
    store=Store(':memory:');scanner=Scanner(Engine(store),FakeBybit())
    store.put_candles('BTCUSDT','5',[Candle(t,1,2,.5,1.5) for t in (0,300000,600000,900000)])
    with TestClient(create_app(scanner)) as client:
        assert [c['time'] for c in client.get('/api/candles/BTCUSDT/5?start=300000&end=900000').json()]==[300000,600000]
        assert [c['time'] for c in client.get('/api/candles/BTCUSDT/5?after=600000').json()]==[900000]
        assert len(client.get('/api/candles/BTCUSDT/5').json())==4
        assert client.get('/api/candles/BTCUSDT/5?after=1&start=2').status_code==400


def test_candle_api_returns_latest_rows_when_live_buffer_exceeds_limit():
    scanner=Scanner(Engine(Store(':memory:')),FakeBybit())
    scanner.buffers[('BTCUSDT','5')]=[
        Candle(t,1,2,.5,1.5) for t in range(0,350*300_000,300_000)]
    with TestClient(create_app(scanner)) as client:
        rows=client.get('/api/candles/BTCUSDT/5?limit=300').json()
        assert len(rows)==300
        assert rows[0]['time']==50*300_000
        assert rows[-1]['time']==349*300_000


def test_candle_range_reads_persistent_store_with_secondary_connection(tmp_path):
    path=tmp_path/'candles.sqlite3';store=Store(str(path))
    store.put_candles('ETHUSDT','15',[Candle(t,1,2,.5,1.5) for t in (0,900000,1800000)])
    rows=store.candles_range('ETHUSDT','15',start=900000,end=1800000)
    assert [row.time for row in rows]==[900000]
    store.close()


def test_signal_context_returns_immutable_snapshot_and_current_view():
    scanner=Scanner(Engine(Store(':memory:')),FakeBybit())
    signal=Signal('BTCUSDT','5','ascending_triangle','long',1_000,0,100,98,110,1,
        filter_results={'decision_snapshot':{'as_of':900,'scenario':'manual_review'}})
    scanner.engine.persist([signal])
    with TestClient(create_app(scanner)) as client:
        response=client.get(f'/api/signals/{signal.id}/context')
        assert response.status_code==200
        assert response.json()['snapshot']['scenario']=='manual_review'
        assert response.json()['current']['signal_id']==signal.id
        statistics=response.json()['current']['compatible_statistics']
        assert statistics['minimum_sample_size']==100
        assert set(statistics['horizons'])=={'5','15','30'}
        assert client.get('/api/signals/missing/context').status_code==404


def test_signal_feed_includes_unpersisted_queue_and_context():
    scanner=Scanner(Engine(Store(':memory:')),FakeBybit())
    signal=Signal('BTCUSDT','5','ascending_triangle','long',1_000,0,100,98,110,1,
        filter_results={'decision_snapshot':{'as_of':900,'scenario':'queued'}})
    scanner.pending.append(signal)
    with TestClient(create_app(scanner)) as client:
        rows=client.get('/api/signals').json()
        assert [row['id'] for row in rows]==[signal.id]
        context=client.get(f'/api/signals/{signal.id}/context')
        assert context.status_code==200
        assert context.json()['snapshot']['scenario']=='queued'
        scanner.engine.persist([signal])
        assert [row['id'] for row in client.get('/api/signals').json()]==[signal.id]


def test_open_signal_pair_in_chrome(monkeypatch,tmp_path):
    import backend.app as app_module
    scanner=Scanner(Engine(Store(':memory:')),FakeBybit())
    scanner.symbols=['BTCUSDT']
    chrome=tmp_path/'chrome.exe'
    chrome.touch()
    calls=[]
    monkeypatch.setattr(app_module,'find_chrome',lambda:chrome)
    monkeypatch.setattr(app_module.subprocess,'Popen',lambda args,**kwargs:calls.append((args,kwargs)))
    with TestClient(create_app(scanner)) as client:
        response=client.post('/api/open-bybit/BTCUSDT')
        assert response.status_code==200
        assert response.json()=={'opened':True,'symbol':'BTCUSDT'}
        assert calls[0][0]==[str(chrome),'--new-tab','https://www.bybit.com/trade/usdt/BTCUSDT']
        assert client.post('/api/open-bybit/other.example').status_code==400


def test_early_setup_api_and_immutable_geometry():
    scanner=Scanner(Engine(Store(':memory:')),FakeBybit())
    setup={'id':'setup123','symbol':'BTCUSDT','timeframe':'15','pattern':'ascending_triangle',
        'side':'long','detected_at':1000,'updated_at':1000,'status':'wait_breakout','reason':'',
        'transitions':[],'snapshot':{'start':1,'detected_on_bar':900,'boundary':100,'stop':95,
            'target':110,'atr':2,'height':10,'lines':[],'pivots':[]}}
    scanner.engine.store.save_setup(setup)
    changed=setup|{'status':'near_boundary','updated_at':2000,'snapshot':setup['snapshot']|{'boundary':200}}
    scanner.engine.store.save_setup(changed)
    with TestClient(create_app(scanner)) as client:
        items=client.get('/api/setups').json()
        assert len(items)==1 and items[0]['snapshot']['boundary']==100
        assert items[0]['current']['scalp_plan']['status']=='quote_stale'
        assert items[0]['scalp_status']=='quote_stale'
        context=client.get('/api/setups/setup123/context')
        assert context.status_code==200
        assert context.json()['current']['projected_rr_gross']==2
        assert context.json()['current']['quote_stale'] is True
        assert context.json()['current']['scalp_plan']['status']=='quote_stale'
        assert 'setup123' in scanner.requested_setup_contexts
        assert 'BTCUSDT' in scanner.selected_setup_symbols()
        assert client.get('/api/setups/missing/context').status_code==404


def test_snapshot_and_event_feed_keep_stable_cursor_order_with_filter_before_limit():
    scanner=Scanner(Engine(Store(':memory:')),FakeBybit())
    rows=[]
    for signal_id,status in (('a','found'),('b','rejected'),('c','found')):
        signal=Signal('BTCUSDT','5','ascending_triangle','long',100,0,100,98,110,2,
            id=signal_id,status=status)
        signal.event_at=100
        rows.append(signal.dict())
    scanner.engine.store.save({},rows)
    with TestClient(create_app(scanner)) as client:
        snapshot=client.get('/api/snapshot').json()
        assert snapshot['type']=='snapshot'
        assert [row['id'] for row in snapshot['signals']]==['c','b','a']
        assert [row['id'] for row in client.get('/api/signals?limit=1').json()]==['c']
        assert [row['id'] for row in client.get(
            '/api/signals?limit=1&before_time=100&before_id=c').json()]==['b']
        assert [row['id'] for row in client.get(
            '/api/signals?limit=1&status=found').json()]==['c']


def test_snapshot_starts_with_one_page_per_feed():
    scanner=Scanner(Engine(Store(':memory:')),FakeBybit())
    signals=[Signal('BTCUSDT','5','ascending_triangle','long',1000+i,0,100,98,110,2,
        id=f'signal-{i:03}',event_at=1000+i) for i in range(60)]
    scanner.engine.persist(signals)
    setups=[]
    for i in range(60):
        detected=1000+i
        setups.append({'id':f'setup-{i:03}','symbol':'BTCUSDT','timeframe':'5',
            'pattern':'ascending_triangle','side':'long','detected_at':detected,
            'updated_at':detected,'event_at':detected,'status':'wait_breakout',
            'reason':'','transitions':[],'snapshot':{'start':0,
                'detected_on_bar':detected,'boundary':100,'stop':98,'target':110,
                'atr':2,'height':10,'lines':[],'pivots':[]}})
    scanner.engine.store.save_setups(setups)

    with TestClient(create_app(scanner)) as client:
        snapshot=client.get('/api/snapshot').json()
        assert len(snapshot['signals'])==50
        assert len(snapshot['setups'])==50
        assert snapshot['signals'][0]['id']=='signal-059'
        assert snapshot['setups'][0]['id']=='setup-059'


def test_recovery_diagnostics_in_state_summary_and_websocket():
    scanner=Scanner(Engine(Store(':memory:')),FakeBybit())
    scanner.phase='waiting_network'
    scanner.candle_recovery_status='waiting_network'
    scanner.candle_recovery_pending=1
    scanner.next_candle_recovery=123456
    expected={'status':'waiting_network','pending_streams':1,'next_retry_at':123456}
    with TestClient(create_app(scanner)) as client:
        assert client.get('/api/state').json()['scanner_diagnostics']['candle_recovery']==expected
        assert client.get('/api/snapshot?view=summary').json()['state']['scanner_diagnostics']['candle_recovery']==expected
        with client.websocket_connect('/ws?v=3',headers={'origin':'http://localhost:8000'}) as ws:
            assert ws.receive_json()['state']['scanner_diagnostics']['candle_recovery']==expected


def test_versioned_websocket_sends_initial_snapshot_and_resync():
    scanner=Scanner(Engine(Store(':memory:')),FakeBybit())
    with TestClient(create_app(scanner)) as client:
        with client.websocket_connect('/ws?v=2',headers={'origin':'http://localhost:8000'}) as ws:
            initial=ws.receive_json()
            assert initial['type']=='snapshot'
            assert initial['revision']==0 and initial['stream_id']
            ws.send_json({'type':'resync'})
            refreshed=ws.receive_json()
            assert refreshed['type']=='snapshot'
            assert refreshed['stream_id']==initial['stream_id']
            assert refreshed['revision']==initial['revision']


def test_v3_websocket_uses_summary_view_and_acknowledges_render():
    scanner=Scanner(Engine(Store(':memory:')),FakeBybit())
    with TestClient(create_app(scanner)) as client:
        with client.websocket_connect('/ws?v=3',headers={'origin':'http://localhost:8000'}) as ws:
            initial=ws.receive_json()
            assert initial['type']=='snapshot' and initial['view']=='summary'
            assert 'revision' in initial and 'stream_id' in initial
            ws.send_json({'type':'render_ack','client_render_ms':12})
            # An acknowledgement is telemetry only; the server remains responsive.
            assert client.get('/api/state').status_code==200


def test_summary_snapshot_and_v3_websocket_keep_feed_compact_and_context_full():
    scanner=Scanner(Engine(Store(':memory:')),FakeBybit())
    signal=Signal('BTCUSDT','15','ascending_triangle','long',1000,0,100,98,110,2,
        lines=[{'label':'resistance','points':[{'time':i,'value':100+i/100} for i in range(100)]}],
        pivots=[{'time':i,'price':100+i/100} for i in range(100)],
        filter_results={'decision_snapshot':{'geometry':[[i,100,101,99,100] for i in range(100)]},
                        'volume_pass':True})
    scanner.engine.persist([signal])
    with TestClient(create_app(scanner)) as client:
        full=client.get('/api/snapshot').json()
        summary=client.get('/api/snapshot?view=summary').json()
        assert full['signals'][0]['lines'] and full['signals'][0]['filter_results']['decision_snapshot']
        assert summary['view']=='summary'
        assert summary['signals'][0]['lines']==[] and summary['signals'][0]['pivots']==[]
        assert 'decision_snapshot' not in summary['signals'][0]['filter_results']
        assert client.get('/api/snapshot?view=unknown').status_code==400
        context=client.get(f'/api/signals/{signal.id}/context').json()
        assert context['signal']['lines'] and context['signal']['filter_results']['decision_snapshot']
        with client.websocket_connect('/ws?v=3',headers={'origin':'http://localhost:8000'}) as ws:
            initial=ws.receive_json()
            assert initial['view']=='summary'
            assert initial['signals'][0]['lines']==[]
