import asyncio
import threading
from concurrent.futures import CancelledError, ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from backend.app import create_app
from backend.engine import Engine
from backend.models import Candle, Signal, now_ms
from backend.service import Scanner
from backend.storage import Store


class FakeBybit:
    offset=0
    fee_mode='standard'


def scanner_with_signals():
    now=now_ms()
    clock=[now]
    scanner=Scanner(Engine(Store(':memory:')),FakeBybit())
    scanner.market_now=lambda:clock[0]
    scanner.assess_signal_entry=lambda signal,now=None:{
        'status':'late' if signal.id=='late' else 'available'}
    def signal(identifier,age,*,recovered=False,delivery=None):
        data=delivery if delivery is not None else {'recovered':recovered}
        return Signal('BTCUSDT','5','rectangle','long',now-age,now-age-300000,
            100,98,110,1,id=identifier,filter_results={'delivery':data})
    scanner.engine.persist([
        signal('recovered-new',1_000,recovered=True),
        signal('late',2_000),
        signal('current-new',3_000,delivery={'mode':'current','source':'rest',
            'timing_version':2,'received_at':now-2000,'detected_at':now-1000,
            'published_at':now-500,'publish_delay_ms':2500,'recovered':False}),
        signal('current-old',4_000),
        signal('old',301_000),
    ])
    return scanner,clock


def test_scope_is_applied_before_page_limit_and_legacy_default_is_all():
    scanner,_=scanner_with_signals()
    try:
        with TestClient(create_app(scanner)) as client:
            first=client.get('/api/signals?signal_scope=current&limit=1&view=summary').json()
            assert [row['id'] for row in first]==['current-new']
            cursor=first[-1]
            second=client.get('/api/signals',params={'signal_scope':'current','limit':1,
                'view':'summary','before_time':cursor['event_at'],'before_id':cursor['id']}).json()
            assert [row['id'] for row in second]==['current-old']
            history=client.get('/api/signals?signal_scope=history&limit=5').json()
            assert [row['id'] for row in history]==['recovered-new','late','old']
            assert len(client.get('/api/signals').json())==5
            assert client.get('/api/signals?signal_scope=invalid').status_code==400
            assert client.get('/api/snapshot?signal_scope=invalid').status_code==400
            timing=first[0]['filter_results']['delivery']
            assert timing['timing_version']==2 and timing['source']=='rest'
            assert timing['received_at'] and timing['publish_delay_ms']==2500
    finally:
        scanner.engine.store.close()
        scanner.detection_executor.shutdown(wait=True,cancel_futures=True)


def test_candle_api_uses_current_buffers_during_history_replay():
    scanner,_=scanner_with_signals()
    scanner.split_mode=True
    scanner.engine.store.put_candles('BTCUSDT','5',[Candle(300000,10,11,9,10)])
    scanner.buffers['BTCUSDT','5']=[Candle(600000,90,91,89,90)]
    scanner.live_buffers['BTCUSDT','5']=[Candle(600000,20,21,19,20),Candle(900000,30,31,29,30)]
    try:
        with TestClient(create_app(scanner)) as client:
            rows=client.get('/api/candles/BTCUSDT/5?limit=10').json()
            assert [(row['time'],row['close']) for row in rows]==[(300000,10),(600000,20),(900000,30)]
            rows=client.get('/api/candles/BTCUSDT/5?after=300000').json()
            assert [(row['time'],row['close']) for row in rows]==[(600000,20),(900000,30)]
    finally:
        scanner.engine.store.close()
        scanner.detection_executor.shutdown(wait=True,cancel_futures=True)


def test_current_scope_scans_past_full_history_pages_and_keeps_cursor_ties():
    now=now_ms()
    scanner=Scanner(Engine(Store(':memory:')),FakeBybit())
    scanner.market_now=lambda:now
    scanner.assess_signal_entry=lambda signal,now=None:{'status':'available'}
    def signal(identifier,event,recovered):
        return Signal('BTCUSDT','5','rectangle','long',event,event-300000,
            100,98,110,1,id=identifier,filter_results={'delivery':{'recovered':recovered}})
    rows=[signal(f'history-{index:03}',now-index-1,True) for index in range(120)]
    rows.extend([signal('current-a',now-5000,False),signal('current-b',now-5000,False)])
    scanner.engine.persist(rows)
    try:
        with TestClient(create_app(scanner)) as client:
            first=client.get('/api/signals?signal_scope=current&limit=1').json()
            assert [row['id'] for row in first]==['current-b']
            second=client.get('/api/signals',params={'signal_scope':'current','limit':1,
                'before_time':first[0]['event_at'],'before_id':first[0]['id']}).json()
            assert [row['id'] for row in second]==['current-a']
    finally:
        scanner.engine.store.close()
        scanner.detection_executor.shutdown(wait=True,cancel_futures=True)


def test_slow_snapshot_read_does_not_block_the_event_loop():
    scanner,_=scanner_with_signals()
    entered=threading.Event()
    release=threading.Event()
    original=scanner.snapshot_payload
    def slow_snapshot(*args,**kwargs):
        entered.set()
        assert release.wait(5)
        return original(*args,**kwargs)
    scanner.snapshot_payload=slow_snapshot
    try:
        with TestClient(create_app(scanner)) as client:
            with ThreadPoolExecutor(max_workers=2) as pool:
                snapshot=pool.submit(client.get,'/api/snapshot')
                try:
                    assert entered.wait(2)
                    state=pool.submit(client.get,'/api/state')
                    assert state.result(timeout=2).status_code==200
                finally:
                    release.set()
                assert snapshot.result(timeout=5).status_code==200
    finally:
        scanner.engine.store.close()
        scanner.detection_executor.shutdown(wait=True,cancel_futures=True)


@pytest.mark.parametrize('version',[2,3])
def test_websocket_snapshot_read_does_not_hide_concurrent_revision(version):
    scanner,_=scanner_with_signals()
    entered=threading.Event()
    release=threading.Event()
    original=scanner.snapshot_payload
    def slow_snapshot(*args,**kwargs):
        entered.set()
        assert release.wait(5)
        return original(*args,**kwargs)
    scanner.snapshot_payload=slow_snapshot
    try:
        with TestClient(create_app(scanner)) as client:
            def connect():
                with client.websocket_connect(f'/ws?v={version}&signal_scope=current',
                        headers={'origin':'http://localhost:8000'}) as ws:
                    return ws.receive_json()
            with ThreadPoolExecutor(max_workers=2) as pool:
                initial=pool.submit(connect)
                try:
                    assert entered.wait(2)
                    state=pool.submit(client.get,'/api/state')
                    assert state.result(timeout=2).status_code==200
                    publisher=(scanner.v2_publishers if version==2 else scanner.v3_publishers)['current']
                    client.portal.call(publisher.publish,{'signals':[],'setups':[],'trades':[]})
                    assert publisher.revision==1
                finally:
                    release.set()
                result=initial.result(timeout=5)
                assert result['stream_id']==publisher.stream_id
                assert result['revision']==0
    finally:
        scanner.engine.store.close()
        scanner.detection_executor.shutdown(wait=True,cancel_futures=True)


@pytest.mark.parametrize('version',[2,3])
def test_scoped_websocket_has_independent_revision_and_resync(version):
    scanner,_=scanner_with_signals()
    try:
        with TestClient(create_app(scanner)) as client:
            headers={'origin':'http://localhost:8000'}
            with client.websocket_connect(f'/ws?v={version}&signal_scope=current',headers=headers) as current:
                with client.websocket_connect(f'/ws?v={version}&signal_scope=history',headers=headers) as history:
                    a=current.receive_json();b=history.receive_json()
                    assert {row['id'] for row in a['signals']}=={'current-new','current-old'}
                    assert {row['id'] for row in b['signals']}=={'recovered-new','late','old'}
                    assert a['stream_id']!=b['stream_id']
                    current.send_json({'type':'resync'})
                    history.send_json({'type':'resync'})
                    ar=current.receive_json();br=history.receive_json()
                    assert ar['stream_id']==a['stream_id'] and ar['revision']==a['revision']
                    assert br['stream_id']==b['stream_id'] and br['revision']==b['revision']
                    assert {row['id'] for row in ar['signals']}=={'current-new','current-old'}
                    assert {row['id'] for row in br['signals']}=={'recovered-new','late','old'}
            with pytest.raises(WebSocketDisconnect):
                with client.websocket_connect(f'/ws?v={version}&signal_scope=invalid',headers=headers):pass
    finally:
        scanner.engine.store.close()
        scanner.detection_executor.shutdown(wait=True,cancel_futures=True)


def test_current_signal_moves_to_history_when_it_expires():
    scanner,clock=scanner_with_signals()
    try:
        with TestClient(create_app(scanner)) as client:
            broadcaster=client.portal.start_task_soon(scanner.broadcast_loop)
            try:
                headers={'origin':'http://localhost:8000'}
                with client.websocket_connect('/ws?v=3&signal_scope=current',headers=headers) as current:
                    with client.websocket_connect('/ws?v=3&signal_scope=history',headers=headers) as history:
                        assert 'current-new' in {row['id'] for row in current.receive_json()['signals']}
                        assert 'current-new' not in {row['id'] for row in history.receive_json()['signals']}
                        clock[0]+=301_000
                        client.portal.call(scanner.broadcast_event.set)
                        for _ in range(10):
                            update=current.receive_json()
                            if ('current-new' in update.get('signals_remove',[]) or
                                (update.get('type')=='snapshot' and
                                 'current-new' not in {row['id'] for row in update.get('signals',[])})):break
                        else:pytest.fail('Current feed did not remove expired signal')
                        for _ in range(10):
                            update=history.receive_json()
                            if 'current-new' in {row['id'] for row in update.get('signals_upsert',update.get('signals',[]))}:break
                        else:pytest.fail('History feed did not receive expired signal')
            finally:
                broadcaster.cancel()
                try:broadcaster.result(timeout=5)
                except CancelledError:pass
    finally:
        scanner.engine.store.close()
        scanner.detection_executor.shutdown(wait=True,cancel_futures=True)
