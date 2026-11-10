import asyncio
import httpx

from backend.account_monitor import BybitAccountMonitor
from backend.app import create_app
from backend.engine import Engine
from backend.service import Scanner
from backend.storage import Store
from fastapi.testclient import TestClient


class MarketClient:
    base = 'https://api.bybit.com'
    ws_url = 'wss://stream.bybit.com/v5/public/linear'
    offset = 0


class FakeBybit(MarketClient):
    fee_mode = 'standard'


def test_account_request_resynchronises_and_retries_timestamp_error(monkeypatch):
    monkeypatch.setenv('BYBIT_MONITOR_API_KEY','test-key')
    monkeypatch.setenv('BYBIT_MONITOR_API_SECRET','test-secret')
    class ClockClient(MarketClient):
        clock_generation=0
        async def sync_time(self,*,expected_generation=None):
            assert expected_generation==0
            self.offset=4000
            self.clock_generation+=1
    monkeypatch.setattr('backend.account_monitor.now_ms',lambda:100000)
    requests=[]
    def respond(request):
        requests.append(request)
        return httpx.Response(200,json={'retCode':10002 if len(requests)==1 else 0,'result':{'ok':True}})
    monitor=BybitAccountMonitor(Store(':memory:'),ClockClient())
    monitor.client=httpx.AsyncClient(base_url='https://api.bybit.com',transport=httpx.MockTransport(respond))
    async def check():
        try:assert await monitor._get('/v5/user/query-api')=={'ok':True}
        finally:await monitor.client.aclose()
    try:asyncio.run(check())
    finally:monitor.store.close()
    assert [request.headers['X-BAPI-TIMESTAMP'] for request in requests]==['100000','104000']


def test_account_monitor_only_exposes_sanitized_read_only_status(monkeypatch):
    monkeypatch.setenv('BYBIT_MONITOR_API_KEY', 'private-test-key')
    monkeypatch.setenv('BYBIT_MONITOR_API_SECRET', 'private-test-secret')
    monitor = BybitAccountMonitor(Store(':memory:'), MarketClient())

    status = monitor.public_status()
    assert status['configured'] is True
    assert 'private-test-key' not in str(status)
    assert 'private-test-secret' not in str(status)
    try:
        asyncio.run(monitor._get('/v5/order/create', {'symbol': 'BTCUSDT'}))
    except ValueError as exc:
        assert 'not allowed' in str(exc)
    else:
        raise AssertionError('Trading endpoints must not be callable from the monitor')


def test_monitor_rejects_key_with_write_permission(monkeypatch):
    monkeypatch.setenv('BYBIT_MONITOR_API_KEY', 'test-key')
    monkeypatch.setenv('BYBIT_MONITOR_API_SECRET', 'test-secret')
    monitor = BybitAccountMonitor(Store(':memory:'), MarketClient())

    async def key_info(path, params=None):
        assert path == '/v5/user/query-api'
        return {'readOnly': 0, 'permissions': {'ContractTrade': ['Order', 'Position']}}

    monitor._get = key_info
    try:
        asyncio.run(monitor.verify_key())
    except RuntimeError:
        pass
    else:
        raise AssertionError('A read/write key must be rejected')
    assert monitor.public_status()['phase'] == 'read_only_required'
    assert monitor.public_status()['read_only'] is False


def test_account_execution_storage_deduplicates_exchange_ids():
    store = Store(':memory:')
    execution = {'execId': 'exec-1', 'execTime': '1000', 'symbol': 'BTCUSDT',
                 'side': 'Buy', 'execPrice': '100', 'execQty': '1'}
    try:
        first = asyncio.run(store.arecord_account_executions([execution]))
        second = asyncio.run(store.arecord_account_executions([execution]))
        assert first == [execution]
        assert second == []
        assert store.account_executions()[0]['execId'] == 'exec-1'
    finally:
        store.close()


def test_account_api_lists_real_data_and_rejects_wrong_side_links(monkeypatch):
    monkeypatch.delenv('BYBIT_MONITOR_API_KEY', raising=False)
    monkeypatch.delenv('BYBIT_MONITOR_API_SECRET', raising=False)
    store = Store(':memory:')
    scanner = Scanner(Engine(store), FakeBybit())
    long_setup = {
        'id': 'long-setup', 'symbol': 'BTCUSDT', 'timeframe': '5',
        'pattern': 'ascending_triangle', 'side': 'long', 'status': 'wait_breakout',
        'detected_at': 900, 'event_at': 900, 'updated_at': 900, 'reason': '',
        'snapshot': {'start': 1, 'detected_on_bar': 900, 'boundary': 100,
                     'stop': 99, 'target': 102, 'height': 2, 'atr': 1,
                     'lines': [], 'pivots': []},
    }
    short_setup = long_setup | {'id': 'short-setup', 'side': 'short'}
    store.save_setups([long_setup, short_setup])
    execution = {'execId': 'exec-1', 'execTime': '1000', 'symbol': 'BTCUSDT',
                 'side': 'Buy', 'execPrice': '100', 'execQty': '1',
                 'closedSize': '0', 'positionIdx': 0}
    asyncio.run(store.arecord_account_executions([execution]))
    try:
        with TestClient(create_app(scanner)) as client:
            status = client.get('/api/account/status').json()['status']
            assert status['phase'] == 'not_configured'
            assert 'secret' not in str(status).lower()
            assert client.get('/api/account/positions').json() == []
            assert client.get('/api/account/trades').json()['executions'][0]['execId'] == 'exec-1'
            assert client.put('/api/account/trades/exec-1/signal',
                              json={'setup_id': 'long-setup'}).status_code == 200
            assert client.put('/api/account/trades/exec-1/signal',
                              json={'setup_id': 'short-setup'}).status_code == 409
    finally:
        store.close()
