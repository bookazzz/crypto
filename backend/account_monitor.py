"""Read-only Bybit account watcher for executions, positions, and orders."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import time
from urllib.parse import urlencode, urlparse

import httpx
import websockets

from .models import now_ms
from .bybit import RECV_WINDOW_MS


READ_PATHS = {
    '/v5/user/query-api',
    '/v5/position/list',
    '/v5/order/realtime',
    '/v5/order/history',
    '/v5/execution/list',
}
PRIVATE_TOPICS = ('execution.linear', 'order.linear', 'position.linear')
MAX_HISTORY_MS = 7 * 24 * 60 * 60_000


class BybitAccountMonitor:
    """Never exposes key material and never calls a trading endpoint."""

    def __init__(self, store, market_client, on_change=None, on_executions=None):
        self.store = store
        self.market_client = market_client
        self.on_change = on_change
        self.on_executions = on_executions
        self.key = os.getenv('BYBIT_MONITOR_API_KEY', '').strip()
        self.secret = os.getenv('BYBIT_MONITOR_API_SECRET', '').strip()
        self.status = {
            'configured': bool(self.key and self.secret),
            'connected': False,
            'read_only': None,
            'phase': 'not_configured' if not (self.key and self.secret) else 'waiting',
            'error': '' if not (self.key and self.secret) else 'Ожидание подключения',
            'last_message': None,
            'last_sync': None,
        }
        self.client: httpx.AsyncClient | None = None
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    def public_status(self):
        return dict(self.status)

    async def start(self):
        if self._task is None and self.status['configured']:
            self._task = asyncio.create_task(self.run(), name='bybit-account-monitor')

    async def stop(self):
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self.client:
            await self.client.aclose()
            self.client = None

    def _base_url(self):
        base = self.market_client.base
        allowed = {
            'api.bybit.com', 'api.bytick.com', 'api.bybit.nl', 'api.bybit-tr.com',
            'api.bybit.kz', 'api.bybitgeorgia.ge', 'api.bybit.ae',
        }
        parsed = urlparse(base)
        if parsed.scheme != 'https' or parsed.hostname not in allowed:
            raise RuntimeError('Нужен официальный HTTPS адрес Bybit')
        return base

    def _ws_url(self):
        host = urlparse(self.market_client.ws_url).hostname
        if host not in {'stream.bybit.com', 'stream.bytick.com'}:
            raise RuntimeError('Нужен официальный WebSocket адрес Bybit')
        return f'wss://{host}/v5/private'

    def _signed_headers(self, query):
        timestamp = str(now_ms() + int(getattr(self.market_client, 'offset', 0) or 0))
        recv = RECV_WINDOW_MS
        signature = hmac.new(self.secret.encode(),
            (timestamp + self.key + recv + query).encode(), hashlib.sha256).hexdigest()
        return {
            'X-BAPI-API-KEY': self.key,
            'X-BAPI-TIMESTAMP': timestamp,
            'X-BAPI-RECV-WINDOW': recv,
            'X-BAPI-SIGN': signature,
        }

    async def _get(self, path, params=None):
        if path not in READ_PATHS:
            raise ValueError('Endpoint is not allowed by the read-only account monitor')
        if self.client is None:
            self.client = httpx.AsyncClient(base_url=self._base_url(), timeout=12)
        query = urlencode(sorted((params or {}).items()))
        for attempt in range(2):
            generation = getattr(self.market_client, 'clock_generation', 0)
            response = await self.client.get(path + ('?' + query if query else ''),
                                             headers=self._signed_headers(query))
            response.raise_for_status()
            payload = response.json()
            if int(payload.get('retCode', -1)) == 10002 and attempt == 0:
                await self.market_client.sync_time(expected_generation=generation)
                continue
            break
        if int(payload.get('retCode', -1)) != 0:
            # Do not include URL, response headers, or credentials in errors.
            raise RuntimeError(f'Bybit account API code {payload.get("retCode", "unknown")}')
        return payload.get('result') or {}

    async def verify_key(self):
        result = await self._get('/v5/user/query-api')
        if int(result.get('readOnly', 0) or 0) != 1:
            self.status.update(connected=False, read_only=False, phase='read_only_required',
                              error='Ключ должен иметь разрешение только для чтения')
            raise RuntimeError(self.status['error'])
        permissions = result.get('permissions') or {}
        contract = set(permissions.get('ContractTrade') or [])
        if not {'Order', 'Position'}.issubset(contract):
            self.status.update(connected=False, read_only=True, phase='permissions_missing',
                              error='Ключу нужны права чтения заявок и позиций контрактов')
            raise RuntimeError(self.status['error'])
        self.status.update(read_only=True, phase='syncing', error='')

    async def sync_rest(self):
        now = now_ms() + int(getattr(self.market_client, 'offset', 0) or 0)
        positions = await self._get('/v5/position/list',
                                    {'category': 'linear', 'settleCoin': 'USDT', 'limit': 200})
        position_rows = [row for row in positions.get('list', [])
                         if float(row.get('size') or 0) > 0]
        await self.store.areplace_account_positions(position_rows, now)

        order_result = await self._get('/v5/order/realtime',
                                       {'category': 'linear', 'openOnly': 0, 'limit': 50})
        await self.store.arecord_account_orders(order_result.get('list', []))
        order_cursor = order_result.get('nextPageCursor') or ''
        for _ in range(20):
            if not order_cursor:
                break
            result = await self._get('/v5/order/realtime', {
                'category': 'linear', 'openOnly': 0, 'limit': 50, 'cursor': order_cursor})
            await self.store.arecord_account_orders(result.get('list', []))
            next_cursor = result.get('nextPageCursor') or ''
            if not next_cursor or next_cursor == order_cursor:
                break
            order_cursor = next_cursor

        history = await self._get('/v5/order/history', {
            'category': 'linear', 'settleCoin': 'USDT', 'startTime': now - MAX_HISTORY_MS,
            'endTime': now, 'limit': 50})
        await self.store.arecord_account_orders(history.get('list', []))
        order_cursor = history.get('nextPageCursor') or ''
        for _ in range(20):
            if not order_cursor:
                break
            result = await self._get('/v5/order/history', {
                'category': 'linear', 'settleCoin': 'USDT', 'startTime': now - MAX_HISTORY_MS,
                'endTime': now, 'limit': 50, 'cursor': order_cursor})
            await self.store.arecord_account_orders(result.get('list', []))
            next_cursor = result.get('nextPageCursor') or ''
            if not next_cursor or next_cursor == order_cursor:
                break
            order_cursor = next_cursor

        latest = self.store.latest_account_execution_time()
        start = max(now - MAX_HISTORY_MS, (latest - 60_000) if latest else now - MAX_HISTORY_MS)
        cursor = ''
        executions = []
        for _ in range(100):
            params = {'category': 'linear', 'startTime': start, 'endTime': now, 'limit': 100}
            if cursor:
                params['cursor'] = cursor
            result = await self._get('/v5/execution/list', params)
            executions.extend(row for row in result.get('list', []) if row.get('execType', 'Trade') == 'Trade')
            next_cursor = result.get('nextPageCursor') or ''
            if not next_cursor or next_cursor == cursor:
                break
            cursor = next_cursor
        new_executions=await self.store.arecord_account_executions(executions)
        if new_executions and self.on_executions:
            await self.on_executions(new_executions)
        self.status.update(last_sync=now, phase='live', error='')
        if self.on_change:
            self.on_change()

    async def _auth(self, ws):
        expires = now_ms() + int(getattr(self.market_client, 'offset', 0) or 0) + 10_000
        signature = hmac.new(self.secret.encode(), f'GET/realtime{expires}'.encode(),
                             hashlib.sha256).hexdigest()
        await ws.send(json.dumps({'op': 'auth', 'args': [self.key, expires, signature]}))
        answer = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
        if answer.get('op') != 'auth' or not answer.get('success'):
            raise RuntimeError('Bybit отклонил авторизацию read-only потока')
        await ws.send(json.dumps({'op': 'subscribe', 'args': list(PRIVATE_TOPICS)}))

    async def _handle(self, message):
        topic = message.get('topic', '')
        rows = message.get('data') or []
        if not isinstance(rows, list):
            return
        category = message.get('topic', '').split('.')[-1]
        if topic.startswith('execution') or category == 'execution':
            values = [row for row in rows if row.get('execType', 'Trade') == 'Trade']
            if values:
                inserted=await self.store.arecord_account_executions(values)
                if inserted and self.on_executions:
                    await self.on_executions(inserted)
        elif topic.startswith('order') or category == 'order':
            await self.store.arecord_account_orders(rows)
            related={str(row.get('orderId')) for row in rows if row.get('orderId')}
            recent=self.store.account_executions(100)
            pending=[row for row in recent if str(row.get('orderId')) in related]
            if pending and self.on_executions:
                await self.on_executions(pending)
        elif topic.startswith('position') or category == 'position':
            current = { (row.get('category') or 'linear', row.get('symbol'), int(row.get('positionIdx') or 0)): row
                        for row in self.store.account_positions() }
            for row in rows:
                key = (row.get('category') or 'linear', row.get('symbol'), int(row.get('positionIdx') or 0))
                if float(row.get('size') or 0) > 0:
                    current[key] = row
                else:
                    current.pop(key, None)
            await self.store.areplace_account_positions(list(current.values()), now_ms())
            changed_symbols={row.get('symbol') for row in rows}
            recent=self.store.account_executions(100)
            pending=[row for row in recent if row.get('symbol') in changed_symbols]
            if pending and self.on_executions:
                await self.on_executions(pending)
        else:
            return
        self.status['last_message'] = int(message.get('creationTime') or now_ms())
        self.status.update(connected=True, phase='live', error='')
        if self.on_change:
            self.on_change()

    async def run(self):
        delay = 1
        while not self._stop.is_set():
            try:
                self.status.update(connected=False, phase='verifying', error='')
                await self.market_client.sync_time()
                await self.verify_key()
                await self.sync_rest()
                async with websockets.connect(self._ws_url(), ping_interval=20,
                                              ping_timeout=20, close_timeout=5,
                                              open_timeout=10, max_size=4_000_000) as ws:
                    await self._auth(ws)
                    self.status.update(connected=True, phase='live', error='')
                    self.on_change() if self.on_change else None
                    delay = 1
                    next_sync = time.monotonic() + 60
                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5)
                        except asyncio.TimeoutError:
                            raw = None
                        if raw:
                            message = json.loads(raw)
                            if message.get('op') == 'ping':
                                await ws.send(json.dumps({'op': 'pong'}))
                            elif message.get('topic'):
                                await self._handle(message)
                        if time.monotonic() >= next_sync:
                            await self.sync_rest()
                            next_sync = time.monotonic() + 60
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                error = str(exc).splitlines()[0][:180] or 'Неизвестная ошибка подключения'
                self.status.update(connected=False,
                    phase=self.status.get('phase') if self.status.get('phase') in
                    ('read_only_required', 'permissions_missing') else 'disconnected', error=error)
                self.on_change() if self.on_change else None
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
