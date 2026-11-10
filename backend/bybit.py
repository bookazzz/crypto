"""Read-only adapter. No order/position/account-mutation endpoints exist here."""
import asyncio
import hashlib
import hmac
import os
from contextlib import asynccontextmanager
from urllib.parse import urlencode, urlparse
import httpx
import websockets
from .models import Candle, Instrument, now_ms

PUBLIC = {'/v5/market/instruments-info','/v5/market/tickers','/v5/market/kline',
          '/v5/market/mark-price-kline','/v5/market/funding/history','/v5/market/time'}
PUBLIC.add('/v5/market/open-interest')
RECV_WINDOW_MS = '10000'


class BybitNetworkError(RuntimeError):
    """The exchange could not be reached after trying the available hosts."""


class Bybit:
    def __init__(self):
        self.base = os.getenv('BYBIT_HTTP_URL','https://api.bybit.com').rstrip('/')
        self.ws_url = os.getenv('BYBIT_WS_URL','wss://stream.bybit.com/v5/public/linear')
        # Prevent an accidentally configured proxy receiving credentials.
        allowed = {'api.bybit.com','api.bytick.com','api.bybit.nl','api.bybit-tr.com',
                   'api.bybit.kz','api.bybitgeorgia.ge','api.bybit.ae'}
        if urlparse(self.base).scheme!='https' or urlparse(self.base).hostname not in allowed:
            raise ValueError('BYBIT_HTTP_URL must be an official Bybit HTTPS host')
        if urlparse(self.ws_url).scheme!='wss' or urlparse(self.ws_url).hostname not in {'stream.bybit.com','stream.bytick.com'}:
            raise ValueError('BYBIT_WS_URL must be an official Bybit stream')
        http_host=urlparse(self.base).hostname
        self.http_bases=[self.base]
        if http_host in {'api.bybit.com','api.bytick.com'}:
            alternative=('https://api.bytick.com' if http_host=='api.bybit.com'
                         else 'https://api.bybit.com')
            self.http_bases.append(alternative)
        ws_host=urlparse(self.ws_url).hostname
        self.ws_hosts=[ws_host]
        if ws_host in {'stream.bybit.com','stream.bytick.com'}:
            self.ws_hosts.append('stream.bytick.com' if ws_host=='stream.bybit.com'
                                 else 'stream.bybit.com')
        self.ws_host=ws_host
        self.spot_ws_url=self.ws_url.rsplit('/',1)[0]+'/spot'
        self.key = os.getenv('BYBIT_API_KEY','')
        self.secret = os.getenv('BYBIT_API_SECRET','')
        self.fee_mode=os.getenv('BYBIT_FEE_MODE','standard').strip().lower()
        if self.fee_mode not in ('standard','account'):
            raise ValueError('BYBIT_FEE_MODE must be standard or account')
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(15,connect=5))
        self.gate = asyncio.Semaphore(8)
        self.offset = 0
        self.clock_uncertainty_ms = None
        self.clock_synced_at = 0
        self.clock_generation = 0
        self.clock_sync_lock = asyncio.Lock()
        self.linear_pairs={}

    async def close(self):
        await self.client.aclose()

    def http_candidates(self):
        return [self.base,*[base for base in self.http_bases if base!=self.base]]

    def ws_candidates(self,url):
        parsed=urlparse(url)
        hosts=[self.ws_host,*[host for host in self.ws_hosts if host!=self.ws_host]]
        suffix=parsed.path+('?' + parsed.query if parsed.query else '')
        return [f'wss://{host}{suffix}' for host in hosts]

    @asynccontextmanager
    async def connect_ws(self,url,**kwargs):
        last_error=None
        for candidate in self.ws_candidates(url):
            manager=websockets.connect(candidate,**kwargs)
            try:
                socket=await manager.__aenter__()
            except (OSError,TimeoutError,websockets.WebSocketException) as exc:
                last_error=exc
                continue
            self.ws_host=urlparse(candidate).hostname
            self.ws_url=f'wss://{self.ws_host}/v5/public/linear'
            self.spot_ws_url=f'wss://{self.ws_host}/v5/public/spot'
            try:
                yield socket
            finally:
                await manager.__aexit__(None,None,None)
            return
        if last_error:
            if isinstance(last_error,(OSError,TimeoutError)):
                raise BybitNetworkError('Bybit: сеть недоступна') from last_error
            raise last_error
        raise RuntimeError('Bybit WebSocket endpoint unavailable')

    async def get(self,path,params=None,private=False,*,_clock_sample=None,_resync_clock=True,
                  _received_at=None,_received_monotonic=None):
        if path not in PUBLIC and path!='/v5/account/fee-rate':
            raise ValueError('Endpoint not permitted by read-only adapter')
        if private != (path=='/v5/account/fee-rate'):
            raise ValueError('Endpoint authentication mismatch')
        if private and (not self.key or not self.secret):
            raise RuntimeError('Не заданы ключи Bybit для чтения комиссии')
        query=urlencode(sorted((params or {}).items()))
        network_failed=False
        transient_failed=False
        access_denied=False
        request_path=path+('?' + query if query else '')
        for attempt in range(4):
            for base in self.http_candidates():
                try:
                    async with self.gate:
                        headers={}
                        if private:
                            # Sign only after acquiring the request slot: parallel
                            # fee lookups may otherwise wait long enough for the
                            # precomputed timestamp to expire before dispatch.
                            timestamp=str(now_ms()+self.offset)
                            recv=RECV_WINDOW_MS
                            signature=hmac.new(self.secret.encode(),
                                (timestamp+self.key+recv+query).encode(),hashlib.sha256).hexdigest()
                            headers={'X-BAPI-API-KEY':self.key,'X-BAPI-TIMESTAMP':timestamp,
                                     'X-BAPI-RECV-WINDOW':recv,'X-BAPI-SIGN':signature}
                            signed_generation=self.clock_generation
                        if _clock_sample is not None:
                            before=now_ms()
                            started=asyncio.get_running_loop().time()
                        response=await self.client.get(base+request_path,headers=headers)
                        if _clock_sample is not None or _received_at is not None or _received_monotonic is not None:
                            received=now_ms()
                            received_monotonic=asyncio.get_running_loop().time()
                        if _clock_sample is not None:
                            elapsed=(received_monotonic-started)*1000
                    if response.status_code==403:
                        access_denied=True
                        transient_failed=True
                        continue
                    if response.status_code in (429,500,502,503,504):
                        transient_failed=True
                        continue
                    response.raise_for_status()
                    data=response.json()
                    if private and data['retCode']==10002 and _resync_clock:
                        # Do not hold the request semaphore while synchronising.
                        # Concurrent failures share one clock update.
                        await self.sync_time(expected_generation=signed_generation)
                        return await self.get(path,params,private,_resync_clock=False,
                                              _received_at=_received_at,
                                              _received_monotonic=_received_monotonic)
                    if data['retCode'] in (10006,10016):
                        transient_failed=True
                        continue
                    if data['retCode']:
                        # Never echo an authenticated request or keys.
                        raise RuntimeError(f'Bybit API: код {data["retCode"]}')
                    self.base=base
                    if _clock_sample is not None:
                        _clock_sample.append((elapsed,before,received,data['result']))
                    if _received_at is not None:
                        _received_at.append(received)
                    if _received_monotonic is not None:
                        _received_monotonic.append(received_monotonic)
                    return data['result']
                except httpx.RequestError:
                    network_failed=True
            if attempt<3:
                await asyncio.sleep(2**attempt)
        if access_denied:
            raise RuntimeError('Bybit: доступ к API отклонён (HTTP 403)')
        if network_failed and not transient_failed:
            raise BybitNetworkError('Bybit: сеть недоступна') from None
        raise RuntimeError('Bybit: превышен лимит запросов / временная ошибка')

    async def sync_time(self,*,expected_generation=None):
        async with self.clock_sync_lock:
            if expected_generation is not None and self.clock_generation!=expected_generation:
                return
            samples=[]
            for _ in range(3):
                await self.get('/v5/market/time',_clock_sample=samples)
            usable=[]
            for rtt,before,received,result in samples:
                # The exchange time was produced between send and receive.
                # Avoid a midpoint biased by event-loop or network delays:
                # a slightly old timestamp is safer than one in the future.
                if rtt>5000 or abs((received-before)-rtt)>1000:
                    continue
                server=int(result.get('timeNano',0))//1_000_000 or int(result['timeSecond'])*1000
                usable.append((rtt,server-received-100))
            if not usable:
                raise RuntimeError('Не удалось точно синхронизировать время с Bybit')
            rtt,offset=min(usable,key=lambda row:row[0])
            self.offset=offset
            self.clock_uncertainty_ms=rtt
            self.clock_synced_at=now_ms()
            self.clock_generation+=1

    async def universe(self,n):
        instruments={}
        cursor=''
        while True:
            params={'category':'linear','limit':1000}
            if cursor:
                params['cursor']=cursor
            result=await self.get('/v5/market/instruments-info',params)
            for row in result['list']:
                if row['status']!='Trading' or row['quoteCoin']!='USDT' or row['contractType']!='LinearPerpetual':
                    continue
                self.linear_pairs[row['symbol']]={
                    'baseCoin':row['baseCoin'],'quoteCoin':row['quoteCoin'],
                    'symbol':row['symbol']}
                lot=row['lotSizeFilter']
                symbol_type=str(row.get('symbolType') or '')
                asset_class={'stock':'stock','ETF':'etf','commodity':'commodity','forex':'forex',
                    'xstocks':'stock','mstocks':'stock','adventure':'crypto'}.get(symbol_type,'crypto')
                instruments[row['symbol']]=Instrument(row['symbol'],float(lot['qtyStep']),float(lot['minOrderQty']),
                    float(lot.get('minNotionalValue',0)),float(row['priceFilter']['tickSize']),
                    int(row.get('fundingInterval',480)),float(lot.get('maxMktOrderQty',lot['maxOrderQty'])),
                    symbol_type,asset_class,str(row.get('marketRegion') or ''),
                    str(row.get('displayName') or row.get('fullName') or row['symbol']),
                    str(row.get('underlyingTicker') or ''),str(row.get('status') or ''),
                    int(row.get('launchTime') or 0),bool(row.get('isPreListing',False)))
            cursor=result.get('nextPageCursor','')
            if not cursor:
                break
        rows=(await self.get('/v5/market/tickers',{'category':'linear'}))['list']
        tickers={r['symbol']:r for r in rows if r['symbol'] in instruments}
        symbols=sorted(tickers,key=lambda s:float(tickers[s].get('turnover24h',0)),reverse=True)[:n]
        return instruments,symbols,tickers

    async def ticker(self,symbol):
        rows=(await self.get('/v5/market/tickers',{'category':'linear','symbol':symbol})).get('list',[])
        return next((row for row in rows if row.get('symbol')==symbol),None)

    async def tickers(self):
        return (await self.get('/v5/market/tickers',{'category':'linear'})).get('list',[])

    async def spot_matches(self,linear_symbols):
        """Return exact, active spot symbols for matching linear USDT contracts."""
        result=await self.get('/v5/market/instruments-info',{'category':'spot'})
        spots={}
        for row in result.get('list',[]):
            if row.get('status')!='Trading' or row.get('quoteCoin')!='USDT':
                continue
            key=(row.get('baseCoin'),row.get('quoteCoin'))
            spots.setdefault(key,[]).append(row)
        matches={}
        for symbol in linear_symbols:
            identity=self.linear_pairs.get(symbol)
            candidates=spots.get((identity.get('baseCoin'),identity.get('quoteCoin')),
                                 []) if identity else []
            exact=[row for row in candidates if row.get('symbol')==symbol]
            matches[symbol]=symbol if len(exact)==1 else None
        return matches

    async def _fetch_candles(self,symbol,tf,start,end,mark,limit,received_at=None,
                             received_monotonic=None):
        if isinstance(limit,bool) or not isinstance(limit,int) or not 1<=limit<=1000:
            raise ValueError('Candle limit must be an integer between 1 and 1000')
        params={'category':'linear','symbol':symbol,'interval':tf,'limit':limit}
        if start is not None:
            params['start']=start
        if end is not None:
            params['end']=end
        path='/v5/market/mark-price-kline' if mark else '/v5/market/kline'
        if received_at is None and received_monotonic is None:
            result=await self.get(path,params)
        else:
            result=await self.get(path,params,_received_at=received_at,
                                  _received_monotonic=received_monotonic)
        return sorted([Candle(int(r[0]),*[float(x) for x in r[1:5]],
                       float(r[5]) if len(r)>5 else 0,
                       float(r[6]) if len(r)>6 and r[6] not in (None,'') else None)
                       for r in result['list']],key=lambda c:c.time)

    async def candles(self,symbol,tf,start=None,end=None,mark=False,*,limit=1000):
        return await self._fetch_candles(symbol,tf,start,end,mark,limit)

    async def candles_with_metadata(self,symbol,tf,start=None,end=None,mark=False,*,limit=1000):
        """Return candles and local wall time when their successful response arrived."""
        rows,received_at,_=await self.candles_with_timing(symbol,tf,start,end,mark,limit=limit)
        return rows,received_at

    async def candles_with_timing(self,symbol,tf,start=None,end=None,mark=False,*,limit=1000):
        """Return candles plus local wall and monotonic times at response receipt."""
        received_at=[]
        received_monotonic=[]
        rows=await self._fetch_candles(symbol,tf,start,end,mark,limit,received_at,received_monotonic)
        return rows,received_at[0],received_monotonic[0]

    async def history(self,symbol,tf,start,end,mark=False):
        # Backward pagination handles gaps longer than 1000 minutes.
        result={}
        cursor=end
        while cursor>=start:
            page=await self.candles(symbol,tf,start,cursor,mark)
            if not page:
                break
            for c in page:
                if start<=c.time<=end:
                    result[c.time]=c
            next_cursor=page[0].time-1
            if next_cursor>=cursor:
                raise RuntimeError('Bybit history pagination stalled')
            cursor=next_cursor
        return sorted(result.values(),key=lambda c:c.time)

    async def history_with_timing(self,symbol,tf,start,end,mark=False):
        """Return history and timing from the page containing its newest candle.

        Both times are None when no candle within the requested range exists.
        """
        result={}
        cursor=end
        newest=None
        newest_wall=None
        newest_monotonic=None
        while cursor>=start:
            page,received_at,received_monotonic=await self.candles_with_timing(
                symbol,tf,start,cursor,mark)
            if not page:
                break
            for candle in page:
                if start<=candle.time<=end:
                    result[candle.time]=candle
                    if newest is None or candle.time>newest:
                        newest=candle.time
                        newest_wall=received_at
                        newest_monotonic=received_monotonic
            next_cursor=page[0].time-1
            if next_cursor>=cursor:
                raise RuntimeError('Bybit history pagination stalled')
            cursor=next_cursor
        return sorted(result.values(),key=lambda c:c.time),newest_wall,newest_monotonic

    async def history_with_metadata(self,symbol,tf,start,end,mark=False):
        """Return history and receipt wall time for its newest candle."""
        rows,received_at,_=await self.history_with_timing(symbol,tf,start,end,mark)
        return rows,received_at

    async def fee_rates(self,symbols):
        await self.sync_time()
        async def fetch(symbol):
            result=await self.get('/v5/account/fee-rate',{'category':'linear','symbol':symbol},private=True)
            row=next((r for r in result['list'] if r['symbol']==symbol),None)
            return (symbol,float(row['takerFeeRate'])) if row else None
        # Fetch the personalized rate for every symbol, but overlap requests so
        # the hourly universe refresh does not hold the market feed offline.
        rows=await asyncio.gather(*(fetch(symbol) for symbol in symbols),return_exceptions=True)
        for row in rows:
            if isinstance(row,BaseException):raise row
        return dict(row for row in rows if row is not None)

    async def funding_history(self,symbol,start,end):
        rows={}
        cursor=end
        while cursor>=start:
            result=await self.get('/v5/market/funding/history',{'category':'linear','symbol':symbol,
                'startTime':start,'endTime':cursor,'limit':200})
            page=result['list']
            if not page:
                break
            for r in page:
                rows[int(r['fundingRateTimestamp'])]=float(r['fundingRate'])
            cursor=min(int(r['fundingRateTimestamp']) for r in page)-1
        return sorted(rows.items())

    async def open_interest(self,symbol):
        result=await self.get('/v5/market/open-interest',{'category':'linear','symbol':symbol,
            'intervalTime':'5min','limit':15})
        return [(int(row['timestamp']),float(row['openInterest'])) for row in result.get('list',[])]
