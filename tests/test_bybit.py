import asyncio
import hashlib
import hmac
import httpx
import pytest
from backend.bybit import Bybit, BybitNetworkError


def test_auth_signature_and_read_only_allowlist(monkeypatch):
    monkeypatch.setenv('BYBIT_API_KEY','test-key')
    monkeypatch.setenv('BYBIT_API_SECRET','test-secret')
    monkeypatch.setattr('backend.bybit.now_ms',lambda:123456789)
    requests=[]
    def respond(request):
        requests.append(request)
        return httpx.Response(200,json={'retCode':0,'result':{'list':[]}})
    async def check():
        b=Bybit()
        await b.client.aclose()
        b.client=httpx.AsyncClient(base_url=b.base,transport=httpx.MockTransport(respond))
        try:
            await b.get('/v5/account/fee-rate',{'symbol':'BTCUSDT','category':'linear'},True)
            with pytest.raises(ValueError):await b.get('/v5/order/create',{},True)
            with pytest.raises(ValueError):await b.get('/v5/account/fee-rate',{})
            await b.get('/v5/market/tickers',{'category':'linear'})
        finally:await b.close()
    asyncio.run(check())
    assert len(requests)==2
    private,public=requests
    expected=hmac.new(b'test-secret',b'123456789test-key10000category=linear&symbol=BTCUSDT',hashlib.sha256).hexdigest()
    assert private.headers['X-BAPI-SIGN']==expected
    assert private.method=='GET' and public.method=='GET'
    assert 'X-BAPI-API-KEY' not in public.headers


def test_backward_pagination_and_chronological_sort():
    class Fake(Bybit):
        async def candles(self,symbol,tf,start=None,end=None,mark=False):
            from backend.models import Candle
            all_bars=[Candle(t,1,2,0,1) for t in range(0,600000,60000)]
            return [c for c in all_bars if start<=c.time<=end][-3:]
    async def check():
        b=Fake()
        try:
            c=await b.history('BTCUSDT','1',120000,599999)
            assert [x.time for x in c]==list(range(120000,600000,60000))
        finally:await b.close()
    asyncio.run(check())


def test_private_host_is_restricted(monkeypatch):
    monkeypatch.setenv('BYBIT_HTTP_URL','https://example.com')
    with pytest.raises(ValueError):Bybit()


def test_transient_bybit_internal_error_is_retried(monkeypatch):
    calls=[]
    def respond(request):
        calls.append(request)
        code=10016 if len(calls)==1 else 0
        return httpx.Response(200,json={'retCode':code,'result':{'list':[]}})
    async def no_sleep(_):
        pass
    monkeypatch.setattr('backend.bybit.asyncio.sleep',no_sleep)
    async def check():
        b=Bybit()
        await b.client.aclose()
        b.client=httpx.AsyncClient(base_url=b.base,transport=httpx.MockTransport(respond))
        try:
            await b.get('/v5/market/tickers',{'category':'linear'})
        finally:
            await b.close()
    asyncio.run(check())
    assert len(calls)==2


def test_network_failure_is_typed_and_requests_recover(monkeypatch):
    offline=[True]
    def respond(request):
        if offline[0]:raise httpx.ConnectError('unavailable',request=request)
        return httpx.Response(200,json={'retCode':0,'result':{'list':[]}})
    async def no_sleep(_):pass
    monkeypatch.setattr('backend.bybit.asyncio.sleep',no_sleep)
    async def check():
        bybit=Bybit()
        await bybit.client.aclose()
        bybit.client=httpx.AsyncClient(transport=httpx.MockTransport(respond))
        try:
            with pytest.raises(BybitNetworkError):
                await bybit.get('/v5/market/tickers',{'category':'linear'})
            offline[0]=False
            assert await bybit.get('/v5/market/tickers',{'category':'linear'})=={'list':[]}
        finally:await bybit.close()
    asyncio.run(check())


def test_concurrent_expired_requests_share_resync_and_are_resigned(monkeypatch):
    now=1_800_000_000_000
    monkeypatch.setattr('backend.bybit.now_ms',lambda:now)
    monkeypatch.setenv('BYBIT_API_KEY','test-key')
    monkeypatch.setenv('BYBIT_API_SECRET','test-secret')
    requests=[]
    initial=[]
    ready=asyncio.Event()
    async def respond(request):
        requests.append(request)
        if request.url.path=='/v5/market/time':
            return httpx.Response(200,json={'retCode':0,'result':{'timeNano':str((now+4000)*1_000_000)}})
        if len(initial)<4:
            initial.append(request)
            if len(initial)==4:ready.set()
            await ready.wait()
            return httpx.Response(200,json={'retCode':10002})
        return httpx.Response(200,json={'retCode':0,'result':{'list':[]}})
    async def check():
        bybit=Bybit()
        await bybit.client.aclose()
        bybit.client=httpx.AsyncClient(transport=httpx.MockTransport(respond))
        try:
            await asyncio.wait_for(asyncio.gather(*(bybit.get('/v5/account/fee-rate',
                {'category':'linear','symbol':f'COIN{i}USDT'},private=True) for i in range(4))),5)
            assert bybit.clock_generation==1 and bybit.offset==3900
        finally:await bybit.close()
    asyncio.run(check())
    assert sum(r.url.path=='/v5/market/time' for r in requests)==3
    retried=[r for r in requests if r.url.path=='/v5/account/fee-rate' and r not in initial]
    assert len(retried)==4
    for request in retried:
        timestamp=request.headers['X-BAPI-TIMESTAMP']
        query=request.url.query.decode()
        expected=hmac.new(b'test-secret',(timestamp+'test-key10000'+query).encode(),hashlib.sha256).hexdigest()
        assert timestamp==str(now+3900)
        assert request.headers['X-BAPI-SIGN']==expected


def test_clock_sampling_does_not_include_wait_for_request_slot(monkeypatch):
    clock=[1_800_000_000_000]
    monkeypatch.setattr('backend.bybit.now_ms',lambda:clock[0])
    def respond(request):
        server=clock[0]+100
        clock[0]+=200
        return httpx.Response(200,json={'retCode':0,'result':{'timeNano':str(server*1_000_000)}})
    async def check():
        bybit=Bybit()
        await bybit.client.aclose()
        bybit.client=httpx.AsyncClient(transport=httpx.MockTransport(respond))
        try:
            for _ in range(8):await bybit.gate.acquire()
            task=asyncio.create_task(bybit.sync_time())
            await asyncio.sleep(0)
            clock[0]+=5000
            for _ in range(8):bybit.gate.release()
            await task
            assert bybit.offset==-200
        finally:await bybit.close()
    asyncio.run(check())


def test_timestamp_error_is_not_retried_forever(monkeypatch):
    monkeypatch.setenv('BYBIT_API_KEY','test-key')
    monkeypatch.setenv('BYBIT_API_SECRET','test-secret')
    attempts=[]
    def respond(request):
        attempts.append(request.url.path)
        if request.url.path=='/v5/market/time':
            import time
            return httpx.Response(200,json={'retCode':0,'result':{'timeNano':str(time.time_ns())}})
        return httpx.Response(200,json={'retCode':10002})
    async def check():
        bybit=Bybit()
        await bybit.client.aclose()
        bybit.client=httpx.AsyncClient(transport=httpx.MockTransport(respond))
        try:
            with pytest.raises(RuntimeError,match='10002'):
                await bybit.get('/v5/account/fee-rate',{},private=True)
        finally:await bybit.close()
    asyncio.run(check())
    assert attempts.count('/v5/account/fee-rate')==2
    assert attempts.count('/v5/market/time')==3


def test_ticker_fetches_one_public_linear_instrument():
    requests=[]
    def respond(request):
        requests.append(request)
        return httpx.Response(200,json={'retCode':0,'result':{'list':[
            {'symbol':'ETHUSDT','lastPrice':'200'},
            {'symbol':'BTCUSDT','lastPrice':'100'}]}})
    async def check():
        bybit=Bybit()
        await bybit.client.aclose()
        bybit.client=httpx.AsyncClient(base_url=bybit.base,transport=httpx.MockTransport(respond))
        try:
            return await bybit.ticker('BTCUSDT')
        finally:
            await bybit.close()
    assert asyncio.run(check())=={'symbol':'BTCUSDT','lastPrice':'100'}
    assert len(requests)==1
    assert requests[0].method=='GET'
    assert requests[0].url.params['category']=='linear'
    assert requests[0].url.params['symbol']=='BTCUSDT'


def test_tickers_fetches_public_linear_snapshot():
    requests=[]
    def respond(request):
        requests.append(request)
        return httpx.Response(200,json={'retCode':0,'result':{'list':[
            {'symbol':'ETHUSDT','lastPrice':'200'},{'symbol':'BTCUSDT','lastPrice':'100'}]}})
    async def check():
        bybit=Bybit()
        await bybit.client.aclose()
        bybit.client=httpx.AsyncClient(base_url=bybit.base,transport=httpx.MockTransport(respond))
        try:return await bybit.tickers()
        finally:await bybit.close()
    assert len(asyncio.run(check()))==2
    assert len(requests)==1
    assert requests[0].url.path=='/v5/market/tickers'
    assert requests[0].url.params['category']=='linear'
    assert 'symbol' not in requests[0].url.params


def test_open_interest_uses_five_minute_public_market_endpoint():
    calls=[]
    def respond(request):
        calls.append(request)
        return httpx.Response(200,json={'retCode':0,'result':{'list':[
            {'timestamp':'300000','openInterest':'12.5'},{'timestamp':'0','openInterest':'10'}]}})
    async def check():
        b=Bybit()
        await b.client.aclose()
        b.client=httpx.AsyncClient(base_url=b.base,transport=httpx.MockTransport(respond))
        try:return await b.open_interest('BTCUSDT')
        finally:await b.close()
    assert asyncio.run(check())==[(300000,12.5),(0,10.)]
    assert calls[0].url.path=='/v5/market/open-interest'
    assert calls[0].url.params['intervalTime']=='5min'
