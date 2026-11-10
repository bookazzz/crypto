import asyncio
from contextvars import ContextVar

import httpx
import pytest

from backend.bybit import Bybit
from backend.models import Candle


def payload():
    return {'retCode':0,'result':{'list':[
        ['600000','100','105','99','103','20','2000'],
        ['300000','99','104','98','100','10','1000']]}}


@pytest.mark.parametrize('limit',[1,251,1000])
def test_candle_limit_is_sent_and_rows_are_sorted(limit):
    requests=[]
    def respond(request):
        requests.append(request)
        return httpx.Response(200,json=payload())
    async def check():
        bybit=Bybit()
        await bybit.client.aclose()
        bybit.client=httpx.AsyncClient(transport=httpx.MockTransport(respond))
        try:
            rows=await bybit.candles('BTCUSDT','5',100,700000,True,limit=limit)
            assert [row.time for row in rows]==[300000,600000]
            assert rows[0].turnover==1000
        finally:await bybit.close()
    asyncio.run(check())
    request=requests[0]
    assert request.url.path=='/v5/market/mark-price-kline'
    assert request.url.params['limit']==str(limit)
    assert request.url.params['start']=='100' and request.url.params['end']=='700000'


def test_default_candle_limit_remains_1000():
    requests=[]
    def respond(request):
        requests.append(request)
        return httpx.Response(200,json=payload())
    async def check():
        bybit=Bybit()
        await bybit.client.aclose()
        bybit.client=httpx.AsyncClient(transport=httpx.MockTransport(respond))
        try:await bybit.candles('BTCUSDT','5')
        finally:await bybit.close()
    asyncio.run(check())
    assert requests[0].url.params['limit']=='1000'


@pytest.mark.parametrize('limit',[0,-1,1001,1.5,'251',True,None])
def test_invalid_candle_limit_is_rejected_before_any_request(limit):
    requests=[]
    def respond(request):
        requests.append(request)
        return httpx.Response(200,json=payload())
    async def check():
        bybit=Bybit()
        await bybit.client.aclose()
        bybit.client=httpx.AsyncClient(transport=httpx.MockTransport(respond))
        try:
            with pytest.raises(ValueError,match='Candle limit'):
                await bybit.candles('BTCUSDT','5',limit=limit)
        finally:await bybit.close()
    asyncio.run(check())
    assert not requests


def test_concurrent_candle_requests_keep_their_own_receipt_time(monkeypatch):
    clock=ContextVar('receipt_clock',default=0)
    monkeypatch.setattr('backend.bybit.now_ms',lambda:clock.get())
    async def check():
        bybit=Bybit()
        await bybit.client.aclose()
        eth_ready=asyncio.Event()
        async def respond(request):
            if request.url.params['symbol']=='BTCUSDT':
                await eth_ready.wait()
                clock.set(2000)
            else:
                clock.set(1000)
                eth_ready.set()
            return httpx.Response(200,json=payload())
        bybit.client=httpx.AsyncClient(transport=httpx.MockTransport(respond))
        try:
            btc,eth=await asyncio.gather(
                bybit.candles_with_metadata('BTCUSDT','5',limit=251),
                bybit.candles_with_metadata('ETHUSDT','5',limit=251))
            assert btc[1]==2000 and eth[1]==1000
            assert [row.time for row in btc[0]]==[300000,600000]
            assert [row.time for row in eth[0]]==[300000,600000]
        finally:await bybit.close()
    asyncio.run(check())


def test_receipt_time_comes_from_successful_retry(monkeypatch):
    clock=[0]
    monkeypatch.setattr('backend.bybit.now_ms',lambda:clock[0])
    attempts=[]
    def respond(request):
        attempts.append(request)
        clock[0]=1000 if len(attempts)==1 else 2000
        if len(attempts)==1:return httpx.Response(503)
        return httpx.Response(200,json=payload())
    async def check():
        bybit=Bybit()
        await bybit.client.aclose()
        bybit.client=httpx.AsyncClient(transport=httpx.MockTransport(respond))
        try:
            rows,received_at=await bybit.candles_with_metadata('BTCUSDT','5',limit=251)
            assert received_at==2000 and len(rows)==2
        finally:await bybit.close()
    asyncio.run(check())
    assert len(attempts)==2


def test_candles_with_timing_reports_wall_and_monotonic_receipt(monkeypatch):
    monkeypatch.setattr('backend.bybit.now_ms',lambda:123456)
    def respond(request):
        return httpx.Response(200,json=payload())
    async def check():
        bybit=Bybit()
        await bybit.client.aclose()
        bybit.client=httpx.AsyncClient(transport=httpx.MockTransport(respond))
        try:
            loop=asyncio.get_running_loop()
            before=loop.time()
            rows,wall,monotonic=await bybit.candles_with_timing('BTCUSDT','5',limit=251)
            after=loop.time()
            assert len(rows)==2 and wall==123456
            assert before<=monotonic<=after
        finally:await bybit.close()
    asyncio.run(check())


def test_paginated_history_timing_is_from_newest_returned_candle():
    class Fake(Bybit):
        def __init__(self):
            super().__init__()
            self.calls=[]
        async def candles_with_timing(self,symbol,tf,start=None,end=None,mark=False,*,limit=1000):
            self.calls.append((symbol,tf,start,end,mark,limit))
            bars=[Candle(t,1,2,0,1) for t in range(0,600000,60000)]
            page=[bar for bar in bars if start<=bar.time<=end][-3:]
            return page,1000+len(self.calls)*100,10.+len(self.calls)
    async def check():
        bybit=Fake()
        try:
            rows,wall,monotonic=await bybit.history_with_timing('BTCUSDT','1',120000,599999,True)
            assert [row.time for row in rows]==list(range(120000,600000,60000))
            assert wall==1100 and monotonic==11.
            assert len(bybit.calls)==3
            assert all(call[4] is True and call[5]==1000 for call in bybit.calls)
            assert [call[3] for call in bybit.calls]==[599999,419999,239999]
        finally:await bybit.close()
    asyncio.run(check())


def test_history_metadata_uses_same_receipt_and_empty_history_has_no_timestamp():
    class Fake(Bybit):
        async def candles_with_timing(self,symbol,tf,start=None,end=None,mark=False,*,limit=1000):
            rows=[Candle(300000,1,2,0,1)] if start<=300000<=end else []
            return rows,1000,20.
    async def check():
        bybit=Fake()
        try:
            rows,wall=await bybit.history_with_metadata('BTCUSDT','5',0,300000)
            assert [row.time for row in rows]==[300000] and wall==1000
            assert await bybit.history_with_timing('BTCUSDT','5',0,200000)==([],None,None)
            assert await bybit.history_with_metadata('BTCUSDT','5',300000,200000)==([],None)
        finally:await bybit.close()
    asyncio.run(check())
