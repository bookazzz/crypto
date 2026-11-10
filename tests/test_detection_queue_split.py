import asyncio

import pytest

from backend.detection_queue import DetectionJob, SplitDetectionQueue


def job(symbol, candle_time, *, sequence=None, recovered=False, timeframe='5'):
    return DetectionJob(1 if recovered else 0,
        candle_time if sequence is None else sequence, symbol, timeframe,
        candle_time, (), recovered, 0., received_at=4.2,
        received_wall_ms=123456, source='rest', context_buffers={'60': ()})


def test_full_history_does_not_block_a_burst_of_160_current_streams():
    async def scenario():
        queue=SplitDetectionQueue(max_history=32)
        for index in range(32):
            assert await queue.put_history(job(f'H{index}',1,recovered=True))
        history_blocked=asyncio.create_task(queue.put_history(job('WAIT',1,recovered=True)))
        await asyncio.sleep(0)
        assert not history_blocked.done()
        live=[job(f'S{index//4}',2,timeframe=('5','15','60','240')[index%4])
              for index in range(160)]
        results=await asyncio.wait_for(asyncio.gather(*(queue.put_current(row) for row in live)),1)
        assert all(result.accepted and result.deferred is None for result in results)
        assert queue.current_qsize()==160 and queue.history_qsize()==32
        seen=[]
        for _ in live:
            current=await asyncio.wait_for(queue.get_current(),1)
            seen.append((current.symbol,current.timeframe))
            await queue.complete_current(current)
        assert len(set(seen))==160
        first=await queue.get_history()
        await queue.complete_history(first)
        assert await asyncio.wait_for(history_blocked,1)
        for _ in range(32):
            pending=await queue.get_history()
            await queue.complete_history(pending)
        await asyncio.wait_for(queue.join(),1)
        assert queue.unfinished==0 and queue.qsize()==0
    asyncio.run(scenario())


def test_current_replacement_defers_previous_without_waiting_for_same_stream_history():
    async def scenario():
        queue=SplitDetectionQueue(max_history=1)
        historical=job('BTCUSDT',1,recovered=True)
        await queue.put_history(historical)
        running_history=await queue.get_history()
        first=job('BTCUSDT',2)
        assert (await queue.put_current(first)).accepted
        running_current=await queue.get_current()
        second=job('BTCUSDT',3)
        third=job('BTCUSDT',4)
        assert (await queue.put_current(second)).accepted
        result=await queue.put_current(third)
        assert result.accepted and result.deferred is second
        older=job('BTCUSDT',0)
        assert (await queue.put_current(older)).deferred is older
        duplicate=await queue.put_current(third)
        assert not duplicate.accepted and duplicate.deferred is None
        waiting=asyncio.create_task(queue.get_current())
        await asyncio.sleep(0)
        assert not waiting.done()
        await queue.complete_current(running_current)
        assert await asyncio.wait_for(waiting,1) is third
        await queue.complete_current(third)
        await queue.join_current()
        assert queue.history_unfinished==1
        await queue.complete_history(running_history)
        await asyncio.wait_for(queue.join(),1)
    asyncio.run(scenario())


def test_history_retry_reserves_capacity_and_preserves_order_and_timing():
    async def scenario():
        queue=SplitDetectionQueue(max_history=2)
        first=job('BTCUSDT',1,recovered=True)
        second=job('BTCUSDT',2,recovered=True)
        await queue.put_history(first)
        await queue.put_history(second)
        running=await queue.get_history()
        blocked=asyncio.create_task(queue.put_history(job('BTCUSDT',3,recovered=True)))
        await asyncio.sleep(0)
        assert not blocked.done()
        assert not await queue.put_history(first)
        await queue.retry_history(running)
        assert queue.history_unfinished==2 and queue.history_qsize()==2
        assert not blocked.done()
        retry=await queue.get_history()
        assert retry.candle_time==1 and retry.retries==1
        assert retry.received_wall_ms==123456 and retry.source=='rest'
        assert retry.context_buffers=={'60': ()}
        await queue.complete_history(retry)
        assert await asyncio.wait_for(blocked,1)
        for expected in (2,3):
            row=await queue.get_history()
            assert row.candle_time==expected
            await queue.complete_history(row)
        await queue.join()
    asyncio.run(scenario())


def test_single_worker_takes_current_first_and_history_remains_ordered():
    async def scenario():
        queue=SplitDetectionQueue()
        await queue.put_history(job('BTCUSDT',1,recovered=True))
        await queue.put_history(job('BTCUSDT',2,recovered=True))
        await queue.put_current(job('BTCUSDT',3))
        lane,current=await queue.get_next()
        assert lane=='current' and current.candle_time==3
        await queue.complete_current(current)
        for expected in (1,2):
            lane,historical=await queue.get_next()
            assert lane=='history' and historical.candle_time==expected
            await queue.complete_history(historical)
        await queue.join()
    asyncio.run(scenario())


def test_overflow_is_explicit_and_replacement_heap_remains_bounded():
    async def scenario():
        queue=SplitDetectionQueue(max_current_streams=1)
        for index in range(1000):
            result=await queue.put_current(job('BTCUSDT',index))
            assert result.accepted
            if index:assert result.deferred.candle_time==index-1
        assert queue.current_qsize()==1 and queue.current_unfinished==1
        assert len(queue.current_ready)<=64
        other=job('ETHUSDT',1001)
        result=await queue.put_current(other)
        assert not result.accepted and result.deferred is other
        row=await queue.get_current()
        assert row.candle_time==999
        await queue.complete_current(row)
        await queue.join()
    asyncio.run(scenario())


def test_history_rejects_out_of_order_and_invalid_completion():
    async def scenario():
        queue=SplitDetectionQueue()
        later=job('BTCUSDT',2,recovered=True)
        await queue.put_history(later)
        with pytest.raises(ValueError,match='candle order'):
            await queue.put_history(job('BTCUSDT',1,recovered=True))
        with pytest.raises(ValueError,match='not running'):
            await queue.complete_history(later)
        assert await queue.get_history() is later
        await queue.complete_history(later)
        await queue.join()
    asyncio.run(scenario())
