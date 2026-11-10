"""Exercise 160 current streams against a full historical detection queue.

The run uses a temporary SQLite database, the real pattern detector and the
same split workers as the service; it never connects to Bybit or uses the
project's database.  Timings begin when the 160 candle messages are received.
"""
import asyncio
from concurrent.futures import ProcessPoolExecutor
import json
import math
from pathlib import Path
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.detection_queue import DetectionJob
from backend.engine import Engine
from backend.models import Candle, Instrument, now_ms
from backend.service import Scanner, _snapshot_candles
from backend.storage import Store


class QuietBybit:
    offset = 0
    fee_mode = 'standard'

    async def close(self):
        pass


def percentile95(values):
    rows=sorted(values)
    return rows[math.ceil(len(rows)*.95)-1] if rows else None


def bars_ending_at(closed_time, interval):
    """A real, recognisable ascending triangle preceded by stable candles."""
    count=283
    first=closed_time-(count-1)*interval
    rows=[Candle(first+i*interval,112.5,113.2,111.8,112.5,10)
          for i in range(count-65)]
    for i in range(65):
        upper=125.0
        lower=100.0+.25*i
        high=upper-min(min(abs(i-j) for j in (4,16,28,40,52)),6)*.9
        low=lower+min(min(abs(i-j) for j in (10,22,34,46,58)),6)*.9
        close=(high+low)/2
        if i==64:
            close=upper+3
            high=max(high,close+.1)
            low=min(low,close-.1)
        rows.append(Candle(first+(count-65+i)*interval,close,high,low,close,10))
    return rows


async def run():
    loop=asyncio.get_running_loop()
    temp_root=Path(tempfile.gettempdir()).resolve()
    with tempfile.TemporaryDirectory(prefix='crypto-live-backlog-',dir=temp_root) as temporary:
        resolved=Path(temporary).resolve()
        if resolved.parent!=temp_root or not resolved.name.startswith('crypto-live-backlog-'):
            raise RuntimeError('Unexpected temporary directory path')
        store=Store(str(Path(temporary)/'load.sqlite3'))
        engine=Engine(store)
        scanner=Scanner(engine,QuietBybit())
        scanner.split_mode=True
        scanner.positions_ready=True
        now=now_ms()
        scanner.market_now=lambda:now
        scanner.symbols=[f'SYN{i:03}USDT' for i in range(40)]
        timeframes=engine.settings.timeframes
        engine.connected=True
        inputs=[]
        initial_candles=[]
        setups=[]
        history_rows=None
        for symbol in scanner.symbols:
            engine.instruments[symbol]=Instrument(symbol,.001,.001,5,.01)
            engine.fees[symbol]={'rate':.00055,'time':now,'source':'standard'}
            scanner.quotes[symbol]={'price':128.,'time':now}
            scanner.tickers[symbol]={'fundingRate':'0','_funding_time':now,'lastPrice':'128'}
            for timeframe in timeframes:
                interval=int(timeframe)*60_000
                close_bar=now//interval*interval-interval
                rows=bars_ending_at(close_bar,interval)
                previous=rows[-2].time
                key=(symbol,timeframe)
                scanner.live_buffers[key]=rows[-251:-1]
                scanner.history_cursor_cache[key]=previous
                scanner.live_scan_cursor_cache[key]=previous
                initial_candles.append((symbol,timeframe,rows[:-1]))
                inputs.append((symbol,timeframe,rows[-1]))
                if symbol==scanner.symbols[0] and timeframe=='5':history_rows=rows
                setups.append({'id':f'known-{symbol}-{timeframe}',
                    'symbol':symbol,'timeframe':timeframe,'pattern':'ascending_triangle',
                    'side':'long','status':'wait_breakout','detected_at':previous,
                    'updated_at':previous,'event_at':previous,'reason':'',
                    'confirmation_profile':'expanded','delivery':{'mode':'current'},
                    'snapshot':{'start':previous-10*interval,'detected_on_bar':previous,
                        'boundary':125.,'stop':95.,'target':200.,'atr':2.,'height':75.,
                        'lines':[],'pivots':[]}})
        assert history_rows is not None
        workers=[];publication_task=None
        try:
            await asyncio.gather(*(store.aput_candles(symbol,tf,rows)
                for symbol,tf,rows in initial_candles))
            await store.asave_setups(setups)
            scanner.active_setup_cache=await store.aactive_setups()
            scanner.active_setup_cache_at=now_ms()
            scanner.active_setup_streams={}
            for setup in scanner.active_setup_cache:
                scanner.active_setup_streams.setdefault((setup['symbol'],setup['timeframe']),[]).append(setup)

            # Every historic job is an older candle of one live stream.  The
            # queue starts completely full, including this same-stream backlog.
            backlog=scanner.split_detection_queue
            for index in range(32):
                end=250+index
                job=DetectionJob(priority=1,sequence=next(scanner.detection_sequence),
                    symbol=scanner.symbols[0],timeframe='5',candle_time=history_rows[end].time,
                    candles=_snapshot_candles(history_rows[end-250:end+1]),recovered=True,
                    queued_at=loop.time(),source='storage')
                assert await backlog.put_history(job)
            history_before=backlog.history_qsize()
            assert history_before==32

            if scanner.detection_processes>1:
                scanner.detection_executor.shutdown(wait=True,cancel_futures=True)
                scanner.detection_executor=ProcessPoolExecutor(max_workers=scanner.detection_processes-1)
                scanner.history_detection_executor=ProcessPoolExecutor(max_workers=1)
                workers=[asyncio.create_task(scanner.split_detection_worker('current'))
                         for _ in range(scanner.detection_processes-1)]
                workers.append(asyncio.create_task(scanner.split_detection_worker('history')))
            else:
                workers=[asyncio.create_task(scanner.split_detection_worker('both'))]

            receipt=loop.time()
            receipt_wall=now_ms()
            confirmation_latencies=[]
            current_with_history=[0]
            same_stream_with_history=[0]
            original_finish=scanner.finish_candle_detection
            async def measure_detection(job,*,archival=False):
                if not archival:
                    if backlog.history_qsize() or backlog.history_running:
                        current_with_history[0]+=1
                    if (job.stream in backlog.history_streams or
                            job.stream in backlog.history_running):
                        same_stream_with_history[0]+=1
                await original_finish(job,archival=archival)
            scanner.finish_candle_detection=measure_detection
            original_confirm=scanner.confirm_live_setups
            async def measure_confirmation(symbol,tf,candle):
                await original_confirm(symbol,tf,candle)
                confirmation_latencies.append((loop.time()-receipt)*1000)
            scanner.confirm_live_setups=measure_confirmation

            publishing=True
            async def publish_to_client():
                while publishing:
                    rows=await asyncio.to_thread(store.signals,1000)
                    scanner.stamp_publications({'signals':rows})
                    await scanner.flush_publications()
                    await asyncio.sleep(.05)
            publication_task=asyncio.create_task(publish_to_client())

            await asyncio.wait_for(asyncio.gather(*(scanner.observe_live_candle(
                symbol,tf,candle,receipt_wall,receipt,'websocket')
                for symbol,tf,candle in inputs)),timeout=90)
            await asyncio.wait_for(backlog.join_current(),timeout=90)
            publishing=False
            await publication_task
            rows=await asyncio.to_thread(store.signals,1000)
            scanner.stamp_publications({'signals':rows})
            await scanner.flush_publications()
            elapsed=(loop.time()-receipt)*1000
            confirmed=store.db.execute("SELECT count(*) FROM setups WHERE id LIKE 'known-%' "
                                       "AND status='breakout_confirmed'").fetchone()[0]
            saved=store.signals(1000)
            live_signals=[row for row in saved if
                (row.get('filter_results',{}).get('delivery') or {}).get('mode')=='current']
            publication_latencies=[row['published_at']-row['received_at'] for row in live_signals
                                   if row.get('received_at') and row.get('published_at')]
            current_cursors=sum(store.live_scan_cursor(symbol,tf)==candle.time
                                for symbol,tf,candle in inputs)
            result={'streams':len(inputs),'history_queue_before':history_before,
                'history_queue_after_current':backlog.history_qsize(),
                'current_scan_cursors':current_cursors,'confirmed_setups':confirmed,
                'current_signals':len(live_signals),
                'current_started_with_history':current_with_history[0],
                'same_stream_started_with_history':same_stream_with_history[0],
                'confirmation_samples':len(confirmation_latencies),
                'detection_samples':len(scanner.live_detection_latency_ms),
                'publication_samples':len(publication_latencies),
                'confirmation_p95_ms':round(percentile95(confirmation_latencies) or 0,2),
                'detection_p95_ms':percentile95(scanner.live_detection_latency_ms),
                'publication_p95_ms':percentile95(publication_latencies),
                'total_current_elapsed_ms':round(elapsed,2)}
            print(json.dumps(result),flush=True)
            assert result['streams']==160
            assert result['confirmation_samples']==160 and confirmed==160
            assert result['detection_samples']==160 and current_cursors==160
            assert result['current_started_with_history']>0, 'No current work overlapped the historical backlog'
            assert result['same_stream_started_with_history']>0, 'Same-stream history finished before the current task'
            assert live_signals, 'Real detector produced no current signals'
            assert len(publication_latencies)==len(live_signals)
            assert all((row.get('filter_results',{}).get('delivery') or {}).get('timing_version')==2
                       and (row.get('filter_results',{}).get('delivery') or {}).get('source')=='websocket'
                       and row['received_at']==receipt_wall
                       and row['received_at']<=row['detected_at']<=row['published_at']
                       for row in live_signals)
            assert result['confirmation_p95_ms']<=1_000, result
            assert result['detection_p95_ms']<=10_000, result
            assert result['publication_p95_ms']<=10_000, result
        finally:
            if publication_task and not publication_task.done():
                publication_task.cancel()
                await asyncio.gather(publication_task,return_exceptions=True)
            for task in workers:task.cancel()
            if workers:await asyncio.gather(*workers,return_exceptions=True)
            # Current detections schedule asynchronous context snapshots which
            # hold read-only SQLite connections. Finish them before closing the
            # database and deleting the isolated temporary directory on Windows.
            snapshots=list(scanner.snapshot_tasks)
            if snapshots:await asyncio.gather(*snapshots,return_exceptions=True)
            if scanner.detection_executor:
                await asyncio.to_thread(scanner.detection_executor.shutdown,wait=True,cancel_futures=True)
            if scanner.history_detection_executor:
                await asyncio.to_thread(scanner.history_detection_executor.shutdown,wait=True,cancel_futures=True)
            await loop.shutdown_default_executor()
            store.close()


if __name__=='__main__':
    asyncio.run(run())
