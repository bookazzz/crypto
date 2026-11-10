import asyncio
from collections import deque, OrderedDict
import contextlib
import hashlib
import json
import logging
import os
import itertools
import signal
import sqlite3
import weakref
import httpx
from dataclasses import asdict, replace
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
import websockets
from .bybit import Bybit, BybitNetworkError
from .models import Candle, Signal, now_ms
from .patterns import atr, detect, detect_combined, EXPERIMENTAL_NAMES
from .engine import Engine
from .strategy import (CONTEXT, VERSION, VARIANTS, FIGURE_TF, ENTRY_TF, BIAS_TF,
    Variant, trend_filter, volume_filter, line_at, prepare_signal,
    classify_market_setup, confirm_breakout_5m)
from .decision_context import build_context, support_chart_levels, _book
from .market_background import build_market_background, compact_market_advisory, FOUR_HOURS_MS
from .market_comparison import apply_orderbook, compare_spot_perpetual
from .market_quality import valid_ohlc
from .entry import assess_entry
from .scalp import assess_scalp_plan
from .account_monitor import BybitAccountMonitor
from .forecast_outcomes import (evaluate_horizons, make_observation,
    build_statistics_index, summarize_filter_ablation, HORIZONS_MINUTES)
from .publishing import VersionedPublisher
from .detection_queue import CandleSnapshot, DetectionJob, PrioritizedStreamQueue, SplitDetectionQueue

ROOT=Path(__file__).resolve().parents[1]


def _detection_worker_init():
    # The server coordinates shutdown; console Ctrl+C must not kill its
    # process-pool children while they are finishing a candle.
    signal.signal(signal.SIGINT,signal.SIG_IGN)


def _detect_in_worker(symbol, timeframe, candles, stage, sensitivity, include_experimental):
    # The detector uses at most 250 prior bars for ATR and the latest 124 bars
    # for geometry. Keeping one extra bar preserves the early-stage ATR rule.
    return detect(symbol,timeframe,list(candles[-251:]),stage=stage,sensitivity=sensitivity,
                  include_experimental=include_experimental)


def _detect_combined_worker(symbol,timeframe,candles,breakout_sensitivity,setup_sensitivity,
                            include_experimental):
    return detect_combined(symbol,timeframe,list(candles[-251:]),
        breakout_sensitivity=breakout_sensitivity,setup_sensitivity=setup_sensitivity,
        include_experimental=include_experimental)


def _snapshot_candles(candles):
    return tuple(candle if isinstance(candle,CandleSnapshot) else CandleSnapshot(
        int(candle.time),float(candle.open),float(candle.high),float(candle.low),
        float(candle.close),float(getattr(candle,'volume',0)),
        float(candle.turnover) if getattr(candle,'turnover',None) is not None else None)
        for candle in candles[-251:])


class Scanner:
    def __init__(self,engine:Engine,bybit:Bybit):
        self.engine,self.bybit=engine,bybit
        self.engine.fee_mode=getattr(bybit,'fee_mode','standard')
        if self.engine.fee_mode=='account':
            old_fees=self.engine.fees
            self.engine.fees={symbol:fee for symbol,fee in old_fees.items()
                              if fee.get('source')=='account'}
            if self.engine.fees!=old_fees:
                self.engine.persist()
        self.symbols=[]
        self.buffers={}
        self.live_buffers={}
        self.market_background_cache=OrderedDict()
        self.live_short_history_start={}
        self.live_buffer_locks={}
        self.live_hydration_tasks={}
        self.live_hydration_retry_at={}
        self.live_hydration_backoff={}
        self.live_observation_tasks=set()
        self.live_observation_by_stream={}
        self.live_observation_pending={}
        self.live_observation_running_time={}
        self.live_retry_tasks={}
        self.live_receipts={}
        self.live_received_close={}
        self.history_bootstrap_complete=False
        self.history_dispatch_event=asyncio.Event()
        self.history_cursor_cache={}
        self.history_scan_cursor_cache={}
        self.history_setup_states={}
        self.history_detection_inflight=set()
        self.history_detection_retry_at={}
        self.history_detection_failures={}
        self.history_gap_tasks={}
        self.history_gap_retry_at={}
        self.live_scan_cursor_cache={}
        self.live_rebuilt=set()
        self.split_mode=False
        self.positions_ready=True
        self.position_recovery_ticks=[]
        self.position_recovery_overflow=False
        self.position_recovery_tick_limit=100_000
        self.quotes={}
        self.scalp_last_tracked_at={}
        self.setup_assessment_sampled_at={}
        self.setup_price_state_sampled_at={}
        self.tickers={}
        self.books={}
        self.spot_books={}
        self.spot_matches={}
        self.spot_symbols=set()
        self.spot_match_error=''
        self.spot_connected=False
        self.spot_stream_error=''
        self.spot_last_message=0
        self.spot_futures_saved={}
        self.fee_refresh_event=asyncio.Event()
        self.open_interest_refresh_event=asyncio.Event()
        self.fee_symbols=set()
        self.live_flow={}
        self.flow_saved={}
        self.seen_trade_ids=OrderedDict()
        self.subscription_request_sequence=itertools.count(1)
        self.subscription_ack_waiters={}
        self.subscription_ack_waiters_by_socket=weakref.WeakKeyDictionary()
        self.subscription_ack_locks=weakref.WeakKeyDictionary()
        self.live_liquidations={}
        self.liquidation_saved={}
        self.pending_open_interest={}
        self.open_interest_sampled={}
        self.ancillary_flow_started_at=None
        self.ancillary_liquidation_started_at=None
        self.ancillary_book_symbols_requested=set()
        self.ancillary_book_symbols=set()
        self.open_interest_error=''
        self.watched_context={}
        self.watched_setups={}
        self.requested_setup_contexts={}
        self.watched_charts={}
        self.live_candles={}
        self.last_closed_candles={}
        self.market_symbols=set()
        self.context_scope={}
        self.selected_context_symbols=set()
        self.context_refresh_event=asyncio.Event()
        self.context_unavailable={}
        self.context_errors={}
        self.context_hydrated=set()
        self.context_load_retry_at={}
        self.quote_locks={}
        self.context_quote_semaphore=asyncio.Semaphore(8)
        self.history_semaphore=asyncio.Semaphore(2)
        self.chart_history_tasks={}
        self.context_quote_batch_lock=asyncio.Lock()
        self.context_quote_tasks={}
        self.snapshot_tasks=set()
        self.snapshot_semaphore=asyncio.Semaphore(2)
        self.forecast_statistics_cache={}
        self.forecast_statistics_updated_at=0
        self.forecast_statistics_refresh_lock=asyncio.Lock()
        self.quote_retry_at={}
        self.candle_locks={}
        self.candle_queue=asyncio.PriorityQueue()
        self.recovery_candle_queue=asyncio.PriorityQueue()
        self.market_message_queue=asyncio.Queue(maxsize=20_000)
        self.market_message_queue_wait_ms=deque(maxlen=300)
        self.market_message_error=''
        self.candle_sequence=itertools.count()
        self.candle_workers=[]
        self.recovery_candle_workers=[]
        self.detection_queue=PrioritizedStreamQueue(maxsize=640)
        self.split_detection_queue=SplitDetectionQueue(max_current_streams=640,max_history=32)
        self.detection_sequence=itertools.count()
        self.detection_workers=[]
        self.detection_replay_task=None
        self.detection_queue_wait_ms=deque(maxlen=1000)
        self.detection_processing_ms=deque(maxlen=1000)
        self.live_detection_latency_ms=deque(maxlen=1000)
        self.live_publish_latency_ms=deque(maxlen=1000)
        self.live_signal_receipts=OrderedDict()
        self.recovered_detection_latency_ms=deque(maxlen=1000)
        self.candle_queue_wait_ms=deque()
        self.candle_processing_ms=deque()
        self.live_candle_queue_wait_ms=deque(maxlen=300)
        self.live_candle_processing_ms=deque(maxlen=300)
        self.live_candle_transition_latency_ms=deque(maxlen=300)
        self.recovered_candle_queue_wait_ms=deque(maxlen=300)
        self.recovered_candle_processing_ms=deque(maxlen=300)
        self.candle_stage_ms={name:deque() for name in ('persist','advance_setups','discover_setups','filter_candidates')}
        self.signal_stage_ms={name:deque() for name in ('detect','dedupe','setup_link','rules')}
        self.event_loop_lag_ms=deque(maxlen=300)
        self.live_metrics_started_at=0
        self.last_exchange_event_at=0
        self.last_quote_broadcast_at=0
        self.quote_broadcast_pending={}
        self.quote_publish_ms=deque(maxlen=300)
        self.network_delay_ms=deque(maxlen=1000)
        self.browser_delivery_ms=deque(maxlen=1000)
        self.browser_render_ms=deque(maxlen=1000)
        self.market_write_ms=deque(maxlen=300)
        self.last_price_received_at=0
        self.last_kline_received_at=0
        self.last_signal_detected_at=0
        self.last_signal_at=0
        self.ancillary_connected=False
        self.ancillary_last_message=0
        self.broadcast_event=asyncio.Event()
        self.feed_cache=None
        self.feed_cache_json=None
        self.feed_cache_at=0
        self.active_setup_cache=None
        self.active_setup_revision=0
        self.active_setup_cache_at=0
        self.setup_cache=None
        self.setup_cache_at=0
        self.active_setup_streams=None
        self.previewed=set()
        self.publisher=VersionedPublisher()
        self.summary_publisher=VersionedPublisher()
        self.v2_publishers={'all':self.publisher,'current':VersionedPublisher(),'history':VersionedPublisher()}
        self.v3_publishers={'all':self.summary_publisher,'current':VersionedPublisher(),'history':VersionedPublisher()}
        self.client_scopes={}
        self.publication_times=OrderedDict()
        self.publication_pending={'signals':{},'setups':{}}
        self.publication_event=asyncio.Event()
        self.v2_clients={}
        self.v3_clients={}
        self.v3_sender_tasks={}
        self.detection_processes=min(4,max(1,(os.cpu_count() or 1)-2))
        self.detection_executor=ProcessPoolExecutor(max_workers=self.detection_processes,initializer=_detection_worker_init)
        self.history_detection_executor=None
        self.context_hydration_task=None
        self.candle_recovery_lock=asyncio.Lock()
        self.next_candle_recovery=0
        self.candle_poll_attempts={}
        self.candle_recovery_backoff=5_000
        self.candle_recovery_status='idle'
        self.candle_recovery_pending=0
        self.pending=[]
        self.clients=set()
        self.account_watchers=set()
        self.error=''
        self.connection_error=''
        self.fee_error=('Комиссия аккаунта ещё загружается'
                        if self.engine.fee_mode=='account' else '')
        self.phase='starting'
        self.last_refresh=0
        self.refreshing_market=False
        self.last_message=0
        self.started_at=now_ms()
        self.last_closed_scan={}
        self.last_received_close={}
        self.scan_events=deque()
        self.recovery_time=0
        self.tasks=[]
        self.account_monitor=BybitAccountMonitor(self.engine.store,self.bybit,
            on_change=self.broadcast_event.set,on_executions=self.on_account_executions)
        self.last_broadcast_at=0
        self.last_legacy_broadcast_at=0
        self.last_v2_broadcast_at=0
        self.broadcast_error=''
        self.research_path=ROOT/'data'/'research'/'summary.json'
        self.research={'status':'not_run','message':'Ожидается историческая проверка'}
        self.research_mtime=0
        self.active_variant=None
        self.load_approved_strategy()

    def load_approved_strategy(self):
        """Only a final, explicitly gated holdout result can enable new entries."""
        if not self.research_path.exists():
            self.engine.strategy_approved=False
            self.engine.strategy_version=''
            self.research_mtime=0
            return
        try:
            mtime=self.research_path.stat().st_mtime_ns
            if mtime==self.research_mtime:
                return
            report=json.loads(self.research_path.read_text(encoding='utf-8'))
            self.research_mtime=mtime
            self.research=report.get('approval',{'status':'not_run'})
            if report.get('version')!=VERSION:
                self.research={'status':'replay_required','eligible':False,
                    'message':'\u041f\u0440\u0430\u0432\u0438\u043b\u0430 \u0441\u0442\u0440\u0430\u0442\u0435\u0433\u0438\u0438 \u0438\u0437\u043c\u0435\u043d\u0438\u043b\u0438\u0441\u044c; \u0437\u0430\u043f\u0443\u0441\u0442\u0438\u0442\u0435 \u043d\u043e\u0432\u0443\u044e \u0438\u0441\u0442\u043e\u0440\u0438\u0447\u0435\u0441\u043a\u0443\u044e \u043f\u0440\u043e\u0432\u0435\u0440\u043a\u0443.'}
                self.active_variant=None
                self.engine.strategy_approved=False
                self.engine.strategy_version=''
                self.engine.strategy_variant={}
                return
            selected=report.get('selected') or {}
            valid_ids={v.id for v in VARIANTS}
            final=selected.get('holdout',{})
            approval=(report.get('version')==VERSION and self.research.get('status')=='passed'
                and self.research.get('eligible') is True and selected.get('id') in valid_ids
                and final.get('closed',0)>=100 and final.get('net',0)>0
                and final.get('profit_factor',0)>=1.2 and final.get('max_drawdown',1)<=.10
                and selected.get('stress_test_net',0)>0)
            if approval and selected.get('entry') in ('immediate','retest'):
                self.active_variant=Variant(selected['entry'],float(selected['net_rr']),selected.get('time_exit'))
                self.engine.strategy_approved=True
                self.engine.strategy_version=VERSION
                self.engine.strategy_variant=self.active_variant.dict()
                self.engine.settings.min_rr=self.active_variant.net_rr
                return
            if self.research.get('status')=='passed':
                self.research={'status':'invalid_report','eligible':False,
                    'message':'Итоговый отчёт не соответствует правилам допуска'}
        except (OSError,ValueError,TypeError,KeyError):
            self.research={'status':'invalid_report','message':'Отчёт исследования не удалось проверить'}
        self.engine.strategy_approved=False
        self.engine.strategy_version=''

    async def start(self):
        if os.getenv('SCANNER_SLOW_CALLBACK_LOG')=='1':
            loop=asyncio.get_running_loop()
            loop.set_debug(True)
            loop.slow_callback_duration=.05
        await self.account_monitor.start()
        self.split_mode=True
        self.positions_ready=not bool(self.engine.positions)
        # Keep the current detector's capacity independent of historical work.
        if self.detection_processes>1:
            if self.detection_executor:
                await asyncio.to_thread(self.detection_executor.shutdown,wait=True,cancel_futures=True)
            self.detection_executor=ProcessPoolExecutor(max_workers=self.detection_processes-1,initializer=_detection_worker_init)
            self.history_detection_executor=ProcessPoolExecutor(max_workers=1,initializer=_detection_worker_init)
        if self.detection_executor is None:
            self.detection_executor=ProcessPoolExecutor(max_workers=self.detection_processes,initializer=_detection_worker_init)
        if self.detection_processes>1:
            self.detection_workers=[asyncio.create_task(self.split_detection_worker('current'),
                name=f'current-detection-{index+1}') for index in range(self.detection_processes-1)]
            self.detection_workers.append(asyncio.create_task(self.split_detection_worker('history'),
                name='history-detection'))
        else:
            self.detection_workers=[asyncio.create_task(self.split_detection_worker('both'),
                name='candle-detection')]
        # Close events arrive in bursts across many independent streams.  These
        # workers do only the lightweight transition/commit stage; heavyweight
        # geometry searches are isolated in the two-process detection pool.
        self.candle_workers=[asyncio.create_task(self.candle_worker()) for _ in range(16)]
        self.recovery_candle_workers=[asyncio.create_task(self.candle_worker(recovery_worker=True)) for _ in range(2)]
        self.tasks=[asyncio.create_task(self.run(),name='scanner'),asyncio.create_task(self.broadcast_loop(),name='broadcast'),
                    asyncio.create_task(self.forecast_outcome_loop(),name='forecast-outcomes'),
                    asyncio.create_task(self.persist_market_minutes_loop(),name='market-minute-writer'),
                    asyncio.create_task(self.market_message_worker(),name='market-message-worker'),
                    asyncio.create_task(self.run_ancillary(),name='ancillary-stream'),
                    asyncio.create_task(self.run_funding_settlement(),name='funding-settlement'),
                    asyncio.create_task(self.poll_open_interest(),name='open-interest'),asyncio.create_task(self.run_spot(),name='spot-stream'),
                    asyncio.create_task(self.poll_fee_rates(),name='fee-rates'),
                    asyncio.create_task(self.run_candle_watchdog(),name='candle-watchdog'),
                    asyncio.create_task(self.history_detection_dispatcher(),name='history-detection-dispatcher'),
                    asyncio.create_task(self.refresh_active_setup_cache(),name='active-setup-cache'),
                    asyncio.create_task(self.refresh_live_warming(),name='live-window-warmup'),
                    asyncio.create_task(self.persist_publications_loop(),name='publication-writer'),
                    asyncio.create_task(self.refresh_watched_context(),name='context-refresh'),
                    asyncio.create_task(self.measure_event_loop(),name='event-loop-monitor')]

    async def stop(self):
        async def cancel_tasks(tasks):
            tasks=[task for task in tasks if task is not None]
            for task in tasks:
                if not task.done():task.cancel()
            results=await asyncio.gather(*tasks,return_exceptions=True)
            for result in results:
                if isinstance(result,Exception):
                    logging.getLogger(__name__).error('Background task failed during shutdown',
                        exc_info=(type(result),result,result.__traceback__))

        # Stop producers before collecting their children so no new history
        # requests can be created while the HTTP client is being closed.
        for task in self.tasks:
            if not task.done():task.cancel()
        try:
            await self.account_monitor.stop()
        except Exception:
            logging.getLogger(__name__).exception('Account monitor shutdown failed')
        await cancel_tasks(self.tasks)
        await cancel_tasks([*self.context_quote_tasks.values(),*self.live_hydration_tasks.values(),
            *self.live_observation_tasks,*self.live_retry_tasks.values(),*self.history_gap_tasks.values(),self.context_hydration_task,
            self.detection_replay_task,*self.snapshot_tasks,*self.chart_history_tasks.values()])
        await cancel_tasks([*self.candle_workers,*self.recovery_candle_workers,*self.detection_workers])
        self.detection_workers.clear()
        try:
            try:
                await self.flush_publications()
            finally:
                await self.engine.persist_async()
        finally:
            try:
                await self.bybit.close()
            finally:
                executor=self.detection_executor
                if executor:
                    await asyncio.to_thread(executor.shutdown,wait=True,cancel_futures=True)
                    self.detection_executor=None
                history_executor=self.history_detection_executor
                if history_executor:
                    await asyncio.to_thread(history_executor.shutdown,wait=True,cancel_futures=True)
                    self.history_detection_executor=None

    def status(self):
        self.load_approved_strategy()
        now=now_ms();event_now=self.market_now();cutoff=max(self.started_at,now-3_600_000)
        events=[event for event in self.scan_events if event['time']>=cutoff]
        latest_signal=self.last_signal_at
        expected_streams=len(self.symbols)*len(self.engine.settings.timeframes)
        stale=[]
        for symbol in self.symbols:
            for tf in self.engine.settings.timeframes:
                interval=int(tf)*60_000;latest_expected=(event_now//interval)*interval-interval
                last=(self.live_received_close if self.split_mode else self.last_received_close).get((symbol,tf),0)
                deadline=latest_expected+interval+30_000
                expected_close=latest_expected+interval
                if event_now>=deadline and last<expected_close:
                    stale.append({'symbol':symbol,'timeframe':tf,'last_close':last,
                        'expected_close':expected_close,'late_by_ms':event_now-deadline})
        last_scan=max(self.last_closed_scan.values(),default=0)
        def p95(values):
            rows=sorted(values)
            return rows[min(len(rows)-1,int(len(rows)*.95))] if rows else 0
        latest_quote=max(self.quotes.items(),key=lambda item:item[1].get('time',0),default=(None,{}))
        latest_ticker=self.tickers.get(latest_quote[0],{}) if latest_quote[0] else {}
        price_age=max(0,event_now-latest_quote[1].get('time',0)) if latest_quote[0] else None
        valid_books=sum(1 for book in self.books.values() if book.get('valid'))
        diagnostics={'started_at':self.started_at,'window_start':cutoff,
            'window_minutes':max(0,min(60,(now-self.started_at)//60_000)),
            'last_closed_candle_scan':last_scan,'checked_closed_candles_last_hour':len(events),
            'raw_breakouts_last_hour':sum(e['raw'] for e in events),
            'new_breakouts_last_hour':sum(e['new'] for e in events),
            'duplicate_breakouts_last_hour':sum(e['duplicate'] for e in events),
            'last_signal_at':latest_signal,'streams_expected':expected_streams,
            'streams_current':expected_streams-len(stale),'stale_streams':stale,
            'candle_recovery':{'status':self.candle_recovery_status,
                'pending_streams':self.candle_recovery_pending,
                'next_retry_at':self.next_candle_recovery or None},
            'live_detection_queue_depth':self.split_detection_queue.current_qsize() if self.split_mode else self.detection_queue.qsize(),
            'history_detection_queue_depth':self.split_detection_queue.history_qsize() if self.split_mode else 0,
            'history_pending_streams':sum(1 for key,cursor in list(self.history_cursor_cache.items())
                if cursor is not None and self.live_received_close.get(key,0)>cursor+(1440 if key[1]=='D' else int(key[1]))*60_000),
            'live_warming_streams':sum(1 for symbol in self.symbols for tf in self.engine.settings.timeframes
                if not self.live_window_ready((symbol,tf))),
            'candle_queue_depth':self.candle_queue.qsize(),
            'recovery_candle_queue_depth':self.recovery_candle_queue.qsize(),
            'market_message_queue_depth':self.market_message_queue.qsize(),
            'market_message_queue_wait_p95_ms':p95(self.market_message_queue_wait_ms),
            'detection_queue_depth':self.split_detection_queue.qsize() if self.split_mode else self.detection_queue.qsize(),
            'detection_queue_wait_p95_ms':p95(self.detection_queue_wait_ms),
            'detection_processing_p95_ms':p95(self.detection_processing_ms),
            'live_detection_latency_p95_ms':p95(self.live_detection_latency_ms),
            'live_detection_samples':len(self.live_detection_latency_ms),
            'live_publish_latency_p95_ms':p95(self.live_publish_latency_ms),
            'live_publish_samples':len(self.live_publish_latency_ms),
            'recovered_detection_latency_p95_ms':p95(self.recovered_detection_latency_ms),
            'candle_queue_dropped':getattr(self,'candle_queue_dropped',0),
            'candle_queue_wait_p95_ms':p95(self.candle_queue_wait_ms),
            'candle_processing_p95_ms':p95(self.candle_processing_ms),
            'live_candle_queue_wait_p95_ms':p95(self.live_candle_queue_wait_ms),
            'live_candle_processing_p95_ms':p95(self.live_candle_processing_ms),
            'live_candle_transition_latency_p95_ms':p95(self.live_candle_transition_latency_ms),
            'live_candle_samples':len(self.live_candle_processing_ms),
            'recovered_candle_queue_wait_p95_ms':p95(self.recovered_candle_queue_wait_ms),
            'recovered_candle_processing_p95_ms':p95(self.recovered_candle_processing_ms),
            'quote_publish_p95_ms':p95(self.quote_publish_ms),
            'network_delay_p95_ms':p95(self.network_delay_ms),
            'database_write_p95_ms':p95(self.market_write_ms),
            'socket_delivery_p95_ms':p95(self.browser_delivery_ms),
            'browser_render_p95_ms':p95(self.browser_render_ms),
            'candle_stage_p95_ms':{name:p95(values) for name,values in self.candle_stage_ms.items()},
            'signal_stage_p95_ms':{name:p95(values) for name,values in self.signal_stage_ms.items()},
            'last_exchange_event_at':self.last_exchange_event_at,
            'last_broadcast_at':self.last_broadcast_at,'broadcast_error':self.broadcast_error,
            'latency_window_started_at':self.live_metrics_started_at,
            'event_loop_lag_p95_ms':p95(self.event_loop_lag_ms),
            'background_tasks':[{'name':task.get_name(),'running':not task.done(),
                'error':(self.safe_error(task.exception()) if task.done() and not task.cancelled() and task.exception() else '')}
                for task in self.tasks],
            'last_signal_detected_at':self.last_signal_detected_at,
            'ancillary_connected':self.ancillary_connected,
            'ancillary_last_message':self.ancillary_last_message,
            'last_price_received_at':self.last_price_received_at,
            'last_kline_received_at':self.last_kline_received_at,
            'market_channels':{
                'ticker':{'status':'valid' if price_age is not None and price_age<=30_000 else
                    'stale' if price_age is not None else 'unavailable','event_at':latest_quote[1].get('time'),
                    'received_at':self.last_price_received_at,'age_ms':price_age},
                'candles':{'status':'valid' if self.engine.connected and self.last_kline_received_at and
                    now-self.last_kline_received_at<=30_000 else 'stale' if self.last_kline_received_at else 'warming_up',
                    'received_at':self.last_kline_received_at},
                'orderbook':{'status':'valid' if self.ancillary_connected and valid_books else
                    'warming_up' if self.ancillary_connected else 'unavailable','valid_books':valid_books,
                    'last_message':self.ancillary_last_message},
                'trade_flow':{'status':'valid' if self.ancillary_connected and self.ancillary_flow_started_at and
                    now-self.ancillary_flow_started_at>=15*60_000 else 'warming_up' if self.ancillary_connected else 'unavailable',
                    'connected_since':self.ancillary_flow_started_at},
                'liquidations':{'status':'valid' if self.ancillary_connected and self.ancillary_liquidation_started_at
                    and now-self.ancillary_liquidation_started_at>=15*60_000 else 'warming_up' if self.ancillary_connected else 'unavailable',
                    'connected_since':self.ancillary_liquidation_started_at},
                'open_interest':{'status':'valid' if latest_ticker.get('_openInterest_time') else 'unavailable',
                    'event_at':latest_ticker.get('_openInterest_time'),
                    'received_at':latest_ticker.get('_openInterest_received_at')},
            },
            'last_closed_scan_by_timeframe':{tf:max((timestamp for (symbol,key_tf),timestamp
                in self.last_closed_scan.items() if key_tf==tf),default=0)
                for tf in self.engine.settings.timeframes}}
        return self.engine.status(now_ms()) | {'symbols':self.symbols,'phase':self.phase,
            'error':self.error,'fee_error':self.fee_error,'fee_mode':getattr(self.bybit,'fee_mode','standard'),'last_message':self.last_message,
            'account_monitor':self.account_monitor.public_status(),
            'pending':len(self.pending),'recovery_time':self.recovery_time,
            'strategy_approval':self.research,'strategy_variant':self.engine.strategy_variant,
            'open_interest_error':self.open_interest_error,
            'spot_connection':{'connected':self.spot_connected,'last_message':self.spot_last_message,
                'error':self.spot_stream_error or self.spot_match_error,
                'matched_pairs':len(self.spot_symbols)},
            'setup_count':len(self.active_setups()),'scanner_diagnostics':diagnostics}

    def signal_feed(self,limit=200,before_time=None,before_id=None,statuses=None,signal_scope='all'):
        if signal_scope not in ('all','current','history'):
            raise ValueError('Invalid signal scope')
        cached=signal_scope=='all' and before_time is None and before_id is None and not statuses
        cached_rows=self.feed_cache
        if cached and limit<=200 and cached_rows is not None and now_ms()-self.feed_cache_at<200:
            return cached_rows[:limit]
        entry_only=bool(statuses and 'entry_available' in statuses)
        storage_statuses=[status for status in (statuses or []) if status!='entry_available'] or None
        source_limit=200 if cached and limit<=200 else limit
        if not entry_only and signal_scope=='all':
            rows=self.engine.store.signals(source_limit,before_time,before_id,storage_statuses)
        else:
            rows=[];cursor_time=before_time;cursor_id=before_id
            batch_size=max(100,limit)
            now=self.market_now()
            after_time=(now-max(map(int,self.engine.settings.timeframes))*60_000
                        if signal_scope=='current' else None)
            while len(rows)<limit:
                page=self.engine.store.signals(batch_size,cursor_time,cursor_id,storage_statuses,after_time=after_time)
                if not page:break
                for row in page:
                    timely=self.signal_time_is_current(row,now)
                    if signal_scope=='current' and not timely:continue
                    if entry_only or (signal_scope!='all' and timely):
                        signal=Signal(**{key:value for key,value in row.items()
                                         if key in Signal.__dataclass_fields__})
                        assessment=self.assess_signal_entry(signal,now)
                        if entry_only and assessment['status']!='available':continue
                        if signal_scope!='all' and self.signal_scope_for(row,assessment,now)!=signal_scope:continue
                    rows.append(row)
                    if len(rows)>=limit:break
                cursor_time=int(page[-1].get('event_at') or page[-1].get('time') or 0)
                cursor_id=page[-1]['id']
                if len(page)<batch_size:break
        by_id={row['id']:row for row in rows}
        if before_time is None:
            for signal in self.pending:
                if not storage_statuses or signal.status in storage_statuses:
                    by_id.setdefault(signal.id,signal.dict())
        result=[]
        now=self.market_now()
        for source in by_id.values():
            row=dict(self.public_signal(source))
            row['event_at']=int(row.get('event_at') or row.get('time',0) or 0)
            signal=Signal(**{key:value for key,value in row.items()
                             if key in Signal.__dataclass_fields__})
            row['entry_assessment']=self.assess_signal_entry(signal,now)
            row['stage']='confirmed'
            row['signal_scope']=self.signal_scope_for(row,row['entry_assessment'],now)
            if entry_only and row['entry_assessment']['status']!='available':
                continue
            if signal_scope!='all' and row['signal_scope']!=signal_scope:
                continue
            result.append(row)
        result.sort(key=lambda row:(row.get('event_at',row.get('time',0)),row.get('id','')),reverse=True)
        self.last_signal_at=max(self.last_signal_at,max((int(row.get('time',0) or 0) for row in result),default=0))
        if cached and limit<=200:
            cache_rows=result[:200]
            self.feed_cache_json=json.dumps(cache_rows,separators=(',',':')).encode('utf-8')
            self.feed_cache=cache_rows
            self.feed_cache_at=now_ms()
            return cache_rows[:limit]
        if before_time is not None:
            result=[row for row in result if (row['event_at']<before_time or
                (row['event_at']==before_time and row['id']<(before_id or '')))]
        return result[:limit]

    @staticmethod
    def signal_time_is_current(row,now):
        delivery=(row.get('filter_results') or {}).get('delivery') or {}
        event=int(row.get('event_at') or row.get('time') or 0)
        try:interval=int(row.get('timeframe') or 0)*60_000
        except (ValueError,TypeError):interval=0
        historical=delivery.get('mode')=='history' or (delivery.get('mode') is None and delivery.get('recovered'))
        return not historical and interval>0 and 0<=now-event<=interval

    @staticmethod
    def signal_scope_for(row,assessment,now):
        terminal=(assessment or {}).get('status') in ('late','scenario_invalidated','target_reached')
        return 'current' if Scanner.signal_time_is_current(row,now) and not terminal else 'history'

    def summary_signal(self,row):
        compact={key:row[key] for key in (
            'id','symbol','timeframe','pattern','side','status','time','start',
            'event_at','received_at','detected_at','published_at','signal_scope','boundary','stop','target','atr',
            'entry','expected_net','expected_fees','rr','strategy_version',
            'strategy_variant','entry_method') if key in row}
        if row.get('reason'):
            compact['reason']=str(row['reason'])[:80]+('…' if len(str(row['reason']))>80 else '')
        compact['lines']=[];compact['pivots']=[]
        filters=row.get('filter_results') or {}
        market=filters.get('market_context') or {}
        compact['filter_results']={
            'market_context':{key:market[key] for key in ('scenario','eligible') if key in market},
            **{key:filters[key] for key in ('confirmation_pass','volume_pass','auto_entry_allowed')
               if key in filters}}
        delivery=filters.get('delivery') or {}
        if delivery:
            compact['filter_results']['delivery']={key:delivery[key] for key in (
                'timing_version','source','mode','received_at','detected_at','published_at',
                'delay_ms','publish_delay_ms','recovered') if key in delivery}
        assessment=row.get('entry_assessment') or {}
        if assessment:
            compact['entry_assessment']={key:assessment[key] for key in (
                'status','price','price_min','price_max','net_rr','quote_age_ms')
                if key in assessment}
        elif 'entry_assessment' in row:
            compact['entry_assessment']=None
        instrument=self.engine.instruments.get(row.get('symbol'))
        background=self.market_background(row.get('symbol'),row.get('side','long'),
            self.market_now(),getattr(instrument,'asset_class','crypto'))
        compact['market_background_summary']=compact_market_advisory(background)
        if instrument:
            compact['asset_class']=instrument.asset_class
            compact['instrument_name']=instrument.display_name
        quote=self.tickers.get(row.get('symbol'),{})
        compact['quote_quality']={'status':'valid' if now_ms()-int(quote.get('_quote_received_at',0) or 0)<=30_000
                else 'stale' if quote.get('_quote_received_at') else 'unavailable',
            'source':'bybit_ticker' if quote.get('_quote_received_at') else None,
            'event_at':quote.get('_quote_time'),'received_at':quote.get('_quote_received_at'),
            'age_ms':max(0,self.market_now()-int(quote.get('_quote_time',0) or 0))
                if quote.get('_quote_received_at') else None}
        return compact

    @staticmethod
    def summary_setup(row):
        compact={key:row[key] for key in ('id','symbol','timeframe','pattern','side','status',
            'detected_at','received_at','published_at','recovered','delivery','updated_at','event_at',
            'preview_at','preview_state','scalp_status')
            if key in row}
        snapshot=row.get('snapshot') or {}
        compact['snapshot']={key:snapshot[key] for key in (
            'start','detected_on_bar','boundary','stop','target','height','atr') if key in snapshot}
        pivots=snapshot.get('pivots') or []
        compact['snapshot']['lines']=[];compact['snapshot']['pivots']=[]
        compact['snapshot']['shape_key']=hashlib.sha256(json.dumps(pivots,
            sort_keys=True,separators=(',',':')).encode()).hexdigest()[:16]
        current=row.get('current') or {}
        compact['current']={}
        compact['current']['market_background_summary']=compact_market_advisory(
            current.get('market_background'))
        plan=current.get('scalp_plan') or {}
        if plan:
            compact['current']['scalp_plan']={key:plan[key] for key in (
                'status','reason','preliminary','boundary','entry','stop','target','net_rr')
                if key in plan}
            compact['current']['scalp_plan']['market_background_summary']=compact_market_advisory(
                plan.get('market_background'))
            reason=compact['current']['scalp_plan'].get('reason')
            if reason:compact['current']['scalp_plan']['reason']=str(reason)[:80]+('…' if len(str(reason))>80 else '')
        return compact

    def summary_status(self):
        state=self.status()
        diagnostics=state.get('scanner_diagnostics') or {}
        diagnostic_keys=(
            'window_start','window_minutes','last_closed_candle_scan',
            'checked_closed_candles_last_hour','raw_breakouts_last_hour','new_breakouts_last_hour',
            'last_signal_at','streams_expected','streams_current','candle_queue_depth',
            'recovery_candle_queue_depth','candle_recovery','detection_queue_depth','live_detection_queue_depth',
            'history_detection_queue_depth','history_pending_streams','live_warming_streams','detection_queue_wait_p95_ms',
            'live_detection_latency_p95_ms','live_detection_samples','live_publish_latency_p95_ms','live_publish_samples',
            'live_candle_transition_latency_p95_ms','live_candle_queue_wait_p95_ms','live_candle_samples','candle_queue_wait_p95_ms','database_write_p95_ms',
            'network_delay_p95_ms','socket_delivery_p95_ms','browser_render_p95_ms',
            'last_exchange_event_at','last_broadcast_at','last_price_received_at',
            'last_kline_received_at','market_channels','last_closed_scan_by_timeframe')
        compact={key:diagnostics[key] for key in diagnostic_keys if key in diagnostics}
        stale=diagnostics.get('stale_streams') or []
        compact['stale_streams']=[{'symbol':row.get('symbol'),'timeframe':row.get('timeframe')}
                                  for row in stale[:12]]
        if len(stale)>12:compact['stale_streams_omitted']=len(stale)-12
        state['scanner_diagnostics']=compact
        return state

    def snapshot_payload(self,limit=200,setup_limit=300,view='full',signal_scope='all'):
        now=self.market_now()
        setups=[dict(item) for item in self.engine.store.setups(setup_limit)]
        for setup in setups:
            quote=self.quotes.get(setup['symbol'],{})
            if setup['status'] in ('wait_breakout','near_boundary','crossing'):
                if quote and now-quote.get('time',0)<=5000:
                    setup['current']=self.setup_price_state(setup,quote['price'],quote['time'],persist=False)
                else:
                    setup['current']={'status':'stale','quote_stale':True}
            self.decorate_setup(setup,now)
        signals=self.signal_feed(limit,signal_scope=signal_scope)
        if view=='summary':
            signals=[self.summary_signal(row) for row in signals]
            setups=[self.summary_setup(row) for row in setups]
        payload={'type':'snapshot','published_at':now_ms(),
            'state':self.summary_status() if view=='summary' else self.status(),
            'signals':signals,'trades':self.engine.trades[-(50 if view=='summary' else 200):],
            'setups':setups}
        if view=='summary':payload['view']='summary'
        return self.publisher.initial(payload)

    def versioned_delta(self,payload):
        return self.publisher.publish(payload)

    def stamp_publications(self,payload):
        """Stamp the first actual handoff to an HTTP or websocket client."""
        published=now_ms()
        for kind in ('signals','setups'):
            for row in payload.get(kind,()):
                filters=row.get('filter_results') if kind=='signals' else row
                if not isinstance(filters,dict):continue
                delivery=filters.get('delivery')
                if not isinstance(delivery,dict) or delivery.get('timing_version')!=2:continue
                existing=row.get('published_at') or delivery.get('published_at')
                if existing:
                    row['published_at']=existing
                    continue
                event=int(delivery.get('exchange_event_at') or row.get('event_at') or 0)
                key=(kind,row.get('id'),delivery.get('mode'),event)
                stamp=self.publication_times.get(key)
                if stamp is None:
                    stamp=published
                    self.publication_times[key]=stamp
                    while len(self.publication_times)>10_000:self.publication_times.popitem(last=False)
                delay=max(0,stamp+int(getattr(self.bybit,'offset',0) or 0)-event) if event else 0
                row['published_at']=stamp
                delivery['published_at']=stamp
                delivery['publish_delay_ms']=delay
                update={'id':row['id'],'published_at':stamp,
                        'delivery':{'mode':delivery.get('mode'),'published_at':stamp,'publish_delay_ms':delay}}
                self.publication_pending[kind][row['id']]=update
                if kind=='signals':
                    receipt=self.live_signal_receipts.pop(row['id'],None)
                    if receipt is not None:
                        self.live_publish_latency_ms.append(max(0,int((asyncio.get_running_loop().time()-receipt)*1000)))
                    for signal in self.pending:
                        if signal.id==row['id']:
                            signal.published_at=stamp
                            signal.filter_results.setdefault('delivery',{}).update(update['delivery'])
                self.publication_event.set()
        return payload

    async def flush_publications(self):
        signals=self.publication_pending['signals'];setups=self.publication_pending['setups']
        self.publication_pending={'signals':{},'setups':{}}
        try:
            if signals:await self.engine.store.aupdate_signal_deliveries(list(signals.values()))
            if setups:await self.engine.store.aupdate_setup_deliveries(list(setups.values()))
        except BaseException:
            for kind,rows in (('signals',signals),('setups',setups)):
                for key,value in rows.items():self.publication_pending[kind].setdefault(key,value)
            raise

    async def persist_publications_loop(self):
        while True:
            await self.publication_event.wait()
            self.publication_event.clear()
            try:await self.flush_publications()
            except asyncio.CancelledError:raise
            except Exception:
                logging.getLogger(__name__).exception('Publication timing persistence failed')
                await asyncio.sleep(1)
                self.publication_event.set()

    async def v3_send_loop(self,ws,queue):
        try:
            while True:
                item=await queue.get()
                payload,queued_at=item if isinstance(item,tuple) else (item,asyncio.get_running_loop().time())
                try:await ws.send_json(payload)
                finally:
                    self.browser_delivery_ms.append(max(0,int((asyncio.get_running_loop().time()-queued_at)*1000)))
                    queue.task_done()
        except asyncio.CancelledError:
            raise
        except Exception:
            self.clients.discard(ws)
            self.v3_clients.pop(ws,None)
            self.v3_sender_tasks.pop(ws,None)
            self.watched_context.pop(ws,None);self.watched_setups.pop(ws,None)
            self.watched_charts.pop(ws,None);self.account_watchers.discard(ws)
            self.context_refresh_event.set()
            with contextlib.suppress(Exception):await ws.close(code=1013,reason='Client send queue failed')

    @property
    def stream_id(self):return self.publisher.stream_id

    @property
    def stream_revision(self):return self.publisher.revision

    def signal_feed_json(self,limit=200):
        if limit==200:
            rows=self.signal_feed(200)
            return json.dumps(rows,separators=(',',':')).encode('utf-8')
        return json.dumps(self.signal_feed(limit),separators=(',',':')).encode('utf-8')

    def invalidate_feed_cache(self):
        self.feed_cache=None
        self.feed_cache_json=None

    def active_setups(self):
        if self.split_mode:
            return self.active_setup_cache or []
        if self.active_setup_cache is None or now_ms()-self.active_setup_cache_at>=1000:
            self.active_setup_cache=self.engine.store.active_setups()
            self.active_setup_cache_at=now_ms()
            by_stream={}
            for setup in self.active_setup_cache:
                by_stream.setdefault((setup['symbol'],setup['timeframe']),[]).append(setup)
            self.active_setup_streams=by_stream
        return self.active_setup_cache

    async def refresh_active_setup_cache(self):
        while True:
            try:
                if self.active_setup_cache is None or now_ms()-self.active_setup_cache_at>=1000:
                    revision=self.active_setup_revision
                    rows=await self.engine.store.aactive_setups()
                    if revision!=self.active_setup_revision:continue
                    self.active_setup_cache=rows
                    self.active_setup_cache_at=now_ms()
                    by_stream={}
                    for setup in rows:
                        by_stream.setdefault((setup['symbol'],setup['timeframe']),[]).append(setup)
                    self.active_setup_streams=by_stream
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.getLogger(__name__).exception('Active setup cache refresh failed')
            await asyncio.sleep(.25)

    def invalidate_active_setups(self):
        self.active_setup_revision+=1
        self.active_setup_cache=None
        self.active_setup_streams=None

    def active_setups_for(self,symbol,timeframe):
        self.active_setups()
        return (self.active_setup_streams or {}).get((symbol,timeframe),())

    def forecast_setups(self):
        """Return the active forecast rows shown first in the setup feed."""
        return sorted(self.active_setups(),
            key=lambda row:(int(row.get('event_at',0) or 0),str(row.get('id',''))),
            reverse=True)[:50]

    def reconcile_active_setup_cache(self,updates):
        """Apply lifecycle changes in memory instead of reloading SQLite rows."""
        if self.active_setup_cache is None or not updates:
            return
        self.active_setup_revision+=1
        changed_ids={row['id'] for row in updates}
        active_statuses={'wait_breakout','near_boundary','crossing'}
        rows=[row for row in self.active_setup_cache if row['id'] not in changed_ids]
        rows.extend(row for row in updates if row.get('status') in active_statuses)
        rows.sort(key=lambda row:(row.get('detected_at',0),row['id']),reverse=True)
        self.active_setup_cache=rows
        by_stream={}
        for row in rows:
            by_stream.setdefault((row['symbol'],row['timeframe']),[]).append(row)
        self.active_setup_streams=by_stream

    def save_setup(self,setup):
        result=self.engine.store.save_setup(setup)
        if result:
            self.setup_cache=None
            self.broadcast_event.set()
        if result in ('created','status'):
            self.invalidate_active_setups()
        return result

    def save_setups(self,setups):
        values=list(setups)
        if not values:return []
        results=self.engine.store.save_setups(values)
        if any(results):
            self.setup_cache=None
            self.broadcast_event.set()
        if any(result in ('created','status') for result in results):
            self.invalidate_active_setups()
        return results

    def market_now(self):
        """Current time in the exchange's event-time domain."""
        return now_ms()+int(getattr(self.bybit,'offset',0) or 0)

    def market_buffers(self):
        return self.live_buffers if self.split_mode else self.buffers

    def market_background(self,symbol,side,timestamp=None,asset_class=None):
        """Reuse the 4h/daily histories and cache until a candle is corrected or closed."""
        timestamp=self.market_now() if timestamp is None else int(timestamp)
        symbol=str(symbol or '').upper()
        instrument=self.engine.instruments.get(symbol)
        asset_class=asset_class or getattr(instrument,'asset_class','crypto')
        histories={}
        source=self.market_buffers()
        for pair,tf,limit,interval in (
                (symbol,'240',181,FOUR_HOURS_MS),(symbol,'D',15,86_400_000),
                ('BTCUSDT','240',181,FOUR_HOURS_MS),('BTCUSDT','D',15,86_400_000)):
            if (pair==symbol and tf in ('240','D')) or (pair=='BTCUSDT' and tf in ('240','D')):
                key=(pair,tf)
                rows=list(source.get(key,()))
                if len(rows)<limit and self.engine.store.path!=':memory:':
                    try:
                        stored=self.engine.store.candles_range(pair,tf,
                            limit=max(limit,len(rows)))
                    except (OSError,RuntimeError,ValueError,sqlite3.Error):
                        stored=[]
                    merged={row.time:row for row in stored}
                    merged.update({row.time:row for row in rows})
                    rows=sorted(merged.values(),key=lambda row:row.time)[-max(limit,len(rows)):]
                histories[key]=[row for row in rows if row.time+interval<=timestamp]
        signature=(symbol,side,asset_class,timestamp//FOUR_HOURS_MS,
            tuple((row.time,row.open,row.high,row.low,row.close)
                  for row in histories.get((symbol,'240'),())[-181:]),
            tuple((row.time,row.open,row.high,row.low,row.close)
                  for row in histories.get((symbol,'D'),())[-15:]),
            tuple((row.time,row.open,row.high,row.low,row.close)
                  for row in histories.get(('BTCUSDT','240'),())[-181:]),
            tuple((row.time,row.open,row.high,row.low,row.close)
                  for row in histories.get(('BTCUSDT','D'),())[-15:]))
        cached=self.market_background_cache.get(signature)
        if cached is not None:
            self.market_background_cache.move_to_end(signature)
            return cached | {'as_of':timestamp}
        result=build_market_background(symbol,side,timestamp,histories,asset_class)
        self.market_background_cache[signature]=result
        self.market_background_cache.move_to_end(signature)
        while len(self.market_background_cache)>512:
            self.market_background_cache.popitem(last=False)
        return result

    def apply_or_buffer_position_tick(self,symbol,price,timestamp):
        if self.positions_ready:
            return self.engine.tick(symbol,price,timestamp,persist=False)
        if any(position['symbol']==symbol for position in self.engine.positions):
            if len(self.position_recovery_ticks)>=self.position_recovery_tick_limit:
                self.position_recovery_ticks.clear()
                self.position_recovery_overflow=True
            self.position_recovery_ticks.append((timestamp,symbol,price))
        return []

    def assess_signal_entry(self,signal,now=None):
        quote=self.quotes.get(signal.symbol,{})
        ticker=self.tickers.get(signal.symbol,{})
        fee=self.engine.fees.get(signal.symbol,{})
        instrument=self.engine.instruments.get(signal.symbol)
        required=max(float(self.engine.settings.min_rr),
            float(signal.filter_results.get('required_net_rr',0) or 0))
        return assess_entry(side=signal.side,price=quote.get('price'),quote_time=quote.get('time'),
            now=self.market_now() if now is None else now,boundary=signal.boundary,
            stop=signal.stop,target=signal.target,atr=signal.atr,
            fee=fee.get('rate'),fee_time=fee.get('time'),fee_source=fee.get('source'),
            fee_mode=self.engine.fee_mode,slippage=self.engine.settings.slippage,
            funding_rate=float(ticker['fundingRate']) if 'fundingRate' in ticker else None,
            funding_time=ticker.get('_funding_time'),
            tick_size=getattr(instrument,'tick_size',0) if instrument else 0,
            min_rr=required)

    def assess_signal_execution(self,signal,price,now):
        instrument=self.engine.instruments.get(signal.symbol)
        fee=self.engine.fees.get(signal.symbol,{}).get('rate',.00055)
        stop_distance=abs(price-signal.stop)
        risk_budget=max(0.,self.engine.equity())*self.engine.settings.risk_fraction
        quantity=risk_budget/stop_distance if stop_distance>0 else 0.
        if instrument:
            margin_unit=price/self.engine.settings.leverage+price*fee
            if margin_unit>0:
                quantity=min(quantity,max(0.,self.engine.available())/margin_unit,instrument.max_qty)
        book=self.books.get(signal.symbol)
        metrics=_book((book or {})|{'now':now},signal.side,quantity,price)
        try:
            max_spread=float(os.getenv('MAX_ENTRY_SPREAD_BPS','15'))
        except (TypeError,ValueError):
            max_spread=15.
        reasons=[]
        if not metrics.get('available'):
            reasons.append('orderbook is unavailable')
        elif metrics.get('age_ms',now)>5000:
            reasons.append('orderbook is stale')
        if metrics.get('spread_bps') is not None and metrics['spread_bps']>max_spread:
            reasons.append(f"spread exceeds {max_spread:g} bps")
        if metrics.get('estimated_fill_ratio') is not None and metrics['estimated_fill_ratio']<.999:
            reasons.append('top 10 orderbook levels do not cover the estimated order')
        if quantity<=0:
            reasons.append('estimated executable quantity is zero')
        return {'ready':not reasons,'status':'available' if not reasons else 'blocked',
                'estimated_quantity':quantity,'max_spread_bps':max_spread,
                'orderbook':metrics,'reasons':reasons}

    def assess_setup_entry(self,setup,now=None,requires_breakout=None):
        quote=self.quotes.get(setup['symbol'],{})
        ticker=self.tickers.get(setup['symbol'],{})
        fee=self.engine.fees.get(setup['symbol'],{})
        snap=setup['snapshot'];timestamp=quote.get('time')
        if requires_breakout is None:
            requires_breakout=setup.get('status')!='breakout_confirmed'
        boundary=self._setup_boundary(setup,setup['side'],timestamp or self.market_now())
        instrument=self.engine.instruments.get(setup['symbol'])
        return assess_entry(side=setup['side'],price=quote.get('price'),quote_time=timestamp,
            now=self.market_now() if now is None else now,boundary=boundary,stop=snap['stop'],
            target=snap['target'],atr=snap['atr'],fee=fee.get('rate'),fee_time=fee.get('time'),
            fee_source=fee.get('source'),fee_mode=self.engine.fee_mode,
            slippage=self.engine.settings.slippage,
            funding_rate=float(ticker['fundingRate']) if 'fundingRate' in ticker else None,
            funding_time=ticker.get('_funding_time'),
            tick_size=getattr(instrument,'tick_size',0) if instrument else 0,
            min_rr=self.engine.settings.min_rr,requires_breakout=requires_breakout)

    async def attach_signal_snapshots(self,signals):
        async def attach(signal):
            async with self.snapshot_semaphore:
                buffers={key:list(rows) for key,rows in self.market_buffers().items()}
                tickers={key:dict(row) for key,row in self.tickers.items()}
                flows={key:dict(row) for key,row in self.live_flow.items()}
                books={key:(dict(row)|{'bids':dict(row.get('bids',{})),
                    'asks':dict(row.get('asks',{}))}) for key,row in self.books.items()}
                snapshot=await asyncio.to_thread(build_context,signal,self.market_now(),buffers,
                    tickers,list(self.symbols),self.engine.store,flows,books,
                    self.engine.store,dict(self.engine.fees),self.engine.settings.slippage,
                    self.engine.fee_mode,dict(self.spot_matches),dict(self.spot_books),self.spot_match_error,
                    instruments=self.engine.instruments,
                    market_background=self.market_background(signal.symbol,signal.side,self.market_now(),
                        getattr(self.engine.instruments.get(signal.symbol),'asset_class','crypto')))
            signal.filter_results['decision_snapshot']=snapshot
            await self.engine.store.aupdate_signal_snapshot(signal.id,snapshot)
            observation=make_observation(signal.dict(),snapshot,
                getattr(self.engine.instruments.get(signal.symbol),'tick_size',0.))
            if observation is not None:
                await self.engine.store.arecord_forecast_observation(observation)
            self.invalidate_feed_cache()
            self.broadcast_event.set()
        await asyncio.gather(*(attach(signal) for signal in signals),return_exceptions=True)

    async def forecast_outcome_loop(self):
        """Resolve frozen 15m forecast snapshots as their future 5m bars close."""
        refreshed_at=0
        while True:
            now=self.market_now()
            try:
                observations=await self.engine.store.apending_forecast_observations(256)
                for observation in observations:
                    start=((int(observation.get('available_at') or 0)+300_000-1)//300_000)*300_000
                    candles=await asyncio.to_thread(self.engine.store.candles_range,
                        observation['symbol'],'5',limit=6,start=start,end=start+30*60_000)
                    outcomes=evaluate_horizons(observation,candles,now)
                    if outcomes:
                        changed=await self.engine.store.aupdate_forecast_outcomes(
                            observation['id'],outcomes,now)
                        if changed:self.broadcast_event.set()
                if now-refreshed_at>=30_000:
                    await self.refresh_forecast_statistics()
                    refreshed_at=now
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.getLogger(__name__).exception('Forecast outcome processing failed')
            await asyncio.sleep(5)

    async def refresh_forecast_statistics(self):
        """Build an in-memory index so HTTP and websocket context stays non-blocking."""
        if self.forecast_statistics_refresh_lock.locked():return
        async with self.forecast_statistics_refresh_lock:
            rows=await self.engine.store.aforecast_dataset(limit=20_000)
            grouped=await asyncio.to_thread(build_statistics_index,rows)
            self.forecast_statistics_cache=grouped
            self.forecast_statistics_updated_at=self.market_now()
            self.broadcast_event.set()

    def record_candle_stage(self,name,started):
        values=self.candle_stage_ms.setdefault(name,deque())
        values.append(max(0,int((asyncio.get_running_loop().time()-started)*1000)))
        if len(values)>300:
            values.popleft()

    def record_signal_stage(self,name,started):
        values=self.signal_stage_ms[name]
        values.append(max(0,int((asyncio.get_running_loop().time()-started)*1000)))
        if len(values)>300:
            values.popleft()

    def signal_context_scope(self):
        """Pairs and intervals needed for active forecasts and selected contexts."""
        scope={}
        now=now_ms()
        self.requested_setup_contexts={sid:expires for sid,expires in self.requested_setup_contexts.items()
                                       if expires>now}
        # Active forecasts need live candles and quotes so their plan can remain
        # current even before a user selects a card.
        rows=[signal.dict() for signal in self.pending]
        rows.extend(self.forecast_setups())
        for signal_id in self.watched_context.values():
            row=self.signal_row(signal_id) if signal_id else None
            if row:
                rows.append(row)
        setup_ids=set(self.watched_setups.values())|set(self.requested_setup_contexts)
        for setup_id in setup_ids:
            setup=self.engine.store.setup(setup_id) if setup_id else None
            if setup:
                rows.append(setup)
        for row in rows:
            symbol=row.get('symbol');tf=str(row.get('timeframe',''))
            if not symbol or tf not in self.engine.settings.timeframes:
                continue
            scope.setdefault(symbol,set()).update((tf,BIAS_TF,FIGURE_TF,ENTRY_TF,'D'))
        if any(getattr(self.engine.instruments.get(symbol),'asset_class','crypto')=='crypto'
               for symbol in scope):
            scope.setdefault('BTCUSDT',set()).update(('240','D'))
        return scope

    def tracked_symbols(self):
        return (set(self.symbols)|{p['symbol'] for p in self.engine.positions}|set(self.context_scope)
                |{c['symbol'] for c in self.watched_charts.values() if c})

    def selected_signal_symbols(self):
        return {row['symbol'] for signal_id in self.watched_context.values() if signal_id
                for row in [self.signal_row(signal_id)] if row}

    def spot_scope_symbols(self):
        return ({p['symbol'] for p in self.engine.positions}|self.selected_signal_symbols()
                |self.selected_setup_symbols())

    def subscription_topics(self):
        base=set(self.symbols)|{p['symbol'] for p in self.engine.positions}
        scope=self.context_scope
        tracked=base|set(scope)
        topics={f'tickers.{symbol}' for symbol in tracked}
        for symbol in base:
            topics.update(f'kline.{tf}.{symbol}' for tf in [*self.engine.settings.timeframes,'D'])
        for symbol,timeframes in scope.items():
            topics.update(f'kline.{tf}.{symbol}' for tf in timeframes)
        return topics

    def ancillary_topics(self):
        base=(set(self.market_symbols)|set(self.symbols)|{p['symbol'] for p in self.engine.positions}
              |self.selected_signal_symbols()|self.selected_setup_symbols())
        virtual_pending={row.get('symbol') for row in self.engine.strategy_pending if row.get('symbol')}
        detail=(self.selected_signal_symbols()|self.selected_setup_symbols()|virtual_pending|
            {p['symbol'] for p in self.engine.positions}|set(self.context_scope))
        topics={f'publicTrade.{symbol}' for symbol in base}
        topics.update(f'orderbook.200.{symbol}' for symbol in detail)
        topics.update(f'allLiquidation.{symbol}' for symbol in base)
        return topics

    def selected_setup_symbols(self):
        setup_ids=set(self.watched_setups.values())|set(self.requested_setup_contexts)
        return {row['symbol'] for setup_id in setup_ids if setup_id
                for row in [self.engine.store.setup(setup_id)] if row}

    def request_setup_context(self,setup_id):
        setup=self.engine.store.setup(setup_id)
        if not setup:
            return None
        self.requested_setup_contexts[setup_id]=now_ms()+30_000
        self.context_refresh_event.set()
        return setup

    @staticmethod
    def public_signal(row):
        """A detected breakout remains a manual signal when auto-entry is gated."""
        if row.get('status')=='rejected' and row.get('reason')=='Новая стратегия ещё не прошла исторический допуск':
            row=dict(row)
            row['status']='found'
            row['reason']='Пробой найден. Автоматический виртуальный вход отключён: стратегия ещё не прошла исторический допуск.'
            row['filter_results']=dict(row.get('filter_results') or {})
            row['filter_results']['auto_entry_allowed']=False
        return row

    def signal_row(self,signal_id):
        saved=self.engine.store.signal(signal_id)
        if saved:return self.public_signal(saved)
        row=next((signal.dict() for signal in self.pending if signal.id==signal_id),None)
        return self.public_signal(row) if row else None

    async def broadcast_loop(self):
        while True:
            try:
                await asyncio.wait_for(self.broadcast_event.wait(),.25)
            except asyncio.TimeoutError:
                pass
            try:
                self.broadcast_event.clear()
                now=now_ms()
                legacy_ready=now-self.last_legacy_broadcast_at>=1000
                v2_ready=now-self.last_v2_broadcast_at>=1000
                legacy_clients=[ws for ws in self.clients if ws not in self.v2_clients and ws not in self.v3_clients]
                v2_clients=list(self.v2_clients)
                full_ready=bool((legacy_ready and legacy_clients) or (v2_ready and v2_clients))
                full_payloads={};v2_deltas={};summary_compacts={};summary_deltas={}
                # Keep legacy snapshots at 1Hz; v3 remains the low-latency
                # compact feed. Feed decoration stays off the market loop.
                scopes=set()
                if legacy_ready:scopes.update(self.client_scopes.get(ws,'all') for ws in legacy_clients)
                if v2_ready:scopes.update(self.client_scopes.get(ws,'all') for ws in v2_clients)
                for scope in scopes:
                    payload=await asyncio.to_thread(self.snapshot_payload,200,300,'full',scope)
                    self.stamp_publications(payload)
                    full_payloads[scope]=payload
                    if v2_ready and any(self.client_scopes.get(ws,'all')==scope for ws in v2_clients):
                        compact=payload|{'signals':payload['signals'][:50],
                            'setups':payload['setups'][:50],'trades':payload['trades'][-50:]}
                        v2_deltas[scope]=self.v2_publishers[scope].publish(compact)
                for scope in {self.client_scopes.get(ws,'all') for ws in self.v3_clients}:
                    summary_payload=await asyncio.to_thread(self.snapshot_payload,50,50,'summary',scope)
                    self.stamp_publications(summary_payload)
                    compact=summary_payload|{'signals':summary_payload['signals'][:50],
                        'setups':summary_payload['setups'][:50],'trades':summary_payload['trades'][-50:]}
                    summary_compacts[scope]=compact
                    summary_deltas[scope]=self.v3_publishers[scope].publish(compact)
                if not (full_ready or self.v3_clients):
                    continue
                quote_batch=dict(self.quote_broadcast_pending)
                async def send_client(ws):
                    try:
                        scope=self.client_scopes.get(ws,'all')
                        if ws in self.v3_clients:
                            message=summary_deltas[scope]
                        elif ws in self.v2_clients:
                            if not v2_ready:return
                            message=v2_deltas[scope]
                        else:
                            if not legacy_ready:return
                            message=full_payloads[scope]
                        sid=self.watched_context.get(ws)
                        if sid:
                            message=message|{'context_signal_id':sid,
                                             'context':self.decision_context(sid)}
                        setup_id=self.watched_setups.get(ws)
                        if setup_id:
                            message=message|{'setup_context_id':setup_id,
                                             'setup_context':self.setup_context(setup_id)}
                        chart=self.watched_charts.get(ws)
                        if chart:
                            key=(chart['symbol'],chart['timeframe'])
                            message=message|{'live_chart':{'symbol':key[0],'timeframe':key[1],
                                'candle':self.live_candles.get(key),
                                'closed_candle':self.last_closed_candles.get(key),
                                'candle_version':(self.last_closed_candles.get(key) or {}).get('time'),
                                'price':self.quotes.get(key[0]),
                                'quote_stale':now_ms()-self.quotes.get(key[0],{}).get('time',0)>5000}}
                        if ws in self.account_watchers:
                            message=message|{'account':self.account_feed(50)}
                        if ws in self.v3_clients:
                            queue=self.v3_clients[ws]
                            if queue.full():
                                while not queue.empty():
                                    with contextlib.suppress(asyncio.QueueEmpty):queue.get_nowait();queue.task_done()
                                publisher=self.v3_publishers[scope]
                                fallback=summary_compacts[scope]|{'type':'snapshot',
                                    'stream_id':publisher.stream_id,'revision':publisher.revision}
                                for key in ('context_signal_id','context','setup_context_id','setup_context',
                                             'live_chart','account'):
                                    if key in message:fallback[key]=message[key]
                                queue.put_nowait((fallback,asyncio.get_running_loop().time()))
                            else:
                                queue.put_nowait((message,asyncio.get_running_loop().time()))
                            return
                        await asyncio.wait_for(ws.send_json(message),.75)
                    except Exception as exc:
                        self.clients.discard(ws)
                        self.v2_clients.pop(ws,None)
                        self.v3_clients.pop(ws,None)
                        self.client_scopes.pop(ws,None)
                        sender=self.v3_sender_tasks.pop(ws,None)
                        if sender and sender is not asyncio.current_task():sender.cancel()
                        self.watched_context.pop(ws,None)
                        self.watched_setups.pop(ws,None)
                        self.watched_charts.pop(ws,None)
                        self.account_watchers.discard(ws)
                        self.context_refresh_event.set()
                        self.open_interest_refresh_event.set()
                        with contextlib.suppress(Exception):
                            await ws.close(code=1013,reason='WebSocket client fell behind')
                        logging.getLogger(__name__).warning('Removed stale WebSocket client: %s',
                                                            self.safe_error(exc))
                await asyncio.gather(*(send_client(ws) for ws in list(self.clients)))
                self.last_broadcast_at=now_ms()
                self.broadcast_error=''
                if legacy_ready and legacy_clients:
                    self.last_legacy_broadcast_at=now
                if v2_ready and v2_clients:
                    self.last_v2_broadcast_at=now
                published_at=asyncio.get_running_loop().time()
                for symbol,received_at in quote_batch.items():
                    if self.quote_broadcast_pending.get(symbol)==received_at:
                        self.quote_broadcast_pending.pop(symbol,None)
                        self.quote_publish_ms.append(max(0,int((published_at-received_at)*1000)))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.broadcast_error=self.safe_error(exc)
                logging.getLogger(__name__).exception('WebSocket broadcast failed')
                await asyncio.sleep(.25)

    async def measure_event_loop(self):
        loop=asyncio.get_running_loop()
        while True:
            expected=loop.time()+1
            await asyncio.sleep(1)
            if self.live_metrics_started_at:
                self.event_loop_lag_ms.append(max(0,int((loop.time()-expected)*1000)))

    def begin_live_latency_window(self):
        if self.live_metrics_started_at:return
        self.live_metrics_started_at=now_ms()
        # Report steady-state service latency separately from historical
        # recovery and the initial market snapshot burst.
        self.event_loop_lag_ms.clear()
        self.market_message_queue_wait_ms.clear()
        self.quote_publish_ms.clear()
        self.market_write_ms.clear()
        self.network_delay_ms.clear()

    def live_window_ready(self,key):
        rows=self.live_buffers.get(key,())
        if len(rows)<25:return False
        if len(rows)<251 and self.live_short_history_start.get(key)!=rows[0].time:return False
        interval=(1440 if key[1]=='D' else int(key[1]))*60_000
        tail=rows[-251:]
        return (all(b.time-a.time==interval for a,b in zip(tail,tail[1:])) and
                tail[-1].time+interval>=(self.market_now()//interval)*interval)

    def request_live_hydration(self,symbol,tf):
        key=(symbol,tf)
        if now_ms()<self.live_hydration_retry_at.get(key,0):return
        previous=self.live_hydration_tasks.get(key)
        if previous and not previous.done():return
        task=asyncio.create_task(self.hydrate_live_stream(symbol,tf),name=f'live-history-{symbol}-{tf}')
        self.live_hydration_tasks[key]=task
        def finished(done):
            if self.live_hydration_tasks.get(key) is done:self.live_hydration_tasks.pop(key,None)
        task.add_done_callback(finished)

    def schedule_live_observation(self,symbol,tf,candle,received_wall_ms,received_monotonic,source):
        key=(symbol,tf)
        observation=(symbol,tf,candle,received_wall_ms,received_monotonic,source)
        running=self.live_observation_by_stream.get(key)
        if running and not running.done():
            previous=self.live_observation_pending.get(key)
            newest=previous[2].time if previous else self.live_observation_running_time.get(key,0)
            if candle.time>newest:
                self.live_observation_pending[key]=observation
                if previous:self.history_dispatch_event.set()
            return
        async def drain():
            item=observation
            try:
                while item:
                    self.live_observation_running_time[key]=item[2].time
                    try:
                        await self.observe_live_candle(*item)
                        if self.error.startswith('Обработка текущей свечи:'):self.error=''
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        self.error=f'Обработка текущей свечи: {self.safe_error(exc)}'
                        logging.getLogger(__name__).exception('Current candle observation failed for %s/%s',*key)
                        self.history_dispatch_event.set()
                        self.request_live_hydration(*key)
                    item=self.live_observation_pending.pop(key,None)
            finally:
                self.live_observation_pending.pop(key,None)
                self.live_observation_running_time.pop(key,None)
                self.live_observation_by_stream.pop(key,None)
        task=asyncio.create_task(drain())
        self.live_observation_by_stream[key]=task
        self.live_observation_tasks.add(task)
        def finished(done):
            self.live_observation_tasks.discard(done)
            if not done.cancelled() and done.exception():
                logging.getLogger(__name__).error('Current candle observation failed',
                    exc_info=(type(done.exception()),done.exception(),done.exception().__traceback__))
        task.add_done_callback(finished)

    async def hydrate_live_stream(self,symbol,tf):
        key=(symbol,tf);interval=(1440 if tf=='D' else int(tf))*60_000
        try:
            expected=(self.market_now()//interval)*interval-interval
            rows=await asyncio.to_thread(self.engine.store.candles_range,symbol,tf,
                limit=251,start=max(0,expected-250*interval),end=expected+interval)
            cached=(len(rows)==251 and rows[-1].time==expected and
                    all(b.time-a.time==interval for a,b in zip(rows,rows[1:])))
            received=None;received_monotonic=None;source='storage'
            if not cached:
                if hasattr(self.bybit,'candles_with_timing'):
                    rows,received,received_monotonic=await self.bybit.candles_with_timing(symbol,tf,limit=252)
                elif hasattr(self.bybit,'candles_with_metadata'):
                    rows,received=await self.bybit.candles_with_metadata(symbol,tf,limit=252)
                    received_monotonic=asyncio.get_running_loop().time()
                else:
                    rows=await self.bybit.candles(symbol,tf)
                    received=now_ms()
                    received_monotonic=asyncio.get_running_loop().time()
                source='rest'
            now=self.market_now()
            closed=sorted((c for c in rows if c.time+interval<=now and valid_ohlc(c)[0]),key=lambda c:c.time)
            if not closed:
                self.live_hydration_retry_at[key]=now_ms()+30_000
                return
            if not cached and 25<=len(closed)<251:
                if hasattr(self.bybit,'candles_with_timing'):
                    older,_,_=await self.bybit.candles_with_timing(symbol,tf,end=closed[0].time-1,limit=1)
                else:
                    older=await self.bybit.candles(symbol,tf,end=closed[0].time-1)
                if not older:self.live_short_history_start[key]=closed[0].time
            elif len(closed)>=251:
                self.live_short_history_start.pop(key,None)
            if not cached:await self.engine.store.aput_candles(symbol,tf,closed,priority=0)
            async with self.live_buffer_locks.setdefault(key,asyncio.Lock()):
                merged={c.time:c for c in closed}
                merged.update({c.time:c for c in self.live_buffers.get(key,())})
                self.live_buffers[key]=[merged[t] for t in sorted(merged)[-251:]]
                latest=self.live_buffers[key][-1]
                self.context_hydrated.add(key)
                self.live_received_close[key]=max(self.live_received_close.get(key,0),latest.time+interval)
                receipt=self.live_receipts.get(key)
                if receipt is None or receipt[0]<latest.time:
                    receipt=(latest.time,received,received_monotonic,source)
                    self.live_receipts[key]=receipt
                if self.live_window_ready(key):
                    self.live_hydration_retry_at.pop(key,None)
                    self.live_hydration_backoff.pop(key,None)
                    await self.enqueue_current_detection(symbol,tf,latest,receipt[1],receipt[2],receipt[3],allow_rebuild=True)
                else:
                    self.live_hydration_retry_at[key]=now_ms()+30_000
            self.broadcast_event.set()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            delay=self.live_hydration_backoff.get(key,5_000)
            self.live_hydration_retry_at[key]=now_ms()+delay
            self.live_hydration_backoff[key]=min(delay*2,30_000)
            logging.getLogger(__name__).warning('Current candle history unavailable for %s/%s: %s',
                symbol,tf,self.safe_error(exc))

    async def refresh_live_warming(self):
        while True:
            await asyncio.sleep(5)
            for symbol in self.symbols:
                for tf in self.engine.settings.timeframes:
                    if not self.live_window_ready((symbol,tf)):
                        self.request_live_hydration(symbol,tf)

    async def observe_live_candle(self,symbol,tf,candle,received_wall_ms,received_monotonic,source):
        key=(symbol,tf);interval=(1440 if tf=='D' else int(tf))*60_000
        loop=asyncio.get_running_loop();started=loop.time()
        if symbol not in self.tracked_symbols():return
        if received_monotonic is not None:
            self.live_candle_queue_wait_ms.append(max(0,int((started-received_monotonic)*1000)))
        async with self.live_buffer_locks.setdefault(key,asyncio.Lock()):
            if symbol not in self.tracked_symbols():return
            merged={c.time:c for c in self.live_buffers.get(key,())}
            merged[candle.time]=candle
            self.live_buffers[key]=[merged[t] for t in sorted(merged)[-251:]]
            previous=self.live_receipts.get(key)
            if previous is None or candle.time>previous[0]:
                self.live_receipts[key]=(candle.time,received_wall_ms,received_monotonic,source)
            if self.live_window_ready(key) and self.live_buffers[key][-1].time==candle.time:
                if tf in self.engine.settings.timeframes:
                    await self.confirm_live_setups(symbol,tf,candle)
                    if received_monotonic is not None:
                        finished=loop.time()
                        self.live_candle_processing_ms.append(max(0,int((finished-started)*1000)))
                        self.live_candle_transition_latency_ms.append(max(0,int((finished-received_monotonic)*1000)))
                else:
                    await self.engine.store.aput_candles(symbol,tf,[candle],priority=0)
                self.live_received_close[key]=max(self.live_received_close.get(key,0),candle.time+interval)
                await self.enqueue_current_detection(symbol,tf,candle,received_wall_ms,received_monotonic,source)
            elif not self.live_window_ready(key):
                await self.engine.store.aput_candles(symbol,tf,[candle],priority=0)
                self.live_received_close[key]=max(self.live_received_close.get(key,0),candle.time+interval)
                self.request_live_hydration(symbol,tf)
            else:
                await self.engine.store.aput_candles(symbol,tf,[candle],priority=0)
        self.history_dispatch_event.set()

    async def enqueue_current_detection(self,symbol,tf,candle,received_wall_ms,received_monotonic,source,*,allow_rebuild=False):
        if tf not in self.engine.settings.timeframes or symbol not in self.symbols:return
        key=(symbol,tf);interval=int(tf)*60_000;event=candle.time+interval
        if self.market_now()-event>interval:return
        last=self.live_scan_cursor_cache.get(key)
        if last is None:
            last=await self.engine.store.aget_live_scan_cursor(symbol,tf)
            if last is not None:self.live_scan_cursor_cache[key]=last
        rebuilding=last is not None and candle.time<=last
        if rebuilding and (not allow_rebuild or key in self.live_rebuilt):return
        context={}
        for context_tf in set((*self.engine.settings.timeframes,'5','15','60')):
            step=int(context_tf)*60_000
            context[context_tf]=_snapshot_candles([c for c in self.live_buffers.get((symbol,context_tf),())
                if c.time+step<=event])
        job=DetectionJob(priority=0,sequence=next(self.detection_sequence),symbol=symbol,timeframe=tf,
            candle_time=candle.time,candles=_snapshot_candles(self.live_buffers[key]),recovered=False,
            queued_at=asyncio.get_running_loop().time(),received_at=received_monotonic,
            detect_signals=not rebuilding,advance_cursor=not rebuilding,
            received_wall_ms=received_wall_ms,source=source,context_buffers=context)
        result=await self.split_detection_queue.put_current(job)
        if rebuilding and result.accepted:self.live_rebuilt.add(key)
        if result.deferred is not None:self.history_dispatch_event.set()

    async def confirm_live_setups(self,symbol,tf,candle):
        key=(symbol,tf);interval=int(tf)*60_000
        history_cursor=self.history_cursor_cache.get(key)
        # Existing setups are changed only when their prior bars were
        # recovered. Newly formed live setups can advance independently.
        allowed=None if history_cursor is not None and history_cursor>=candle.time-interval else {
            setup['id'] for setup in self.active_setups_for(symbol,tf)
            if (setup.get('delivery') or {}).get('mode')=='current'}
        updates=self.advance_setups(symbol,tf,candle,self.live_buffers.get(key,()),
                                    persist=False,eligible_ids=allowed)
        try:
            await self.engine.store.acommit_current_candle(symbol,tf,candle,updates)
        except Exception:
            self.invalidate_active_setups()
            self.live_buffers.pop(key,None)
            raise
        if updates:self.broadcast_event.set()

    async def enqueue_candle(self,msg,wait=False,recovered=False):
        if not self.candle_workers:
            _,tf,symbol=msg['topic'].split('.')
            async with self.candle_locks.setdefault((symbol,tf),asyncio.Lock()):
                # The inline path is used by unit tests and one-shot callers;
                # keep its completion semantics synchronous.
                await self.kline_message(msg,tf,symbol,recovered=recovered)
            return True
        future=asyncio.get_running_loop().create_future() if wait else None
        item=(0,next(self.candle_sequence),msg,
              asyncio.get_running_loop().time(),future,recovered)
        target=self.recovery_candle_queue if recovered else self.candle_queue
        await target.put(item)
        if future:
            return await future
        return True

    async def receive_kline(self,msg):
        """Apply partial bars cheaply; queue only exchange-confirmed closes."""
        _,tf,symbol=msg['topic'].split('.')
        if symbol not in self.tracked_symbols():
            return
        self.last_kline_received_at=int(msg.get('_received_wall_ms') or now_ms())
        event_at=int(msg.get('ts',0) or 0)
        if event_at:
            offset=int(getattr(self.bybit,'offset',0) or 0)
            self.network_delay_ms.append(max(0,self.last_kline_received_at-(event_at-offset)))
        for row in msg.get('data',[]):
            try:
                candle=Candle(int(row['start']),float(row['open']),float(row['high']),
                    float(row['low']),float(row['close']),float(row.get('volume',0)),
                    float(row['turnover']) if row.get('turnover') not in (None,'') else None)
            except (KeyError,TypeError,ValueError) as exc:
                self.error=f'Некорректная свеча {symbol}/{tf}: {exc}'
                continue
            valid,reason=valid_ohlc(candle)
            if not valid:
                self.error=f'Некорректная свеча OHLC {symbol}/{tf} в {candle.time}: {reason}'
                continue
            if not row.get('confirm'):
                self.live_candles[(symbol,tf)]=asdict(candle)
            else:
                self.live_candles.pop((symbol,tf),None)
                if self.split_mode:
                    self.last_closed_candles[(symbol,tf)]=asdict(candle)
                    self.schedule_live_observation(symbol,tf,candle,self.last_kline_received_at,
                        msg.get('_received_monotonic') or asyncio.get_running_loop().time(),
                        msg.get('_source','websocket'))
                else:
                    await self.enqueue_candle({'topic':msg['topic'],'data':[row]})

    async def market_message_worker(self):
        """Keep SQLite and state updates off the WebSocket receive path."""
        loop=asyncio.get_running_loop()
        while True:
            queued_at,msg=await self.market_message_queue.get()
            self.market_message_queue_wait_ms.append(max(0,int((loop.time()-queued_at)*1000)))
            try:
                topic=msg.get('topic','')
                if topic.startswith('kline.'):
                    await self.receive_kline(msg)
                elif topic.startswith('tickers.'):
                    await self.message(msg,fast=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logging.getLogger(__name__).exception('Market frame processing failed')
                self.market_message_error=self.safe_error(exc)
            finally:
                self.market_message_queue.task_done()

    async def candle_worker(self,recovery_worker=False):
        queue=self.recovery_candle_queue if recovery_worker else self.candle_queue
        while True:
            _,_,msg,queued_at,future,recovered=await queue.get()
            started=asyncio.get_running_loop().time()
            try:
                _,tf,symbol=msg['topic'].split('.')
                async with self.candle_locks.setdefault((symbol,tf),asyncio.Lock()):
                    await self.kline_message(msg,tf,symbol,recovered=recovered,
                        defer_detection=True,received_monotonic=queued_at)
                queue_ms=max(0,int((started-queued_at)*1000))
                processing_ms=max(0,int((asyncio.get_running_loop().time()-started)*1000))
                self.candle_queue_wait_ms.append(queue_ms)
                self.candle_processing_ms.append(processing_ms)
                if recovered:
                    self.recovered_candle_queue_wait_ms.append(queue_ms)
                    self.recovered_candle_processing_ms.append(processing_ms)
                else:
                    self.live_candle_queue_wait_ms.append(queue_ms)
                    self.live_candle_processing_ms.append(processing_ms)
                    self.live_candle_transition_latency_ms.append(max(0,int(
                        (asyncio.get_running_loop().time()-queued_at)*1000)))
                if len(self.candle_queue_wait_ms)>300:
                    self.candle_queue_wait_ms.popleft();self.candle_processing_ms.popleft()
                if future and not future.done():
                    future.set_result(True)
                if self.error.startswith('\u041e\u0431\u0440\u0430\u0431\u043e\u0442\u043a\u0430 \u0441\u0432\u0435\u0447\u0438 Bybit:'):
                    self.error=''
            except BybitNetworkError as exc:
                _,tf,symbol=msg['topic'].split('.')
                self.last_received_close.pop((symbol,tf),None)
                self.candle_recovery_status='waiting_network'
                self.candle_recovery_pending=max(1,self.candle_recovery_pending)
                if future and not future.done():
                    future.set_exception(exc)
            except Exception as exc:
                _,tf,symbol=msg['topic'].split('.')
                self.last_received_close.pop((symbol,tf),None)
                self.invalidate_active_setups()
                if future and not future.done():
                    future.set_exception(exc)
                else:
                    logging.getLogger(__name__).exception('Queued kline processing failed')
                    self.error=f'\u041e\u0431\u0440\u0430\u0431\u043e\u0442\u043a\u0430 \u0441\u0432\u0435\u0447\u0438 Bybit: {self.safe_error(exc)}'
            finally:
                queue.task_done()

    async def enqueue_detection_job(self,symbol,tf,candles,candle_time,*,recovered=False,
                                    detect_signals=True,detect_setups=True,advance_cursor=True,
                                    received_at=None):
        loop=asyncio.get_running_loop()
        queued_at=loop.time()
        job=DetectionJob(priority=1 if recovered else 0,
            sequence=next(self.detection_sequence),symbol=symbol,timeframe=tf,
            candle_time=int(candle_time),candles=_snapshot_candles(candles),
            recovered=bool(recovered),queued_at=queued_at,
            received_at=queued_at if received_at is None else received_at,
            detect_signals=detect_signals,detect_setups=detect_setups,
            advance_cursor=advance_cursor)
        await self.detection_queue.put(job)

    async def detection_worker(self):
        loop=asyncio.get_running_loop()
        while True:
            job=await self.detection_queue.get()
            started=loop.time()
            self.detection_queue_wait_ms.append(max(0,int((started-job.queued_at)*1000)))
            retry=False
            try:
                await self.finish_candle_detection(job)
                if job.advance_cursor:
                    await self.engine.store.aset_scan_cursor(
                        job.symbol,job.timeframe,job.candle_time)
                latency=max(0,int((loop.time()-(job.received_at or job.queued_at))*1000))
                (self.recovered_detection_latency_ms if job.recovered
                    else self.live_detection_latency_ms).append(latency)
                if self.error.startswith('Candle detection:'):
                    self.error=''
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logging.getLogger(__name__).exception(
                    'Candle detection failed for %s %s at %s',
                    job.symbol,job.timeframe,job.candle_time)
                self.error=f'Candle detection: {self.safe_error(exc)}'
                retry=True
            finally:
                self.detection_processing_ms.append(max(0,int((loop.time()-started)*1000)))
                if retry:
                    await asyncio.sleep(min(30,2 ** min(job.retries,5)))
                    await self.detection_queue.retry(job)
                else:
                    await self.detection_queue.complete(job)

    async def split_detection_worker(self,lane):
        queue=self.split_detection_queue
        while True:
            if lane=='both':
                selected,job=await queue.get_next()
            elif lane=='current':
                selected='current';job=await queue.get_current()
            else:
                selected='history';job=await queue.get_history()
            loop=asyncio.get_running_loop();started=loop.time()
            self.detection_queue_wait_ms.append(max(0,int((started-job.queued_at)*1000)))
            retry=False
            try:
                await self.finish_candle_detection(job,archival=selected=='history')
                if selected=='history' and job.advance_cursor:
                    await self.engine.store.aset_scan_cursor(job.symbol,job.timeframe,job.candle_time,priority=1)
                    self.history_scan_cursor_cache[job.stream]=job.candle_time
                    self.history_detection_failures.pop(job.stream,None)
                    self.history_detection_retry_at.pop(job.stream,None)
                elif selected=='current' and job.advance_cursor:
                    await self.engine.store.aset_live_scan_cursor(job.symbol,job.timeframe,job.candle_time)
                    self.live_scan_cursor_cache[job.stream]=job.candle_time
                latency=max(0,int((loop.time()-(job.received_at or job.queued_at))*1000))
                (self.recovered_detection_latency_ms if selected=='history'
                    else self.live_detection_latency_ms).append(latency)
                if self.error.startswith('Candle detection:'):self.error=''
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logging.getLogger(__name__).exception('%s candle detection failed for %s %s at %s',
                    selected,job.symbol,job.timeframe,job.candle_time)
                self.error=f'Candle detection: {self.safe_error(exc)}'
                retry=selected=='history'
                if selected=='current':
                    self.history_dispatch_event.set()
                    previous=self.live_retry_tasks.get(job.stream)
                    if previous is None or previous.done():
                        task=asyncio.create_task(self.retry_current_detection(job))
                        self.live_retry_tasks[job.stream]=task
            finally:
                self.detection_processing_ms.append(max(0,int((loop.time()-started)*1000)))
                if selected=='history':
                    if retry:
                        attempts=self.history_detection_failures.get(job.stream,0)
                        self.history_detection_failures[job.stream]=attempts+1
                        self.history_detection_retry_at[job.stream]=now_ms()+min(30_000,1000*(2 ** min(attempts,5)))
                    await queue.complete_history(job)
                    self.history_detection_inflight.discard(job.stream)
                    self.history_dispatch_event.set()
                else:
                    await queue.complete_current(job)

    async def retry_current_detection(self,job):
        try:
            await asyncio.sleep(min(30,2 ** min(job.retries,5)))
            interval=int(job.timeframe)*60_000
            if job.symbol in self.symbols and self.market_now()-(job.candle_time+interval)<=interval:
                result=await self.split_detection_queue.put_current(replace(job,
                    queued_at=asyncio.get_running_loop().time(),retries=job.retries+1))
                if result.deferred is not None:self.history_dispatch_event.set()
        finally:
            if self.live_retry_tasks.get(job.stream) is asyncio.current_task():
                self.live_retry_tasks.pop(job.stream,None)

    async def restore_detection_gap(self,symbol,tf,start,end):
        key=(symbol,tf)
        try:
            async with self.history_semaphore:
                rows=await self.bybit.history(symbol,tf,start,end)
            if rows:await self.engine.store.aput_candles(symbol,tf,rows,priority=1)
            if not rows or min(row.time for row in rows)>start:
                self.history_gap_retry_at[key]=now_ms()+30_000
            else:self.history_gap_retry_at.pop(key,None)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.history_gap_retry_at[key]=now_ms()+30_000
            logging.getLogger(__name__).warning('Historical scan gap unavailable for %s/%s: %s',
                symbol,tf,self.safe_error(exc))
        finally:
            self.history_gap_tasks.pop(key,None)
            self.history_dispatch_event.set()

    async def history_detection_dispatcher(self):
        while True:
            try:
                if self.history_bootstrap_complete:
                    for key,cursor in list(self.history_cursor_cache.items()):
                        symbol,tf=key
                        if (symbol not in self.symbols or cursor is None or key in self.history_detection_inflight
                                or now_ms()<self.history_detection_retry_at.get(key,0)):continue
                        interval=int(tf)*60_000
                        scan=self.history_scan_cursor_cache.get(key)
                        if scan is None:
                            scan=cursor-interval
                            await self.engine.store.aset_scan_cursor(symbol,tf,scan,priority=1)
                            self.history_scan_cursor_cache[key]=scan
                        if scan>=cursor:continue
                        pending=await asyncio.to_thread(self.engine.store.candles_range,symbol,tf,
                            limit=1,after=scan,end=cursor+interval)
                        if not pending or pending[0].time!=scan+interval:
                            if key not in self.history_gap_tasks and now_ms()>=self.history_gap_retry_at.get(key,0):
                                end=min(cursor,pending[0].time-interval if pending else cursor)
                                task=asyncio.create_task(self.restore_detection_gap(symbol,tf,scan+interval,end))
                                self.history_gap_tasks[key]=task
                            continue
                        candle=pending[0]
                        history=await asyncio.to_thread(self.engine.store.candles_range,symbol,tf,
                            limit=251,start=max(0,candle.time-250*interval),end=candle.time+interval)
                        by_time={c.time:c for c in history if c.time<=candle.time}
                        history=[by_time[t] for t in sorted(by_time)]
                        if not history or history[-1].time!=candle.time:continue
                        job=DetectionJob(priority=1,sequence=next(self.detection_sequence),symbol=symbol,
                            timeframe=tf,candle_time=candle.time,candles=_snapshot_candles(history),
                            recovered=True,queued_at=asyncio.get_running_loop().time(),source='storage')
                        if await self.split_detection_queue.put_history(job):
                            self.history_detection_inflight.add(key)
                self.history_dispatch_event.clear()
                try:await asyncio.wait_for(self.history_dispatch_event.wait(),timeout=1)
                except asyncio.TimeoutError:pass
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.getLogger(__name__).exception('Historical detection dispatcher failed')
                await asyncio.sleep(2)

    async def replay_pending_detections(self):
        """Resume scans not committed before the last process shutdown."""
        for symbol in sorted(self.market_symbols):
            for tf in self.engine.settings.timeframes:
                interval=int(tf)*60_000
                candle_cursor=self.engine.store.cursor(symbol,tf)
                if candle_cursor is None:
                    continue
                scan_cursor=self.engine.store.scan_cursor(symbol,tf)
                if scan_cursor is None:
                    scan_cursor=candle_cursor-interval
                    await self.engine.store.aset_scan_cursor(symbol,tf,scan_cursor)
                if scan_cursor>=candle_cursor:
                    continue
                pending=self.engine.store.candles_range(symbol,tf,limit=1500,
                    end=candle_cursor+interval,after=scan_cursor)
                for candle in pending:
                    history=self.engine.store.candles_range(symbol,tf,limit=1000,
                        start=max(0,candle.time-interval*999),end=candle.time+interval)
                    if not history or history[-1].time!=candle.time:
                        continue
                    await self.enqueue_detection_job(symbol,tf,history,candle.time,
                        recovered=True)
        # Rebuild only the current early geometry after restart. These jobs do
        # not advance the durable scan cursor or replay signal history.
        for symbol in sorted(self.market_symbols):
            for tf in self.engine.settings.timeframes:
                buf=self.buffers.get((symbol,tf),[])
                cursor=self.engine.store.cursor(symbol,tf)
                if buf and cursor is not None and buf[-1].time>=cursor:
                    await self.enqueue_detection_job(symbol,tf,buf,buf[-1].time,
                        recovered=True,detect_signals=False,advance_cursor=False)

    def watch_context(self,ws,signal_id):
        selected=signal_id if isinstance(signal_id,str) and len(signal_id)<=64 else None
        if self.watched_context.get(ws)!=selected:
            self.watched_context[ws]=selected
            self.context_refresh_event.set()
            self.open_interest_refresh_event.set()

    def watch_setup(self,ws,setup_id):
        selected=setup_id if isinstance(setup_id,str) and len(setup_id)<=64 else None
        if self.watched_setups.get(ws)!=selected:
            self.watched_setups[ws]=selected
            self.context_refresh_event.set()

    def watch_chart(self,ws,symbol,timeframe):
        if isinstance(symbol,str) and len(symbol)<=40 and timeframe in self.engine.settings.timeframes:
            self.watched_charts[ws]={'symbol':symbol,'timeframe':timeframe}
        else:
            self.watched_charts.pop(ws,None)
        self.context_refresh_event.set()

    def watch_account(self,ws,enabled=True):
        if enabled:self.account_watchers.add(ws)
        else:self.account_watchers.discard(ws)
        self.broadcast_event.set()

    def account_feed(self,limit=50):
        return {'status':self.account_monitor.public_status(),
            'positions':self.engine.store.account_positions(),
            'orders':self.engine.store.account_orders(100),
            'executions':self.engine.store.account_executions(limit),
            'updated_at':self.market_now()}

    def account_trade_review(self,exec_id):
        row=next((item for item in self.engine.store.account_executions(500)
                  if item.get('execId')==exec_id),None)
        if not row:return None
        side={'Buy':'long','Sell':'short'}.get(row.get('side'))
        timestamp=int(row.get('execTime') or 0)
        linked=row.get('setup_id')
        setups=(self.engine.store.account_setups(row['symbol'],None,500)
                if not linked else [self.engine.store.setup(linked)])
        candidates=[]
        for setup in setups:
            if not setup:continue
            if setup['symbol']!=row.get('symbol') or (linked is None and setup['side']!=side):continue
            history=self.engine.store.setup_assessments(setup['id'],200)
            before=[item for item in history if int(item.get('as_of') or 0)<=timestamp]
            assessment=before[-1] if before else None
            candidates.append({'setup':setup,'assessment':assessment,
                'as_of':int(assessment.get('as_of') or 0) if assessment else None})
        if linked:
            selected=next((item for item in candidates if item['setup']['id']==linked),None)
        else:
            fresh=[item for item in candidates if item['assessment'] and
                   0<=timestamp-int(item['as_of'] or 0)<=5000]
            selected=fresh[0] if len(fresh)==1 else None
        warnings=[]
        if selected and selected['assessment']:
            setup=selected['setup'];assessment=selected['assessment']
            if assessment.get('preliminary'):
                warnings.append('\u041d\u0430 \u043c\u043e\u043c\u0435\u043d\u0442 \u043f\u043e\u0441\u043b\u0435\u0434\u043d\u0435\u0439 \u0441\u043e\u0445\u0440\u0430\u043d\u0451\u043d\u043d\u043e\u0439 \u043e\u0446\u0435\u043d\u043a\u0438 \u043f\u0440\u043e\u0431\u043e\u0439 \u0435\u0449\u0451 \u043d\u0435 \u0431\u044b\u043b \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0451\u043d')
            if assessment.get('status')=='waiting_breakout' and assessment.get('confirmed_candle_at'):
                warnings.append('\u041f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d\u0438\u0435 \u043f\u0440\u043e\u0431\u043e\u044f \u043a \u043c\u043e\u043c\u0435\u043d\u0442\u0443 \u043e\u0446\u0435\u043d\u043a\u0438 \u0431\u044b\u043b\u043e \u043f\u043e\u0442\u0435\u0440\u044f\u043d\u043e \u0438\u043b\u0438 \u043e\u0436\u0438\u0434\u0430\u043b\u043e \u043d\u043e\u0432\u043e\u0433\u043e \u0437\u0430\u043a\u0440\u044b\u0442\u0438\u044f 5\u043c')
            warnings.extend(assessment.get('warnings') or [])
            if timestamp-int(assessment.get('as_of') or 0)>5000:
                warnings.append('\u041e\u0446\u0435\u043d\u043a\u0430 \u0441\u0438\u0433\u043d\u0430\u043b\u0430 \u0441\u0442\u0430\u0440\u0448\u0435 5 \u0441\u0435\u043a\u0443\u043d\u0434 \u043e\u0442\u043d\u043e\u0441\u0438\u0442\u0435\u043b\u044c\u043d\u043e \u0438\u0441\u043f\u043e\u043b\u043d\u0435\u043d\u0438\u044f')
        else:
            setup=None;assessment=None
            warnings.append('\u0421\u0442\u0430\u0442\u0443\u0441 \u043d\u0430 \u043c\u043e\u043c\u0435\u043d\u0442 \u0438\u0441\u043f\u043e\u043b\u043d\u0435\u043d\u0438\u044f \u043d\u0435\u0438\u0437\u0432\u0435\u0441\u0442\u0435\u043d: \u043d\u0435\u0442 \u043e\u0434\u043d\u043e\u0437\u043d\u0430\u0447\u043d\u043e\u0439 \u0441\u0432\u0435\u0436\u0435\u0439 \u043e\u0446\u0435\u043d\u043a\u0438')
        position_idx=int(row.get('positionIdx') or 0)
        position=next((item for item in self.engine.store.account_positions()
            if item.get('symbol')==row.get('symbol') and
               int(item.get('positionIdx') or 0)==position_idx),None)
        order=self.engine.store.account_order(row.get('orderId'))
        if not position or not float(position.get('stopLoss') or 0):
            warnings.append('\u0411\u0438\u0440\u0436\u0435\u0432\u043e\u0439 \u0441\u0442\u043e\u043f \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d; \u043b\u043e\u043a\u0430\u043b\u044c\u043d\u044b\u0439 \u0441\u0442\u043e\u043f Tiger.Trade \u0447\u0435\u0440\u0435\u0437 Bybit \u043d\u0435 \u0432\u0438\u0434\u0435\u043d')
        if position and not float(position.get('takeProfit') or 0):
            warnings.append('\u0411\u0438\u0440\u0436\u0435\u0432\u043e\u0439 \u0442\u0435\u0439\u043a-\u043f\u0440\u043e\u0444\u0438\u0442 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d')
        if assessment and position and float(position.get('stopLoss') or 0):
            planned=assessment.get('stop');actual=float(position['stopLoss'])
            if planned is not None:
                expanded=(side=='long' and actual<float(planned)) or (side=='short' and actual>float(planned))
                if expanded:
                    warnings.append('\u0411\u0438\u0440\u0436\u0435\u0432\u043e\u0439 \u0441\u0442\u043e\u043f \u0434\u0430\u043b\u044c\u0448\u0435 \u043e\u0442 \u0446\u0435\u043d\u044b, \u0447\u0435\u043c \u0441\u0442\u043e\u043f \u0432 \u0441\u043e\u0445\u0440\u0430\u043d\u0451\u043d\u043d\u043e\u043c \u043f\u043b\u0430\u043d\u0435')
        return {'execution':row,'status':'captured' if selected and
                    timestamp-int(selected.get('as_of') or 0)<=5000 else
                    'stale' if selected else 'unknown',
                'setup':selected['setup'] if selected else None,
                'assessment':selected['assessment'] if selected else None,
                'candidates':candidates,'order':order,'position':position,
                'warnings':list(dict.fromkeys(warnings))}

    def request_chart_history(self,symbol,tf,start,end):
        interval=(1440 if tf=='D' else int(tf))*60_000
        start=int(start)//interval*interval
        end=(int(end)+interval-1)//interval*interval
        key=(symbol,tf,start,end)
        task=self.chart_history_tasks.get(key)
        if task and not task.done():return
        self.chart_history_tasks[key]=asyncio.create_task(self.load_chart_history(*key))

    async def load_chart_history(self,symbol,tf,start,end):
        try:
            async with self.history_semaphore:
                if hasattr(self.bybit,'history'):
                    candles=await self.bybit.history(symbol,tf,start,end)
                else:
                    candles=await self.bybit.candles(symbol,tf,start=start,end=end)
            interval=(1440 if tf=='D' else int(tf))*60_000
            now=now_ms()+int(getattr(self.bybit,'offset',0) or 0)
            closed=[c for c in candles if c.time+interval<=now]
            if self.engine.store.path==':memory:':
                self.engine.store.put_candles_secondary(symbol,tf,closed)
            else:
                await asyncio.to_thread(self.engine.store.put_candles_secondary,symbol,tf,closed)
            if closed:self.broadcast_event.set()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.broadcast_error=f'Chart history {symbol}/{tf}: {self.safe_error(exc)}'

    def setup_context(self,setup_id):
        setup=self.engine.store.setup(setup_id)
        if not setup:return None
        quote=self.quotes.get(setup['symbol'],{})
        now=self.market_now()
        stale=not quote or now-quote.get('time',0)>5000
        price=quote.get('price') if quote and not stale else None
        context={'price':price,'quote_time':quote.get('time') if quote else None,
                 'quote_stale':stale,'updated_at':now}
        context['market_background']=self.market_background(setup['symbol'],setup['side'],now,
            getattr(self.engine.instruments.get(setup['symbol']),'asset_class','crypto'))
        snap=setup['snapshot'];fee_row=self.engine.fees.get(setup['symbol'],{})
        context['chart_levels']=support_chart_levels(
            setup['symbol'],now,price if price is not None else snap['boundary'],
            setup['side'],setup['timeframe'],snap.get('atr'),self.market_buffers())
        fee_available=bool(fee_row and fee_row.get('rate') is not None and
            now-fee_row.get('time',0)<=86400000 and
            (self.engine.fee_mode!='account' or fee_row.get('source')=='account'))
        risk=abs(snap['boundary']-snap['stop']);reward=abs(snap['target']-snap['boundary'])
        context['projected_rr_gross']=reward/risk if risk>0 else None
        context['fee_available']=fee_available
        context['projected_rr_net']=None
        if fee_available and risk>0:
            cost=snap['boundary']*(2*float(fee_row['rate'])+2*self.engine.settings.slippage)
            net_reward=reward-cost
            if net_reward>0:context['projected_rr_net']=net_reward/(risk+cost)
        tick=(getattr(self.engine.instruments.get(setup['symbol']),'tick_size',0) or
              max(abs(snap['boundary'])*1e-8,1e-8))
        sign=1 if setup['side']=='long' else -1
        ticker=self.tickers.get(setup['symbol'],{})
        try:
            next_funding=int(ticker.get('nextFundingTime',0) or 0)
            funding_rate=float(ticker.get('fundingRate',0) or 0) if next_funding and next_funding<=now+8*60*60_000 else 0.
        except (TypeError,ValueError):funding_rate=0.
        boundary=self._setup_boundary(setup,setup['side'],quote.get('time',now))
        early_assessment=self.assess_setup_entry(setup,now,requires_breakout=True)
        confirmed_assessment=self.assess_setup_entry(setup,now,requires_breakout=False)
        entry_assessment=(confirmed_assessment if setup['status']=='breakout_confirmed'
                          else early_assessment)
        early_status={'available':'preliminary','late':'late','late_entry':'late',
            'insufficient_rr':'insufficient_rr','waiting':'waiting',
            'target_reached':'target_reached','scenario_invalidated':'cancelled',
            'unavailable':'unavailable','costs_unavailable':'unavailable'}.get(
                early_assessment['status'],'waiting')
        entry_min=early_assessment.get('price_min')
        entry_max=early_assessment.get('price_max')
        if entry_min is None and early_assessment['status']!='insufficient_rr':
            entry_min=boundary+sign*tick
        entry_low=(min(entry_min,entry_max) if entry_max is not None else entry_min)
        entry_high=max(entry_min,entry_max) if entry_max is not None else None
        confirmed_status=('confirmed' if setup['status']=='breakout_confirmed' else
                          'target_reached' if setup['status']=='target_reached' else
                          'cancelled' if setup['status'] in ('cancelled','expired') else 'waiting')
        retest_status='waiting_confirmation'
        retest_reason='Ждём подтверждённый пробой и касание границы в пределах трёх свечей'
        retest_stop=snap['stop'];retest_rr=None
        if setup.get('confirmed_at'):
            interval=int(setup['timeframe'])*60_000
            later=[c for c in self.market_buffers().get((setup['symbol'],setup['timeframe']),[])
                   if setup['confirmed_at']<=c.time+interval and c.time>=setup['confirmed_at']]
            if later:
                bars=later[:3]
                touched=next((c for c in bars if c.low<=boundary+.25*snap['atr'] and c.high>=boundary-.25*snap['atr']),None)
                confirmed=bool(touched and sign*(touched.close-boundary)>=.1*snap['atr'])
                if confirmed:
                    extreme=touched.low if sign>0 else touched.high
                    retest_stop=extreme-sign*.2*snap['atr']
                    if sign*(boundary-retest_stop)<.8*snap['atr']:
                        retest_status='unavailable';retest_reason='После ретеста расстояние до стопа меньше 0,8 ATR'
                    elif not fee_available:
                        retest_status='unavailable';retest_reason='Нет актуальной комиссии для чистого RR'
                    else:
                        from .strategy import clean_rr
                        retest_rr=clean_rr(boundary,retest_stop,
                            snap['target'],setup['side'],float(fee_row['rate']),
                            self.engine.settings.slippage,funding_rate,tick)[0]
                        retest_status='ready' if retest_rr>=self.engine.settings.min_rr else 'unavailable'
                        retest_reason='Граница протестирована и свеча закрылась за ней' if retest_status=='ready' else 'Чистое RR ретеста ниже установленного минимума'
                elif len(bars)>=3:
                    retest_status='expired';retest_reason='Ретест не подтвердился за три свечи'
        context['entry_plans']=[
            {'scenario':'early_breakout','status':early_status,'activation':f'Котировка пересекла границу на {tick:g}',
             'price_min':entry_low,'price_max':entry_high,'stop':snap['stop'],'target':snap['target'],
             'projection':snap['target'],'net_rr':context['projected_rr_net'],
             'cancel_if':'Стоп или цель; возврат внутрь снимает предварительный пробой',
             'reason':early_assessment.get('reason',''),
             'experimental':snap.get('detection_profile')=='experimental'},
            {'scenario':'confirmed_breakout','status':confirmed_status,'activation':'Закрытие свечи минимум на 0,1 ATR за границей',
             'price_min':entry_low,'price_max':entry_high,'stop':snap['stop'],'target':snap['target'],
             'projection':snap['target'],'net_rr':context['projected_rr_net'],'cancel_if':'Стоп или возврат за линию пробоя',
             'reason':setup.get('reason') or 'Используются правила закрытых свечей'},
            {'scenario':'retest','status':retest_status,'activation':'Касание ±0,25 ATR и закрытие за границей на 0,1 ATR',
             'price_min':boundary-.25*snap['atr'],'price_max':boundary+.25*snap['atr'],'stop':retest_stop,
             'target':snap['target'],'projection':snap['target'],'net_rr':retest_rr,
             'cancel_if':'Стоп, возврат внутрь или отсутствие подтверждения за три свечи','reason':retest_reason},
        ]
        if price is not None and setup['status'] in ('wait_breakout','near_boundary','crossing'):
            context.update(self.setup_price_state(setup,price,quote['time'],persist=False))
        elif price is not None:
            boundary=self._setup_boundary(setup,setup['side'],quote['time'])
            distance=abs(price-boundary)
            context.update({'status':setup['status'],'boundary':boundary,'distance':distance,
                'distance_atr':distance/snap['atr'],'explanation':setup.get('reason','')})
        lifecycle_status=setup.get('status')
        lifecycle_assessment={
            'cancelled':('scenario_invalidated',setup.get('reason','Сценарий отменён')),
            'target_reached':('target_reached',setup.get('reason','Цель достигнута')),
            'expired':('unavailable',setup.get('reason','Наблюдение истекло')),
        }.get(lifecycle_status)
        if lifecycle_assessment:
            entry_assessment=entry_assessment | {'status':lifecycle_assessment[0],
                'reason':lifecycle_assessment[1]}
            early_assessment=early_assessment | {'status':lifecycle_assessment[0],
                'reason':lifecycle_assessment[1]}
        elif lifecycle_status=='breakout_confirmed':
            entry_assessment=confirmed_assessment
        context['entry_assessment']=entry_assessment
        context['projected_rr_net']=entry_assessment.get('net_rr')
        for plan in context['entry_plans']:
            if plan['scenario']=='early_breakout':
                assessment=early_assessment
                plan['status']={'available':'preliminary','late':'late','late_entry':'late',
                    'insufficient_rr':'insufficient_rr','waiting':'waiting',
                    'target_reached':'target_reached','scenario_invalidated':'cancelled',
                    'unavailable':'unavailable','costs_unavailable':'unavailable'}.get(
                        assessment['status'],'waiting')
            else:
                assessment=confirmed_assessment
            plan.update({'price_min':assessment.get('price_min'),
                         'price_max':assessment.get('price_max'),
                         'net_rr':assessment.get('net_rr'),
                         'reason':assessment.get('reason','')})
            if plan['scenario']=='confirmed_breakout' and lifecycle_status!='breakout_confirmed':
                plan['status']=('target_reached' if lifecycle_status=='target_reached' else
                    'cancelled' if lifecycle_status in ('cancelled','expired') else 'waiting_confirmation')
                plan['reason']=setup.get('reason') or 'Сценарий ожидает подтверждения закрытой свечой.'
            elif plan['scenario']=='confirmed_breakout':
                plan['status']=assessment['status']
        context['setup_id']=setup_id
        context['scalp_plan']=self.scalp_plan(setup,now)
        context['market_background']=context['scalp_plan'].get('market_background')
        context['compatible_statistics']=self.compatible_forecast_statistics(
            setup.get('pattern'),setup.get('side'),setup.get('symbol'))
        context['statistics_basis']='Подтверждённые сценарии 15м с подтверждением 5м'
        context['scalp_plan']['compatible_statistics']=context['compatible_statistics']
        return {'setup':setup,'current':context}

    def scalp_plan(self,setup,now=None):
        """Build a short-horizon advisory from closed local candles and live costs."""
        now=self.market_now() if now is None else int(now)
        symbol=setup['symbol'];side=setup['side']
        quote=self.quotes.get(symbol,{})
        quote_time=int(quote.get('time',0) or 0)
        quote_age=now-quote_time if quote_time else None
        price=float(quote['price']) if quote and quote_age is not None and 0<=quote_age<=5000 else None
        quote_source='bybit_ticker' if price is not None else None
        book=self.books.get(symbol)
        book_time=int((book or {}).get('time',0) or 0)
        book_age=now-book_time if book_time else None
        book_requested=symbol in self.ancillary_book_symbols_requested
        book_stream_live=bool(self.ancillary_connected and book_requested and
                              symbol in self.ancillary_book_symbols and
                              book and book.get('valid'))
        if price is None and book and book.get('valid') and book_age is not None and 0<=book_age<=5000:
            try:
                bid=max(map(float,book.get('bids') or {}))
                ask=min(map(float,book.get('asks') or {}))
                if 0<bid<ask:
                    price=(bid+ask)/2
                    quote_time=book_time
                    quote_age=book_age
                    quote_source='bybit_orderbook_mid'
            except (TypeError,ValueError):
                pass
        stale=price is None
        candles=self.market_buffers().get((symbol,'5'),[])
        closed_times=[c.time+300_000 for c in candles if c.time+300_000<=now]
        close_at=max(closed_times,default=now//300_000*300_000)
        boundary=self._setup_boundary(setup,side,close_at)
        current_boundary=(self._setup_boundary(setup,side,quote_time)
                          if quote_time else boundary)
        fee=self.engine.fees.get(symbol,{})
        ticker=self.tickers.get(symbol,{})
        try:
            funding_rate=float(ticker['fundingRate'])
            next_funding=int(ticker['nextFundingTime'])
            funding_time=int(ticker['_funding_time'])
        except (KeyError,TypeError,ValueError):
            funding_rate=next_funding=funding_time=None
        tick=getattr(self.engine.instruments.get(symbol),'tick_size',0.) or 0.
        plan=assess_scalp_plan(setup=setup,candles=candles,
            boundary=boundary,current_boundary=current_boundary,
            price=price,quote_time=quote_time if not stale else None,now=now,
            book=book,book_now=self.market_now(),fee=fee.get('rate'),
            fee_time=fee.get('time'),fee_source=fee.get('source'),
            fee_mode=self.engine.fee_mode,funding_time=funding_time,
            slippage=self.engine.settings.slippage,
            funding_rate=funding_rate,next_funding=next_funding,tick_size=tick,
            book_stream_live=book_stream_live)
        if plan['status']=='book_stale':
            if not book_requested:
                plan.update(status='book_not_subscribed',
                    reason='Стакан загрузится после выбора наблюдения')
            elif not self.ancillary_connected:
                plan.update(status='book_unavailable',
                    reason='Нет активного соединения со стаканом Bybit')
            elif not book_stream_live:
                plan.update(status='book_loading',
                    reason='Подписка активна; ждём снимок стакана Bybit')
        plan['quote_age_ms']=max(0,int(quote_age or 0)) if not stale else None
        plan['quote_source']=quote_source
        plan['book_age_ms']=max(0,int(book_age)) if book_age is not None and book_age>=0 else None
        plan['book_source']='bybit_orderbook' if book else None
        plan['market_context']=self._scalp_market_context(setup,now)
        background=self.market_background(symbol,side,now,
            getattr(self.engine.instruments.get(symbol),'asset_class','crypto'))
        plan['market_background']=background
        plan['warnings']=(list(plan['market_context']['warnings'])+
                          list(background.get('warnings') or []))
        plan['rules_version']='scalp-context-v1'
        confirmation=setup.get('scalp_confirmation') or {}
        if confirmation.get('invalidated_at') and plan.get('confirmed_candle_at') is not None:
            if int(plan['confirmed_candle_at'])<=int(confirmation['invalidated_at']):
                plan.update(status='waiting_breakout',preliminary=True,
                    confirmation_reason='После возврата за границу ждём новое закрытие 5-минутной свечи',
                    reason='После возврата за границу ждём новое закрытие 5-минутной свечи')
        if setup.get('status') in ('cancelled','expired','target_reached'):
            plan.update(status='cancelled',preliminary=False,
                        confirmation_reason=setup.get('reason') or 'Раннее наблюдение завершено',
                        reason=setup.get('reason') or 'Раннее наблюдение завершено')
        return plan

    def _scalp_market_context(self,setup,now):
        symbol=setup['symbol'];side=setup['side']
        rows=[c for c in self.market_buffers().get((symbol,'15'),[])
              if c.time+15*60_000<=now]
        trend_as_of=rows[-1].time+15*60_000 if rows else None
        trend=None;trend_ready=False
        if trend_as_of and now-trend_as_of<=15*60_000:
            long_ok,long_values=trend_filter(rows,'long')
            short_ok,short_values=trend_filter(rows,'short')
            trend_ready=long_ok is not None and short_ok is not None
            trend=('up' if long_ok is True else 'down' if short_ok is True
                   else 'mixed' if trend_ready else None)
        bars=[c for c in self.market_buffers().get((symbol,'5'),[])
              if c.time+5*60_000<=now][-3:]
        momentum_as_of=bars[-1].time+5*60_000 if bars else None
        momentum=None
        if len(bars)==3 and all(b.time-a.time==5*60_000 for a,b in zip(bars,bars[1:])):
            if now-momentum_as_of<=5*60_000:
                rising=(bars[0].high<bars[1].high<bars[2].high and
                        bars[0].low<bars[1].low<bars[2].low)
                falling=(bars[0].high>bars[1].high>bars[2].high and
                         bars[0].low>bars[1].low>bars[2].low)
                momentum='up' if rising else 'down' if falling else 'mixed'
        warnings=[]
        if trend is None:
            warnings.append('Тренд 15м недоступен или устарел')
        elif trend in ('up','down') and trend!=('up' if side=='long' else 'down'):
            warnings.append(f"Сценарий {'лонг' if side=='long' else 'шорт'}, тренд 15м {'вверх' if trend=='up' else 'вниз'}")
        if momentum is None:
            warnings.append('Локальное движение 5м недоступно или устарело')
        elif momentum in ('up','down') and momentum!=side:
            warnings.append(f"Сценарий {'лонг' if side=='long' else 'шорт'}, локальное движение 5м {'вверх' if momentum=='up' else 'вниз'}")
        return {'side':side,'trend_15m':trend,'trend_15m_as_of':trend_as_of,
            'trend_15m_ready':trend_ready,'momentum_5m':momentum,
            'momentum_5m_as_of':momentum_as_of,'warnings':warnings}

    async def track_scalp_setups(self,symbol,price,timestamp):
        """Persist confirmation/reclaim events and sparse causal plan snapshots."""
        candles=sorted((c for c in self.market_buffers().get((symbol,'5'),[])
                        if c.time+5*60_000<=timestamp),key=lambda c:c.time)[-48:]
        if len(candles)<48 or any(b.time-a.time!=5*60_000 for a,b in zip(candles,candles[1:])):
            return
        close=candles[-1];close_at=close.time+5*60_000
        if timestamp-close_at>5*60_000:return
        atr5=atr(candles[-15:])
        if atr5<=0:return
        changed=[];assessments=[]
        # A scalp assessment is only valid around the latest completed 5m bar.
        # Keep live setups plus breakouts from that window; scanning every
        # historical confirmed pattern on every quote creates needless SQLite
        # reads and can starve the market stream.
        for setup in self.engine.store.scalp_setups(symbol,timestamp-5*60_000):
            sign=1 if setup.get('side')=='long' else -1
            tf_ms=int(setup.get('timeframe') or 5)*60_000
            snap=setup.get('snapshot') or {};delivery=setup.get('delivery') or {}
            formed=int(delivery.get('exchange_event_at') or setup.get('formed_at') or
                (snap.get('detected_on_bar',0)+tf_ms if snap.get('detected_on_bar') else 0) or
                setup.get('event_at') or setup.get('detected_at') or 0)
            state=dict(setup.get('scalp_confirmation') or {})
            original=dict(state)
            new_candle=int(setup.get('scalp_assessed_at') or 0)<close_at
            boundary=self._setup_boundary(setup,setup['side'],close_at)
            current_boundary=self._setup_boundary(setup,setup['side'],timestamp)
            tick=(getattr(self.engine.instruments.get(symbol),'tick_size',0.) or
                 max(abs(boundary)*1e-8,1e-8))
            confirmed=(close_at>formed and sign*(close.close-boundary)>=max(tick,.1*atr5))
            held=sign*(price-current_boundary)>0
            if state.get('confirmed_at') and not held:
                state={'invalidated_at':int(timestamp)}
            elif confirmed and held and (not state.get('invalidated_at') or
                                         close_at>int(state['invalidated_at'])):
                state={'confirmed_at':int(close_at)}
            state_changed=state!=original
            if state_changed or new_candle:
                setup['scalp_confirmation']=state
                setup['scalp_assessed_at']=int(close_at)
                setup['scalp_rules_version']='scalp-context-v1'
                setup['updated_at']=int(timestamp)
                changed.append(setup)
            # Keep a one second, short-lived audit trail while a plan is
            # actionable so a received fill can be compared with a real
            # pre-fill assessment instead of a hindsight reconstruction.
            plan=self.scalp_plan(setup,int(timestamp))
            last_saved=int(setup.get('scalp_assessment_sampled_at') or 0)
            sample_due=plan.get('status')=='available' and timestamp-last_saved>=1000
            if sample_due:
                setup['scalp_assessment_sampled_at']=int(timestamp)
                if setup not in changed:changed.append(setup)
            if not (state_changed or new_candle or sample_due):continue
            assessments.append({'setup_id':setup['id'],'as_of':int(timestamp),
                'confirmed_candle_at':plan.get('confirmed_candle_at'),
                'status':plan.get('status'),'preliminary':plan.get('preliminary'),
                'side':plan.get('side'),'entry':plan.get('entry'),'stop':plan.get('stop'),
                'target':plan.get('target'),'net_rr':plan.get('net_rr'),
                'market_context':plan.get('market_context'),'warnings':plan.get('warnings'),
                'reason':plan.get('reason'),'rules_version':plan.get('rules_version')})
        if changed:
            await self.engine.store.asave_setups(changed)
            for value in assessments:
                await self.engine.store.arecord_setup_assessment(
                    value['setup_id'],value['as_of'],value)
            self.invalidate_active_setups();self.setup_cache=None
            self.invalidate_feed_cache();self.broadcast_event.set()

    async def on_account_executions(self,rows):
        """Auto-link only unambiguous new entries with a fresh saved plan."""
        for execution in rows:
            symbol=execution.get('symbol');exec_id=execution.get('execId')
            side={'Buy':'long','Sell':'short'}.get(execution.get('side'))
            if not symbol or not exec_id or not side:
                continue
            try:
                executed=float(execution.get('execQty') or 0)
                closed=float(execution.get('closedSize') or 0)
            except (TypeError,ValueError):
                continue
            if executed<=0 or closed>=executed:
                continue
            order=self.engine.store.account_order(execution.get('orderId'))
            if not order or bool(order.get('reduceOnly')):
                continue
            position=next((row for row in self.engine.store.account_positions()
                if row.get('symbol')==symbol and int(row.get('positionIdx') or 0)==
                int(execution.get('positionIdx') or 0) and float(row.get('size') or 0)>0),None)
            if not position or position.get('side')!=execution.get('side'):
                continue
            try:exec_time=int(execution.get('execTime') or 0)
            except (TypeError,ValueError):continue
            updated=int(position.get('updatedTime') or 0)
            if not exec_time or abs(updated-exec_time)>10_000:
                continue
            candidates=[]
            for setup in self.engine.store.account_setups(symbol,side,500):
                for saved in reversed(self.engine.store.setup_assessments(setup['id'],200)):
                    as_of=int(saved.get('as_of') or 0)
                    if (saved.get('status')=='available' and as_of<=exec_time and
                            exec_time-as_of<=5000 and saved.get('side')==side):
                        candidates.append((setup,saved))
                        break
            if len(candidates)==1:
                setup,_=candidates[0]
                await self.engine.store.alink_account_execution(exec_id,setup['id'],exec_time)

    def decorate_setup(self,setup,now=None):
        """Attach the same advisory status to setup feeds and selected rows."""
        now=self.market_now() if now is None else int(now)
        current=setup.setdefault('current',{})
        plan=self.scalp_plan(setup,now)
        if plan['status']=='book_stale':
            symbol=setup['symbol']
            if symbol not in self.ancillary_book_symbols_requested:
                plan.update(status='book_not_subscribed',
                    reason='Стакан загрузится после выбора наблюдения')
            elif not self.ancillary_connected:
                plan.update(status='book_unavailable',
                    reason='Нет активного соединения со стаканом Bybit')
            elif symbol not in self.ancillary_book_symbols or not (self.books.get(symbol) or {}).get('valid'):
                plan.update(status='book_loading',
                    reason='Подписка активна; ждём снимок стакана Bybit')
        current['scalp_plan']=plan
        current['market_background_summary']=compact_market_advisory(plan.get('market_background'))
        setup['scalp_status']=plan['status']
        setup['scalp_reason']=plan['reason']
        return setup

    @staticmethod
    def _setup_boundary(setup,side,at):
        snap=setup['snapshot']; lines=snap.get('lines',[])
        candidates=[]
        for line in lines:
            pts=line.get('points',[])
            if len(pts)>=2 and pts[1]['time']!=pts[0]['time']:
                m=(pts[1]['value']-pts[0]['value'])/(pts[1]['time']-pts[0]['time'])
                val=pts[1]['value']+m*(at-pts[1]['time'])
                candidates.append((abs(val-snap['boundary']),val))
        return min(candidates)[1] if candidates else snap['boundary']

    @staticmethod
    def _setup_line_value(line,at):
        pts=line.get('points',[])
        if len(pts)<2 or pts[1]['time']==pts[0]['time']:return pts[-1]['value'] if pts else 0
        m=(pts[1]['value']-pts[0]['value'])/(pts[1]['time']-pts[0]['time'])
        return pts[1]['value']+m*(at-pts[1]['time'])

    def setup_price_state(self,setup,price,timestamp,persist=True,include_entry_assessment=True):
        snap=setup['snapshot']; side=setup['side']; sign=1 if side=='long' else -1
        previous_status=setup['status']
        previous_assessment=(setup.get('current') or {}).get('entry_assessment')
        if sign*(price-snap['stop'])<=0:
            setup.update({'status':'cancelled','updated_at':timestamp,
                          'event_at':setup.get('preview_at') or timestamp,
                          'reason':'Цена достигла исходного стопового уровня сценария.'})
            changed=previous_status!='cancelled'
            if changed and persist:self.save_setup(setup)
            return {'status':'cancelled','boundary':snap['boundary'],'distance':None,
                    'distance_atr':None,'explanation':setup['reason'],'_persist':changed and not persist}
        if sign*(price-snap['target'])>=0:
            setup.update({'status':'target_reached','updated_at':timestamp,
                          'event_at':setup.get('preview_at') or timestamp,
                          'reason':'Цена достигла цели сценария до нового входа.'})
            changed=previous_status!='target_reached'
            if changed and persist:self.save_setup(setup)
            return {'status':'target_reached','boundary':snap['boundary'],'distance':None,
                    'distance_atr':None,'explanation':setup['reason'],'_persist':changed and not persist}
        boundary=self._setup_boundary(setup,side,timestamp)
        delta=sign*(price-boundary)
        distance=abs(price-boundary)
        instrument=self.engine.instruments.get(setup['symbol'])
        tick=(getattr(instrument,'tick_size',0) if instrument else 0) or max(abs(boundary)*1e-8,1e-8)
        status='crossing' if delta>=tick else 'near_boundary' if distance<=.5*snap['atr'] else 'wait_breakout'
        interval=int(setup['timeframe'])*60_000
        candle_start=timestamp//interval*interval
        first_cross=(status=='crossing' and setup.get('preview_bar_start')!=candle_start)
        reason='Цена вышла за линию; подтверждение возможно только после закрытия свечи.' if status=='crossing' else ''
        setup.update({'status':status,'updated_at':timestamp,'current_price':price,
            'current_boundary':boundary,'distance':distance,'distance_atr':distance/snap['atr'],
            'quote_time':timestamp,'reason':reason})
        if first_cross:
            setup.update({'event_at':timestamp,'preview_at':timestamp,
                          'preview_bar_start':candle_start,'preview_state':'preliminary',
                          'received_at':now_ms(),'published_at':now_ms()})
        elif previous_status=='crossing' and status!='crossing':
            setup['preview_state']='retracted'
        elif status=='crossing' and setup.get('preview_bar_start')==candle_start:
            setup['preview_state']='preliminary'
        needs_persist=first_cross or status!=previous_status
        changed=self.save_setup(setup) if needs_persist and persist else False
        if changed:
            self.invalidate_feed_cache()
        current={'status':status,'boundary':boundary,'distance':distance,
                'distance_atr':distance/snap['atr'],'quote_time':timestamp,
                'price':price,'quote_stale':False,'explanation':reason}
        if include_entry_assessment:
            current['entry_assessment']=self.assess_setup_entry(setup)
            setup['entry_assessment_as_of']=int(timestamp)
            current['entry_assessment_as_of']=int(timestamp)
        elif previous_assessment is not None:
            current['entry_assessment']=previous_assessment
            current['entry_assessment_as_of']=setup.get('entry_assessment_as_of')
        if needs_persist and not persist:current['_persist']=True
        setup['current']=current
        return current

    def setup_price_state_transition_due(self,setup,price):
        snap=setup['snapshot'];sign=1 if setup['side']=='long' else -1
        if sign*(price-snap['stop'])<=0 or sign*(price-snap['target'])>=0:
            return True
        current=setup.get('current') or {}
        boundary=float(current.get('boundary') or snap['boundary'])
        instrument=self.engine.instruments.get(setup.get('symbol'))
        tick=(getattr(instrument,'tick_size',0.) if instrument else 0.) or max(abs(boundary)*1e-8,1e-8)
        crossing=sign*(price-boundary)>=tick
        return crossing!=(setup.get('status')=='crossing')

    async def discover_setups(self,symbol,tf,buf,recovered=False,publish=True,detected=None,*,
                              received_wall_ms=None,source='websocket',archival=False):
        # Pattern search is CPU-heavy. Keep it off the event loop so the API,
        # websocket heartbeat and incoming market frames remain responsive.
        # Early observation geometry has a wider tolerance than trade signals.
        # Its ATR excludes the decision candle, so a breakout candle cannot
        # retroactively make a setup look like it existed beforehand.
        if detected is None:
            loop=asyncio.get_running_loop()
            found=await loop.run_in_executor(self.detection_executor,_detect_in_worker,
                symbol,tf,_snapshot_candles(buf),'setup','early',True)
        else:
            found=list(detected)
        to_save=[]
        original_ids=[]
        for sig in found:
            anchors='|'.join(f"{p['time']}:{p['kind']}" for p in sig.pivots)
            key=f"{symbol}:{tf}:{sig.pattern}:{sig.side}:{anchors}"
            original_ids.append((key,hashlib.sha256(key.encode()).hexdigest()[:24]))
        existing=(await self.engine.store.afetch_setups([item[1] for item in original_ids])
                  if self.split_mode else {})
        exchange_event_at=buf[-1].time+int(tf)*60_000
        live_ids={hashlib.sha256((key+':current').encode()).hexdigest()[:24]
                  for key,setup_id in original_ids if setup_id in existing and existing[setup_id].get('recovered')
                  and int(existing[setup_id].get('event_at') or 0)!=exchange_event_at}
        if live_ids:existing.update(await self.engine.store.afetch_setups(live_ids))
        for sig,(key,setup_id) in zip(found,original_ids):
            pivots=sig.pivots
            if archival and setup_id in self.history_setup_states.get((symbol,tf),{}):continue
            prior=existing.get(setup_id) if self.split_mode else self.engine.store.setup(setup_id)
            if prior:
                if archival or not prior.get('recovered'):continue
                if int(prior.get('event_at') or 0)!=exchange_event_at:
                    setup_id=hashlib.sha256((key+':current').encode()).hexdigest()[:24]
                    if setup_id in existing:continue
            detected_at=now_ms() if self.split_mode else self.market_now()
            received_at=(received_wall_ms or 0) if self.split_mode else now_ms()
            exchange_event_at=buf[-1].time+int(tf)*60_000
            setup={'id':setup_id,'symbol':symbol,'timeframe':tf,'pattern':sig.pattern,
                'side':sig.side,'detected_at':detected_at,'updated_at':detected_at,'event_at':exchange_event_at,
                'formed_at':exchange_event_at,'status':'wait_breakout',
                'received_at':received_at,'published_at':0 if self.split_mode else received_at,'stage':'early',
                'delivery':{'exchange_event_at':exchange_event_at,'detected_at':detected_at,
                    'received_at':received_at,'delay_ms':max(0,detected_at+int(getattr(self.bybit,'offset',0) or 0)-exchange_event_at) if self.split_mode else max(0,detected_at-exchange_event_at),
                    'recovered':bool(recovered),
                    **({'timing_version':2,'source':source,'mode':'history' if archival else 'current'} if self.split_mode else {})},
                'reason':'','recovered':bool(recovered),
                'confirmation_profile':'standard' if self.engine.strategy_approved and self.active_variant else 'expanded',
                'transitions':[{'time':detected_at,'from':None,'to':'wait_breakout','reason':'Структура восстановлена по последней закрытой истории.' if recovered else 'Структура сформирована по закрытым свечам.'}],
                'snapshot':{'pattern':sig.pattern,'side':sig.side,'start':sig.start,
                    'detected_on_bar':buf[-1].time,'boundary':sig.boundary,'stop':sig.stop,
                    'target':sig.target,'atr':sig.atr,'height':abs(sig.target-sig.boundary),
                    'lines':sig.lines,'pivots':pivots,
                    'detection_profile':'experimental' if sig.pattern in EXPERIMENTAL_NAMES else 'early'}}
            to_save.append(setup)
            # Current quote may already be outside during a live candle. Show
            # this as pending close, never as a confirmed breakout.
            quote=self.quotes.get(symbol)
        if to_save:
            quote=self.quotes.get(symbol)
            if not archival and quote and self.market_now()-quote.get('time',0)<=5000:
                for setup in to_save:
                    self.setup_price_state(setup,quote['price'],quote['time'],persist=False)
                    if setup.get('preview_at'):
                        setup['transitions'].append({'time':setup['preview_at'],'from':'wait_breakout',
                            'to':'crossing','reason':'Цена пересекла границу внутри незакрытой свечи.'})
            if archival:
                await self.engine.store.asave_history_setup_states(to_save)
                states=self.history_setup_states.setdefault((symbol,tf),{})
                states.update({setup['id']:setup for setup in to_save})
            elif self.split_mode:
                await self.engine.store.asave_current_setups(to_save)
            else:
                await self.engine.store.asave_setups(to_save)
            if self.split_mode:
                if not archival:self.reconcile_active_setup_cache(to_save)
            else:self.invalidate_active_setups()
            self.setup_cache=None
            if publish:self.broadcast_event.set()
        return to_save

    def advance_setups(self,symbol,tf,candle,buf,persist=True,eligible_ids=None,setups_override=None):
        interval=int(tf)*60000
        changed=[]
        rows=self.active_setups_for(symbol,tf) if setups_override is None else setups_override
        for setup in rows:
            if eligible_ids is not None and setup.get('id') not in eligible_ids:continue
            snap=setup['snapshot']; sign=1 if setup['side']=='long' else -1
            if candle.time<=int(snap.get('detected_on_bar',0) or 0):continue
            if (setup.get('delivery') or {}).get('mode') in ('current','history') and candle.time<=int(setup.get('last_closed_candle',0) or 0):continue
            boundary=self._setup_boundary(setup,setup['side'],candle.time+interval)
            status=setup['status']; reason=''
            candles_since=max(0,(candle.time-snap['detected_on_bar'])//interval)
            shape_age=max(0,(candle.time-snap['start'])//interval)
            stop_touched=(candle.low<=snap['stop'] if sign==1 else candle.high>=snap['stop'])
            target_touched=(candle.high>=snap['target'] if sign==1 else candle.low<=snap['target'])
            if stop_touched:
                status='cancelled';reason='Свеча достигла исходного стопового уровня сценария.'
            elif target_touched:
                status='target_reached';reason='Свеча достигла цели сценария до нового входа.'
            elif sign*(candle.close-boundary)>=(.1 if setup.get('confirmation_profile')=='standard' else .05)*snap['atr']:
                status='breakout_confirmed';reason='Закрытая свеча подтвердила пробой по порогу профиля детектора.'
                setup['confirmed_at']=candle.time+interval
                setup['event_at']=setup['confirmed_at']
                setup['stage']='confirmed'
            elif candles_since>=12:
                status='expired';reason='Прошло 12 свечей с момента обнаружения.'
            elif shape_age>120:
                status='expired';reason='Возраст структуры превысил 120 свечей.'
            elif len(snap.get('lines',[]))>=2:
                vals=[self._setup_line_value(line,candle.time+interval) for line in snap['lines']]
                width=abs(vals[-1]-vals[0]) if len(vals)>=2 else float('inf')
                if setup['pattern'] in ('ascending_triangle','descending_triangle','symmetric_triangle','rising_wedge','falling_wedge') and width<.1*snap['atr']:
                    status='expired';reason='Границы фигуры сошлись до минимальной ширины.'
            if status==setup['status'] and not reason:
                status='near_boundary' if abs(candle.close-boundary)<=.5*snap['atr'] else 'wait_breakout'
            if status!=setup['status'] or reason:
                setup.update({'status':status,'updated_at':candle.time+interval,'reason':reason})
                setup['last_closed_candle']=candle.time
                if status in ('cancelled','expired','target_reached'):
                    setup['event_at']=setup.get('preview_at') or candle.time+interval
                changed.append(setup)
            elif (setup.get('delivery') or {}).get('mode') in ('current','history'):
                setup['last_closed_candle']=candle.time
                changed.append(setup)
        if changed and persist:self.save_setups(changed)
        if changed and setups_override is None:self.reconcile_active_setup_cache(changed)
        return changed

    async def refresh_decision_context(self,signal_id):
        # Keep the HTTP context endpoint responsive. The feed and watched
        # signal loops own network refreshes and coalesce work across clients.
        self.context_refresh_event.set()
        row=self.signal_row(signal_id)
        if row and row['symbol'] not in self.context_unavailable:
            self.open_interest_refresh_event.set()
            symbol=row['symbol']
            quote_time=int(self.tickers.get(symbol,{}).get('_quote_time',0) or 0)
            task=self.context_quote_tasks.get(symbol)
            if (now_ms()-quote_time>30_000 and (task is None or task.done())
                    and now_ms()-self.quote_retry_at.get(symbol,0)>=30_000):
                self.context_quote_tasks[symbol]=asyncio.create_task(self.refresh_context_quotes([symbol]))
        return self.decision_context(signal_id)

    def compatible_forecast_statistics(self,pattern,side,symbol,*,rules_version=None,market_scenario=None):
        rules_version=rules_version or f'geometry-v1+{VERSION}'
        scenario=market_scenario if market_scenario in ('continuation','reversal') else None
        cached=self.forecast_statistics_cache.get((pattern,side,rules_version,scenario))
        if cached:
            own=cached.get('symbols',{}).get(symbol,{})
            return {key:value for key,value in cached.items() if key!='symbols'} | {
                'basis':(cached.get('basis') or {})|{'symbol':symbol},
                'horizons':{horizon:row|{'this_symbol':own.get(horizon,
                    {'status':'insufficient_data','sample_size':0,'minimum_sample_size':100})}
                    for horizon,row in cached.get('horizons',{}).items()}}
        return {'status':'insufficient_data','sample_size':0,'minimum_sample_size':100,
             'as_of':self.forecast_statistics_updated_at,
             'basis':{'pattern':pattern,'side':side,'rules_version':rules_version,
                      'market_scenario':scenario,'symbol':symbol},
             'horizons':{str(h):{'all_symbols':{'status':'insufficient_data','sample_size':0,
                 'minimum_sample_size':100},'this_symbol':{'status':'insufficient_data',
                 'sample_size':0,'minimum_sample_size':100}} for h in HORIZONS_MINUTES}}

    def decision_context(self,signal_id):
        signal_row=self.signal_row(signal_id)
        if not signal_row:
            return None
        signal=Signal(**{k:v for k,v in signal_row.items() if k in Signal.__dataclass_fields__})
        snapshot=signal.filter_results.get('decision_snapshot')
        live=build_context(signal,self.market_now(),self.market_buffers(),self.tickers,self.symbols,
            self.engine.store,self.live_flow,self.books,self.engine.store,self.engine.fees,
            self.engine.settings.slippage,self.engine.fee_mode,
            self.spot_matches,self.spot_books,self.spot_match_error,
            quote_unavailable_reason=self.context_unavailable.get(signal.symbol),
            tick_size=getattr(self.engine.instruments.get(signal.symbol),'tick_size',0.),
            flow_connected=self.ancillary_connected,
            flow_connected_since=self.ancillary_flow_started_at,
            liquidations=self.live_liquidations,
            liquidation_connected=self.ancillary_connected,
            liquidation_connected_since=self.ancillary_liquidation_started_at,
            instruments=self.engine.instruments,
            market_background=self.market_background(signal.symbol,signal.side,self.market_now(),
                getattr(self.engine.instruments.get(signal.symbol),'asset_class','crypto')))
        market_scenario=(signal.filter_results.get('market_context') or {}).get('scenario')
        rules_version=f"geometry-v1+{signal.strategy_version or VERSION}"
        live['compatible_statistics']=self.compatible_forecast_statistics(signal.pattern,
            signal.side,signal.symbol,rules_version=rules_version,market_scenario=market_scenario)
        assessment=self.assess_signal_entry(signal)
        live['entry_assessment']=assessment
        market=signal.filter_results.get('market_context',{})
        checks=signal.filter_results
        for plan in live.get('entry_plans',[]):
            plan.update({'price_min':assessment.get('price_min'),
                         'price_max':assessment.get('price_max'),
                         'net_rr':assessment.get('net_rr')})
            setup_ready=(signal.timeframe==FIGURE_TF and market.get('eligible')
                         and checks.get('volume_pass'))
            if live['scenario'] in ('scenario_invalidated','target_reached'):
                plan['status']=live['scenario']
                plan['reason']=live.get('plain_summary','')
                continue
            if not setup_ready:
                plan['status']='unavailable'
                plan['reason']=live.get('plain_summary','Контекст 1ч/15м или оборот не прошёл фильтр')
            elif not checks.get('confirmation_pass'):
                plan['status']='waiting_confirmation'
                plan['reason']='Ждём закрытия 5м свечи за границей фигуры'
            elif plan.get('scenario')=='confirmed_breakout':
                plan['status']=assessment['status']
                plan['reason']=assessment['reason']
        return {'signal':signal_row,'snapshot':snapshot,'current':live}

    async def forecast_outcome_report(self):
        rows=await self.engine.store.aforecast_dataset(limit=20_000)
        ablation=await asyncio.to_thread(summarize_filter_ablation,rows)
        return {'version':'forecast-outcomes-v1','updated_at':self.forecast_statistics_updated_at,
            'candidate_count':len(rows),'evaluation_model':(
                'Цена входа из первого сохранённого контекста после публикации; закрытые 5м OHLC; '
                'Bybit комиссия, проскальзывание и оценка funding из снимка. '
                'Тестовый отчёт не меняет допуск или параметры симуляции.'),
            'ablation':ablation}

    async def poll_open_interest(self):
        while True:
            self.open_interest_refresh_event.clear()
            # OI is supplemental. Keep it current for positions and the
            # signal being reviewed without queuing hundreds of REST calls.
            selected=({p['symbol'] for p in self.engine.positions}|self.selected_signal_symbols())
            symbols=sorted(symbol for symbol in selected if
                now_ms()-int(self.tickers.get(symbol,{}).get('_openInterest_time',0) or 0)>600_000)
            if symbols:
                errors=[]
                async def fetch(symbol):
                    try:
                        rows=await self.bybit.open_interest(symbol)
                        await asyncio.to_thread(self.engine.store.save_open_interest,symbol,rows)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        errors.append(self.safe_error(exc))
                await asyncio.gather(*(fetch(s) for s in symbols))
                self.open_interest_error=errors[0] if errors else ''
            try:
                await asyncio.wait_for(self.open_interest_refresh_event.wait(),timeout=300 if symbols else 5)
            except asyncio.TimeoutError:
                pass

    async def poll_fee_rates(self):
        while True:
            if self.engine.fee_mode!='account':
                await asyncio.sleep(3600)
                continue
            symbols=sorted(self.tracked_symbols())
            if not symbols:
                await asyncio.sleep(1)
                continue
            self.fee_refresh_event.clear()
            retry_delay=3600
            try:
                rates=await self.bybit.fee_rates(symbols)
                for symbol,rate in rates.items():
                    self.engine.set_fee(symbol,rate,self.market_now(),'account',persist=False)
                missing=sorted(set(symbols)-set(rates))
                self.fee_error=(f"Не получена комиссия аккаунта для: {', '.join(missing)}"
                                if missing else '')
                self.fee_symbols=set(symbols)
                await self.engine.persist_async()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.fee_error=self.safe_error(exc)
                retry_delay=60
            current=sorted(self.tracked_symbols())
            if current!=symbols:
                self.fee_refresh_event.set()
                continue
            self.fee_refresh_event.clear()
            try:
                await asyncio.wait_for(self.fee_refresh_event.wait(),timeout=retry_delay)
            except asyncio.TimeoutError:
                pass

    async def refresh_spot_scope(self):
        symbols=sorted(self.spot_scope_symbols())
        try:
            matches=await self.bybit.spot_matches(symbols)
            keep=set(self.market_symbols)
            self.spot_matches={symbol:spot for symbol,spot in self.spot_matches.items()
                               if symbol in keep}
            self.spot_matches.update(matches)
            self.spot_matches.update({symbol:None for symbol in symbols if symbol not in matches})
            self.spot_symbols={spot for spot in matches.values() if spot}
            self.spot_match_error=''
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.spot_match_error=self.safe_error(exc)

    async def persist_market_minutes(self):
        write_started=asyncio.get_running_loop().time()
        minute_rows=[]
        flow_save_marks={}
        now=now_ms()
        # The live 1/5/15m flow windows are calculated from memory. Persist each
        # archive minute after it closes, instead of rewriting the same partial
        # aggregate every five seconds while trades arrive.
        minute=now//60_000*60_000-60_000
        archive_symbols=set(self.symbols)|{symbol for symbol,flow_minute in self.live_flow
                                          if flow_minute==minute}
        for symbol in archive_symbols:
            key=(symbol,minute)
            flow=self.live_flow.get(key,{'buy':0.,'sell':0.,'updated':minute+60_000})
            updated=int(flow.get('updated',0) or minute+60_000)
            if updated<=self.flow_saved.get(key,0):
                continue
            covered=max(0,min(60_000,minute+60_000-
                max(minute,int(self.ancillary_flow_started_at or minute+60_000))))
            if covered<=0:
                continue
            book=self.market_book_summary(symbol)
            minute_rows.append({'symbol':symbol,'time':minute,'buy':flow.get('buy',0.),
                'sell':flow.get('sell',0.),'updated':updated,'spread_bps':book.get('spread_bps'),
                'bid_depth_usdt':book.get('bid_depth_usdt_top10'),
                'ask_depth_usdt':book.get('ask_depth_usdt_top10'),'coverage_ms':covered,
                'window_ms':60_000,'source':'bybit_public_trade',
                'quality':'valid' if covered>=60_000 and self.ancillary_connected else 'partial'})
            flow_save_marks[key]=updated
        oi_rows=list(self.pending_open_interest.values())
        if oi_rows:
            await self.engine.store.asave_open_interest(oi_rows)
            self.pending_open_interest.clear()
        liquidation_rows=[]
        for (symbol,minute),event in list(self.live_liquidations.items()):
            if event.get('updated',0)<=self.liquidation_saved.get((symbol,minute),0):continue
            quality_state='valid' if self.ancillary_connected and self.ancillary_liquidation_started_at is not None else 'partial'
            liquidation_rows.append({'symbol':symbol,'time':minute,'long_usdt':event['long_usdt'],
                'short_usdt':event['short_usdt'],'count':event['count'],'quality':quality_state})
            self.liquidation_saved[(symbol,minute)]=event['updated']
        if liquidation_rows:
            await self.engine.store.asave_liquidation_minutes(liquidation_rows)
        cutoff=now_ms()-20*60_000
        self.live_flow={key:value for key,value in self.live_flow.items() if key[1]>=cutoff}
        self.flow_saved={key:value for key,value in self.flow_saved.items() if key[1]>=cutoff}
        timestamp=now_ms();minute=timestamp//60_000*60_000;spot_rows=[]
        for symbol in self.spot_matches:
            if self.spot_futures_saved.get(symbol)==minute:
                continue
            comparison=self.spot_futures_context(symbol,timestamp)
            if comparison['status'] not in ('ready','partial'):
                continue
            payload={key:comparison[key] for key in ('spot_symbol','status','price_basis_pct',
                'imbalance_difference_pp','spot','perpetual','explanation')}
            spot_rows.append({'symbol':symbol,'time':minute,'payload':payload})
            self.spot_futures_saved[symbol]=minute
        if minute_rows or spot_rows:
            await asyncio.to_thread(self._write_market_batches,minute_rows,spot_rows)
            self.flow_saved.update(flow_save_marks)
        self.market_write_ms.append(max(0,int((asyncio.get_running_loop().time()-write_started)*1000)))

    def _write_market_batches(self,minute_rows,spot_rows):
        store=self.engine.store
        store.save_market_minute_batch(minute_rows)
        store.save_spot_futures_batch(spot_rows)

    async def persist_market_minutes_loop(self):
        while True:
            await asyncio.sleep(5)
            try:
                await self.persist_market_minutes()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.broadcast_error=f'Market minute writer: {self.safe_error(exc)}'

    def market_book_summary(self,symbol):
        from .decision_context import _book
        book=self.books.get(symbol)
        if not book:return {}
        return _book(book|{'now':now_ms()},'long',0,self.quotes.get(symbol,{}).get('price',1))

    def spot_futures_context(self,symbol,timestamp=None):
        return compare_spot_perpetual(symbol,self.spot_matches.get(symbol),
            now_ms() if timestamp is None else timestamp,self.books,self.spot_books,
            self.spot_match_error)

    async def run_spot(self):
        backoff=1
        while True:
            desired=tuple(sorted(self.spot_symbols))
            pending=set()
            if not desired:
                self.spot_connected=False
                if not self.spot_match_error:
                    self.spot_stream_error='Нет сопоставленных спотовых пар USDT'
                await asyncio.sleep(5)
                continue
            try:
                # Bybit expects JSON application pings; avoid competing RFC6455 pings.
                async with self.bybit.connect_ws(self.bybit.spot_ws_url,ping_interval=None,
                        max_size=4*1024*1024) as socket:
                    topics={f'orderbook.200.{symbol}' for symbol in desired}
                    pending.add(asyncio.create_task(self.request_subscription(socket,'subscribe',sorted(topics))))
                    connected=False
                    heartbeat=now_ms()
                    while True:
                        current=set(self.spot_symbols)
                        if current!=set(desired):
                            new_topics={f'orderbook.200.{symbol}' for symbol in current}
                            removed=sorted(topics-new_topics);added=sorted(new_topics-topics)
                            if removed:pending.add(asyncio.create_task(
                                self.request_subscription(socket,'unsubscribe',removed)))
                            if added:pending.add(asyncio.create_task(
                                self.request_subscription(socket,'subscribe',added)))
                            desired=tuple(sorted(current));topics=new_topics
                        for task in tuple(pending):
                            if task.done():
                                await task;pending.discard(task)
                        if not connected and not pending:
                            connected=True;self.spot_connected=True
                            self.spot_stream_error=self.spot_match_error;backoff=1
                        try:
                            raw=await asyncio.wait_for(socket.recv(),1)
                        except asyncio.TimeoutError:
                            raw=None
                        if raw:
                            message=json.loads(raw)
                            if self.resolve_subscription_ack(message):continue
                            topic=message.get('topic','')
                            if topic.startswith('orderbook.'):
                                symbol=topic.rsplit('.',1)[-1]
                                if apply_orderbook(self.spot_books,symbol,message,now_ms(),getattr(self.bybit,'offset',0)):
                                    self.spot_last_message=now_ms()
                                    self.spot_stream_error=''
                        if now_ms()-heartbeat>20000:
                            await socket.send(json.dumps({'op':'ping'}))
                            heartbeat=now_ms()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.spot_stream_error=self.safe_error(exc)
            finally:
                self.spot_connected=False
                self.spot_books={}
                for task in pending:
                    if not task.done():task.cancel()
                if pending:await asyncio.gather(*pending,return_exceptions=True)
            await asyncio.sleep(backoff)
            backoff=min(backoff*2,30)

    async def refresh(self,background=False):
        self.refreshing_market=True
        self.phase='loading'
        instruments,symbols,tickers=await self.bybit.universe(self.engine.settings.top_n)
        self.engine.instruments=instruments
        self.symbols=symbols
        scope=self.signal_context_scope()
        retained=set(symbols)|{p['symbol'] for p in self.engine.positions}|set(scope)
        base=set(symbols)|{p['symbol'] for p in self.engine.positions}
        self.market_symbols=retained
        self.context_scope=scope
        self.context_unavailable={symbol:reason for symbol,reason in self.context_unavailable.items()
                                  if symbol in scope}
        self.context_hydrated={key for key in self.context_hydrated
                               if key[0] in retained and (key[0] not in scope or key[1] in scope[key[0]] or key[0] in base)}
        for symbol in scope:
            if symbol not in instruments:
                self.context_unavailable[symbol]='Инструмент больше не торгуется как USDT-фьючерс Bybit'
            else:
                self.context_unavailable.pop(symbol,None)
        if self.engine.fee_mode=='account' and retained!=self.fee_symbols:
            self.fee_refresh_event.set()
        self.selected_context_symbols=self.selected_signal_symbols()
        if background:
            task=asyncio.create_task(self.refresh_spot_scope(),name='spot-scope-refresh')
            self.snapshot_tasks.add(task)
            task.add_done_callback(self.snapshot_tasks.discard)
        else:
            await self.refresh_spot_scope()
        refreshed_at=self.market_now()
        received_at=now_ms()
        self.tickers.update({s:r for s,r in tickers.items() if s in retained})
        for symbol in retained:
            if symbol in tickers and tickers[symbol].get('lastPrice'):
                self.tickers[symbol]['_quote_time']=refreshed_at
                self.tickers[symbol]['_quote_received_at']=received_at
                self.tickers[symbol]['_ticker_event_at']=refreshed_at
                self.tickers[symbol]['_ticker_received_at']=received_at
                if 'fundingRate' in self.tickers[symbol]:
                    self.tickers[symbol]['_funding_time']=refreshed_at
                if self.tickers[symbol].get('openInterest') not in (None,''):
                    self.tickers[symbol]['_openInterest_time']=refreshed_at
                    self.tickers[symbol]['_openInterest_received_at']=received_at
                    try:
                        minute=refreshed_at//60_000*60_000
                        self.pending_open_interest[symbol]={'symbol':symbol,'time':minute,
                            'value':float(self.tickers[symbol]['openInterest']),
                            'value_usdt':float(self.tickers[symbol]['openInterestValue'])
                                if self.tickers[symbol].get('openInterestValue') not in (None,'') else None,
                            'source':'rest_ticker','received_at':received_at}
                        self.open_interest_sampled[symbol]=minute
                    except (TypeError,ValueError):
                        pass
                self.quotes[symbol]={'price':float(tickers[symbol]['lastPrice']),'time':refreshed_at}
        now=refreshed_at
        if background:
            self.last_refresh=now_ms()
            self.refreshing_market=False
            for tf in self.engine.settings.timeframes:
                for symbol in sorted(base):
                    if symbol in instruments:self.request_live_hydration(symbol,tf)
            if self.bybit.fee_mode=='standard':
                for symbol in retained:
                    self.engine.set_fee(symbol,.00055,self.market_now(),'bybit_vip0_standard',persist=False)
                if retained:await self.engine.persist_async()
                self.fee_error=''
            missing_history=any((symbol,tf) not in self.history_cursor_cache
                for symbol in retained for tf in self.engine.settings.timeframes)
            if (not self.history_bootstrap_complete or missing_history) and (self.detection_replay_task is None or self.detection_replay_task.done()):
                self.detection_replay_task=asyncio.create_task(self.bootstrap_history_cursors(),
                    name='history-cursor-bootstrap')
            return
        refresh_keys=[]
        for symbol in sorted(retained):
            if symbol in self.context_unavailable and symbol not in base:
                continue
            # Feed-only pairs are hydrated in small background batches after
            # the detector connects. Loading hundreds of histories here would
            # delay both the initial WebSocket and the detector's startup.
            if symbol not in base:
                continue
            timeframes=([*self.engine.settings.timeframes,'D'] if symbol in base
                        else sorted(scope.get(symbol,{'5','D'})))
            for tf in timeframes:
                key=(symbol,tf)
                if key not in self.buffers:
                    cached=self.engine.store.candles(symbol,tf,1000)
                    if cached:
                        self.buffers[key]=cached
                tf_minutes=1440 if tf=='D' else int(tf)
                buf=self.buffers.get(key,[])
                start=buf[-1].time+tf_minutes*60000 if buf else None
                if start is None or start<=now-tf_minutes*60000:
                    # Catch up only bars missed during downtime. Reload the
                    # latest window if the saved tail is older than 1000 bars.
                    if buf and now-buf[-1].time>tf_minutes*60000*1000:
                        start=None
                    refresh_keys.append((symbol,tf,start))
                elif symbol in scope:
                    self.context_hydrated.add(key)
        async def load_history(symbol,tf,start):
            rows=await self.bybit.candles(symbol,tf,start=start,end=now if start is not None else None)
            tf_minutes=1440 if tf=='D' else int(tf)
            closed=[c for c in rows if c.time+tf_minutes*60000<=now]
            existing=self.buffers.get((symbol,tf),[])
            by_time={c.time:c for c in existing}
            by_time.update({c.time:c for c in closed})
            merged=sorted(by_time.values(),key=lambda c:c.time)[-1000:]
            await self.engine.store.aput_candles(symbol,tf,merged)
            return (symbol,tf),merged
        if refresh_keys:
            loaded=await asyncio.gather(*(load_history(symbol,tf,start) for symbol,tf,start in refresh_keys),
                                        return_exceptions=True)
            for request,item in zip(refresh_keys,loaded):
                if not isinstance(item,BaseException):
                    key,closed=item
                    self.buffers[key]=closed
                    self.context_hydrated.add(key)
        self.buffers={k:v for k,v in self.buffers.items() if k[0] in retained}
        seed_cursors={}
        for (symbol,tf),buf in self.buffers.items():
            if buf:
                minutes=1440 if tf=='D' else int(tf)
                cursor=self.engine.store.cursor(symbol,tf)
                if cursor is None:
                    cursor=buf[-1].time
                    seed_cursors[(symbol,tf)]=cursor
                close=cursor+minutes*60000
                self.last_closed_scan[(symbol,tf)]=close
                self.last_received_close[(symbol,tf)]=close
        await self.engine.store.aset_cursors(seed_cursors)
        # Queue startup recovery and current-geometry reconstruction behind live
        # work so history loading never keeps the Bybit socket disconnected.
        if self.detection_workers and (self.detection_replay_task is None or
                                       self.detection_replay_task.done()):
            self.detection_replay_task=asyncio.create_task(
                self.replay_pending_detections(),name='pending-candle-detection-replay')
        if self.bybit.fee_mode=='standard':
            for symbol in retained:
                self.engine.set_fee(symbol,.00055,self.market_now(),'bybit_vip0_standard',persist=False)
            if retained:await self.engine.persist_async()
            self.fee_error=''
        self.last_refresh=now_ms()
        self.refreshing_market=False

    async def bootstrap_history_cursors(self):
        while True:
            try:
                for symbol in sorted(self.market_symbols):
                    for tf in self.engine.settings.timeframes:
                        key=(symbol,tf);interval=int(tf)*60_000
                        if key in self.history_cursor_cache:continue
                        cursor=await self.engine.store.aget_cursor(symbol,tf)
                        scan_cursor=await self.engine.store.aget_scan_cursor(symbol,tf)
                        if cursor is not None:
                            rows=await asyncio.to_thread(self.engine.store.candles_range,symbol,tf,
                                limit=251,start=max(0,cursor-250*interval),end=cursor+interval)
                            self.buffers[key]=rows
                            self.last_closed_scan[key]=cursor+interval
                            self.last_received_close[key]=cursor+interval
                        self.history_cursor_cache[key]=cursor
                        self.history_scan_cursor_cache[key]=scan_cursor
                self.history_bootstrap_complete=True
                self.history_dispatch_event.set()
                if self.error=='Не удалось подготовить восстановление истории свечей':self.error=''
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.getLogger(__name__).exception('History cursor bootstrap failed')
                self.error='Не удалось подготовить восстановление истории свечей'
                await asyncio.sleep(5)

    async def refresh_watched_context(self):
        """Hydrate newly selected old signals and recover stale signal quotes."""
        while True:
            try:
                if not self.last_refresh or self.refreshing_market:
                    await asyncio.sleep(1)
                    continue
                try:
                    await asyncio.wait_for(self.context_refresh_event.wait(),timeout=2)
                except asyncio.TimeoutError:
                    pass
                self.context_refresh_event.clear()
                scope=self.signal_context_scope()
                for chart in self.watched_charts.values():
                    if chart:
                        symbol,tf=chart['symbol'],chart['timeframe']
                        scope.setdefault(symbol,set()).update((tf,BIAS_TF,FIGURE_TF,ENTRY_TF,'D'))
                selected=self.spot_scope_symbols()
                if selected!=self.selected_context_symbols:
                    self.selected_context_symbols=selected
                    await self.refresh_spot_scope()
                if scope!=self.context_scope:
                    self.context_scope=scope
                    self.context_unavailable={symbol:reason for symbol,reason in self.context_unavailable.items()
                                              if symbol in scope}
                    self.context_refresh_event.set()
                self.market_symbols=(set(self.symbols)|{p['symbol'] for p in self.engine.positions}
                                     |set(scope))
                now=self.market_now()
                forecast_symbols={row.get('symbol') for row in self.forecast_setups()
                                  if row.get('symbol')}
                stale=[]
                fast_quote_refresh=False
                for symbol in scope:
                    quote_time=int(self.tickers.get(symbol,{}).get('_quote_time',0) or 0)
                    forecast=symbol in forecast_symbols
                    max_age=3_000 if forecast else 30_000
                    retry_after=2_500 if forecast else 30_000
                    if (now-quote_time>max_age and now-self.quote_retry_at.get(symbol,0)>=retry_after
                            and symbol not in self.context_unavailable):
                        stale.append(symbol)
                        fast_quote_refresh=fast_quote_refresh or forecast
                # Give stale quotes priority over candle backfills. Limiting the
                # batch prevents hundreds of low activity pairs from occupying
                # every REST slot at once.
                if stale:
                    await self.refresh_context_quotes(stale,
                        retry_after_ms=2_500 if fast_quote_refresh else 30_000)
                changed={symbol:{tf for tf in timeframes if (symbol,tf) not in self.context_hydrated}
                         for symbol,timeframes in scope.items()}
                changed={symbol:timeframes for symbol,timeframes in changed.items() if timeframes and
                         now-self.context_load_retry_at.get(symbol,0)>=30_000}
                changed=dict(list(changed.items())[:4])
                if changed and (self.context_hydration_task is None or self.context_hydration_task.done()):
                    for symbol in changed:self.context_load_retry_at[symbol]=now
                    self.context_hydration_task=asyncio.create_task(self.hydrate_context_batch(changed))
                now=now_ms()
                self.books={symbol:book for symbol,book in self.books.items()
                            if symbol in self.market_symbols}
                self.live_flow={key:value for key,value in self.live_flow.items()
                                if key[0] in self.market_symbols}
                self.spot_matches={symbol:spot for symbol,spot in self.spot_matches.items()
                                   if symbol in self.market_symbols}
                self.buffers={key:value for key,value in self.buffers.items()
                              if key[0] in self.market_symbols}
            except asyncio.CancelledError:
                raise
            except BybitNetworkError:
                await asyncio.sleep(2)
            except Exception as exc:
                logging.getLogger(__name__).exception('Signal context refresh failed')
                self.error=f"Обновление контекста сигналов: {self.safe_error(exc)}"
                await asyncio.sleep(2)

    async def hydrate_context_batch(self,scope):
        try:
            await self.load_context_symbols(scope)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logging.getLogger(__name__).warning('Could not hydrate signal context: %s',
                self.safe_error(exc))

    async def refresh_context_quotes(self,symbols,retry_after_ms=30_000):
        """Refresh many quiet feed pairs with one market-wide REST snapshot."""
        async with self.context_quote_batch_lock:
            now=now_ms()
            requested=[symbol for symbol in symbols
                if symbol not in self.context_unavailable
                and now-self.quote_retry_at.get(symbol,0)>=retry_after_ms]
            if not requested:
                return
            for symbol in requested:
                self.quote_retry_at[symbol]=now
            try:
                requested_at=now+int(getattr(self.bybit,'offset',0) or 0)
                rows=await self.bybit.tickers()
                by_symbol={row.get('symbol'):row for row in rows if row.get('symbol')}
                updated=False
                for symbol in requested:
                    if self.quotes.get(symbol,{}).get('time',0)>requested_at:
                        continue
                    row=by_symbol.get(symbol)
                    if not row or not row.get('lastPrice'):
                        if symbol not in self.engine.instruments:
                            self.context_unavailable[symbol]='Инструмент отсутствует среди торгуемых USDT-фьючерсов Bybit'
                            self.tickers.pop(symbol,None)
                            self.quotes.pop(symbol,None)
                        continue
                    received_at=now_ms()
                    timestamp=received_at+int(getattr(self.bybit,'offset',0) or 0)
                    row=dict(row);row['_quote_time']=timestamp
                    row['_quote_received_at']=received_at
                    row['_ticker_event_at']=timestamp
                    row['_ticker_received_at']=received_at
                    if 'fundingRate' in row:row['_funding_time']=timestamp
                    self.context_unavailable.pop(symbol,None)
                    self.context_errors.pop(symbol,None)
                    self.tickers[symbol]=row
                    price=float(row['lastPrice'])
                    self.quotes[symbol]={'price':price,'time':timestamp}
                    updated=True
                    closed=self.apply_or_buffer_position_tick(symbol,price,timestamp)
                    if closed:
                        await self.engine.persist_async(closed)
                if updated:
                    self.invalidate_feed_cache();self.broadcast_event.set()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                error=self.safe_error(exc)
                for symbol in requested:
                    self.context_errors[symbol]=error
                logging.getLogger(__name__).warning('Could not refresh feed quotes: %s',error)

    async def refresh_context_quote(self,symbol):
        lock=self.quote_locks.setdefault(symbol,asyncio.Lock())
        async with lock:
            now=now_ms()
            if now-self.quote_retry_at.get(symbol,0)<30_000:
                return
            self.quote_retry_at[symbol]=now
            try:
                async with self.context_quote_semaphore:
                    row=await self.bybit.ticker(symbol)
                requested_at=now+int(getattr(self.bybit,'offset',0) or 0)
                received_at=now_ms()
                timestamp=received_at+int(getattr(self.bybit,'offset',0) or 0)
                if self.quotes.get(symbol,{}).get('time',0)>requested_at:
                    return
                if not row or not row.get('lastPrice'):
                    self.context_unavailable[symbol]='Инструмент отсутствует среди торгуемых USDT-фьючерсов Bybit'
                    self.tickers.pop(symbol,None)
                    self.quotes.pop(symbol,None)
                    return
                self.context_unavailable.pop(symbol,None)
                self.context_errors.pop(symbol,None)
                row['_quote_time']=timestamp
                row['_quote_received_at']=received_at
                row['_ticker_event_at']=timestamp
                row['_ticker_received_at']=received_at
                if 'fundingRate' in row:row['_funding_time']=timestamp
                self.tickers[symbol]=row
                price=float(row['lastPrice'])
                self.quotes[symbol]={'price':price,'time':timestamp}
                closed_signals=self.apply_or_buffer_position_tick(symbol,price,timestamp)
                if closed_signals:
                    await self.engine.persist_async(closed_signals)
                self.invalidate_feed_cache();self.broadcast_event.set()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.context_errors[symbol]=self.safe_error(exc)
                logging.getLogger(__name__).warning('Could not refresh quote for %s: %s',
                    symbol,self.context_errors[symbol])

    async def load_context_symbols(self,scope):
        symbols=sorted(scope)
        if not symbols:
            return
        async def update_quote(symbol):
            if symbol in self.context_unavailable:
                return
            if symbol not in self.tickers or now_ms()-int(self.tickers[symbol].get('_quote_time',0) or 0)>30_000:
                await self.refresh_context_quote(symbol)
        await asyncio.gather(*(update_quote(symbol) for symbol in symbols))
        if self.split_mode:
            for symbol,timeframes in scope.items():
                if symbol in self.context_unavailable:
                    self.context_hydrated.update((symbol,tf) for tf in timeframes)
                    continue
                for tf in timeframes:self.request_live_hydration(symbol,tf)
            return
        for symbol,timeframes in scope.items():
            if symbol in self.context_unavailable:
                self.context_hydrated.update((symbol,tf) for tf in timeframes)
        now=now_ms()
        async def load_history(symbol,tf):
            key=(symbol,tf)
            if key not in self.buffers:
                self.buffers[key]=self.engine.store.candles(symbol,tf,1000)
            buf=self.buffers[key]
            minutes=1440 if tf=='D' else int(tf)
            start=buf[-1].time+minutes*60_000 if buf else None
            if start is not None and start>now-minutes*60_000:
                self.context_hydrated.add(key)
                return
            if buf and now-buf[-1].time>minutes*60_000*1000:
                start=None
            async with self.history_semaphore:
                rows=await self.bybit.candles(symbol,tf,start=start,end=now if start is not None else None)
            closed=[c for c in rows if c.time+minutes*60_000<=now]
            merged={c.time:c for c in buf}
            merged.update({c.time:c for c in closed})
            result=sorted(merged.values(),key=lambda c:c.time)[-1000:]
            await self.engine.store.aput_candles(symbol,tf,result)
            self.buffers[key]=result
            if result:
                self.last_closed_scan[(symbol,tf)]=result[-1].time+minutes*60_000
        jobs=[load_history(symbol,tf) for symbol,timeframes in scope.items()
              if symbol not in self.context_unavailable for tf in timeframes]
        results=await asyncio.gather(*jobs,return_exceptions=True)
        errors=[]
        for (symbol,tf),result in zip(((symbol,tf) for symbol,timeframes in scope.items()
                if symbol not in self.context_unavailable for tf in timeframes),results):
            if isinstance(result,BaseException):
                errors.append(result)
                self.context_errors[symbol]=self.safe_error(result)
            else:
                self.context_hydrated.add((symbol,tf))
                self.context_errors.pop(symbol,None)
        if errors:
            raise errors[0]
        await self.refresh_spot_scope()
        if self.engine.fee_mode=='standard':
            for symbol in symbols:
                if symbol in self.context_unavailable:
                    continue
                self.engine.set_fee(symbol,.00055,self.market_now(),'bybit_vip0_standard',persist=False)
            await self.engine.persist_async()
        else:
            self.fee_refresh_event.set()

    @staticmethod
    def safe_error(exc):
        # Network exception strings can contain URLs; display only controlled text.
        connection_closed=getattr(websockets,'ConnectionClosed',None)
        if connection_closed and isinstance(exc,connection_closed):
            close=getattr(exc,'rcvd',None) or getattr(exc,'sent',None)
            if close:
                reason=' '.join(str(getattr(close,'reason','')).split())[:120]
                detail=f'код {close.code}' + (f': {reason}' if reason else '')
                return f'WebSocket Bybit закрыл соединение ({detail})'
            return 'WebSocket Bybit закрыл соединение без кода и причины'
        if isinstance(exc,(httpx.RequestError,TimeoutError,OSError)):
            return f'Ошибка подключения ({type(exc).__name__})'
        if isinstance(exc,(TypeError,ValueError,KeyError,IndexError,json.JSONDecodeError)):
            return f'Ошибка обработки данных ({type(exc).__name__})'
        return str(exc) if isinstance(exc,RuntimeError) else f'Ошибка выполнения ({type(exc).__name__})'

    async def recover(self,*,set_phase=True):
        if set_phase:self.phase='recovering'
        now=self.market_now()
        # Reconstruct exits and funding together in chronological order.
        events=[]
        for p in list(self.engine.positions):
            symbol=p['symbol']
            start=self.engine.market_times.get(symbol,p['opened'])
            bars=await self.bybit.history(symbol,'1',start//60000*60000,now)
            for c in bars:
                end=min(c.time+60000-1,now)
                if end>start:
                    events.append((end,1,symbol,c))
            funding=await self.bybit.funding_history(symbol,p['funding_cursor']+1,now)
            due=p.get('next_funding',0)
            if due and p['funding_cursor']<due<=now and not any(t>=due for t,_ in funding):
                raise RuntimeError('Ожидание опубликованного funding для восстановления')
            for timestamp,rate in funding:
                marks=await self.bybit.candles(symbol,'1',timestamp,timestamp,mark=True)
                if not marks:
                    raise RuntimeError('Нет mark price для восстановления funding')
                events.append((timestamp,0,symbol,(rate,marks[0].open)))
        for timestamp,kind,symbol,value in sorted(events,key=lambda e:(e[0],e[1],e[2])):
            if kind==0:
                self.engine.funding_payment(symbol,value[0],timestamp,value[1])
            else:
                self.engine.bar(symbol,value,timestamp,True)
        await self.engine.persist_async()
        self.recovery_time=now

    async def recover_positions_with_live_ticks(self):
        self.positions_ready=False
        while True:
            self.position_recovery_overflow=False
            await self.recover(set_phase=False)
            cutoff=self.recovery_time
            if self.position_recovery_overflow:
                continue
            while self.position_recovery_ticks:
                ticks=sorted(self.position_recovery_ticks,key=lambda row:row[0])
                self.position_recovery_ticks=[]
                changed=[]
                for timestamp,symbol,price in ticks:
                    if timestamp<=cutoff:continue
                    if any(p.get('next_funding',0) and p['next_funding']<=timestamp and
                           p.get('funding_cursor',0)<p['next_funding'] for p in self.engine.positions):
                        await self.settle_due_funding(as_of=timestamp)
                    changed.extend(self.engine.tick(symbol,price,timestamp,persist=False))
                await self.engine.persist_async(changed)
                if changed:
                    self.invalidate_feed_cache();self.broadcast_event.set()
                if self.position_recovery_overflow:break
            if self.position_recovery_overflow:
                continue
            # No await between checking the buffer and enabling normal ticks.
            self.positions_ready=True
            return

    async def settle_due_funding(self,*,as_of=None):
        now=self.market_now() if as_of is None else as_of
        for p in list(self.engine.positions):
            ticker=self.tickers.get(p['symbol'],{})
            next_time=int(ticker.get('nextFundingTime',0) or 0)
            # Stream may advance nextFundingTime before settlement; retain a due
            # timestamp on each position instead of relying on the latest ticker.
            due=p.get('next_funding',next_time)
            if due and due<=p['funding_cursor']:
                due=next_time if next_time>now else p['funding_cursor']+self.engine.instruments[p['symbol']].funding_interval*60000
                p['next_funding']=due
            if due and now>=due:
                rows=await self.bybit.funding_history(p['symbol'],p['funding_cursor']+1,now)
                if not any(t>=due for t,_ in rows):
                    raise RuntimeError('Ожидание опубликованного funding')
                for timestamp,rate in rows:
                    marks=await self.bybit.candles(p['symbol'],'1',timestamp,timestamp,mark=True)
                    if not marks:
                        raise RuntimeError('Нет mark price для funding')
                    self.engine.funding_payment(p['symbol'],rate,timestamp,marks[0].open)
                if p['funding_cursor']>=due:
                    p['next_funding']=next_time if next_time>now else due+self.engine.instruments[p['symbol']].funding_interval*60000
            elif due:
                p['next_funding']=due

    async def flush(self):
        now=self.market_now()
        # Batch concurrent timeframe candidates for direction-conflict selection.
        ready=[s for s in self.pending if now-s.time>=1500 and
               (self.quotes.get(s.symbol,{}).get('time',0)>=s.time or now-s.time>30000)]
        if ready:
            ids={s.id for s in ready}
            self.pending=[s for s in self.pending if s.id not in ids]
            rates={s:float(t.get('fundingRate',0) or 0) for s,t in self.tickers.items()}
            if not self.engine.strategy_approved:
                for s in ready:
                    s.status='found'
                    s.reason='Пробой найден. Автоматический виртуальный вход отключён: стратегия ещё не прошла исторический допуск.'
                    s.filter_results['auto_entry_allowed']=False
                await self.engine.persist_async(ready)
                self.invalidate_feed_cache()
                self.broadcast_event.set()
                return
            executable=[]
            for signal in ready:
                if (signal.strategy_version!=VERSION or not signal.filter_results.get('auto_eligible')
                        or not signal.filter_results.get('volume_pass')):
                    signal.status='rejected';signal.reason='Фильтры стратегии отсутствуют или не пройдены'
                    continue
                fee=self.engine.fees.get(signal.symbol,{}).get('rate',.00055)
                tick=self.engine.instruments.get(signal.symbol).tick_size if signal.symbol in self.engine.instruments else 0.
                quote=self.quotes.get(signal.symbol,{})
                if not self.active_variant or not quote:
                    signal.status='rejected'; signal.reason='Нет активного проверенного варианта или котировки'
                    continue
                ok,reason,details=prepare_signal(signal,self.active_variant,quote['price'],fee,
                    self.engine.settings.slippage,rates.get(signal.symbol,0.),tick)
                signal.filter_results.update(details)
                if not ok:
                    signal.status='rejected'; signal.reason=reason
                else:
                    execution=self.assess_signal_execution(signal,quote['price'],now)
                    signal.filter_results['execution']=execution
                    if execution['ready']:
                        executable.append(signal)
                    else:
                        signal.status='rejected'
                        signal.reason='Execution blocked: '+', '.join(execution['reasons'])
            rejected=[s for s in ready if s.status=='rejected']
            if rejected:
                await self.engine.persist_async(rejected)
                self.invalidate_feed_cache()
                self.broadcast_event.set()
            ready=executable
            if not ready:
                return
            processed=self.engine.process(ready,self.quotes,now,rates,persist=False)
            await self.engine.persist_async(processed)
            for p in self.engine.positions:
                if 'next_funding' not in p:
                    p['next_funding']=int(self.tickers.get(p['symbol'],{}).get('nextFundingTime',0) or 0)
            await self.engine.persist_async()
            self.invalidate_feed_cache()
            self.broadcast_event.set()

    async def message(self,msg,fast=False):
        topic=msg.get('topic','')
        if topic.startswith('kline.'):
            await self.enqueue_candle(msg,wait=True)
            return
        if topic.startswith('tickers.'):
            symbol=topic.split('.')[1]
            data=msg.get('data') or {}
            timestamp=int(msg.get('ts',0) or data.get('ts',0) or 0)
            received_at=now_ms()
            offset=int(getattr(self.bybit,'offset',0) or 0)
            if timestamp:self.network_delay_ms.append(max(0,received_at-(timestamp-offset)))
            existing=self.quotes.get(symbol,{})
            has_price='lastPrice' in data
            incoming=dict(data)
            if has_price and timestamp and timestamp<=existing.get('time',0):
                incoming.pop('lastPrice',None)
                has_price=False
            ticker=self.tickers.setdefault(symbol,{})
            ticker.update(incoming)
            ticker['_ticker_event_at']=timestamp or None
            ticker['_ticker_received_at']=received_at
            if 'openInterest' in incoming:
                try:
                    oi=float(incoming['openInterest'])
                    oi_value=float(incoming['openInterestValue']) if incoming.get('openInterestValue') not in (None,'') else None
                    if not (oi>0 and __import__('math').isfinite(oi) and
                            (oi_value is None or (oi_value>0 and __import__('math').isfinite(oi_value)))):
                        raise ValueError('invalid open interest')
                    ticker['_openInterest_time']=timestamp or received_at
                    ticker['_openInterest_received_at']=received_at
                    minute=(timestamp or received_at)//60_000*60_000
                    if self.open_interest_sampled.get(symbol)!=minute:
                        self.pending_open_interest[symbol]={'symbol':symbol,'time':minute,'value':oi,
                            'value_usdt':oi_value,'source':'ticker','received_at':received_at}
                        self.open_interest_sampled[symbol]=minute
                except (TypeError,ValueError):
                    ticker['_openInterest_invalid_at']=received_at
            # Funding must be applied before exits crossing its settlement time.
            if has_price and timestamp:
                self.quote_broadcast_pending[symbol]=asyncio.get_running_loop().time()
                self.tickers[symbol]['_quote_time']=timestamp
                self.tickers[symbol]['_quote_received_at']=received_at
                if 'fundingRate' in data or 'nextFundingTime' in data:
                    self.tickers[symbol]['_funding_time']=timestamp
                try:price=float(data['lastPrice'])
                except (TypeError,ValueError):price=float('nan')
                if not __import__('math').isfinite(price) or price<=0:
                    self.market_data_errors=getattr(self,'market_data_errors',{})
                    self.market_data_errors[symbol]={'status':'invalid','reason':'invalid_ticker_price',
                        'event_at':timestamp,'received_at':received_at}
                    return
                self.quotes[symbol]={'price':price,'time':timestamp}
                self.last_price_received_at=received_at
                self.last_exchange_event_at=timestamp
                price_changed=price!=existing.get('price')
                closed_signals=(self.apply_or_buffer_position_tick(symbol,price,timestamp)
                                if price_changed else [])
                if closed_signals:
                    await self.engine.persist_async(closed_signals)
                    self.invalidate_feed_cache();self.broadcast_event.set()
                changed=[]
                for tf in self.engine.settings.timeframes:
                    for setup in self.active_setups_for(symbol,tf):
                        transition_due=(not fast or
                            self.setup_price_state_transition_due(setup,price))
                        periodic_due=(timestamp-self.setup_price_state_sampled_at.get(setup['id'],0)>=250)
                        if fast and not (periodic_due or transition_due):
                            continue
                        assessment_due=(timestamp-self.setup_assessment_sampled_at.get(setup['id'],0)>=1000)
                        include_assessment=(not fast or assessment_due or
                            symbol in self.context_scope or transition_due)
                        current=self.setup_price_state(setup,price,timestamp,persist=False,
                            include_entry_assessment=include_assessment)
                        if include_assessment:self.setup_assessment_sampled_at[setup['id']]=timestamp
                        self.setup_price_state_sampled_at[setup['id']]=timestamp
                        if current.pop('_persist',False):changed.append(setup)
                if changed:
                    try:
                        results=await self.engine.store.asave_setups(changed)
                    except Exception:
                        self.invalidate_active_setups()
                        raise
                    self.reconcile_active_setup_cache(changed)
                    if any(results):self.broadcast_event.set()
                scalp_watch=(self.engine.strategy_approved or symbol in self.context_scope or
                    symbol in {position.get('symbol') for position in self.engine.positions} or
                    symbol in {row.get('symbol') for row in self.engine.strategy_pending})
                if (scalp_watch and price_changed and
                        timestamp-self.scalp_last_tracked_at.get(symbol,0)>=1000):
                    self.scalp_last_tracked_at[symbol]=timestamp
                    await self.track_scalp_setups(symbol,price,timestamp)
                monotonic_now=asyncio.get_running_loop().time()
                if closed_signals:
                    self.last_quote_broadcast_at=monotonic_now
                    self.broadcast_event.set()
                elif monotonic_now-self.last_quote_broadcast_at>=.25:
                    self.last_quote_broadcast_at=monotonic_now
                    self.broadcast_event.set()
        elif topic.startswith('orderbook.'):
            symbol=topic.rsplit('.',1)[-1]
            apply_orderbook(self.books,symbol,msg,now_ms(),getattr(self.bybit,'offset',0))
        elif topic.startswith('publicTrade.'):
            for trade in msg.get('data',[]):
                symbol=trade.get('s') or topic.split('.',1)[1]
                try:
                    time=int(trade['T']);notional=float(trade['p'])*float(trade['v'])
                except (ValueError,TypeError,KeyError):
                    continue
                trade_id=str(trade.get('i') or '')
                dedupe_key=(symbol,trade_id) if trade_id else None
                if dedupe_key and dedupe_key in self.seen_trade_ids:
                    continue
                if dedupe_key:
                    self.seen_trade_ids[dedupe_key]=None
                    while len(self.seen_trade_ids)>100_000:self.seen_trade_ids.popitem(last=False)
                received_at=now_ms()
                key=(symbol,time//60_000*60_000)
                bucket=self.live_flow.setdefault(key,{'buy':0.,'sell':0.,'updated':time,
                    'first_received_at':received_at,'coverage_ms':0,'quality':'partial'})
                if trade.get('S')=='Buy':bucket['buy']+=notional
                elif trade.get('S')=='Sell':bucket['sell']+=notional
                bucket['updated']=received_at
                bucket['last_trade_at']=time
        elif topic.startswith('allLiquidation.'):
            for event in msg.get('data',[]):
                try:
                    symbol=str(event['s']);event_time=int(event['T'])
                    notional=float(event['p'])*float(event['v'])
                    if not symbol or not __import__('math').isfinite(notional) or notional<=0:continue
                except (KeyError,TypeError,ValueError):
                    continue
                key=(symbol,event_time//60_000*60_000)
                bucket=self.live_liquidations.setdefault(key,{'long_usdt':0.,'short_usdt':0.,
                    'count':0,'updated':event_time,'received_at':now_ms()})
                # Bybit's event price is the bankruptcy price. Side indicates
                # the liquidation order side: Sell closes longs; Buy closes shorts.
                if event.get('S')=='Sell':bucket['long_usdt']+=notional
                elif event.get('S')=='Buy':bucket['short_usdt']+=notional
                else:continue
                bucket['count']+=1;bucket['updated']=event_time;bucket['received_at']=now_ms()
            self.liquidation_last_message=now_ms()


    async def kline_message(self,msg,tf,symbol,recovered=False,defer_detection=False,
                            received_monotonic=None):
        for row in msg['data']:
            if symbol not in self.tracked_symbols():
                continue
            valid,reason=valid_ohlc(row)
            if not valid:
                errors=getattr(self,'market_data_errors',{})
                errors[symbol]={'status':'invalid','reason':reason,
                    'event_at':int(msg.get('ts',0) or 0),'received_at':now_ms(),'timeframe':tf}
                self.market_data_errors=errors
                self.next_candle_recovery=0
                continue
            key=(symbol,tf)
            c=Candle(int(row['start']),float(row['open']),float(row['high']),float(row['low']),
                     float(row['close']),float(row['volume']),
                     float(row['turnover']) if row.get('turnover') not in (None,'') else None)
            if not row.get('confirm'):
                self.live_candles[(symbol,tf)]=asdict(c)
                continue
            self.live_candles.pop((symbol,tf),None)
            self.last_closed_candles[(symbol,tf)]=asdict(c)
            full_buffer=list(self.buffers.get(key,[]))
            cursor=await self.engine.store.aget_cursor(symbol,tf)
            tf_minutes=1440 if tf=='D' else int(tf)
            interval=tf_minutes*60000
            if cursor is not None and c.time<=cursor:
                continue
            if cursor is None and full_buffer and c.time<=full_buffer[-1].time:
                continue
            prior=[bar for bar in full_buffer if bar.time<c.time]
            expected=(cursor+interval if cursor is not None else
                      prior[-1].time+interval if prior else None)
            if expected is not None and c.time!=expected:
                if c.time<expected:
                    raise RuntimeError(
                        f'{symbol}/{tf}: закрытая свеча {c.time} пришла раньше ожидаемой {expected}')
                if self.split_mode:
                    # The historical dispatcher fetches gaps before acquiring
                    # this stream lock; do not hold it across network I/O.
                    raise RuntimeError(f'{symbol}/{tf}: отсутствует закрытая свеча {expected}')
                # Live bars and REST backfill use separate queues. A live worker
                # can therefore win the per-stream lock while an older REST bar
                # is waiting. Restore the exact gap here before committing this
                # later close, so it is neither dropped nor scanned out of order.
                if hasattr(self.bybit,'history'):
                    missing=await self.bybit.history(symbol,tf,expected,c.time-1)
                else:
                    missing=await self.bybit.candles(symbol,tf,start=expected,end=c.time-1)
                if not missing:
                    missing=await self.bybit.candles(symbol,tf,start=expected,end=c.time-1)
                if symbol not in self.tracked_symbols():
                    return
                expected_missing=expected
                for bar in sorted({item.time:item for item in missing}.values(),key=lambda item:item.time):
                    if symbol not in self.tracked_symbols():
                        return
                    if bar.time<expected_missing:
                        continue
                    if bar.time!=expected_missing or bar.time+interval>c.time:
                        break
                    recovered_row={'start':bar.time,'open':bar.open,'high':bar.high,'low':bar.low,
                        'close':bar.close,'volume':bar.volume,'turnover':bar.turnover,'confirm':True}
                    await self.kline_message({'topic':f'kline.{tf}.{symbol}','data':[recovered_row]},
                        tf,symbol,recovered=True,defer_detection=defer_detection)
                    if symbol not in self.tracked_symbols():
                        return
                    committed=await self.engine.store.aget_cursor(symbol,tf)
                    if committed is None or committed<bar.time:
                        raise RuntimeError(f'{symbol}/{tf}: не обработана восстановленная свеча {bar.time}')
                    expected_missing+=interval
                if expected_missing!=c.time:
                    raise RuntimeError(
                        f'{symbol}/{tf}: REST не восстановил закрытую свечу {expected_missing}')
                full_buffer=list(self.buffers.get(key,[]))
                cursor=await self.engine.store.aget_cursor(symbol,tf)
                prior=[bar for bar in full_buffer if bar.time<c.time]
                expected=(cursor+interval if cursor is not None else
                          prior[-1].time+interval if prior else None)
                if expected is not None and c.time!=expected:
                    raise RuntimeError(
                        f'{symbol}/{tf}: после REST-восстановления ожидалась свеча {expected}, получена {c.time}')
            if cursor is not None and (not prior or prior[-1].time<cursor):
                # Startup may have loaded newer bars into the buffer. Recover
                # the actual historical context before scanning this candle.
                saved=self.engine.store.candles_range(symbol,tf,limit=1000,
                    start=max(0,c.time-interval*999),end=c.time)
                prior=sorted({bar.time:bar for bar in [*saved,*prior] if bar.time<c.time}.values(),
                             key=lambda bar:bar.time)
            buf=prior[-999:]+[c]
            self.buffers[key]=buf
            updates=[];found=[];discovered=[];cancelled=[]
            if tf!='D' and tf in self.engine.settings.timeframes:
                stage_started=asyncio.get_running_loop().time()
                updates=self.advance_setups(symbol,tf,c,buf,persist=False)
                self.record_candle_stage('advance_setups',stage_started)
                cancelled=self.advance_retests(symbol,tf,c,persist=False)
            if defer_detection:
                interval=tf_minutes*60_000
                self.buffers[key]=buf
                stage_started=asyncio.get_running_loop().time()
                await self.engine.store.acommit_candle_transition(
                    symbol,tf,c,updates,c.time,
                    scan_floor=(cursor if cursor is not None else
                                prior[-1].time if prior else c.time-interval),
                    priority=1 if self.split_mode and recovered else 0)
                self.record_candle_stage('persist',stage_started)
                if updates:
                    self.setup_cache=None
                if cancelled:
                    await self.engine.persist_async(cancelled)
                self.last_closed_scan[key]=c.time+interval
                self.last_received_close[key]=max(self.last_received_close.get(key,0),c.time+interval)
                if updates or cancelled:
                    self.broadcast_event.set()
                if self.split_mode and tf!='D' and tf in self.engine.settings.timeframes and symbol in self.symbols:
                    self.history_cursor_cache[key]=c.time
                    self.history_dispatch_event.set()
                elif tf!='D' and tf in self.engine.settings.timeframes and symbol in self.symbols:
                    await self.enqueue_detection_job(symbol,tf,self.buffers[key],c.time,
                        recovered=recovered,received_at=received_monotonic)
                else:
                    await self.engine.store.aset_scan_cursor(symbol,tf,c.time,priority=1 if self.split_mode else 0)
                    if self.split_mode:self.history_cursor_cache[key]=c.time
                continue

            stage_started=asyncio.get_running_loop().time()
            await self.engine.store.aput_candles(symbol,tf,[c])
            self.record_candle_stage('persist',stage_started)
            if updates:
                await self.engine.store.asave_setups(updates)
                self.setup_cache=None
            if cancelled:await self.engine.persist_async(cancelled)
            if tf!='D' and tf in self.engine.settings.timeframes:
                if symbol in self.symbols:
                    stage_started=asyncio.get_running_loop().time()
                    found=await self.filter_candidates(symbol,tf,buf,recovered=recovered)
                    self.record_candle_stage('filter_candidates',stage_started)
                    if found:
                        self.pending.extend(found)
                        for signal in found:signal.published_at=now_ms()
                        await self.engine.persist_async(found)
                        self.last_signal_at=max(self.last_signal_at,max((signal.time for signal in found),default=0))
                        self.invalidate_feed_cache()
                        task=asyncio.create_task(self.attach_signal_snapshots(found))
                        self.snapshot_tasks.add(task)
                        task.add_done_callback(self.snapshot_tasks.discard)
                if symbol in self.symbols:
                    stage_started=asyncio.get_running_loop().time()
                    discovered=await self.discover_setups(symbol,tf,buf,recovered=recovered,publish=False)
                    self.record_candle_stage('discover_setups',stage_started)
                else:
                    discovered=[]
            interval=(1440 if tf=='D' else int(tf))*60_000
            self.buffers[key]=buf
            await self.engine.store.aset_cursor(symbol,tf,c.time)
            self.last_closed_scan[(symbol,tf)]=c.time+interval
            self.last_received_close[(symbol,tf)]=max(
                self.last_received_close.get((symbol,tf),0),c.time+interval)
            if updates or found or discovered or cancelled:self.broadcast_event.set()
            await self.engine.store.aset_scan_cursor(symbol,tf,c.time)

    async def finish_candle_detection(self,job,*,archival=False):
        """Run new-shape and manual-signal searches after the close is committed."""
        symbol,tf=job.symbol,job.timeframe
        buf=list(job.candles)
        try:
            if archival and buf:
                key=(symbol,tf)
                if key not in self.history_setup_states:
                    saved=await self.engine.store.ahistory_setups(symbol,tf)
                    terminal_saved=[row for row in saved if row.get('status') in
                        ('breakout_confirmed','cancelled','expired','target_reached')]
                    if terminal_saved:await self.engine.store.apublish_historical_setups(terminal_saved)
                    self.history_setup_states[key]={row['id']:row for row in saved if row.get('status') in
                        ('wait_breakout','near_boundary','crossing')}
                states=self.history_setup_states[key]
                active=[row for row in states.values()
                        if row.get('status') in ('wait_breakout','near_boundary','crossing')]
                transitions=self.advance_setups(symbol,tf,buf[-1],buf,persist=False,setups_override=active)
                if transitions:
                    await self.engine.store.asave_history_setup_states(transitions)
                    terminal=[row for row in transitions if row.get('status') in
                              ('breakout_confirmed','cancelled','expired','target_reached')]
                    if terminal:
                        terminal_ids={row['id'] for row in terminal}
                        saved=await self.engine.store.ahistory_setups(symbol,tf)
                        await self.engine.store.apublish_historical_setups(
                            [row for row in saved if row['id'] in terminal_ids])
                        for row in terminal:states.pop(row['id'],None)
                        self.setup_cache=None
            detected_signals=detected_setups=None
            if job.detect_signals and job.detect_setups:
                # Breakout and early-setup profiles share one process task,
                # candle snapshot, and pivot preparation. Their ATR and
                # sensitivity rules remain independent.
                sensitivity=('standard' if self.engine.strategy_approved and self.active_variant
                              else 'expanded')
                loop=asyncio.get_running_loop()
                detect_started=loop.time()
                executor=self.history_detection_executor if archival and self.history_detection_executor else self.detection_executor
                detected_signals,detected_setups=await loop.run_in_executor(
                    executor,_detect_combined_worker,symbol,tf,
                    _snapshot_candles(buf),sensitivity,'early',True)
                self.record_signal_stage('detect',detect_started)
            found=[]
            if job.detect_signals:
                stage_started=asyncio.get_running_loop().time()
                if self.split_mode:
                    if archival:
                        context={}
                        event=job.candle_time+int(tf)*60_000
                        for context_tf in ('5','15','60'):
                            step=int(context_tf)*60_000
                            context[(symbol,context_tf)]=await asyncio.to_thread(
                                self.engine.store.candles_range,symbol,context_tf,limit=251,
                                start=max(0,event-251*step),end=event-step+1)
                    else:
                        context={(symbol,key):value for key,value in (job.context_buffers or {}).items()}
                    found=await self.filter_candidates(symbol,tf,buf,recovered=archival,
                        detected=detected_signals,received_wall_ms=job.received_wall_ms,
                        source=job.source,context_buffers=context,archival=archival)
                else:
                    found=await self.filter_candidates(symbol,tf,buf,recovered=job.recovered,
                        detected=detected_signals)
                self.record_candle_stage('filter_candidates',stage_started)
            if found:
                if archival:
                    inserted=set(await self.engine.store.asave_historical_signals([signal.dict() for signal in found]))
                    found=[signal for signal in found if signal.id in inserted]
                else:
                    if not self.split_mode:
                        for signal in found:signal.published_at=now_ms()
                    if self.split_mode and job.received_at is not None:
                        for signal in found:
                            self.live_signal_receipts[signal.id]=job.received_at
                            self.live_signal_receipts.move_to_end(signal.id)
                        while len(self.live_signal_receipts)>10_000:self.live_signal_receipts.popitem(last=False)
                    await self.engine.persist_async(found)
                    self.pending.extend(found)
                self.last_signal_at=max(self.last_signal_at,max((signal.time for signal in found),default=0))
                self.invalidate_feed_cache()
                if found and not archival:
                    task=asyncio.create_task(self.attach_signal_snapshots(found))
                    self.snapshot_tasks.add(task)
                    task.add_done_callback(self.snapshot_tasks.discard)
            discovered=[]
            if job.detect_setups:
                stage_started=asyncio.get_running_loop().time()
                if self.split_mode:
                    discovered=await self.discover_setups(symbol,tf,buf,recovered=archival,
                        publish=False,detected=detected_setups,received_wall_ms=job.received_wall_ms,
                        source=job.source,archival=archival)
                else:
                    discovered=await self.discover_setups(symbol,tf,buf,
                        recovered=job.recovered,publish=False,detected=detected_setups)
                self.record_candle_stage('discover_setups',stage_started)
            if found or discovered:
                self.broadcast_event.set()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logging.getLogger(__name__).exception('Deferred candle detection failed for %s %s',symbol,tf)
            self.error=f'Candle detection: {self.safe_error(exc)}'
            raise

    async def recover_closed_candles(self,stale):
        """Fetch and scan bars missed by the live stream without resetting the socket."""
        async with self.candle_recovery_lock:
            return await self._recover_closed_candles(stale)

    async def _recover_closed_candles(self,stale):
        now=now_ms()+int(getattr(self.bybit,'offset',0) or 0)
        pairs=sorted({(row['symbol'],row['timeframe']) for row in stale})
        async def recover_one(symbol,tf):
            interval=int(tf)*60_000
            buf=self.buffers.get((symbol,tf),[])
            cursor=await self.engine.store.aget_cursor(symbol,tf)
            start=(cursor+interval) if cursor is not None else (buf[-1].time+interval if buf else None)
            received=None;received_monotonic=None
            if start is not None and hasattr(self.bybit,'history'):
                if self.split_mode and hasattr(self.bybit,'history_with_timing'):
                    rows,received,received_monotonic=await self.bybit.history_with_timing(symbol,tf,start,now)
                else:
                    rows=await self.bybit.history(symbol,tf,start,now)
                    received=now_ms();received_monotonic=asyncio.get_running_loop().time()
                if not rows:
                    rows=await self.bybit.candles(symbol,tf,start=start,end=now)
                    received=now_ms();received_monotonic=asyncio.get_running_loop().time()
            else:
                if self.split_mode and hasattr(self.bybit,'candles_with_timing'):
                    rows,received,received_monotonic=await self.bybit.candles_with_timing(symbol,tf,start=start,end=now)
                else:
                    rows=await self.bybit.candles(symbol,tf,start=start,end=now)
                    received=now_ms();received_monotonic=asyncio.get_running_loop().time()
            closed=[c for c in rows if (start is None or c.time>=start) and c.time+interval<=now]
            if self.split_mode and closed and self.market_now()-(closed[-1].time+interval)<=interval:
                self.schedule_live_observation(symbol,tf,closed[-1],received,received_monotonic,'rest')
            recovered=0
            for c in closed:
                if symbol not in self.tracked_symbols():
                    break
                row={'start':c.time,'open':c.open,'high':c.high,'low':c.low,
                     'close':c.close,'volume':c.volume,'turnover':c.turnover,'confirm':True}
                await self.enqueue_candle({'topic':f'kline.{tf}.{symbol}','data':[row]},
                                           wait=True,recovered=True)
                if symbol not in self.tracked_symbols():
                    break
                committed=await self.engine.store.aget_cursor(symbol,tf)
                if committed is None or committed<c.time:
                    raise RuntimeError(f'{symbol}/{tf}: не обработана восстановленная свеча {c.time}')
                recovered+=1
            return recovered
        results=await asyncio.gather(*(recover_one(symbol,tf) for symbol,tf in pairs),
                                     return_exceptions=True)
        if any(isinstance(result,asyncio.CancelledError) for result in results):
            raise asyncio.CancelledError
        recovered=sum(result for result in results if isinstance(result,int))
        errors=[(symbol,tf,result)
                for (symbol,tf),result in zip(pairs,results) if isinstance(result,BaseException)]
        return recovered,errors

    async def run_candle_watchdog(self):
        """REST-poll a closed scanner bar two seconds after its expected close."""
        while True:
            await asyncio.sleep(1)
            if (not self.last_refresh or self.refreshing_market or
                    (self.split_mode and not self.history_bootstrap_complete)):
                continue
            if now_ms()<self.next_candle_recovery:
                continue
            try:
                now=now_ms(); event_now=self.market_now()
                stale=[]
                for symbol in self.symbols:
                    for tf in self.engine.settings.timeframes:
                        interval=int(tf)*60_000
                        close=(event_now//interval)*interval
                        key=(symbol,tf)
                        received=self.last_received_close.get(key,0)
                        if close and event_now>=close+2_000 and received<close:
                            last_try=self.candle_poll_attempts.get(key,0)
                            if now-last_try>=2_000:
                                stale.append({'symbol':symbol,'timeframe':tf,'expected_close':close})
                                if not self.split_mode:self.candle_poll_attempts[key]=now
                for chart in self.watched_charts.values():
                    if not chart:continue
                    symbol,tf=chart['symbol'],chart['timeframe'];key=(symbol,tf)
                    if symbol in self.symbols:continue
                    interval=int(tf)*60_000
                    close=(event_now//interval)*interval
                    received=self.last_received_close.get(key,0)
                    if close and event_now>=close+2_000 and received<close and now-self.candle_poll_attempts.get(key,0)>=2_000:
                        stale.append({'symbol':symbol,'timeframe':tf,'expected_close':close})
                        if not self.split_mode:self.candle_poll_attempts[key]=now
                # Long disconnects also restore any stream already known to
                # diagnostics as stale (including startup gaps).
                for row in self.status()['scanner_diagnostics']['stale_streams']:
                    key=(row['symbol'],row['timeframe'])
                    if key not in {(item['symbol'],item['timeframe']) for item in stale}:
                        last_try=self.candle_poll_attempts.get(key,0)
                        if now-last_try>=2_000:
                            stale.append(row)
                            if not self.split_mode:self.candle_poll_attempts[key]=now
                if self.last_message and now-self.last_message>30_000:
                    self.engine.connected=False
                if not stale:
                    if self.error.startswith(('REST recovery failed for ','Восстановление свечей ')):
                        self.error=''
                    self.candle_recovery_status='idle'
                    self.candle_recovery_pending=0
                    self.candle_recovery_backoff=5_000
                    self.next_candle_recovery=0
                    continue
                self.candle_recovery_status='recovering'
                self.candle_recovery_pending=len({(row['symbol'],row['timeframe']) for row in stale})
                self.next_candle_recovery=0
                if self.split_mode:
                    stale.sort(key=lambda row:(self.candle_poll_attempts.get((row['symbol'],row['timeframe']),0),
                                               row['symbol'],row['timeframe']))
                    selected=stale[:2]
                    for row in selected:self.candle_poll_attempts[(row['symbol'],row['timeframe'])]=now
                else:selected=stale
                recovered,errors=await self.recover_closed_candles(selected)
                if errors:
                    self.candle_recovery_pending=len(errors)+len(stale)-len(selected)
                    other_errors=[item for item in errors if not isinstance(item[2],BybitNetworkError)]
                    self.candle_recovery_status='waiting_network' if not other_errors else 'error'
                    if other_errors:
                        symbol,tf,exc=other_errors[0]
                        self.error=f'Восстановление свечей {symbol}/{tf}: {self.safe_error(exc)}'
                    self.next_candle_recovery=(now_ms()+self.candle_recovery_backoff
                        if not self.split_mode or len(stale)==len(selected) else 0)
                    self.candle_recovery_backoff=min(self.candle_recovery_backoff*2,30_000)
                    continue
                if self.error.startswith('REST recovery failed for '):
                    self.error=''
                if self.error.startswith('Восстановление свечей '):
                    self.error=''
                self.candle_recovery_status='recovering' if self.split_mode and len(stale)>len(selected) else 'idle'
                self.candle_recovery_pending=len(stale)-len(selected) if self.split_mode else 0
                self.candle_recovery_backoff=5_000
                self.next_candle_recovery=0
                if recovered:
                    # Don't report a dead websocket as live; the panel still sees
                    # each recovered candle and scan timestamp independently.
                    self.recovery_time=now_ms()
                    if self.phase not in ('live','recovering'):
                        self.phase='rest_fallback'
                    if self.phase=='rest_fallback':
                        self.error='WebSocket недоступен; сканирование закрытых свечей идёт через REST Bybit'
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logging.getLogger(__name__).exception('REST candle watchdog failed')
                self.error=f'Candle processing: {self.safe_error(exc)}'
                self.candle_recovery_status='error'
                self.next_candle_recovery=now_ms()+self.candle_recovery_backoff
                self.candle_recovery_backoff=min(self.candle_recovery_backoff*2,30_000)

    async def filter_candidates(self,symbol,tf,buf,recovered=False,detected=None,*,
                                received_wall_ms=None,source='websocket',context_buffers=None,archival=False):
        # Broaden the manual signal feed while the historical strategy remains
        # unapproved. If an approved strategy is ever activated, entries keep
        # using the stricter, validated detector profile.
        sensitivity='standard' if self.engine.strategy_approved and self.active_variant else 'expanded'
        stage_started=asyncio.get_running_loop().time()
        if detected is None:
            loop=asyncio.get_running_loop()
            detected=await loop.run_in_executor(self.detection_executor,_detect_in_worker,symbol,tf,
                _snapshot_candles(buf),'breakout',sensitivity,True)
            self.record_signal_stage('detect',stage_started)
        else:
            detected=list(detected)
        detected_at=now_ms() if self.split_mode else self.market_now()
        received_at=(received_wall_ms or 0) if self.split_mode else now_ms()
        for signal in detected:
            signal.event_at=signal.time
            signal.received_at=received_at
            signal.detected_at=detected_at
        pending_ids={s.id for s in self.pending}
        pending_ids.update(str(s.get('id')) for s in self.engine.strategy_pending)
        stage_started=asyncio.get_running_loop().time()
        if self.split_mode:
            saved=await self.engine.store.afetch_signals([s.id for s in detected])
            if not archival:
                promoted=[]
                for signal in detected:
                    prior=saved.get(signal.id) or {}
                    delivery=(prior.get('filter_results') or {}).get('delivery') or {}
                    if delivery.get('mode')=='history' or (delivery.get('mode') is None and delivery.get('recovered')):
                        prior_event=int(prior.get('event_at') or prior.get('time') or 0)
                        if prior_event==signal.event_at:
                            saved.pop(signal.id,None)
                        else:
                            signal.id=hashlib.sha256((signal.id+':current').encode()).hexdigest()[:24]
                            promoted.append(signal.id)
                if promoted:saved.update(await self.engine.store.afetch_signals(promoted))
            existing={sid for sid,row in saved.items() if row.get('status')!='pending_retest'}
        else:
            existing={s.id for s in detected if self.engine.store.has_signal(s.id)}
        signals=[s for s in detected if s.id not in existing and s.id not in pending_ids]
        self.record_signal_stage('dedupe',stage_started)
        minutes=1440 if tf=='D' else int(tf)
        self.last_closed_scan[(symbol,tf)]=max(self.last_closed_scan.get((symbol,tf),0),
                                               buf[-1].time+minutes*60000)
        now=now_ms()
        self.last_signal_detected_at=now if signals else self.last_signal_detected_at
        self.scan_events.append({'time':now,'raw':len(detected),'new':len(signals),
                                 'duplicate':len(detected)-len(signals)})
        while self.scan_events and self.scan_events[0]['time']<now-3_600_000:
            self.scan_events.popleft()
        if not signals:
            return []
        result=[]
        recent_setups=([] if archival else await self.engine.store.arecent_setups(symbol,tf,200)
                       if self.split_mode else self.engine.store.recent_setups(symbol,tf,200))
        for signal in signals:
            stage_started=asyncio.get_running_loop().time()
            linked=[]
            for setup in recent_setups:
                if (setup.get('pattern')==signal.pattern
                        and setup.get('side')==signal.side
                        and abs(setup['snapshot']['start']-signal.start)<=int(tf)*60000*3):
                    setup['linked_signal_id']=signal.id
                    signal.setup_id=setup['id']
                    linked.append(setup)
            if linked:
                await self.engine.store.asave_setups(linked)
                if self.split_mode:self.reconcile_active_setup_cache(linked)
                else:self.invalidate_active_setups()
                self.setup_cache=None
            self.record_signal_stage('setup_link',stage_started)
            stage_started=asyncio.get_running_loop().time()
            source_buffers=context_buffers if context_buffers is not None else self.buffers
            hourly=[c for c in source_buffers.get((symbol,BIAS_TF),[])
                    if c.time+int(BIAS_TF)*60000<=signal.time]
            fifteen=[c for c in source_buffers.get((symbol,FIGURE_TF),[])
                     if c.time+int(FIGURE_TF)*60000<=signal.time]
            five=[c for c in source_buffers.get((symbol,ENTRY_TF),[])
                  if c.time+int(ENTRY_TF)*60000<=signal.time]
            market=classify_market_setup(hourly,fifteen,signal.side)
            confirmation_ok,confirmation=confirm_breakout_5m(five,signal)
            volume_ok,volume=volume_filter(five,ENTRY_TF)
            _,trend=trend_filter(hourly,signal.side)
            entry_atr=float(confirmation.get('atr_5m') or signal.atr/3)
            self.record_signal_stage('rules',stage_started)
            if self.engine.strategy_approved and self.active_variant:
                signal.strategy_version=VERSION
                signal.strategy_variant=self.active_variant.id
                signal.entry_method=self.active_variant.entry
            signal.filter_results={'trend':trend,'trend_pass':market['eligible'],
                                   'market_context':market,'confirmation_5m':confirmation,
                                   'confirmation_pass':confirmation_ok,'entry_atr':entry_atr,
                                   'volume':volume,'volume_pass':volume_ok,
                                   'auto_eligible':(tf==FIGURE_TF and market['eligible']
                                       and confirmation_ok and volume_ok
                                       and signal.pattern not in EXPERIMENTAL_NAMES),
                                   'detection_profile':sensitivity,
                                   **({'experimental_pattern':True,'auto_entry_allowed':False}
                                      if signal.pattern in EXPERIMENTAL_NAMES else {}),
                                   'delivery':{'exchange_event_at':buf[-1].time+int(tf)*60000,
                                       'detected_at':detected_at,'received_at':received_at,
                                       'delay_ms':max(0,detected_at+int(getattr(self.bybit,'offset',0) or 0)-(buf[-1].time+int(tf)*60000)) if self.split_mode else max(0,detected_at-(buf[-1].time+int(tf)*60000)),
                                       'recovered':bool(recovered),
                                       **({'timing_version':2,'source':source,'mode':'history' if archival else 'current'} if self.split_mode else {})},
                                   **({'required_net_rr':self.active_variant.net_rr,
                                       'max_hold_ms':(self.active_variant.time_exit or 0)*int(ENTRY_TF)*60000}
                                      if self.active_variant else {})}
            if archival or not self.positions_ready:
                signal.filter_results['auto_entry_allowed']=False
                signal.filter_results['auto_eligible']=False
                result.append(signal)
                continue
            if not self.engine.strategy_approved or not self.active_variant:
                # Context is descriptive; the historical approval gate still blocks entries.
                result.append(signal)
                continue
            if signal.pattern in EXPERIMENTAL_NAMES:
                # Experimental detections are manual observations only and
                # never enter the research-approved virtual strategy.
                result.append(signal)
                continue
            if tf!=FIGURE_TF:
                signal.status='rejected'; signal.reason='Auto entry is evaluated only for 15m figures'
            elif not market.get('eligible'):
                signal.status='rejected'; signal.reason='1h context and 15m structure did not confirm continuation or reversal'
            elif not confirmation_ok:
                signal.status='rejected'; signal.reason='No closed 5m candle confirmed the break beyond the figure boundary'
            elif not volume_ok:
                signal.status='rejected'
                if volume.get('notional_pass') is False:
                    signal.reason=(f"5m turnover {volume['breakout_turnover_usdt']:.0f} USDT "
                        f"is below the {volume['minimum_notional_usdt']:.0f} USDT minimum")
                else:
                    signal.reason='5m breakout turnover is below 1.5x its recent median'
            elif self.active_variant.entry=='retest':
                self.engine.strategy_pending=[x for x in self.engine.strategy_pending if x.get('id')!=signal.id]
                self.engine.strategy_pending.append(signal.dict() | {'retest_bars':0,'retest_extreme':None})
                signal.status='pending_retest'
                await self.engine.persist_async([signal])
                signal.status='pending_retest'
            else:
                distance=(signal.entry or signal.boundary)-signal.stop if signal.side=='long' else signal.stop-(signal.entry or signal.boundary)
                if distance < .8*entry_atr:
                    signal.status='rejected'; signal.reason='Дистанция до стопа меньше 0,8 ATR'
                else:
                    result.append(signal)
        rejected=[s for s in signals if s.status=='rejected']
        if rejected:
            await self.engine.persist_async(rejected)
        if signals:
            self.invalidate_feed_cache()
        return result

    def advance_retests(self,symbol,tf,candle,persist=True):
        if not self.engine.strategy_approved or not self.active_variant or self.active_variant.entry!='retest':
            return
        if tf!=ENTRY_TF:
            return
        tf_ms=int(tf)*60000
        keep=[];cancelled=[]
        for raw in self.engine.strategy_pending:
            if raw['symbol']!=symbol or raw['timeframe']!=FIGURE_TF:
                keep.append(raw); continue
            signal=Signal(**{k:v for k,v in raw.items() if k in Signal.__dataclass_fields__})
            n=int(raw.get('retest_bars',0))+1
            sign=1 if signal.side=='long' else -1
            entry_atr=atr(self.buffers.get((symbol,ENTRY_TF),[])[-30:])
            if entry_atr<=0:
                entry_atr=float(signal.filter_results.get('entry_atr',signal.atr/3) or signal.atr/3)
            boundary=line_at(signal,candle.time)
            stop_hit=candle.low<=signal.stop if sign>0 else candle.high>=signal.stop
            returned_inside=sign*(candle.close-boundary)<-.25*entry_atr
            if stop_hit or returned_inside or n>3:
                signal.status='rejected'
                signal.reason=('Retest cancelled at the original stop' if stop_hit else
                    'Retest cancelled after price returned inside the figure' if returned_inside else
                    'Retest did not form within three 5m candles')
                cancelled.append(signal)
                continue
            touched=(candle.low<=boundary+.25*entry_atr and candle.high>=boundary-.25*entry_atr)
            confirmed=sign*(candle.close-boundary)>=.1*entry_atr
            extreme=min(candle.low,float(raw['retest_extreme'])) if sign>0 and raw.get('retest_extreme') is not None else candle.low if sign>0 else max(candle.high,float(raw['retest_extreme'])) if raw.get('retest_extreme') is not None else candle.high
            if touched and confirmed:
                signal.boundary=boundary
                signal.stop=extreme-sign*.2*entry_atr
                signal.entry_method='retest'
                signal.strategy_version=VERSION
                signal.strategy_variant=self.active_variant.id
                signal.filter_results.update({'entry_atr':entry_atr,
                    'required_net_rr':self.active_variant.net_rr,
                    'max_hold_ms':(self.active_variant.time_exit or 0)*tf_ms})
                if sign*(candle.close-signal.stop)>=.8*entry_atr:
                    signal.status='found'
                    self.pending.append(signal)
                    self.invalidate_feed_cache()
                    self.broadcast_event.set()
                else:
                    signal.status='rejected';signal.reason='Retest stop is closer than 0.8 5m ATR'
                    cancelled.append(signal)
                continue
            raw['retest_bars']=n; raw['retest_extreme']=extreme
            keep.append(raw)
        self.engine.strategy_pending=keep
        if cancelled and persist:self.engine.persist(cancelled)
        if cancelled and persist:
            self.invalidate_feed_cache()
            self.broadcast_event.set()
        return cancelled

    async def bybit_heartbeat(self,socket):
        while True:
            await asyncio.sleep(20)
            await socket.send(json.dumps({'op':'ping'}))

    async def request_subscription(self,socket,operation,topics):
        topics=list(topics)
        # Bybit's linear public stream permits large subscriptions, with a
        # 21k-character args ceiling. Send one bounded batch for the common
        # case (40 symbols x 5 candle streams + tickers) instead of 24 tiny
        # requests and serialize only when dynamic context grows beyond it.
        batches=[]
        batch=[]
        for topic in topics:
            candidate=[*batch,topic]
            if batch and len(json.dumps(candidate,separators=(',',':'))) > 20_000:
                batches.append(batch)
                batch=[topic]
            else:
                batch=candidate
        if batch:batches.append(batch)
        lock=self.subscription_ack_locks.get(socket)
        if lock is None:
            lock=asyncio.Lock()
            self.subscription_ack_locks[socket]=lock
        async with lock:
            for batch in batches:
                request_id=f'{operation}-{next(self.subscription_request_sequence)}'
                future=asyncio.get_running_loop().create_future()
                self.subscription_ack_waiters[request_id]=future
                self.subscription_ack_waiters_by_socket[socket]=future
                try:
                    await socket.send(json.dumps({'op':operation,'args':batch,
                                                  'req_id':request_id}))
                    await asyncio.wait_for(future,12)
                except asyncio.TimeoutError:
                    logging.getLogger(__name__).warning(
                        'Bybit subscription acknowledgement timed out: op=%s request_id=%s topics=%s',
                        operation,request_id,','.join(batch))
                    raise
                finally:
                    self.subscription_ack_waiters.pop(request_id,None)
                    if self.subscription_ack_waiters_by_socket.get(socket) is future:
                        self.subscription_ack_waiters_by_socket.pop(socket,None)

    def resolve_subscription_ack(self,message,socket=None):
        request_id=message.get('req_id')
        future=self.subscription_ack_waiters.get(request_id)
        # Public stream acknowledgements may omit req_id. Since requests are
        # single-flight per socket, the outstanding acknowledgement is unambiguous.
        if (future is None and socket is not None and
                message.get('op') in ('subscribe','unsubscribe') and
                'success' in message):
            future=self.subscription_ack_waiters_by_socket.get(socket)
        if (message.get('op') in ('subscribe','unsubscribe') or
                (message.get('success') is not None and message.get('ret_msg')!='pong')):
            logging.getLogger(__name__).info(
                'Bybit subscription response: op=%r req_id=%r success=%r ret_msg=%r matched=%s',
                message.get('op'),request_id,message.get('success'),message.get('ret_msg'),
                future is not None)
        if future is None:return False
        if future.done():return True
        if message.get('success') is False:
            future.set_exception(RuntimeError('Bybit rejected subscription: '+
                str(message.get('ret_msg') or message.get('retCode') or 'unknown error')))
        else:
            future.set_result(True)
        return True

    async def run_ancillary(self):
        backoff=1
        while True:
            heartbeat=None
            pending=set()
            try:
                topics=self.ancillary_topics()
                self.ancillary_book_symbols_requested={
                    topic.rsplit('.',1)[-1] for topic in topics
                    if topic.startswith('orderbook.200.')}
                async with self.bybit.connect_ws(self.bybit.ws_url,ping_interval=None,max_size=4*1024*1024) as socket:
                    heartbeat=asyncio.create_task(self.bybit_heartbeat(socket))
                    if topics:
                        pending.add(asyncio.create_task(self.request_subscription(socket,'subscribe',sorted(topics))))
                    else:
                        self.ancillary_connected=True
                    initial_pending=bool(pending)
                    while True:
                        for task in tuple(pending):
                            if task.done():
                                await task
                                pending.discard(task)
                        if not pending:
                            self.ancillary_book_symbols=set(self.ancillary_book_symbols_requested)
                        if initial_pending and not pending:
                            initial_pending=False
                            self.ancillary_connected=True
                            self.ancillary_flow_started_at=now_ms()
                            self.ancillary_liquidation_started_at=now_ms()
                            backoff=1
                        desired=self.ancillary_topics()
                        if desired!=topics:
                            self.ancillary_book_symbols_requested={
                                topic.rsplit('.',1)[-1] for topic in desired
                                if topic.startswith('orderbook.200.')}
                            removed=sorted(topics-desired);added=sorted(desired-topics)
                            for topic in added:
                                if topic.startswith('orderbook.200.'):
                                    self.books.pop(topic.rsplit('.',1)[-1],None)
                            if removed:pending.add(asyncio.create_task(
                                self.request_subscription(socket,'unsubscribe',removed)))
                            if added:pending.add(asyncio.create_task(
                                self.request_subscription(socket,'subscribe',added)))
                            topics=desired
                        try:
                            raw=await asyncio.wait_for(socket.recv(),1)
                        except asyncio.TimeoutError:
                            continue
                        if not raw: continue
                        msg=json.loads(raw)
                        if self.resolve_subscription_ack(msg,socket):continue
                        if msg.get('topic','').startswith(('orderbook.','publicTrade.','allLiquidation.')):
                            self.ancillary_last_message=now_ms()
                            await self.message(msg)
            except asyncio.CancelledError:
                raise
            except BybitNetworkError:
                pass
            except Exception as exc:
                logging.getLogger(__name__).warning('Ancillary Bybit stream unavailable: %s',self.safe_error(exc))
            finally:
                self.reset_ancillary_stream()
                for task in pending:
                    if not task.done():task.cancel()
                if pending:
                    await asyncio.gather(*pending,return_exceptions=True)
                if heartbeat:
                    heartbeat.cancel()
                    # The peer may close the socket while the heartbeat is
                    # sending its next ping. Do not let that completed task's
                    # ConnectionClosed exception escape cleanup and kill the
                    # reconnect loop.
                    with contextlib.suppress(asyncio.CancelledError, Exception): await heartbeat
            await asyncio.sleep(backoff)
            backoff=min(backoff*2,30)

    def reset_ancillary_stream(self):
        """Invalidate connection-scoped data until a fresh subscription and snapshot arrive."""
        self.ancillary_connected=False
        self.ancillary_flow_started_at=None
        self.ancillary_liquidation_started_at=None
        self.ancillary_book_symbols_requested=set()
        self.ancillary_book_symbols=set()
        self.books.clear()
        self.live_flow={key:value for key,value in self.live_flow.items()
                        if key[1]>=now_ms()-15*60_000}

    async def run_funding_settlement(self):
        while True:
            await asyncio.sleep(1)
            if self.split_mode and not self.positions_ready:continue
            try:
                await self.settle_due_funding()
                await self.flush()
            except asyncio.CancelledError:
                raise
            except BybitNetworkError:
                continue
            except Exception as exc:
                logging.getLogger(__name__).exception('Funding settlement failed')
                self.error=f'Candle processing: {self.safe_error(exc)}'

    async def run(self):
        backoff=1
        while True:
            heartbeat_task=None
            refresh_task=None
            position_recovery_task=None
            pending=set()
            try:
                self.engine.connected=False
                if self.split_mode:
                    await self.refresh(background=True)
                    self.positions_ready=not bool(self.engine.positions)
                    position_recovery_task=asyncio.create_task(self.recover_positions_with_live_ticks(),name='position-recovery')
                else:
                    await self.refresh()
                    await self.recover()
                subscribed=set(self.subscription_topics())
                async with self.bybit.connect_ws(self.bybit.ws_url,ping_interval=None,max_size=4*1024*1024) as socket:
                    heartbeat_task=asyncio.create_task(self.bybit_heartbeat(socket))
                    if subscribed:
                        pending.add(asyncio.create_task(self.request_subscription(socket,'subscribe',sorted(subscribed))))
                    initial_pending=bool(pending)
                    self.phase='live' if not initial_pending else 'subscribing'
                    if self.phase=='live' and self.error.startswith(
                            'WebSocket недоступен; сканирование закрытых свечей'):
                        self.error=''
                    if self.connection_error and self.error==self.connection_error:
                        self.error=''
                    self.connection_error=''
                    self.last_message=now_ms()
                    last_save=0
                    while True:
                        if position_recovery_task and position_recovery_task.done():
                            await position_recovery_task
                            position_recovery_task=None
                            self.positions_ready=True
                        for task in tuple(pending):
                            if task.done():
                                await task;pending.discard(task)
                        if initial_pending and not pending:
                            initial_pending=False;self.phase='live';backoff=1
                            if self.error.startswith(
                                    'WebSocket недоступен; сканирование закрытых свечей'):
                                self.error=''
                            self.begin_live_latency_window()
                        if self.market_message_error:
                            error=self.market_message_error
                            self.market_message_error=''
                            raise RuntimeError('Market frame processing failed: '+error)
                        if now_ms()-self.last_refresh>=3_600_000 and refresh_task is None:
                            refresh_task=asyncio.create_task(self.refresh(background=True) if self.split_mode else self.refresh(),name='market-universe-refresh')
                        if refresh_task and refresh_task.done():
                            await refresh_task;refresh_task=None
                            if self.split_mode:
                                if position_recovery_task is None:
                                    self.positions_ready=not bool(self.engine.positions)
                                    position_recovery_task=asyncio.create_task(self.recover_positions_with_live_ticks(),name='position-recovery')
                            else:
                                await self.recover()
                        desired=set(self.subscription_topics())
                        if desired!=subscribed:
                            removed=sorted(subscribed-desired); added=sorted(desired-subscribed)
                            if removed:pending.add(asyncio.create_task(
                                self.request_subscription(socket,'unsubscribe',removed)))
                            if added:pending.add(asyncio.create_task(
                                self.request_subscription(socket,'subscribe',added)))
                            subscribed=desired
                        try:
                            raw=await asyncio.wait_for(socket.recv(),1)
                            received_monotonic=asyncio.get_running_loop().time()
                            received_wall=now_ms()
                        except asyncio.TimeoutError:
                            raw=None
                        if raw:
                            msg=json.loads(raw)
                            msg['_received_wall_ms']=received_wall
                            msg['_received_monotonic']=received_monotonic
                            msg['_source']='websocket'
                            if self.resolve_subscription_ack(msg,socket):continue
                            topic=msg.get('topic','')
                            if topic:
                                self.last_message=now_ms()
                                self.engine.connected=True
                                # Partial kline frames are validation + in-memory
                                # candle updates only; handling them here avoids
                                # queueing hundreds of redundant snapshots per
                                # second. Confirmed bars still use the worker.
                                if (topic.startswith('kline.') and (self.split_mode or
                                        not any(row.get('confirm') for row in msg.get('data',[])))):
                                    await self.receive_kline(msg)
                                    continue
                                try:
                                    self.market_message_queue.put_nowait((
                                        asyncio.get_running_loop().time(),msg))
                                except asyncio.QueueFull as exc:
                                    raise RuntimeError('Bybit market message queue is full') from exc
                        if self.last_message and now_ms()-self.last_message>30000:
                            raise RuntimeError('Bybit market stream is stale')
                        if now_ms()-last_save>5000:
                            await self.engine.persist_async(); last_save=now_ms()
            except asyncio.CancelledError:
                raise
            except BybitNetworkError:
                self.refreshing_market=False
                if self.connection_error and self.error==self.connection_error:
                    self.error=''
                self.connection_error=''
                self.phase='waiting_network'
            except Exception as exc:
                logging.getLogger(__name__).exception('Bybit scanner loop failed')
                self.refreshing_market=False
                self.connection_error=self.safe_error(exc)
                self.error=self.connection_error
                self.phase='reconnecting'
            finally:
                for task in pending:
                    if not task.done():task.cancel()
                if pending:await asyncio.gather(*pending,return_exceptions=True)
                if refresh_task and not refresh_task.done():
                    refresh_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):await refresh_task
                if position_recovery_task:
                    if not position_recovery_task.done():position_recovery_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError,Exception):await position_recovery_task
                if heartbeat_task:
                    heartbeat_task.cancel()
                    # A normal Bybit rotation can close the websocket while
                    # this task is sending a ping. Cleanup must still reach
                    # the retry delay below so the scanner reconnects.
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await heartbeat_task
                self.engine.connected=False
                await self.engine.persist_async()
            await asyncio.sleep(backoff)
            backoff=min(backoff*2,30)
