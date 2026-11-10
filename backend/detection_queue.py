"""Bounded, prioritized candle scans with in-stream ordering."""
import asyncio
import heapq
import itertools
from collections import deque
from dataclasses import dataclass, replace


@dataclass(frozen=True)
class CandleSnapshot:
    time: int
    open: float
    high: float
    low: float
    close: float
    volume: float = 0
    turnover: float | None = None


@dataclass(frozen=True)
class DetectionJob:
    priority: int
    sequence: int
    symbol: str
    timeframe: str
    candle_time: int
    candles: tuple
    recovered: bool
    queued_at: float
    received_at: float | None = None
    retries: int = 0
    detect_signals: bool = True
    detect_setups: bool = True
    advance_cursor: bool = True
    received_wall_ms: int | None = None
    source: str = 'websocket'
    context_buffers: dict | None = None

    @property
    def stream(self):
        return self.symbol, self.timeframe


class PrioritizedStreamQueue:
    """A bounded global queue that runs one scan at a time per stream."""

    def __init__(self, maxsize=640):
        self.maxsize = max(1, int(maxsize))
        self.condition = asyncio.Condition()
        self.streams = {}
        self.ready = []
        self.busy = set()
        self.pending = 0
        self.unfinished = 0
        self.finished = asyncio.Event()
        self.finished.set()

    async def put(self, job):
        async with self.condition:
            while self.pending >= self.maxsize:
                await self.condition.wait()
            stream = job.stream
            rows = self.streams.get(stream)
            if rows is None:
                rows = self.streams[stream] = deque()
            is_first = not rows
            rows.append(job)
            self.pending += 1
            self.unfinished += 1
            self.finished.clear()
            if is_first and stream not in self.busy:
                heapq.heappush(self.ready, (job.priority, job.sequence, stream))
            self.condition.notify_all()

    async def get(self):
        async with self.condition:
            while not self.ready:
                await self.condition.wait()
            _, _, stream = heapq.heappop(self.ready)
            rows = self.streams[stream]
            job = rows.popleft()
            self.busy.add(stream)
            self.pending -= 1
            self.condition.notify_all()
            return job

    async def complete(self, job):
        async with self.condition:
            stream = job.stream
            self.busy.discard(stream)
            rows = self.streams.get(stream)
            if rows:
                head = rows[0]
                heapq.heappush(self.ready, (head.priority, head.sequence, stream))
            else:
                self.streams.pop(stream, None)
            self.unfinished = max(0, self.unfinished - 1)
            if not self.unfinished:
                self.finished.set()
            self.condition.notify_all()

    async def retry(self, job):
        """Put a failed job back before newer jobs from its own stream."""
        async with self.condition:
            stream = job.stream
            self.busy.discard(stream)
            rows = self.streams.setdefault(stream, deque())
            retried = replace(job, priority=max(job.priority, 1), recovered=True,
                queued_at=asyncio.get_running_loop().time(), retries=job.retries + 1)
            rows.appendleft(retried)
            self.pending += 1
            if rows and len(rows) == 1:
                heapq.heappush(self.ready, (retried.priority, retried.sequence, stream))
            self.condition.notify_all()

    async def join(self):
        await self.finished.wait()

    def qsize(self):
        return self.pending


@dataclass(frozen=True)
class CurrentEnqueueResult:
    """A deferred job must be replayed from its already durable candle."""

    accepted: bool
    deferred: DetectionJob | None = None


class SplitDetectionQueue:
    """Independent current/history lanes with bounded pending work.

    The producer must commit the candle before calling ``put_current``.  If
    ``deferred`` is returned, its durable candle and the historical cursor
    are the source of truth; do not insert it directly into the history lane.
    A single history dispatcher owns chronological scheduling per stream.
    """

    def __init__(self, max_current_streams=640, max_history=32):
        self.max_current_streams = max(1, int(max_current_streams))
        self.max_history = max(1, int(max_history))
        self.condition = asyncio.Condition()
        self.current_pending = {}
        self.current_running = {}
        self.current_ready = []
        self.history_streams = {}
        self.history_running = {}
        self.history_ready = []
        self.history_keys = set()
        self.history_pending = 0
        self.current_unfinished = 0
        self.history_unfinished = 0
        self.current_finished = asyncio.Event()
        self.history_finished = asyncio.Event()
        self.finished = asyncio.Event()
        self.current_finished.set()
        self.history_finished.set()
        self.finished.set()
        self._serial = itertools.count()

    @staticmethod
    def _history_key(job):
        return (job.stream, job.candle_time, job.detect_signals,
                job.detect_setups, job.advance_cursor)

    def _ready_current(self, job):
        heapq.heappush(self.current_ready, (job.sequence, next(self._serial), job))
        # Replacements invalidate heap entries. Keep their memory bounded even
        # if one busy stream receives many newer candles before it is served.
        if len(self.current_ready) > max(64, 2 * len(self.current_pending)):
            self.current_ready = [(pending.sequence, next(self._serial), pending)
                for stream, pending in self.current_pending.items()
                if stream not in self.current_running]
            heapq.heapify(self.current_ready)

    def _ready_history(self, job):
        heapq.heappush(self.history_ready, (job.sequence, next(self._serial), job.stream))

    def _finish_events(self):
        if self.current_unfinished == 0:
            self.current_finished.set()
        if self.history_unfinished == 0:
            self.history_finished.set()
        if self.current_unfinished == 0 and self.history_unfinished == 0:
            self.finished.set()

    async def put_current(self, job):
        """Enqueue newest current job without waiting for historical work."""
        async with self.condition:
            stream = job.stream
            running = self.current_running.get(stream)
            if running and job.candle_time <= running.candle_time:
                return CurrentEnqueueResult(False, job if job.candle_time < running.candle_time else None)
            previous = self.current_pending.get(stream)
            if previous and job.candle_time <= previous.candle_time:
                return CurrentEnqueueResult(False, job if job.candle_time < previous.candle_time else None)
            if previous is None and len(self.current_pending) >= self.max_current_streams:
                return CurrentEnqueueResult(False, job)
            if previous is None:
                self.current_unfinished += 1
            self.current_pending[stream] = job
            self.current_finished.clear()
            self.finished.clear()
            if stream not in self.current_running:
                self._ready_current(job)
            self.condition.notify_all()
            return CurrentEnqueueResult(True, previous)

    async def put_history(self, job):
        """Queue one history job, waiting for bounded capacity if necessary.

        Returns False for a duplicate already waiting or running. Jobs for a
        stream must be submitted in candle order by one history dispatcher.
        """
        async with self.condition:
            key = self._history_key(job)
            while self.history_unfinished >= self.max_history:
                if key in self.history_keys:
                    return False
                await self.condition.wait()
            if key in self.history_keys:
                return False
            stream = job.stream
            rows = self.history_streams.get(stream)
            tail = rows[-1] if rows else self.history_running.get(stream)
            if tail and job.candle_time < tail.candle_time:
                raise ValueError('History tasks must be submitted in candle order per stream')
            if rows is None:
                rows = self.history_streams[stream] = deque()
            first = not rows
            rows.append(job)
            self.history_keys.add(key)
            self.history_pending += 1
            self.history_unfinished += 1
            self.history_finished.clear()
            self.finished.clear()
            if first and stream not in self.history_running:
                self._ready_history(job)
            self.condition.notify_all()
            return True

    def _pop_current(self):
        while self.current_ready:
            _, _, job = heapq.heappop(self.current_ready)
            stream = job.stream
            if self.current_pending.get(stream) is not job or stream in self.current_running:
                continue
            del self.current_pending[stream]
            self.current_running[stream] = job
            return job
        return None

    def _pop_history(self):
        while self.history_ready:
            _, _, stream = heapq.heappop(self.history_ready)
            rows = self.history_streams.get(stream)
            if not rows or stream in self.history_running:
                continue
            job = rows.popleft()
            self.history_pending -= 1
            self.history_running[stream] = job
            return job
        return None

    async def get_current(self):
        async with self.condition:
            while True:
                job = self._pop_current()
                if job is not None:
                    return job
                await self.condition.wait()

    async def get_history(self):
        async with self.condition:
            while True:
                job = self._pop_history()
                if job is not None:
                    return job
                await self.condition.wait()

    async def get_next(self):
        """For one worker, start history only when no current work exists."""
        async with self.condition:
            while True:
                job = self._pop_current()
                if job is not None:
                    return 'current', job
                if not self.current_pending and not self.current_running:
                    job = self._pop_history()
                    if job is not None:
                        return 'history', job
                await self.condition.wait()

    async def complete_current(self, job):
        async with self.condition:
            stream = job.stream
            if self.current_running.get(stream) is not job:
                raise ValueError('Current job is not running')
            del self.current_running[stream]
            self.current_unfinished -= 1
            if stream in self.current_pending:
                self._ready_current(self.current_pending[stream])
            self._finish_events()
            self.condition.notify_all()

    async def complete_history(self, job):
        async with self.condition:
            stream = job.stream
            if self.history_running.get(stream) is not job:
                raise ValueError('History job is not running')
            del self.history_running[stream]
            self.history_keys.discard(self._history_key(job))
            self.history_unfinished -= 1
            rows = self.history_streams.get(stream)
            if rows:
                self._ready_history(rows[0])
            else:
                self.history_streams.pop(stream, None)
            self._finish_events()
            self.condition.notify_all()

    async def retry_history(self, job):
        """Keep a failed historical job ahead of newer jobs in its stream."""
        async with self.condition:
            stream = job.stream
            if self.history_running.get(stream) is not job:
                raise ValueError('History job is not running')
            del self.history_running[stream]
            retried = replace(job, recovered=True, priority=max(job.priority, 1),
                queued_at=asyncio.get_running_loop().time(), retries=job.retries+1)
            rows = self.history_streams.setdefault(stream, deque())
            rows.appendleft(retried)
            self.history_pending += 1
            self._ready_history(retried)
            self.condition.notify_all()

    @property
    def unfinished(self):
        return self.current_unfinished + self.history_unfinished

    def qsize(self):
        return len(self.current_pending) + self.history_pending

    def current_qsize(self):
        return len(self.current_pending)

    def history_qsize(self):
        return self.history_pending

    async def join(self):
        await self.finished.wait()

    async def join_current(self):
        await self.current_finished.wait()

    async def join_history(self):
        await self.history_finished.wait()
