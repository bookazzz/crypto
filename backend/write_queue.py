"""A single, transaction-owning SQLite writer with short write batches."""
from __future__ import annotations

import queue
import sqlite3
import threading
import time
import itertools
from concurrent.futures import Future, InvalidStateError

_STOP = object()


class SQLiteWriteQueue:
    def __init__(self, path: str, batch_window: float = .01, max_batch_size: int = 32):
        self.path = path
        self.batch_window = batch_window
        self.max_batch_size = max(1,int(max_batch_size))
        self.jobs: queue.PriorityQueue = queue.PriorityQueue()
        self.sequence = itertools.count()
        self.submit_lock = threading.Lock()
        self.stop_future = None
        self.ready = threading.Event()
        self.thread = threading.Thread(target=self._run, name='sqlite-writer', daemon=True)
        self.thread.start()
        if not self.ready.wait(5):
            raise RuntimeError('SQLite writer failed to start')

    def submit(self, operation, *, priority: int = 0) -> Future:
        # Only independent historical operations may use priority 1. State
        # updates that depend on one another must use the same priority.
        if priority not in (0,1):
            raise ValueError('SQLite writer priority must be 0 or 1')
        future = Future()
        with self.submit_lock:
            if self.stop_future is not None:
                raise RuntimeError('SQLite writer is closed')
            self.jobs.put((priority,next(self.sequence),operation,future))
        return future

    def _run(self):
        db = sqlite3.connect(self.path, timeout=5)
        db.execute('PRAGMA busy_timeout=5000')
        self.ready.set()
        stopping = False
        while not stopping:
            first = self.jobs.get()
            batch = [first]
            deadline = time.monotonic() + self.batch_window
            while len(batch)<self.max_batch_size and time.monotonic() < deadline:
                try:
                    batch.append(self.jobs.get(timeout=max(0, deadline-time.monotonic())))
                except queue.Empty:
                    break
            batch.sort(key=lambda item:(item[0],item[1]))
            stopping = any(operation is _STOP for _,_,operation,_ in batch)
            stop_futures = [future for _,_,operation,future in batch if operation is _STOP]
            item_count = len(batch)
            batch = [(operation,future) for _,_,operation,future in batch if operation is not _STOP]
            results = []
            if batch:
                try:
                    db.execute('BEGIN IMMEDIATE')
                    for index, (operation, future) in enumerate(batch):
                        savepoint = f'write_{index}'
                        db.execute(f'SAVEPOINT {savepoint}')
                        try:
                            value=operation(db)
                            db.execute(f'RELEASE {savepoint}')
                            results.append((future,value,None,savepoint))
                        except Exception as exc:
                            db.execute(f'ROLLBACK TO {savepoint}')
                            db.execute(f'RELEASE {savepoint}')
                            results.append((future, None, exc, savepoint))
                    db.commit()
                except Exception as exc:
                    db.rollback()
                    results = [(future,None,exc,None) for _,future in batch]
            for future, value, error, _ in results:
                try:
                    if error is not None:
                        future.set_exception(error)
                    else:
                        future.set_result(value)
                except InvalidStateError:
                    # The awaiting coroutine can be cancelled while the
                    # transaction is committing. The write remains durable.
                    pass
            for _ in range(item_count):
                self.jobs.task_done()
            for future in stop_futures:
                try:future.set_result(None)
                except InvalidStateError:pass
        db.close()

    def close(self):
        # The stop marker is ordered after both priority classes and prevents
        # further submissions, so shutdown drains all previously accepted work.
        with self.submit_lock:
            if self.stop_future is None:
                self.stop_future=Future()
                self.jobs.put((2,next(self.sequence),_STOP,self.stop_future))
            future=self.stop_future
        # A bounded backlog can outlast a single slow batch on a busy disk.
        # Do not release the database while accepted transactions are pending.
        future.result(timeout=30)
        self.thread.join(timeout=5)
