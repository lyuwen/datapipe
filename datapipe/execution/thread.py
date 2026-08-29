"""ThreadExecutor: bounded local concurrency with threads.

Its API matches ``ProcessExecutor``; both reuse the shared bounded scheduler.
Useful for IO-heavy record processing where GIL contention is acceptable.
"""

from __future__ import annotations

import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable, Iterable

from datapipe.context import WorkerContext
from datapipe.execution.base import BoundedMapExecutor, _Job
from datapipe.runtime.context import RuntimeContext


class _ThreadWorker:
    """Wraps a compiled pipeline so setup/teardown run once per thread pool.

    Teardown runs exactly once when the pool is shut down.
    """

    def __init__(self, worker, runtime: RuntimeContext) -> None:
        self._worker = worker
        self._ctx = WorkerContext(
            rank=runtime.rank,
            world_size=runtime.world_size,
            worker_id=0,
            local_rank=runtime.local_rank,
        )
        self._setup_lock = threading.Lock()
        self._done_setup = False
        self._done_teardown = False

    def process(self, seq: int, value: Any) -> Any:
        if not self._done_setup:
            with self._setup_lock:
                if not self._done_setup and hasattr(self._worker, "setup"):
                    self._worker.setup(self._ctx)
                    self._done_setup = True
        self._ctx.record_index = seq
        return self._worker.process(value, self._ctx)

    def teardown(self) -> None:
        if self._done_teardown:
            return
        self._done_teardown = True
        if hasattr(self._worker, "teardown"):
            self._worker.teardown(self._ctx)


class ThreadExecutor(BoundedMapExecutor):
    """Bounded thread-based executor with the same API as ProcessExecutor."""

    def __init__(
        self,
        workers: int | None = None,
        max_in_flight: int | None = None,
    ) -> None:
        super().__init__(workers=workers, max_in_flight=max_in_flight)
        self._pool: ThreadPoolExecutor | None = None
        self._thread_worker: _ThreadWorker | None = None

    def _start_backend(self, runtime: RuntimeContext, worker) -> None:
        self._thread_worker = _ThreadWorker(worker, runtime)
        self._pool = ThreadPoolExecutor(max_workers=self.workers)

    def _submit(self, job: _Job) -> Future:
        return self._pool.submit(
            self._thread_worker.process, job.seq, job.value
        )

    def _shutdown_backend(self, cancel_futures: bool = False) -> None:
        if self._pool is not None:
            self._pool.shutdown(wait=False, cancel_futures=cancel_futures)
            self._pool = None
            if self._thread_worker is not None:
                self._thread_worker.teardown()
            self._thread_worker = None
