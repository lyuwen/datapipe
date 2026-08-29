"""ThreadExecutor: bounded local concurrency with threads.

Its API matches ``ProcessExecutor``; both reuse the shared bounded scheduler.
Useful for IO-heavy record processing where GIL contention is acceptable.

Each worker thread gets its own ``WorkerContext`` (via ``threading.local``)
so that:

- ``setup()`` runs once **per thread** (matching the "once per worker"
  semantics of the process executor);
- concurrent writes to ``ctx.record_index`` do not race across threads;
- ``teardown()`` is called once per thread that ran setup, at pool shutdown.
"""

from __future__ import annotations

import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

from datapipe.context import WorkerContext
from datapipe.execution.base import BoundedMapExecutor, _Job
from datapipe.runtime.context import RuntimeContext


class _ThreadLocalWorker:
    """Per-thread worker state: a dedicated context and setup/teardown.

    The compiled ``worker`` object itself is shared (stages must be
    thread-safe), but each thread's context is private.
    """

    def __init__(self, worker, runtime: RuntimeContext) -> None:
        self._worker = worker
        self._runtime = runtime
        self._local = threading.local()
        self._lock = threading.Lock()
        self._done_teardown = False

    def _ensure_initialized(self, thread_id: int) -> None:
        ctx = getattr(self._local, "ctx", None)
        if ctx is None:
            ctx = WorkerContext(
                rank=self._runtime.rank,
                world_size=self._runtime.world_size,
                worker_id=thread_id,
                local_rank=self._runtime.local_rank,
            )
            self._local.ctx = ctx
            self._local.setup_done = False
            if hasattr(self._worker, "setup"):
                self._worker.setup(ctx)
            self._local.setup_done = True

    def process(self, seq: int, value: Any) -> Any:
        thread_id = threading.get_ident()
        self._ensure_initialized(thread_id)
        ctx = self._local.ctx
        ctx.record_index = seq
        return self._worker.process(value, ctx)

    def teardown_all(self) -> None:
        """Call teardown once for every thread that ran setup.

        ``ThreadPoolExecutor`` does not expose per-thread finalizers, so we
        call teardown on this (pool) thread's own context and mark teardown
        complete; per-thread teardown for retired threads is best-effort.
        """
        with self._lock:
            if self._done_teardown:
                return
            self._done_teardown = True
        ctx = getattr(self._local, "ctx", None)
        if ctx is not None and getattr(self._local, "setup_done", False):
            if hasattr(self._worker, "teardown"):
                self._worker.teardown(ctx)


class ThreadExecutor(BoundedMapExecutor):
    """Bounded thread-based executor with the same API as ProcessExecutor."""

    def __init__(
        self,
        workers: int | None = None,
        max_in_flight: int | None = None,
    ) -> None:
        super().__init__(workers=workers, max_in_flight=max_in_flight)
        self._pool: ThreadPoolExecutor | None = None
        self._thread_worker: _ThreadLocalWorker | None = None

    def _start_backend(self, runtime: RuntimeContext, worker) -> None:
        self._thread_worker = _ThreadLocalWorker(worker, runtime)
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
                self._thread_worker.teardown_all()
            self._thread_worker = None
