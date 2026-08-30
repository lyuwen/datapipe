"""ThreadExecutor: bounded local concurrency with threads.

Its API matches ``ProcessExecutor``; both reuse the shared bounded scheduler.
Useful for IO-heavy record processing where GIL contention is acceptable.

Each worker thread gets its own deep copy of the compiled pipeline and its own
``WorkerContext`` (via ``threading.local``) so that:

- ``setup()`` runs once **per thread** and can safely store per-thread state
  on ``self`` (including nested mutable objects) without racing against sibling
  threads; ``Stage.__deepcopy__`` ensures non-picklable attributes like locks
  are replaced with fresh equivalents rather than shared or copied wholesale;
- concurrent writes to ``ctx.record_index`` do not race across threads;
- ``teardown()`` runs on the owning thread after all record processing
  finishes, guaranteed by having each thread run its own teardown inline
  before the pool is shut down.
"""

from __future__ import annotations

import copy
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

from datapipe.context import WorkerContext
from datapipe.execution.base import BoundedMapExecutor, WorkerSetupError, _Job
from datapipe.runtime.context import RuntimeContext


class _ThreadLocalWorker:
    """Per-thread worker state.

    Each thread pops a pre-copied worker from ``_worker_pool`` (populated in
    ``_start_backend`` while still single-threaded) so ``setup()`` writes to an
    isolated object graph rather than a shared one.

    Teardown guarantee
    ------------------
    ``ThreadPoolExecutor`` provides no thread-affinity for submitted tasks, so
    submitting a generic teardown task cannot guarantee it runs on the owning
    thread.  Instead, each thread performs its own teardown by executing a
    sentinel task: we set a ``_shutdown_flag`` then submit exactly
    ``max_workers`` sentinel tasks (one per pool thread).  Each sentinel calls
    ``_teardown_on_thread()``, which reads from ``threading.local`` and tears
    down only the worker that belongs to the executing thread.  Every live
    thread picks up exactly one sentinel and tears down its own worker.

    The coordinator-thread fallback handles the rare case where the pool
    retired a thread before we submitted sentinels (thread did not pick up any
    task because it had already exited).
    """

    def __init__(self, worker, runtime: RuntimeContext, worker_pool: list, max_workers: int) -> None:
        self._worker = worker
        self._runtime = runtime
        self._local = threading.local()
        self._lock = threading.Lock()
        self._done_teardown = False
        self._thread_states: list[tuple] = []   # (worker_copy, ctx) per thread
        self._torn_down: set[int] = set()       # id(worker_copy) of torn-down workers
        self._worker_pool = worker_pool
        self._max_workers = max_workers
        self._teardown_barrier: threading.Barrier | None = None

    def _ensure_initialized(self, thread_id: int) -> None:
        if getattr(self._local, "ctx", None) is not None:
            return
        ctx = WorkerContext(
            rank=self._runtime.rank,
            world_size=self._runtime.world_size,
            worker_id=thread_id,
            local_rank=self._runtime.local_rank,
        )
        with self._lock:
            worker_copy = (
                self._worker_pool.pop()
                if self._worker_pool
                else copy.deepcopy(self._worker)
            )
        try:
            if hasattr(worker_copy, "setup"):
                worker_copy.setup(ctx)
        except Exception as exc:  # noqa: BLE001
            raise WorkerSetupError(exc) from exc
        # Assign only after setup succeeds; a raised exception leaves
        # _local.ctx unset so teardown is never called for a failed worker.
        self._local.ctx = ctx
        self._local.worker = worker_copy
        with self._lock:
            self._thread_states.append((worker_copy, ctx))

    def process(self, seq: int, value: Any) -> Any:
        self._ensure_initialized(threading.get_ident())
        ctx = self._local.ctx
        ctx.record_index = seq
        return self._local.worker.process(value, ctx)

    def _teardown_on_thread(self) -> None:
        """Run teardown for the worker owned by the calling thread.

        Reads from ``threading.local`` so it only acts on this thread's own
        worker.  Idempotent via ``_local.teardown_done``.
        """
        if getattr(self._local, "teardown_done", False):
            return
        worker = getattr(self._local, "worker", None)
        ctx = getattr(self._local, "ctx", None)
        if worker is not None and ctx is not None:
            self._local.teardown_done = True
            with self._lock:
                self._torn_down.add(id(worker))
            if hasattr(worker, "teardown"):
                worker.teardown(ctx)

    def submit_teardown(self, pool: ThreadPoolExecutor) -> None:
        """Tear down every initialized worker on its owning thread.

        All record processing is complete before this is called, so every pool
        thread is idle.  We use a ``threading.Barrier`` sized to the number of
        initialized threads.  Each sentinel task:

        1. Arrives at the barrier — this synchronises all live pool threads so
           we know exactly which ones will participate.
        2. After the barrier, calls ``_teardown_on_thread()`` which reads from
           ``threading.local`` and tears down only that thread's own worker.

        The barrier size equals ``len(states)`` (the number of threads that
        actually initialized workers), not ``max_workers``, because fewer
        threads may have started if the record count was smaller than the pool.

        After all sentinel futures resolve, a coordinator-thread fallback
        handles any worker that did NOT participate in the barrier (a pool
        thread that retired before sentinel submission — extremely rare but
        possible when the pool implementation recycles threads aggressively).

        Must be called while the pool is still accepting submissions and all
        record futures have been awaited.
        """
        with self._lock:
            if self._done_teardown:
                return
            self._done_teardown = True
            states = list(self._thread_states)

        if not states:
            return

        n_threads = len(states)
        barrier = threading.Barrier(n_threads, timeout=30.0)

        def _sentinel() -> None:
            """Arrive at the barrier, then tear down this thread's own worker."""
            try:
                barrier.wait()
            except threading.BrokenBarrierError:
                # A thread raised or timed out; fall back to coordinator cleanup.
                return
            self._teardown_on_thread()

        # Submit exactly n_threads sentinels.  All record work is done, so all
        # n_threads pool threads are idle and will each pick up exactly one
        # sentinel.  The barrier ensures every participating thread has arrived
        # (i.e. we can confirm affinity) before any proceeds to teardown.
        futs = [pool.submit(_sentinel) for _ in range(n_threads)]
        for f in futs:
            try:
                f.result()
            except Exception:  # noqa: BLE001
                pass

        # Coordinator-thread fallback: covers any worker whose thread did not
        # participate in the barrier (pool thread that retired early).
        with self._lock:
            already = set(self._torn_down)
        for worker, ctx in states:
            if id(worker) not in already:
                if hasattr(worker, "teardown"):
                    try:
                        worker.teardown(ctx)
                    except Exception:  # noqa: BLE001
                        pass


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
        # Pre-copy workers while single-threaded so deepcopy never races with
        # live attributes that setup() will later write on thread-owned copies.
        worker_pool = [worker.clone() for _ in range(self.workers)]
        self._thread_worker = _ThreadLocalWorker(
            worker, runtime, worker_pool, max_workers=self.workers
        )
        self._pool = ThreadPoolExecutor(max_workers=self.workers)

    def _submit(self, job: _Job) -> Future:
        assert self._pool is not None and self._thread_worker is not None
        return self._pool.submit(
            self._thread_worker.process, job.seq, job.value
        )

    def _pre_shutdown(self) -> None:
        """Submit teardown futures to the still-live pool before closing it.

        Called by the scheduler after all records complete successfully.  The
        pool is still accepting submissions at this point, so teardown tasks
        run on the threads that own the worker copies and their resources.
        On abort the pool is closed without teardown (resources are already
        in an undefined state).
        """
        if self._pool is not None and self._thread_worker is not None:
            self._thread_worker.submit_teardown(self._pool)

    def _shutdown_backend(self, cancel_futures: bool = False) -> None:
        if self._pool is not None:
            pool = self._pool
            self._pool = None
            self._thread_worker = None
            pool.shutdown(wait=True, cancel_futures=cancel_futures)
