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
- ``teardown()`` runs on the owning thread via a per-thread ``threading.Event``
  mechanism that guarantees the correct thread performs its own teardown.
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

    Teardown guarantee
    ------------------
    During ``_ensure_initialized``, each thread stores a ``threading.Event``
    (its "teardown signal") both in ``threading.local`` and in a shared dict
    keyed by thread id.  When ``submit_teardown`` is called:

    1. It sets every initialized thread's teardown-signal event.
    2. It submits ``max_workers`` tasks.  Each task checks
       ``threading.local`` for a teardown signal.  If the signal is set and
       teardown hasn't run yet, the thread tears down its own worker, then
       clears the signal so re-entry is impossible.
    3. After all task futures resolve, the coordinator verifies that every
       initialized thread has been torn down.  Any that haven't (their thread
       exited before running the pool task) are torn down on the coordinator
       thread as a genuine last resort.

    Key invariant: a task only runs teardown when its own teardown signal is
    set AND it hasn't been done yet.  Because signals are stored in
    ``threading.local``, a task executing on thread T can only see T's own
    signal.  Setting ALL signals before submitting tasks means every idle pool
    thread will find its own signal set and run its own teardown the moment it
    picks up a task.
    """

    def __init__(self, worker, runtime: RuntimeContext, worker_pool: list, max_workers: int) -> None:
        self._worker = worker
        self._runtime = runtime
        self._local = threading.local()
        self._lock = threading.Lock()
        self._done_teardown = False
        self._thread_states: list[tuple] = []   # (worker_copy, ctx, thread_id, signal_event)
        self._torn_down: set[int] = set()       # thread_ids that completed teardown
        self._worker_pool = worker_pool
        self._max_workers = max_workers

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

        # Each thread gets a unique teardown-signal event stored in both
        # threading.local and the shared state list.
        signal = threading.Event()
        self._local.ctx = ctx
        self._local.worker = worker_copy
        self._local.teardown_signal = signal
        with self._lock:
            self._thread_states.append((worker_copy, ctx, thread_id, signal))

    def process(self, seq: int, value: Any) -> Any:
        self._ensure_initialized(threading.get_ident())
        ctx = self._local.ctx
        ctx.record_index = seq
        return self._local.worker.process(value, ctx)

    def _check_and_run_teardown(self) -> None:
        """If this thread has a pending teardown signal, run teardown now.

        Each thread stores its own teardown-signal event in threading.local.
        This method checks only the calling thread's own signal, so it never
        accidentally tears down a different thread's worker.  Idempotent.
        """
        signal = getattr(self._local, "teardown_signal", None)
        if signal is None or not signal.is_set():
            return
        if getattr(self._local, "teardown_done", False):
            return
        self._local.teardown_done = True
        # Record by thread id (not worker object id) so the coordinator check works.
        with self._lock:
            self._torn_down.add(threading.get_ident())
        worker = getattr(self._local, "worker", None)
        ctx = getattr(self._local, "ctx", None)
        if worker is not None and ctx is not None and hasattr(worker, "teardown"):
            try:
                worker.teardown(ctx)
            except Exception:  # noqa: BLE001
                pass

    def submit_teardown(self, pool: ThreadPoolExecutor) -> None:
        """Tear down every initialized worker on its owning thread.

        1. Snapshots all initialized states.
        2. Sets every thread's teardown-signal event so the next pool task
           on each thread will call teardown.
        3. Submits ``max_workers`` tasks.  Each task calls
           ``_check_and_run_teardown()``, which acts only on the calling
           thread's own signal.  Since every signal is already set, each
           thread tears down its worker on its first available task.
        4. Coordinator fallback: any thread whose signal was never consumed
           (thread exited before picking up a task) is handled here.

        Must be called while the pool is still accepting submissions.
        """
        with self._lock:
            if self._done_teardown:
                return
            self._done_teardown = True
            states = list(self._thread_states)

        if not states:
            return

        # Signal every initialized thread to tear down on its next task.
        for _, _, _, signal in states:
            signal.set()

        # Submit max_workers tasks — enough for every live thread to get one.
        futs = [pool.submit(self._check_and_run_teardown) for _ in range(self._max_workers)]
        for f in futs:
            try:
                f.result()
            except Exception:  # noqa: BLE001
                pass

        # Coordinator-thread fallback for threads whose tasks were cancelled
        # or whose threads exited before running a task.
        with self._lock:
            already = set(self._torn_down)
        for worker, ctx, tid, signal in states:
            if tid not in already:
                # Thread didn't run its own teardown — do it on coordinator.
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
        """Submit teardown tasks to the still-live pool before closing it.

        Called by the scheduler's finally block on both normal completion and
        abort so worker resources are always released.
        """
        if self._pool is not None and self._thread_worker is not None:
            self._thread_worker.submit_teardown(self._pool)

    def _shutdown_backend(self, cancel_futures: bool = False) -> None:
        if self._pool is not None:
            pool = self._pool
            self._pool = None
            self._thread_worker = None
            pool.shutdown(wait=True, cancel_futures=cancel_futures)
