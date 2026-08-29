"""ProcessExecutor: bounded local multiprocessing.

Worker lifecycle (plan §9):
- ``ProcessPoolExecutor(initializer=..., initargs=...)`` initializes each
  worker once with the compiled pipeline + worker context.
- ``setup()`` runs once per worker; ``teardown()`` is best-effort via
  process-local ``atexit`` (documented: never rely on it for correctness).
- Only the smallest necessary payload (seq + value) crosses the boundary.
"""

from __future__ import annotations

import multiprocessing as mp
from concurrent.futures import Future, ProcessPoolExecutor
from typing import Any, Callable, Iterable

from datapipe.context import WorkerContext
from datapipe.execution.base import BoundedMapExecutor, _Job
from datapipe.execution.worker import _init_worker, _process_payload
from datapipe.runtime.context import RuntimeContext


class ProcessExecutor(BoundedMapExecutor):
    """Local bounded multiprocessing via ``ProcessPoolExecutor``.

    Args:
        workers: number of worker processes (default: ``os.cpu_count()``).
        max_in_flight: max concurrently submitted tasks (default: ``workers*4``).
        mp_context: ``"spawn"``, ``"fork"`` or ``"forkserver"`` — or a
            ``multiprocessing.context.BaseContext``. Defaults to the platform
            default; never silently forced globally.
    """

    def __init__(
        self,
        workers: int | None = None,
        max_in_flight: int | None = None,
        mp_context: str | mp.context.BaseContext | None = None,
    ) -> None:
        super().__init__(workers=workers, max_in_flight=max_in_flight)
        self._mp_context = mp_context
        self._pool: ProcessPoolExecutor | None = None

    def _start_backend(self, runtime: RuntimeContext, worker) -> None:
        runtime_info = {
            "rank": runtime.rank,
            "world_size": runtime.world_size,
            "local_rank": runtime.local_rank,
        }
        ctx = self._resolve_mp_context()
        self._pool = ProcessPoolExecutor(
            max_workers=self.workers,
            mp_context=ctx,
            initializer=_init_worker,
            initargs=(worker, runtime_info),
        )

    def _resolve_mp_context(self):
        if isinstance(self._mp_context, str):
            return mp.get_context(self._mp_context)
        if self._mp_context is not None:
            return self._mp_context
        # Default to the platform context, but prefer "spawn" where available
        # because fork is unsafe in multi-threaded hosts (and with libraries
        # like pyarrow/OpenMP). Users may force "fork" explicitly for
        # Linux-only high-throughput workloads.
        try:
            return mp.get_context("spawn")
        except ValueError:  # pragma: no cover - spawn unavailable
            return mp.get_context()

    def _submit(self, job: _Job) -> Future:
        return self._pool.submit(_process_payload, (job.seq, job.value))

    def _shutdown_backend(self, cancel_futures: bool = False) -> None:
        if self._pool is not None:
            self._pool.shutdown(wait=False, cancel_futures=cancel_futures)
            self._pool = None
