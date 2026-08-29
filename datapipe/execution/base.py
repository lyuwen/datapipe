"""Executor abstraction and the shared bounded-future scheduler.

The core requirement (plan §8, §10): the executor must NEVER eagerly submit
the full dataset. It maintains a bounded number of in-flight futures and
gathers each result immediately upon completion, then submits a replacement.

Every completed record is delivered to ``on_result`` as a ``TaskResult``
(with ``error`` populated on failure). Error policies (raise/skip/return)
are applied by the pipeline's gather loop, not by the executor.
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from concurrent.futures import FIRST_COMPLETED, Future, wait
from dataclasses import dataclass
from typing import Any, Callable, Iterable

from datapipe.context import WorkerContext
from datapipe.result import ExecutionStats, TaskResult
from datapipe.runtime.context import RuntimeContext


class Executor(ABC):
    """Owns local parallelism only."""

    @abstractmethod
    def run(
        self,
        *,
        records: Iterable[Any],
        worker: Callable[[Any, WorkerContext], Any],
        runtime: RuntimeContext,
        on_result: Callable[[TaskResult], None],
        max_in_flight: int | None = None,
        stats: ExecutionStats | None = None,
    ) -> ExecutionStats:
        raise NotImplementedError

    def shutdown(self, cancel_futures: bool = False) -> None:
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        self.shutdown(cancel_futures=True)


@dataclass
class _Job:
    seq: int
    value: Any


def _wrap_result(job: _Job, outcome: Any) -> TaskResult:
    """Wrap a successful (or dropped) worker outcome into a TaskResult."""
    if outcome is None:
        return TaskResult(seq=job.seq, value=None)
    from datapipe.sentinels import DROP

    if outcome is DROP:
        return TaskResult(seq=job.seq, value=None, dropped=True)
    return TaskResult(seq=job.seq, value=outcome)


def _wrap_error(job: _Job, exc: BaseException) -> TaskResult:
    return TaskResult(seq=job.seq, value=None, error=exc)


class BoundedMapExecutor(Executor):
    """Shared bounded-future scheduling logic for future-based executors.

    Subclasses must implement ``submit`` and ``shutdown_backend``; the
    scheduler loop (bounded window, FIRST_COMPLETED gather, immediate
    replacement) is generic.
    """

    def __init__(
        self,
        workers: int | None = None,
        max_in_flight: int | None = None,
    ) -> None:
        self.workers = workers if workers is not None else os.cpu_count() or 1
        self.max_in_flight = (
            max_in_flight if max_in_flight is not None else self.workers * 4
        )
        if self.workers < 1:
            raise ValueError("workers must be >= 1")
        if self.max_in_flight < 1:
            raise ValueError("max_in_flight must be >= 1")

    # -- backend hooks -----------------------------------------------------

    def _start_backend(self, runtime: RuntimeContext, worker) -> None:
        raise NotImplementedError

    def _submit(self, job: _Job) -> Future:
        raise NotImplementedError

    def _shutdown_backend(self, cancel_futures: bool = False) -> None:
        pass

    # -- generic scheduler -------------------------------------------------

    def run(
        self,
        *,
        records: Iterable[Any],
        worker: Callable[[Any, WorkerContext], Any],
        runtime: RuntimeContext,
        on_result: Callable[[TaskResult], None],
        max_in_flight: int | None = None,
        stats: ExecutionStats | None = None,
    ) -> ExecutionStats:
        window = max_in_flight if max_in_flight is not None else self.max_in_flight
        stats = stats or ExecutionStats(
            rank=runtime.rank, world_size=runtime.world_size
        )
        self._start_backend(runtime, worker)
        try:
            self._scheduler_loop(
                records=records,
                runtime=runtime,
                on_result=on_result,
                window=window,
                stats=stats,
            )
        finally:
            self._shutdown_backend(cancel_futures=True)
        return stats

    def _scheduler_loop(
        self,
        *,
        records: Iterable[Any],
        runtime: RuntimeContext,
        on_result: Callable[[TaskResult], None],
        window: int,
        stats: ExecutionStats,
    ) -> None:
        source_iter = iter(records)
        pending: dict[Future, _Job] = {}
        next_seq = 0
        source_exhausted = False
        aborting = False

        def fill() -> None:
            nonlocal next_seq, source_exhausted
            while not source_exhausted and len(pending) < window:
                try:
                    value = next(source_iter)
                except StopIteration:
                    source_exhausted = True
                    break
                job = _Job(seq=next_seq, value=value)
                next_seq += 1
                future = self._submit(job)
                pending[future] = job
                stats.max_in_flight_observed = max(
                    stats.max_in_flight_observed, len(pending)
                )

        try:
            fill()
            while pending:
                done, _ = wait(
                    list(pending),
                    return_when=FIRST_COMPLETED,
                )
                for future in done:
                    job = pending.pop(future)
                    try:
                        result = future.result()
                    except BaseException as exc:  # noqa: BLE001
                        # A worker raised while processing this record.
                        on_result(_wrap_error(job, exc))
                    else:
                        on_result(_wrap_result(job, result))
                # If the consumer's on_result raised (e.g. error policy
                # "raise", or KeyboardInterrupt), stop filling new work and
                # let the finally block cancel the remainder.
                fill()
        finally:
            for future in list(pending):
                future.cancel()
            self._shutdown_backend(cancel_futures=True)

    def shutdown(self, cancel_futures: bool = False) -> None:
        self._shutdown_backend(cancel_futures=cancel_futures)


__all__ = [
    "Executor",
    "BoundedMapExecutor",
    "ExecutionStats",
]
