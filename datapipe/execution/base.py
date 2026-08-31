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
from datapipe.io.base import SourceRecordError
from datapipe.result import ExecutionStats, TaskResult
from datapipe.runtime.context import RuntimeContext


class WorkerSetupError(Exception):
    """Raised when a worker's setup() fails.

    Wraps the original exception so the scheduler can distinguish an
    initialization failure (which must abort regardless of error policy)
    from a per-record processing failure (which routes through the policy).
    """

    def __init__(self, cause: BaseException) -> None:
        super().__init__(f"worker setup failed: {cause}")
        self.cause = cause


class SubmissionAccounting:
    """Live submitted / in-flight counters owned by the scheduler.

    The scheduler is the only component that knows the true dispatch state, so
    it owns these counters rather than letting the coordinator approximate them
    by wrapping the record iterator (which cannot see source-error markers, and
    which produced negative in-flight counts).

    Semantics
    ---------
    ``submitted``
        Records pulled from the source and accounted for: either dispatched to
        a worker, or short-circuited as a ``SourceRecordError`` marker. Counted
        *before* the corresponding ``on_result`` call so ``submitted`` never
        lags ``processed``.
    ``in_flight``
        Dispatched tasks that have not yet been delivered to ``on_result``. A
        record is removed from the in-flight set before its result is
        delivered, so during an ``on_result`` call the record being reported is
        not counted.

    Both counters are written only by the scheduler and read only from the
    thread that drives it (``on_result`` runs on the coordinator), so no lock
    is needed. The invariant ``submitted - processed == in_flight`` holds at
    every ``on_result`` entry.
    """

    __slots__ = ("submitted", "in_flight")

    def __init__(self) -> None:
        self.submitted = 0
        self.in_flight = 0

    def reset(self) -> None:
        self.submitted = 0
        self.in_flight = 0

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (
            f"SubmissionAccounting(submitted={self.submitted}, "
            f"in_flight={self.in_flight})"
        )


class Executor(ABC):
    """Owns local parallelism only."""

    @property
    def accounting(self) -> SubmissionAccounting:
        """Live submitted / in-flight counters for the current run.

        Created lazily so subclasses need not call ``super().__init__``, and
        reset at the start of every ``run()``.
        """
        acc = getattr(self, "_accounting", None)
        if acc is None:
            acc = SubmissionAccounting()
            self._accounting = acc
        return acc

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

    def _pre_shutdown(self) -> None:
        """Called after all records complete, before the pool is torn down.

        Override to submit teardown work to the still-live pool so that
        teardown runs on the owning worker threads rather than the coordinator.
        The default no-op is correct for executors that don't need this.
        """

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
        self.accounting.reset()
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
        acc = self.accounting

        def fill() -> None:
            nonlocal next_seq, source_exhausted
            while not source_exhausted and len(pending) < window:
                try:
                    value = next(source_iter)
                except StopIteration:
                    source_exhausted = True
                    break
                except SourceRecordError as exc:
                    # A source may *raise* SourceRecordError; treat it the
                    # same as a yielded marker below.  It consumes a sequence
                    # number and counts as submitted (so submitted stays >=
                    # processed) but it never becomes in-flight: no task is
                    # dispatched and the result is delivered inline.
                    acc.submitted += 1
                    on_result(
                        _wrap_error(_Job(seq=next_seq, value=None), exc.exc)
                    )
                    next_seq += 1
                    continue
                except BaseException:
                    # A genuine source failure (IO error, KeyboardInterrupt,
                    # etc.) is not a per-record error; propagate so the
                    # pipeline aborts rather than silently truncating output.
                    raise
                if isinstance(value, SourceRecordError):
                    # Resumable per-record failure: report it without
                    # submitting a job and keep pulling subsequent records.
                    acc.submitted += 1
                    on_result(
                        _wrap_error(_Job(seq=next_seq, value=None), value.exc)
                    )
                    next_seq += 1
                    continue
                job = _Job(seq=next_seq, value=value)
                next_seq += 1
                future = self._submit(job)
                pending[future] = job
                acc.submitted += 1
                acc.in_flight = len(pending)
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
                    # Drop out of the in-flight set *before* delivering the
                    # result: the coordinator builds its progress snapshot
                    # inside on_result, and the record being reported there is
                    # no longer in flight.
                    acc.in_flight = len(pending)
                    try:
                        result = future.result()
                    except WorkerSetupError:
                        # Worker initialization failure is not a per-record
                        # error; abort regardless of the configured error
                        # policy by re-raising out of the scheduler loop.
                        raise
                    except (KeyboardInterrupt, SystemExit):
                        raise
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
            # Cancelled work will never be delivered to on_result, so it is no
            # longer in flight.  Leaving the count non-zero would leak a stale
            # number into any snapshot published during teardown.
            acc.in_flight = 0
            # Let subclasses (e.g. ThreadExecutor) submit teardown futures to
            # the still-live pool before it is closed.  Runs on both normal
            # completion and abort so worker resources are always released.
            self._pre_shutdown()
            self._shutdown_backend(cancel_futures=True)

    def shutdown(self, cancel_futures: bool = False) -> None:
        self._shutdown_backend(cancel_futures=cancel_futures)


__all__ = [
    "Executor",
    "BoundedMapExecutor",
    "ExecutionStats",
    "SubmissionAccounting",
]
