"""SequentialExecutor: deterministic single-threaded execution.

Critical for testing, debugging, deterministic reproduction, profiling, and
environments where multiprocessing is undesirable. The exact same pipeline
must work under this executor as under process/thread executors.
"""

from __future__ import annotations

from typing import Any, Callable, Iterable

from datapipe.context import WorkerContext
from datapipe.execution.base import Executor, _wrap_error, _wrap_result
from datapipe.io.base import SourceRecordError
from datapipe.result import ExecutionStats, TaskResult
from datapipe.runtime.context import RuntimeContext


class SequentialExecutor(Executor):
    """Runs all records sequentially in the calling process."""

    def __init__(self) -> None:
        self.workers = 1

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
        stats = stats or ExecutionStats(
            rank=runtime.rank, world_size=runtime.world_size
        )
        ctx = WorkerContext(
            rank=runtime.rank,
            world_size=runtime.world_size,
            worker_id=0,
            local_rank=runtime.local_rank,
        )
        # Setup once.
        if hasattr(worker, "setup"):
            worker.setup(ctx)
        # Sequential execution keeps the same submitted / in-flight semantics
        # as the bounded scheduler: a record counts as submitted when it is
        # pulled from the source, is in flight only while the worker runs it,
        # and is out of flight by the time its result reaches ``on_result``.
        acc = self.accounting
        acc.reset()
        try:
            for seq, value in enumerate(records):
                ctx.record_index = seq
                acc.submitted += 1
                if isinstance(value, SourceRecordError):
                    # Resumable per-record source failure: report it without
                    # running the pipeline and keep processing.  Never becomes
                    # in-flight because no work is dispatched for it.
                    on_result(_wrap_error(_seq_job(seq), value.exc))
                    stats.max_in_flight_observed = max(
                        stats.max_in_flight_observed, 1
                    )
                    continue
                acc.in_flight = 1
                try:
                    result = worker.process(value, ctx)
                except (KeyboardInterrupt, SystemExit):
                    acc.in_flight = 0
                    raise
                except BaseException as exc:  # noqa: BLE001
                    acc.in_flight = 0
                    on_result(_wrap_error(_seq_job(seq), exc))
                else:
                    acc.in_flight = 0
                    on_result(_wrap_result(_seq_job(seq), result))
                stats.max_in_flight_observed = max(
                    stats.max_in_flight_observed, 1
                )
        finally:
            acc.in_flight = 0
            if hasattr(worker, "teardown"):
                worker.teardown(ctx)
        return stats


def _seq_job(seq: int):
    from datapipe.execution.base import _Job

    return _Job(seq=seq, value=None)
