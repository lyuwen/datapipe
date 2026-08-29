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
        try:
            for seq, value in enumerate(records):
                ctx.record_index = seq
                if isinstance(value, SourceRecordError):
                    # Resumable per-record source failure: report it without
                    # running the pipeline and keep processing.
                    on_result(_wrap_error(_seq_job(seq), value.exc))
                    stats.max_in_flight_observed = max(
                        stats.max_in_flight_observed, 1
                    )
                    continue
                try:
                    result = worker.process(value, ctx)
                except BaseException as exc:  # noqa: BLE001
                    on_result(_wrap_error(_seq_job(seq), exc))
                else:
                    on_result(_wrap_result(_seq_job(seq), result))
                stats.max_in_flight_observed = max(
                    stats.max_in_flight_observed, 1
                )
        finally:
            if hasattr(worker, "teardown"):
                worker.teardown(ctx)
        return stats


def _seq_job(seq: int):
    from datapipe.execution.base import _Job

    return _Job(seq=seq, value=None)
