"""Pipeline: a per-record program composed of stages, plus the run loop."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from datapipe.context import WorkerContext
from datapipe.errors import (
    PipelineValidationError,
    StageExecutionError,
)
from datapipe.execution import (
    Executor,
    ProcessExecutor,
)
from datapipe.io.base import Source, Sink
from datapipe.io.iterable import CallableSink, IterableSource
from datapipe.progress import NullProgress, ProgressReporter, TqdmProgress
from datapipe.result import ExecutionStats, TaskResult
from datapipe.runtime import RuntimeContext, default_sharding_for
from datapipe.sentinels import DROP
from datapipe.stage import Stage, coerce_stage

logger = logging.getLogger("datapipe")

_ERROR_POLICIES = ("raise", "skip", "return")


@dataclass
class RunConfig:
    """Resolved configuration for a single ``run()`` call."""

    executor: Executor
    runtime: RuntimeContext
    sharding: Any
    ordered: bool
    progress: bool
    errors: str
    error_sink: Sink | None = None
    max_in_flight: int | None = None


class CompiledPipeline:
    """Worker-local fused program. Must be pickleable.

    The runtime parallelizes only this object's ``process`` callable; stages
    never get their own process pools.
    """

    def __init__(self, stages: Sequence[Stage]) -> None:
        self.stages = list(stages)

    def setup(self, ctx: WorkerContext) -> None:
        for stage in self.stages:
            stage.setup(ctx)

    def process(self, value: Any, ctx: WorkerContext) -> Any:
        x = value
        for stage in self.stages:
            try:
                x = stage.process(x, ctx)
            except StageExecutionError:
                raise
            except Exception as exc:  # noqa: BLE001
                raise StageExecutionError(
                    stage_name=stage.name,
                    record_seq=ctx.record_index if ctx else None,
                    cause=exc,
                ) from exc
            if x is DROP:
                return DROP
        return x

    def teardown(self, ctx: WorkerContext) -> None:
        for stage in reversed(self.stages):
            try:
                stage.teardown(ctx)
            except Exception:  # noqa: BLE001
                logger.exception("error during stage teardown")

    def clone(self) -> "CompiledPipeline":
        """Return a new CompiledPipeline with deep-copied stage instances.

        ``copy.deepcopy`` gives each thread worker a fully independent copy of
        every stage's construction-time state (nested dicts, lists, objects),
        matching the isolation guarantee of process workers.  ``Stage.__deepcopy__``
        handles non-copyable attributes such as ``threading.Lock`` by substituting
        fresh equivalents so deepcopy never raises ``TypeError``.
        """
        import copy as _copy
        return CompiledPipeline([_copy.deepcopy(s) for s in self.stages])

    def __repr__(self) -> str:
        return f"CompiledPipeline(stages={self.stages!r})"


class Pipeline:
    """A list of stages executed sequentially per record.

    The ``Pipeline`` object is inert until ``run()`` is called (invariant 1).
    """

    def __init__(
        self,
        stages: Iterable[Any],
        *,
        name: str | None = None,
    ) -> None:
        entries = list(stages)
        if not entries:
            raise PipelineValidationError("Pipeline requires at least one stage")
        self.stages: list[Stage] = [
            coerce_stage(entry, i) for i, entry in enumerate(entries)
        ]
        self.name = name or getattr(self.stages[0], "name", "pipeline")
        self._dedupe_stage_names()

    def _dedupe_stage_names(self) -> None:
        """Reject explicit duplicate names; dedupe auto-generated ones."""
        seen: set[str] = set()
        counts: dict[str, int] = {}
        for stage in self.stages:
            counts[stage.name] = counts.get(stage.name, 0) + 1
        for stage in self.stages:
            name = stage.name
            if stage._name_explicit:
                if name in seen:
                    raise PipelineValidationError(
                        f"duplicate explicit stage name {name!r}; "
                        "stage names must be stable and unique"
                    )
                seen.add(name)
            else:
                if name in seen:
                    counts[name] += 1
                    stage.name = f"{name}_{counts[name]}"
                seen.add(stage.name)

    # -- compilation -------------------------------------------------------

    def compile(self) -> CompiledPipeline:
        return CompiledPipeline(self.stages)

    # -- introspection -----------------------------------------------------

    def stage_names(self) -> list[str]:
        return [s.name for s in self.stages]

    def __len__(self) -> int:
        return len(self.stages)

    def __iter__(self):
        return iter(self.stages)

    def __repr__(self) -> str:
        names = ", ".join(repr(s) for s in self.stages)
        return f"Pipeline([{names}])"

    # -- running -----------------------------------------------------------

    def run(
        self,
        source: Source,
        sink: Sink,
        *,
        executor: Executor | None = None,
        sharding: Any | None = None,
        runtime: RuntimeContext | None = None,
        ordered: bool = True,
        progress: bool = True,
        errors: str = "raise",
        error_sink: Sink | None = None,
        max_in_flight: int | None = None,
        progress_reporter: ProgressReporter | None = None,
    ) -> ExecutionStats:
        """Execute the pipeline over ``source`` writing to ``sink``.

        ``errors`` policy:
          - ``"raise"``: first processing error aborts the run (default).
          - ``"skip"``: failed records are counted and omitted.
          - ``"return"``: errors are delivered to ``error_sink`` (or emitted
            as structured ``TaskResult`` values if no error_sink is given).
        """
        if errors not in _ERROR_POLICIES:
            raise PipelineValidationError(
                f"errors must be one of {_ERROR_POLICIES}, got {errors!r}"
            )
        # Convenience coercion: string paths become JSONL adapters, plain
        # iterables become IterableSource, callables become CallableSink.
        if isinstance(source, str):
            from datapipe.io.jsonl import JsonlSource

            source = JsonlSource(source)
        elif not isinstance(source, Source):
            source = IterableSource(source)
        if isinstance(sink, str):
            from datapipe.io.jsonl import JsonlSink

            sink = JsonlSink(sink)
        elif not isinstance(sink, Sink):
            if callable(sink):
                sink = CallableSink(sink)
            else:
                raise TypeError(
                    "sink must be a Sink or a callable, "
                    f"got {type(sink).__name__}"
                )
        runtime = runtime or RuntimeContext.auto()
        executor = executor or ProcessExecutor()
        sharding = sharding or default_sharding_for(runtime)

        config = RunConfig(
            executor=executor,
            runtime=runtime,
            sharding=sharding,
            ordered=ordered,
            progress=progress,
            errors=errors,
            error_sink=error_sink,
            max_in_flight=max_in_flight,
        )
        compiled = self.compile()

        logger.info(
            "Pipeline %r | executor=%s | workers=%s | max_in_flight=%s | "
            "rank=%s/%s | ordered=%s | errors=%s",
            self.name,
            type(executor).__name__,
            getattr(executor, "workers", "?"),
            getattr(executor, "max_in_flight", "?"),
            runtime.rank,
            runtime.world_size,
            ordered,
            errors,
        )

        start = time.monotonic()
        primary_error: BaseException | None = None
        try:
            stats = self._run_impl(
                source=source,
                sink=sink,
                compiled=compiled,
                config=config,
                progress_reporter=progress_reporter,
            )
        except BaseException as exc:  # noqa: BLE001
            # Remember the run error (if any) so a finalization failure does
            # not mask the original failure.
            primary_error = exc
            raise
        finally:
            # Finalize sinks first. A failure here must NOT be swallowed
            # (e.g. ParquetSink.close() writes the final buffered batch), but
            # it must not mask an already-raised run error either.
            finalize_error: BaseException | None = None
            try:
                sink.close()
            except BaseException as exc:  # noqa: BLE001
                finalize_error = exc
                logger.error("error closing sink: %s", exc)
            try:
                source.close()
            except BaseException as exc:  # noqa: BLE001
                if finalize_error is None:
                    finalize_error = exc
                logger.error("error closing source: %s", exc)
            try:
                if config.error_sink is not None:
                    config.error_sink.close()
            except BaseException as exc:  # noqa: BLE001
                if finalize_error is None:
                    finalize_error = exc
                logger.error("error closing error_sink: %s", exc)
            if finalize_error is not None and primary_error is None:
                raise finalize_error

        stats.elapsed_seconds = time.monotonic() - start
        stats.records_per_second = (
            stats.completed_records / stats.elapsed_seconds
            if stats.elapsed_seconds > 0
            else 0.0
        )
        return stats

    def _run_impl(
        self,
        *,
        source: Source,
        sink: Sink,
        compiled: CompiledPipeline,
        config: RunConfig,
        progress_reporter: ProgressReporter | None,
    ) -> ExecutionStats:
        runtime = config.runtime

        source.open(runtime)
        sink.open(runtime)
        if config.error_sink is not None:
            config.error_sink.open(runtime)
        records = source.iter_for_runtime(runtime, config.sharding)

        reporter: ProgressReporter = (
            progress_reporter
            or (TqdmProgress() if config.progress else NullProgress())
        )
        reporter.start(total=source.total)

        stats = ExecutionStats(rank=runtime.rank, world_size=runtime.world_size)
        ordered_buffer: dict[int, TaskResult] = {}
        next_to_emit = 0

        def on_result(result: TaskResult) -> None:
            """Single entry point for every completed record."""
            nonlocal next_to_emit
            stats.completed_records += 1

            if result.error is not None:
                stats.failed_records += 1

            # Policy "raise": abort the run immediately on the first error,
            # regardless of position. Completed-but-unemitted results in the
            # reorder buffer are dropped on abort (see finally below), so the
            # sink keeps only the contiguous prefix written before the error.
            if result.error is not None and config.errors == "raise":
                self._raise_result(result)
                return  # unreachable

            if config.ordered:
                # Buffer EVERY result (success, drop, error) at its seq
                # position. Errors under "skip"/"return" are emitted at their
                # position too, so next_to_emit always advances contiguously
                # and the buffer stays bounded regardless of error count.
                ordered_buffer[result.seq] = result
                stats.max_reorder_buffer_observed = max(
                    stats.max_reorder_buffer_observed, len(ordered_buffer)
                )
                while next_to_emit in ordered_buffer:
                    emit = ordered_buffer.pop(next_to_emit)
                    next_to_emit += 1
                    self._emit(emit, sink, stats, config, reporter)
            else:
                self._emit(result, sink, stats, config, reporter)

        try:
            stats = config.executor.run(
                records=records,
                worker=compiled,
                runtime=runtime,
                on_result=on_result,
                max_in_flight=config.max_in_flight,
                stats=stats,
            )
        except KeyboardInterrupt:
            logger.warning("KeyboardInterrupt: stopping pipeline")
            raise
        finally:
            reporter.close()
            # On abort (error policy "raise" or KeyboardInterrupt), do NOT
            # flush the reorder buffer. An interrupted ordered sink must
            # contain only the contiguous completed prefix; flushing buffered
            # results that follow a gap would violate input order.

        stats.input_records = stats.completed_records
        return stats

    @staticmethod
    def _emit(
        result: TaskResult,
        sink: Sink,
        stats: ExecutionStats,
        config: RunConfig,
        reporter: ProgressReporter,
    ) -> None:
        """Write one result per policy.

        ``errors == "raise"`` is never routed here (aborts in ``on_result``).
        For ``errors in ("skip", "return")``, error results reach this point
        at their sequence position (ordered mode) or immediately (unordered).
        """
        if result.error is not None:
            if config.errors == "skip":
                # Failed records are counted and omitted.
                reporter.update(1, errors=stats.failed_records)
                return
            # errors == "return": deliver to error_sink if given, else expose
            # the structured TaskResult to the main sink.
            if config.error_sink is not None:
                config.error_sink.write(_error_payload(result))
            else:
                sink.write(result)
            stats.output_records += 1
            reporter.update(1, errors=stats.failed_records)
            return
        if result.dropped:
            stats.dropped_records += 1
            reporter.update(1, errors=stats.failed_records)
            return
        sink.write(result.value)
        stats.output_records += 1
        reporter.update(1, errors=stats.failed_records)

    def _raise_result(self, result: TaskResult) -> None:
        """Raise the wrapped error (with stage attribution)."""
        exc = result.error
        if isinstance(exc, StageExecutionError):
            raise exc from exc.cause
        raise exc

    def __repr__(self) -> str:
        return f"Pipeline(name={self.name!r}, stages={len(self.stages)})"


def _error_payload(result: TaskResult) -> dict:
    """Structured error record for the error sink.

    When the error is a ``StageExecutionError``, we unwrap the *cause* for
    the type/message fields so the record names the original failure, while
    still carrying the stage attribution and the full traceback.
    """
    exc = result.error
    tb = None
    cause = exc.cause if isinstance(exc, StageExecutionError) else None
    detail = cause if cause is not None else exc

    if detail is not None:
        import traceback

        tb = "".join(
            traceback.format_exception(type(detail), detail, detail.__traceback__)
        )
    return {
        "seq": result.seq,
        "error_type": type(detail).__name__ if detail is not None else None,
        "error_message": str(detail) if detail is not None else None,
        "traceback": tb,
        "stage_name": getattr(exc, "stage_name", None) if exc is not None else None,
        "metadata": result.metadata,
    }
