"""Structured errors for datapipe."""

from __future__ import annotations


class DataPipeError(Exception):
    """Base class for all datapipe errors."""


class StageExecutionError(DataPipeError):
    """Raised inside a worker when a specific stage fails on a record.

    Wraps the underlying exception so the runtime can attribute a failure to
    a named stage and a record sequence number.
    """

    def __init__(
        self,
        *,
        stage_name: str,
        record_seq: int,
        cause: BaseException,
    ) -> None:
        self.stage_name = stage_name
        self.record_seq = record_seq
        self.cause = cause
        super().__init__(
            f"stage {stage_name!r} failed for record {record_seq}: "
            f"{type(cause).__name__}: {cause}"
        )

    def __reduce__(self):
        return (
            _rebuild_stage_execution_error,
            (self.stage_name, self.record_seq, self.cause),
        )


def _rebuild_stage_execution_error(
    stage_name: str, record_seq: int, cause: BaseException
) -> StageExecutionError:
    return StageExecutionError(
        stage_name=stage_name,
        record_seq=record_seq,
        cause=cause,
    )


class PipelineValidationError(DataPipeError):
    """Raised when a Pipeline is constructed with invalid stages/config."""


class ShardingError(DataPipeError):
    """Raised for invalid sharding configuration."""


class SourceError(DataPipeError):
    """Raised for source IO problems."""


class SinkError(DataPipeError):
    """Raised for sink IO problems."""


class ParquetError(DataPipeError):
    """Raised for Parquet-specific problems (pyarrow unavailable/mismatch)."""


class RuntimeDetectionError(DataPipeError):
    """Raised when a runtime context cannot be resolved."""
