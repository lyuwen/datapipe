"""Structured results and execution statistics."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class TaskResult:
    """The structured outcome of processing a single record.

    Exactly one of ``value``, ``error``, or ``dropped`` is meaningful for a
    given result.
    """

    seq: int
    value: Any = None
    error: BaseException | None = None
    dropped: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.error is None and not self.dropped


@dataclass
class ExecutionStats:
    """Summary of a pipeline run."""

    input_records: int = 0
    completed_records: int = 0
    output_records: int = 0
    dropped_records: int = 0
    failed_records: int = 0

    elapsed_seconds: float = 0.0
    records_per_second: float = 0.0

    rank: int = 0
    world_size: int = 1

    # High-water marks for introspection.
    max_in_flight_observed: int = 0
    max_reorder_buffer_observed: int = 0

    def __repr__(self) -> str:
        return (
            f"ExecutionStats("
            f"input={self.input_records}, "
            f"output={self.output_records}, "
            f"dropped={self.dropped_records}, "
            f"failed={self.failed_records}, "
            f"elapsed={self.elapsed_seconds:.3f}s, "
            f"rate={self.records_per_second:.0f} rec/s, "
            f"rank={self.rank}/{self.world_size})"
        )
