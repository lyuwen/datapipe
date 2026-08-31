"""Progress reporter base classes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ProgressSnapshot:
    """Structured progress state for one reporting event.

    Frozen so reporters can safely store snapshots without risk of aliasing.

    Attributes
    ----------
    processed:
        Worker results received by the coordinator (advances on completion,
        before ordered buffering drains).
    written:
        Records emitted to the primary or error sink.
    dropped:
        Records that produced a DROP sentinel (intentionally omitted).
    buffered:
        Completed results waiting in the reorder buffer for an ordering gap
        to close (always 0 in unordered mode).
    in_flight:
        Submitted tasks that have not yet completed.
    failed:
        Records that produced an error (across all error policies).
    """

    processed: int = 0
    written: int = 0
    dropped: int = 0
    buffered: int = 0
    in_flight: int = 0
    failed: int = 0


class ProgressReporter:
    """Small progress abstraction decoupling the runtime from any UI library."""

    def start(self, total: int | None = None) -> None:
        ...

    def update(self, n: int = 1, snapshot: ProgressSnapshot | None = None, **stats: Any) -> None:
        ...

    def close(self) -> None:
        ...


class NullProgress(ProgressReporter):
    """A no-op reporter."""

    def start(self, total: int | None = None) -> None:
        pass

    def update(self, n: int = 1, snapshot: ProgressSnapshot | None = None, **stats: Any) -> None:
        pass

    def close(self) -> None:
        pass
