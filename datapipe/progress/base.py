"""Progress reporter base classes."""

from __future__ import annotations

from typing import Any


class ProgressReporter:
    """Small progress abstraction decoupling the runtime from any UI library."""

    def start(self, total: int | None = None) -> None:
        ...

    def update(self, n: int = 1, **stats: Any) -> None:
        ...

    def close(self) -> None:
        ...


class NullProgress(ProgressReporter):
    """A no-op reporter."""

    def start(self, total: int | None = None) -> None:
        pass

    def update(self, n: int = 1, **stats: Any) -> None:
        pass

    def close(self) -> None:
        pass
