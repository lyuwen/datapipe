"""tqdm-backed progress reporter."""

from __future__ import annotations

from typing import Any

from datapipe.progress.base import ProgressReporter

try:  # pragma: no cover - env dependent
    from tqdm.auto import tqdm as _tqdm
except ImportError:  # pragma: no cover
    _tqdm = None


class TqdmProgress(ProgressReporter):
    """Progress bar using tqdm (falls back to a no-op if unavailable)."""

    def __init__(self, *, desc: str = "Processing", unit: str = "rec") -> None:
        self.desc = desc
        self.unit = unit
        self._bar = None
        self._error_count = 0

    def start(self, total: int | None = None) -> None:
        if _tqdm is None:
            return
        self._bar = _tqdm(
            total=total,
            desc=self.desc,
            unit=self.unit,
            unit_scale=True,
            dynamic_ncols=True,
        )
        if total is None:
            self._bar.total = None
            self._bar.initial = 0

    def update(self, n: int = 1, **stats: Any) -> None:
        errors = stats.get("errors")
        if errors is not None:
            self._error_count = int(errors)
        if self._bar is None:
            return
        self._bar.update(n)
        if self._error_count:
            self._bar.set_postfix(errors=self._error_count)

    def close(self) -> None:
        if self._bar is not None:
            self._bar.close()
            self._bar = None
