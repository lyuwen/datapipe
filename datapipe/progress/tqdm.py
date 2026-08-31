"""tqdm-backed progress reporter."""

from __future__ import annotations

from typing import Any

from datapipe.progress.base import ProgressReporter, ProgressSnapshot

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

    def update(
        self,
        n: int = 1,
        snapshot: ProgressSnapshot | None = None,
        **stats: Any,
    ) -> None:
        # Accept structured snapshot or legacy keyword-argument style.
        errors = snapshot.failed if snapshot is not None else stats.get("errors")
        if errors is not None:
            self._error_count = int(errors)
        if self._bar is None:
            return
        self._bar.update(n)
        # The bar surfaces the §12 fields: processed/written/buffered/in_flight
        # (plus failed when non-zero).  Every field is set unconditionally so
        # the postfix reflects current state — omitting zeros would leave a
        # stale non-zero value stuck on the bar after the count drains.
        postfix: dict[str, Any] = {}
        if snapshot is not None:
            postfix["processed"] = snapshot.processed
            postfix["written"] = snapshot.written
            postfix["buffered"] = snapshot.buffered
            postfix["in_flight"] = snapshot.in_flight
        postfix["errors"] = self._error_count
        self._bar.set_postfix(postfix)

    def close(self) -> None:
        if self._bar is not None:
            self._bar.close()
            self._bar = None
