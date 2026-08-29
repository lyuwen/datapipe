"""Progress/stats helpers (rate tracking)."""

from __future__ import annotations

import time


class RateTracker:
    """Tracks elapsed time and a running records-per-second estimate."""

    def __init__(self) -> None:
        self._start = 0.0
        self._count = 0
        self._running = False

    def start(self) -> None:
        self._start = time.monotonic()
        self._count = 0
        self._running = True

    def tick(self, n: int = 1) -> None:
        self._count += n

    @property
    def elapsed(self) -> float:
        if not self._running:
            return 0.0
        return time.monotonic() - self._start

    @property
    def rate(self) -> float:
        elapsed = self.elapsed
        if elapsed <= 0:
            return 0.0
        return self._count / elapsed
