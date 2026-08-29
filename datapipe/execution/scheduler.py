"""Bounded scheduler utilities (re-exported for a stable internal layout).

The bounded-future scheduling loop lives in ``datapipe.execution.base``
(``BoundedMapExecutor``). This module exists to keep the documented package
layout (``execution/scheduler.py``) truthful and to host small scheduler
helpers that do not belong to any single executor.
"""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, wait

__all__ = ["FIRST_COMPLETED", "Future", "wait"]
