"""Worker-process entrypoints for the ProcessExecutor.

These are module-level functions/globals so they pickle cleanly under the
``spawn`` start method (no closures, no local functions).
"""

from __future__ import annotations

import atexit
import multiprocessing as mp
from typing import Any

from datapipe.context import WorkerContext

# Process-local state, installed by _init_worker via the pool initializer.
_WORKER_PIPELINE = None
_WORKER_CONTEXT: WorkerContext | None = None


def _init_worker(compiled_pipeline, runtime_info) -> None:
    """Pool initializer: build the worker context and run pipeline setup."""
    global _WORKER_PIPELINE, _WORKER_CONTEXT
    _WORKER_PIPELINE = compiled_pipeline

    identity = getattr(mp.current_process(), "_identity", None)
    worker_id = identity[0] if identity else 0
    _WORKER_CONTEXT = WorkerContext(
        rank=runtime_info["rank"],
        world_size=runtime_info["world_size"],
        worker_id=worker_id,
        local_rank=runtime_info.get("local_rank"),
    )
    if hasattr(_WORKER_PIPELINE, "setup"):
        _WORKER_PIPELINE.setup(_WORKER_CONTEXT)
    # Best-effort teardown at process exit (never relied upon for correctness).
    atexit.register(_worker_atexit)


def _worker_atexit() -> None:
    global _WORKER_PIPELINE, _WORKER_CONTEXT
    if _WORKER_PIPELINE is not None and hasattr(_WORKER_PIPELINE, "teardown"):
        try:
            _WORKER_PIPELINE.teardown(_WORKER_CONTEXT)
        except Exception:  # noqa: BLE001
            pass


def _process_payload(payload: tuple[int, Any]) -> Any:
    """Top-level worker entrypoint: process one ``(seq, value)`` payload."""
    seq, value = payload
    ctx = _WORKER_CONTEXT
    if ctx is not None:
        ctx.record_index = seq
    return _WORKER_PIPELINE.process(value, ctx)
