"""Worker context handed to stages."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class WorkerContext:
    """Per-worker context passed to ``setup``/``process``/``teardown``.

    ``rank``/``world_size`` describe the global position of this process.
    ``worker_id`` is a locally unique id of this worker within the executor.
    ``record_index`` is set to the current record's sequence number while a
    record is being processed (``None`` otherwise).
    """

    rank: int = 0
    world_size: int = 1

    worker_id: int = 0
    local_rank: int | None = None

    record_index: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
