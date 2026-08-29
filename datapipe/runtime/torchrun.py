"""torchrun-style environment helpers."""

from __future__ import annotations

import os


def torchrun_env() -> dict[str, str | None]:
    """Return the relevant torchrun-style environment variables."""
    return {
        "rank": os.environ.get("RANK"),
        "world_size": os.environ.get("WORLD_SIZE"),
        "local_rank": os.environ.get("LOCAL_RANK"),
        "local_world_size": os.environ.get("LOCAL_WORLD_SIZE"),
    }
