"""RuntimeContext: where this process is running."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from datapipe.runtime import detect as _detect
from datapipe.sharding import (
    ModuloSharding,
    NoSharding,
    ShardingStrategy,
)


@dataclass
class RuntimeContext:
    """Global position of this process in a (possibly distributed) run.

    ``rank``/``world_size`` define global record ownership; ``local_rank`` and
    ``node_rank`` describe placement within a node. ``metadata`` carries any
    job metadata discovered from the environment.
    """

    rank: int = 0
    world_size: int = 1
    local_rank: int | None = None
    node_rank: int | None = None
    job_id: str | None = None
    environment: str = "local"
    metadata: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.world_size < 1:
            raise ValueError("world_size must be >= 1")
        if not (0 <= self.rank < self.world_size):
            raise ValueError(
                f"rank {self.rank} out of range for world_size {self.world_size}"
            )

    @classmethod
    def auto(cls, **overrides: Any) -> "RuntimeContext":
        """Detect the current launch environment.

        Overrides win over detection (e.g. ``RuntimeContext.auto(rank=2)``).
        """
        result = _detect.detect()
        fields = dict(
            rank=result.rank,
            world_size=result.world_size,
            local_rank=result.local_rank,
            node_rank=result.node_rank,
            job_id=result.job_id,
            environment=result.environment,
        )
        fields.update(overrides)
        return cls(**fields)

    def __repr__(self) -> str:
        return (
            f"RuntimeContext(rank={self.rank}, world_size={self.world_size}, "
            f"environment={self.environment!r})"
        )


def default_sharding_for(runtime: RuntimeContext) -> ShardingStrategy:
    """Pick a sensible default sharding strategy.

    Single-rank runs use ``NoSharding``; multi-rank runs default to
    ``ModuloSharding`` (the universal logical fallback).
    """
    if runtime.world_size <= 1:
        return NoSharding()
    return ModuloSharding()


def is_rank_aware_env() -> bool:
    """True if any recognized distributed env var is present."""
    keys = (
        "RANK",
        "SLURM_PROCID",
        "JOB_COMPLETION_INDEX",
    )
    return any(os.environ.get(k) is not None for k in keys)
