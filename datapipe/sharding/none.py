"""NoSharding: rank 0 owns everything."""

from __future__ import annotations

from typing import Any

from datapipe.sharding.base import ShardingStrategy


class NoSharding(ShardingStrategy):
    """Everything belongs to rank 0 (i.e. the sole local rank)."""

    def owns(
        self,
        *,
        seq: int,
        value: Any,
        rank: int,
        world_size: int,
    ) -> bool:
        return rank == 0
