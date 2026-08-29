"""ModuloSharding: round-robin assignment by sequence number."""

from __future__ import annotations

from typing import Any

from datapipe.sharding.base import ShardingStrategy


class ModuloSharding(ShardingStrategy):
    """Assigns record ``seq`` to rank ``seq % world_size``.

    The generic fallback strategy: universally applicable, but every rank
    must read the whole source (logical sharding).
    """

    def owns(
        self,
        *,
        seq: int,
        value: Any,
        rank: int,
        world_size: int,
    ) -> bool:
        if world_size == 1:
            return True
        return seq % world_size == rank
