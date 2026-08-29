"""RangeSharding: contiguous range assignment by sequence number."""

from __future__ import annotations

from typing import Any

from datapipe.errors import ShardingError
from datapipe.sharding.base import ShardingStrategy


class RangeSharding(ShardingStrategy):
    """Assigns contiguous ranges of ``seq`` to ranks.

    Ranges are computed by splitting ``[0, total)`` into ``world_size``
    contiguous chunks of (nearly) equal size, where ``total`` is provided by
    the caller (or the source's ``total``).

    Useful for sources with known total cardinality.
    """

    def __init__(self, total: int | None = None) -> None:
        self.total = total

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
        total = self.total
        if total is None:
            raise ShardingError(
                "RangeSharding requires a known total; pass total=... or "
                "use a source that reports total"
            )
        if total <= 0:
            raise ShardingError("RangeSharding requires total > 0")
        if not (0 <= rank < world_size):
            raise ShardingError(f"rank {rank} out of range for world_size {world_size}")

        base = total // world_size
        rem = total % world_size
        # Rank r owns [start, end).
        start = rank * base + min(rank, rem)
        end = start + base + (1 if rank < rem else 0)
        return start <= seq < end
