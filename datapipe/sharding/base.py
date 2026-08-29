"""Base sharding strategy."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class ShardingStrategy(ABC):
    """Decides whether a given record belongs to this rank.

    ``world_size == 1`` must always be owned by rank 0 regardless of strategy.
    """

    @abstractmethod
    def owns(
        self,
        *,
        seq: int,
        value: Any,
        rank: int,
        world_size: int,
    ) -> bool:
        raise NotImplementedError

    def __repr__(self) -> str:  # pragma: no cover - trivial
        return f"{type(self).__name__}()"
