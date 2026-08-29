"""Base Source/Sink abstractions.

A ``Source`` yields record *values*. The runtime assigns each yielded value a
monotonic ``seq`` and wraps it in a ``Record``.

``iter_shard``/``total``/``open``/``close`` support the distributed and
progress-total designs without forcing every adapter to implement them.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Iterable, Iterator

from datapipe.runtime.context import RuntimeContext


class Source(ABC):
    """Abstract record source."""

    #: Whether this source can read only its rank's portion physically.
    supports_physical_sharding: bool = False

    @abstractmethod
    def __iter__(self) -> Iterator[Any]:
        raise NotImplementedError

    def iter_shard(
        self,
        rank: int,
        world_size: int,
    ) -> Iterator[Any] | None:
        """Yield only records owned by ``rank``.

        Return ``None`` to signal the runtime should apply logical sharding.
        """
        return None

    def iter_for_runtime(
        self,
        runtime: RuntimeContext,
        sharding,
    ) -> Iterator[Any]:
        """Yield the records this rank should process.

        Uses physical sharding when supported, else applies the logical
        sharding strategy.
        """
        if self.supports_physical_sharding:
            shard = self.iter_shard(runtime.rank, runtime.world_size)
            if shard is not None:
                return shard

        return _logically_shard(
            self.__iter__(),
            sharding=sharding,
            rank=runtime.rank,
            world_size=runtime.world_size,
        )

    @property
    def total(self) -> int | None:
        """Known total record count, if cheaply discoverable (else ``None``)."""
        return None

    def open(self, runtime: RuntimeContext) -> None:
        """Optional per-run initialization."""

    def close(self) -> None:
        """Optional per-run cleanup."""

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def _logically_shard(
    records: Iterable[Any],
    *,
    sharding,
    rank: int,
    world_size: int,
) -> Iterator[Any]:
    """Filter an iterable to the records owned by ``rank``."""
    if world_size <= 1:
        yield from records
        return
    for seq, value in enumerate(records):
        if sharding.owns(
            seq=seq,
            value=value,
            rank=rank,
            world_size=world_size,
        ):
            yield value


class Sink(ABC):
    """Abstract record sink."""

    def open(self, runtime: RuntimeContext) -> None:
        """Optional per-run initialization (e.g. rank-aware paths)."""

    @abstractmethod
    def write(self, record: Any) -> None:
        raise NotImplementedError

    def close(self) -> None:
        """Flush and release resources."""


# Re-export for consumers who import from datapipe.io.base directly.
__all__ = ["Source", "Sink"]
