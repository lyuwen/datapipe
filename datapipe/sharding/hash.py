"""HashSharding: stable hash of a record key determines ownership."""

from __future__ import annotations

import hashlib
from typing import Any, Callable

from datapipe.sharding.base import ShardingStrategy


def stable_hash_bytes(data: bytes) -> int:
    """Stable, process-independent hash of bytes.

    Uses SHA-256, so it is immune to Python's randomized hash seed and is
    stable across processes and restarts.
    """
    return int.from_bytes(hashlib.sha256(data).digest()[:8], "big")


class HashSharding(ShardingStrategy):
    """Assigns records to ranks by ``stable_hash(key(value)) % world_size``.

    Useful when records carry a stable item key and a fixed assignment is
    desired (e.g. all rows for one key land on one rank).
    """

    def __init__(
        self,
        key: Callable[[Any], Any] | None = None,
        *,
        hash_fn: Callable[[Any], int] = stable_hash_bytes,
    ) -> None:
        if key is not None and not callable(key):
            raise TypeError("key must be callable or None")
        self.key = key
        self.hash_fn = hash_fn

    def _digest(self, value: Any) -> int:
        if self.key is None:
            payload = value
        else:
            payload = self.key(value)
        return self.hash_fn(_encode(payload))

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
        return self._digest(value) % world_size == rank


def _encode(payload: Any) -> bytes:
    """Serialize an arbitrary payload into stable bytes for hashing."""
    if isinstance(payload, str):
        return payload.encode("utf-8")
    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, (int, float, bool, type(None))):
        return repr(payload).encode("utf-8")
    # Fall back to a deterministic repr. Not guaranteed stable across object
    # layouts, so users with non-primitive keys should pass an explicit key fn.
    return repr(payload).encode("utf-8")
