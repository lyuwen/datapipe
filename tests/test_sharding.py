"""Sharding strategy tests."""

from __future__ import annotations

import pytest

from datapipe import (
    HashSharding,
    ModuloSharding,
    NoSharding,
    RangeSharding,
)
from datapipe.errors import ShardingError


def test_modulo_ownership():
    s = ModuloSharding()
    for seq in range(100):
        assert s.owns(seq=seq, value=None, rank=seq % 4, world_size=4)
        # Only the owning rank claims it.
        owners = [
            r
            for r in range(4)
            if s.owns(seq=seq, value=None, rank=r, world_size=4)
        ]
        assert owners == [seq % 4]


def test_modulo_world_size_one():
    s = ModuloSharding()
    assert s.owns(seq=0, value=None, rank=0, world_size=1)
    assert s.owns(seq=99, value=None, rank=0, world_size=1)


def test_no_sharding():
    s = NoSharding()
    assert s.owns(seq=0, value=1, rank=0, world_size=1)
    assert not s.owns(seq=0, value=1, rank=1, world_size=4)
    # NoSharding: rank 0 owns everything regardless of world_size.
    assert s.owns(seq=0, value=1, rank=0, world_size=4)


def test_hash_sharding_stable():
    s = HashSharding(key=lambda v: v["id"])
    # Same value maps to same rank regardless of call order / process.
    r1 = s.owns(seq=0, value={"id": "abc"}, rank=1, world_size=8)
    r2 = s.owns(seq=999, value={"id": "abc"}, rank=1, world_size=8)
    assert r1 == r2


def test_hash_sharding_partitions():
    s = HashSharding(key=lambda v: v["id"])
    world = 8
    buckets = [0] * world
    for i in range(2000):
        v = {"id": f"key-{i}"}
        owned = [
            r
            for r in range(world)
            if s.owns(seq=0, value=v, rank=r, world_size=world)
        ]
        assert len(owned) == 1
        buckets[owned[0]] += 1
    # All keys covered exactly once.
    assert sum(buckets) == 2000
    # Roughly balanced.
    assert min(buckets) > 0


def test_range_sharding_contiguous():
    s = RangeSharding(total=1000)
    world = 4
    ranges = []
    for rank in range(world):
        owned = [seq for seq in range(1000) if s.owns(seq=seq, value=None, rank=rank, world_size=world)]
        ranges.append(owned)
    # Contiguous and disjoint.
    for rank, owned in enumerate(ranges):
        if owned:
            assert owned == list(range(owned[0], owned[0] + len(owned)))
    assert sum(len(r) for r in ranges) == 1000


def test_range_sharding_requires_total():
    s = RangeSharding(total=None)
    with pytest.raises(ShardingError):
        s.owns(seq=0, value=None, rank=0, world_size=4)


def test_range_sharding_rank_out_of_range():
    s = RangeSharding(total=100)
    with pytest.raises(ShardingError):
        s.owns(seq=0, value=None, rank=5, world_size=4)
