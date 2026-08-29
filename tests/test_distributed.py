"""Distributed simulation tests without a cluster (plan §41).

Simulate ranks locally: each rank runs the same pipeline with its own
RuntimeContext; verify the union of outputs equals the full dataset with no
duplicates and no loss.
"""

from __future__ import annotations

import json
import os

from datapipe import (
    GenericStage,
    JsonlSink,
    JsonlSource,
    ListSink,
    Pipeline,
    RuntimeContext,
    SequentialExecutor,
    ThreadExecutor,
)


def _process_row(r):
    return {"id": r["id"], "double": r["id"] * 2}


def _identity(r):
    return r


def test_logical_sharding_simulation(tmp_path):
    """Logical sharding: every rank reads the whole source, keeps its own."""
    n = 1000
    world = 4
    rows = [{"id": i} for i in range(n)]

    all_out = []
    for rank in range(world):
        sink = ListSink()
        Pipeline([GenericStage(process=_process_row, name="t")]).run(
            source=rows,
            sink=sink,
            executor=SequentialExecutor(),
            runtime=RuntimeContext(rank=rank, world_size=world),
            progress=False,
        )
        all_out.extend(sink.items)

    ids = [r["id"] for r in all_out]
    assert len(ids) == n
    assert len(set(ids)) == n  # no duplicates
    assert set(ids) == set(range(n))  # no loss


def test_physical_sharding_jsonl(tmp_path):
    """Physical sharding: ranks read disjoint files (plan §16.3)."""
    d = tmp_path / "dataset"
    d.mkdir()
    for i in range(4):
        with open(str(d / f"part-0000{i}.jsonl"), "w") as f:
            for j in range(50):
                f.write(json.dumps({"id": i * 50 + j}) + "\n")

    world = 2
    all_out = []
    for rank in range(world):
        sink = ListSink()
        Pipeline([GenericStage(process=_process_row, name="t")]).run(
            source=JsonlSource(str(d)),
            sink=sink,
            executor=ThreadExecutor(workers=2),
            runtime=RuntimeContext(rank=rank, world_size=world),
            progress=False,
        )
        all_out.extend(sink.items)

    ids = [r["id"] for r in all_out]
    assert len(ids) == 200
    assert len(set(ids)) == 200
    assert set(ids) == set(range(200))


def test_ranked_output_shards(tmp_path):
    """Distributed output writes one shard per rank (plan §23).

    Four records with ids 0..3; with ModuloSharding each rank owns the record
    whose seq % 4 == rank and writes it to its own shard file.
    """
    out_dir = tmp_path / "out"
    world = 4
    for rank in range(world):
        Pipeline([GenericStage(process=_identity, name="id")]).run(
            source=[{"id": i} for i in range(world)],
            sink=JsonlSink(str(out_dir) + "/"),
            executor=SequentialExecutor(),
            runtime=RuntimeContext(rank=rank, world_size=world),
            progress=False,
        )
    files = sorted(os.listdir(str(out_dir)))
    assert files == [
        "part-00000.jsonl",
        "part-00001.jsonl",
        "part-00002.jsonl",
        "part-00003.jsonl",
    ]
    # Each shard contains exactly its rank's record.
    for f in files:
        rank = int(f.split("part-")[1].split(".")[0])
        got = json.loads(open(str(out_dir / f)).read())
        assert got == {"id": rank}
