"""Parquet source/sink tests (require pyarrow)."""

from __future__ import annotations

import os

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from datapipe import (  # noqa: E402
    GenericStage,
    IterableSource,
    ListSink,
    ParquetSink,
    ParquetSource,
    Pipeline,
    RuntimeContext,
    SequentialExecutor,
)


def _write_input(path, n=100):
    table = pa.table({"id": list(range(n)), "text": [f"row{i}" for i in range(n)]})
    pq.write_table(table, path)
    return table


def test_parquet_roundtrip(tmp_path):
    inp = str(tmp_path / "in.parquet")
    _write_input(inp)
    out_dir = str(tmp_path / "out")
    Pipeline([GenericStage(process=lambda r: {"id": r["id"], "len": len(r["text"])}, name="t")]).run(
        source=ParquetSource(inp),
        sink=ParquetSink(out_dir + "/"),
        executor=SequentialExecutor(),
        progress=False,
    )
    out_file = str(tmp_path / "out" / "part-00000.parquet")
    t = pq.read_table(out_file)
    assert t.num_rows == 100
    assert t.column_names == ["id", "len"]


def test_parquet_columns_selection(tmp_path):
    inp = str(tmp_path / "in.parquet")
    _write_input(inp)
    rows = list(ParquetSource(inp, columns=["id"]))
    assert rows == [{"id": i} for i in range(100)]


def test_parquet_directory_dataset(tmp_path):
    d = tmp_path / "dataset"
    d.mkdir()
    for i in range(3):
        _write_input(str(d / f"part-0000{i}.parquet"), n=20)
    sink = ListSink()
    Pipeline([GenericStage(process=lambda r: r, name="id")]).run(
        source=ParquetSource(str(d)),
        sink=sink,
        executor=SequentialExecutor(),
        progress=False,
    )
    assert len(sink.items) == 60


def test_parquet_physical_sharding_files(tmp_path):
    d = tmp_path / "dataset"
    d.mkdir()
    for i in range(4):
        # Distinct id ranges per file so we can detect overlap/loss.
        table = pa.table({"id": list(range(i * 10, i * 10 + 10))})
        pq.write_table(table, str(d / f"part-0000{i}.parquet"))
    world = 2
    all_ids = []
    for rank in range(world):
        sink = ListSink()
        Pipeline([GenericStage(process=lambda r: r["id"], name="id")]).run(
            source=ParquetSource(str(d)),
            sink=sink,
            executor=SequentialExecutor(),
            runtime=RuntimeContext(rank=rank, world_size=world),
            progress=False,
        )
        all_ids.extend(sink.items)
    assert len(all_ids) == 40
    assert len(set(all_ids)) == 40  # no overlap, no loss


def test_parquet_row_group_sharding(tmp_path):
    inp = str(tmp_path / "in.parquet")
    table = pa.table({"id": list(range(100))})
    pq.write_table(table, inp, row_group_size=10)  # 10 row groups
    world = 3
    all_ids = []
    for rank in range(world):
        sink = ListSink()
        Pipeline([GenericStage(process=lambda r: r["id"], name="id")]).run(
            source=ParquetSource(inp),
            sink=sink,
            executor=SequentialExecutor(),
            runtime=RuntimeContext(rank=rank, world_size=world),
            progress=False,
        )
        all_ids.extend(sink.items)
    assert sorted(all_ids) == list(range(100))


def test_parquet_explicit_schema(tmp_path):
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    out = str(tmp_path / "out") + "/"
    Pipeline([GenericStage(process=lambda r: r, name="id")]).run(
        source=IterableSource([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]),
        sink=ParquetSink(out, schema=schema),
        executor=SequentialExecutor(),
        progress=False,
    )
    t = pq.read_table(str(tmp_path / "out" / "part-00000.parquet"))
    assert t.schema.equals(schema)


def test_parquet_batched_write_small_batch(tmp_path):
    """Even with a small batch_size, all rows land in the output."""
    out = str(tmp_path / "out") + "/"
    Pipeline([GenericStage(process=lambda r: r, name="id")]).run(
        source=IterableSource([{"v": i} for i in range(50)]),
        sink=ParquetSink(out, batch_size=8),
        executor=SequentialExecutor(),
        progress=False,
    )
    t = pq.read_table(str(tmp_path / "out" / "part-00000.parquet"))
    assert t.num_rows == 50


# ---------------------------------------------------------------------------
# A4. Parquet filter may reference a column not in the column projection
# ---------------------------------------------------------------------------


def test_parquet_filter_can_reference_unprojected_column(tmp_path):
    """columns=["name"] with filters=field("id") >= 3 must not raise ArrowInvalid
    and must return only the projected column for matching rows."""
    import pyarrow.dataset as ds

    inp = str(tmp_path / "in.parquet")
    pq.write_table(
        pa.table({"id": list(range(10)), "name": [f"row{i}" for i in range(10)]}),
        inp,
    )

    # Expression filter
    rows = list(
        __import__("datapipe").ParquetSource(
            inp, columns=["name"], filters=ds.field("id") >= 3
        )
    )
    assert all(set(r.keys()) == {"name"} for r in rows), "only 'name' column expected"
    assert [r["name"] for r in rows] == [f"row{i}" for i in range(3, 10)]

    # Legacy tuple filter
    rows2 = list(
        __import__("datapipe").ParquetSource(
            inp, columns=["name"], filters=[("id", ">=", 3)]
        )
    )
    assert [r["name"] for r in rows2] == [f"row{i}" for i in range(3, 10)]
