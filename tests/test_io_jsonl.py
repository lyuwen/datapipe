"""JSONL source/sink tests."""

from __future__ import annotations

import gzip
import json
import os

import pytest

from datapipe import (
    GenericStage,
    IterableSource,
    JsonDumpStage,
    JsonlSink,
    JsonlSource,
    JsonLoadStage,
    ListSink,
    ModuloSharding,
    Pipeline,
    RuntimeContext,
    SequentialExecutor,
)
from datapipe.errors import SourceError, StageExecutionError


def _write(path, lines):
    with open(path, "w", encoding="utf-8") as f:
        f.writelines(l + "\n" for l in lines)


def test_jsonl_parsed_mode(tmp_path):
    p = tmp_path / "in.jsonl"
    _write(str(p), ['{"x": 1}', '{"x": 2}', '{"x": 3}'])
    out = tmp_path / "out.jsonl"
    Pipeline([GenericStage(process=lambda r: {"x": r["x"] * 10}, name="m")]).run(
        source=JsonlSource(str(p)),
        sink=JsonlSink(str(out)),
        executor=SequentialExecutor(),
        progress=False,
    )
    got = [json.loads(l) for l in open(out).read().splitlines()]
    assert got == [{"x": 10}, {"x": 20}, {"x": 30}]


def test_jsonl_raw_mode_with_stages(tmp_path):
    p = tmp_path / "in.jsonl"
    lines = ['{"a": 1}', '{"a": 2}']
    _write(str(p), lines)
    out = tmp_path / "out.jsonl"
    Pipeline([JsonLoadStage(), JsonDumpStage()]).run(
        source=JsonlSource(str(p), raw=True),
        sink=JsonlSink(str(out), raw=True),
        executor=SequentialExecutor(),
        progress=False,
    )
    assert open(out).read().splitlines() == lines


def test_jsonl_gzip_roundtrip(tmp_path):
    p = tmp_path / "in.jsonl.gz"
    with gzip.open(str(p), "wt", encoding="utf-8") as f:
        f.write('{"x": 1}\n{"x": 2}\n')
    out = tmp_path / "out.jsonl.gz"
    Pipeline([GenericStage(process=lambda r: r, name="id")]).run(
        source=JsonlSource(str(p)),
        sink=JsonlSink(str(out)),
        executor=SequentialExecutor(),
        progress=False,
    )
    with gzip.open(str(out), "rt", encoding="utf-8") as f:
        assert [json.loads(l) for l in f.read().splitlines()] == [
            {"x": 1},
            {"x": 2},
        ]


def test_jsonl_zstd_roundtrip(tmp_path):
    zstandard = pytest.importorskip("zstandard")
    p = tmp_path / "in.jsonl.zst"
    cctx = zstandard.ZstdCompressor()
    with open(str(p), "wb") as f:
        with cctx.stream_writer(f) as w:
            w.write(b'{"x": 9}\n')
    out = tmp_path / "out.jsonl.zst"
    Pipeline([GenericStage(process=lambda r: r, name="id")]).run(
        source=JsonlSource(str(p)),
        sink=JsonlSink(str(out)),
        executor=SequentialExecutor(),
        progress=False,
    )
    sink = ListSink()
    Pipeline([GenericStage(process=lambda r: r, name="id")]).run(
        source=JsonlSource(str(out)),
        sink=sink,
        executor=SequentialExecutor(),
        progress=False,
    )
    assert sink.items == [{"x": 9}]


def test_jsonl_empty_file(tmp_path):
    p = tmp_path / "empty.jsonl"
    p.write_text("")
    sink = ListSink()
    Pipeline([GenericStage(process=lambda r: r, name="id")]).run(
        source=JsonlSource(str(p)),
        sink=sink,
        executor=SequentialExecutor(),
        progress=False,
    )
    assert sink.items == []


def test_jsonl_malformed_raises(tmp_path):
    """errors='raise' surfaces the decode error (from the worker stage)."""
    p = tmp_path / "bad.jsonl"
    _write(str(p), ['{"ok": 1}', "NOT JSON", '{"ok": 3}'])
    with pytest.raises(StageExecutionError) as ei:
        Pipeline([JsonLoadStage()]).run(
            source=JsonlSource(str(p), raw=True),
            sink=ListSink(),
            executor=SequentialExecutor(),
            errors="raise",
            progress=False,
        )
    assert isinstance(ei.value.cause, json.JSONDecodeError)


def test_jsonl_malformed_skip_continues(tmp_path):
    """errors='skip' skips a malformed line and processes the rest."""
    p = tmp_path / "bad.jsonl"
    _write(str(p), ['{"ok": 1}', "NOT JSON", '{"ok": 3}'])
    sink = ListSink()
    stats = Pipeline([JsonLoadStage()]).run(
        source=JsonlSource(str(p), raw=True),
        sink=sink,
        executor=SequentialExecutor(),
        errors="skip",
        progress=False,
    )
    assert sink.items == [{"ok": 1}, {"ok": 3}]
    assert stats.failed_records == 1
    assert stats.output_records == 2


def test_jsonl_malformed_return(tmp_path):
    """errors='return' records the decode error in the error_sink."""
    p = tmp_path / "bad.jsonl"
    _write(str(p), ['{"ok": 1}', "NOT JSON", '{"ok": 3}'])
    esink = ListSink()
    sink = ListSink()
    stats = Pipeline([JsonLoadStage()]).run(
        source=JsonlSource(str(p), raw=True),
        sink=sink,
        executor=SequentialExecutor(),
        errors="return",
        error_sink=esink,
        progress=False,
    )
    assert sink.items == [{"ok": 1}, {"ok": 3}]
    assert len(esink.items) == 1
    assert esink.items[0]["seq"] == 1
    assert esink.items[0]["error_type"] == "JSONDecodeError"


def test_jsonl_unicode(tmp_path):
    p = tmp_path / "u.jsonl"
    _write(str(p), ['{"text": "héllo wörld ☃"}'])
    out = tmp_path / "out.jsonl"
    Pipeline([GenericStage(process=lambda r: r, name="id")]).run(
        source=JsonlSource(str(p)),
        sink=JsonlSink(str(out)),
        executor=SequentialExecutor(),
        progress=False,
    )
    assert json.loads(open(out).read())["text"] == "héllo wörld ☃"


def test_jsonl_directory_dataset(tmp_path):
    d = tmp_path / "dataset"
    d.mkdir()
    for i in range(3):
        _write(str(d / f"part-0000{i}.jsonl"), [f'{{"i": {i}, "j": {j}}}' for j in range(10)])
    sink = ListSink()
    Pipeline([GenericStage(process=lambda r: r, name="id")]).run(
        source=JsonlSource(str(d)),
        sink=sink,
        executor=SequentialExecutor(),
        progress=False,
    )
    assert len(sink.items) == 30


def test_jsonl_directory_physical_sharding(tmp_path):
    """Multiple ranks read disjoint files (physical sharding)."""
    d = tmp_path / "dataset"
    d.mkdir()
    for i in range(4):
        _write(str(d / f"part-0000{i}.jsonl"), [json.dumps({"shard": i, "j": j}) for j in range(10)])
    world = 2
    all_rows = []
    for rank in range(world):
        sink = ListSink()
        Pipeline([GenericStage(process=lambda r: r, name="id")]).run(
            source=JsonlSource(str(d)),
            sink=sink,
            executor=SequentialExecutor(),
            runtime=RuntimeContext(rank=rank, world_size=world),
            sharding=None,
            progress=False,
        )
        all_rows.extend(sink.items)
    # Union covers all 40 rows exactly once (physical file assignment).
    keys = [(r["shard"], r["j"]) for r in all_rows]
    assert len(keys) == 40
    assert len(set(keys)) == 40


def test_jsonl_sink_ranked_directory(tmp_path):
    """Sink writes part-NNNNN.jsonl per rank into a directory."""
    d = tmp_path / "outdir"
    Pipeline([GenericStage(process=lambda r: r, name="id")]).run(
        source=IterableSource([1, 2, 3]),
        sink=JsonlSink(str(d) + "/"),
        executor=SequentialExecutor(),
        runtime=RuntimeContext(rank=2, world_size=4),
        sharding=ModuloSharding(),
        progress=False,
    )
    files = sorted(os.listdir(str(d)))
    assert files == ["part-00002.jsonl"]
    got = [json.loads(l) for l in open(str(d / "part-00002.jsonl")).read().splitlines()]
    # ModuloSharding: seq%4==2 -> values 2 (seq 2 only; seqs 0,1,3 go to
    # ranks 0,1,3). Our source is [1,2,3] with seqs 0,1,2, so only value 3.
    assert got == [3]


def test_jsonl_source_missing_file_raises(tmp_path):
    """A nonexistent single-file path surfaces as an IO error at iteration."""
    from datapipe.io.utils import open_reader

    with pytest.raises(FileNotFoundError):
        open_reader(str(tmp_path / "nope.jsonl"), None)


def test_jsonl_source_empty_directory_raises(tmp_path):
    d = tmp_path / "emptydir"
    d.mkdir()
    with pytest.raises(SourceError):
        JsonlSource(str(d))._resolve_files()
