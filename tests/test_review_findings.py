"""Regression tests for the review findings (review.md).

Each test maps to one numbered finding in the review.
"""

from __future__ import annotations

import json
import os
import threading
import time

import pytest

from datapipe import (
    GenericStage,
    IterableSource,
    JsonlSink,
    JsonlSource,
    ListSink,
    Pipeline,
    ProcessExecutor,
    RangeSharding,
    RuntimeContext,
    SequentialExecutor,
    ThreadExecutor,
    TransformStage,
)
from datapipe.io.base import Source, SourceRecordError
from datapipe.stage import Stage

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
ds = pytest.importorskip("pyarrow.dataset")


# ---------------------------------------------------------------------------
# Finding 1: sink finalization failures must not be swallowed
# ---------------------------------------------------------------------------


def test_sink_close_failure_propagates(tmp_path):
    """ParquetSink.close() performs the final buffered write; a schema
    mismatch there must surface as a raised error, not a silent success."""

    class FailingSink(ListSink):
        def close(self):
            raise OSError("disk full on close")

    with pytest.raises(OSError, match="disk full on close"):
        Pipeline([GenericStage(process=lambda r: r, name="id")]).run(
            source=IterableSource([1, 2, 3]),
            sink=FailingSink(),
            executor=SequentialExecutor(),
            progress=False,
        )


def test_source_close_failure_propagates(tmp_path):
    class FailingSource(IterableSource):
        def close(self):
            raise OSError("source close boom")

    with pytest.raises(OSError, match="source close boom"):
        Pipeline([GenericStage(process=lambda r: r, name="id")]).run(
            source=FailingSource([1, 2, 3]),
            sink=ListSink(),
            executor=SequentialExecutor(),
            progress=False,
        )


def test_finalize_error_does_not_mask_run_error(tmp_path):
    """If both the run and the finalization fail, the run error wins."""

    class FailingSink(ListSink):
        def close(self):
            raise OSError("sink close boom")

    def boom(x):
        raise ValueError("run boom")

    from datapipe.errors import StageExecutionError

    with pytest.raises(StageExecutionError) as ei:
        Pipeline([GenericStage(process=boom, name="b")]).run(
            source=IterableSource([1]),
            sink=FailingSink(),
            executor=SequentialExecutor(),
            progress=False,
        )
    assert isinstance(ei.value.cause, ValueError)
    assert str(ei.value.cause) == "run boom"


# ---------------------------------------------------------------------------
# Finding 2: ordered errors must not grow the reorder buffer unboundedly
# ---------------------------------------------------------------------------


def _boom(x):
    if x == 0:
        raise ValueError("first record fails")
    return x


def test_ordered_skip_memory_bounded():
    """A failure at seq 0 must not buffer every later result."""
    sink = ListSink()
    stats = Pipeline([TransformStage(_boom, name="boom")]).run(
        source=IterableSource(range(1000)),
        sink=sink,
        executor=SequentialExecutor(),
        ordered=True,
        errors="skip",
        progress=False,
    )
    # All records after the failure are processed and emitted.
    assert len(sink.items) == 999
    # The reorder buffer never grows large: only results that are out of
    # order are buffered. Sequential executor emits in order, so with the fix
    # the buffer high-water stays at 1.
    assert stats.max_reorder_buffer_observed <= 1


def test_ordered_skip_memory_bounded_threaded():
    """Same bounded-memory guarantee under a concurrent executor."""
    sink = ListSink()
    stats = Pipeline([TransformStage(_boom, name="boom")]).run(
        source=IterableSource(range(2000)),
        sink=sink,
        executor=ThreadExecutor(workers=4, max_in_flight=16),
        ordered=True,
        errors="skip",
        progress=False,
    )
    assert len(sink.items) == 1999
    # A single early failure must not pin the whole reorder buffer.
    assert stats.max_reorder_buffer_observed <= 32


# ---------------------------------------------------------------------------
# Finding 3: aborted ordered runs keep only the contiguous prefix
# ---------------------------------------------------------------------------


class _SlowFirstThenFail(Stage):
    """Delays record 0 so later records complete out of order; fails on 2."""

    name = "slow_fail"

    def process(self, value, ctx):
        if value == 0:
            time.sleep(0.1)
        if value == 2:
            raise ValueError("record 2 fails")
        return value


def test_aborted_ordered_prefix_only(tmp_path):
    """On errors='raise', an interrupted ordered sink must contain only a
    contiguous completed prefix — never buffered results that follow the gap
    (the review reproduced [1, 3, 4, 5, 6, 7] here)."""
    from datapipe.errors import StageExecutionError

    out = tmp_path / "out.jsonl"
    with pytest.raises(StageExecutionError) as ei:
        Pipeline([_SlowFirstThenFail()]).run(
            source=IterableSource(range(8)),
            sink=JsonlSink(str(out)),
            executor=ThreadExecutor(workers=4, max_in_flight=16),
            ordered=True,
            errors="raise",
            progress=False,
        )
    assert str(ei.value.cause) == "record 2 fails"
    lines = (
        [json.loads(l) for l in open(out).read().splitlines()]
        if out.exists()
        else []
    )
    # The output must be a contiguous prefix of the input (0, 1, ...) and
    # must never contain a record at or beyond the failing seq (2).
    assert all(v < 2 for v in lines)
    assert lines == list(range(len(lines)))


def test_aborted_ordered_prefix_sequential(tmp_path):
    """Sequential abort: records 0,1 complete before 2 fails, so the sink
    holds exactly the [0, 1] prefix."""
    from datapipe.errors import StageExecutionError

    out = tmp_path / "out.jsonl"
    with pytest.raises(StageExecutionError):
        Pipeline([_SlowFirstThenFail()]).run(
            source=IterableSource(range(8)),
            sink=JsonlSink(str(out)),
            executor=SequentialExecutor(),
            ordered=True,
            errors="raise",
            progress=False,
        )
    lines = (
        [json.loads(l) for l in open(out).read().splitlines()]
        if out.exists()
        else []
    )
    assert lines == [0, 1]


# ---------------------------------------------------------------------------
# Finding 4: ThreadExecutor per-thread lifecycle and context safety
# ---------------------------------------------------------------------------


class _ThreadCtxProbe(Stage):
    """Records (thread_id, ctx.worker_id, ctx.record_index) per record."""

    name = "probe"

    def __init__(self):
        self.records = []
        self.setup_threads = set()
        self._lock = threading.Lock()

    def setup(self, ctx):
        with self._lock:
            self.setup_threads.add(threading.get_ident())

    def process(self, value, ctx):
        with self._lock:
            self.records.append((threading.get_ident(), ctx.worker_id, ctx.record_index))
        time.sleep(0.0005)  # encourage interleaving
        return ctx.record_index


def test_thread_executor_per_thread_context():
    """Each record must observe a record_index equal to its own seq, and
    setup must run once per worker thread (no cross-thread ctx race)."""
    stage = _ThreadCtxProbe()
    n = 300
    sink = ListSink()
    Pipeline([stage]).run(
        source=IterableSource(range(n)),
        sink=sink,
        executor=ThreadExecutor(workers=4, max_in_flight=32),
        ordered=False,
        progress=False,
    )
    # The stage returns ctx.record_index, so the sink collects one value per
    # record. Since seq == value, a clean run yields exactly range(n) — any
    # cross-thread contamination (shared mutable ctx) would corrupt it.
    assert sorted(sink.items) == list(range(n))
    # Per-thread contexts: worker_id is unique per thread and setup ran on
    # multiple threads (not just once for the whole pool).
    observed = stage.records
    assert len(observed) == n
    worker_ids = {r[1] for r in observed}
    assert len(worker_ids) >= 2
    assert len(stage.setup_threads) >= 2


# ---------------------------------------------------------------------------
# Finding 5: file-backed error_sink is opened and closed
# ---------------------------------------------------------------------------


def test_file_error_sink_usable(tmp_path):
    """errors='return' with a JsonlSink error_sink must work (opened/closed)."""
    err_path = tmp_path / "errors.jsonl"
    out_path = tmp_path / "out.jsonl"

    def boom(x):
        if x == 3:
            raise ValueError("bad row")
        return x

    stats = Pipeline([GenericStage(process=boom, name="b")]).run(
        source=IterableSource(range(5)),
        sink=JsonlSink(str(out_path)),
        executor=SequentialExecutor(),
        errors="return",
        error_sink=JsonlSink(str(err_path)),
        progress=False,
    )
    err_lines = [json.loads(l) for l in open(err_path).read().splitlines()]
    assert len(err_lines) == 1
    assert err_lines[0]["seq"] == 3
    assert err_lines[0]["error_type"] == "ValueError"
    out_lines = [json.loads(l) for l in open(out_path).read().splitlines()]
    assert out_lines == [0, 1, 2, 4]


# ---------------------------------------------------------------------------
# Finding 6: ParquetSource.filters filters rows
# ---------------------------------------------------------------------------


def test_parquet_source_filters(tmp_path):
    from datapipe import ParquetSource

    p = tmp_path / "in.parquet"
    pq.write_table(pa.table({"id": list(range(10))}), str(p))
    rows = list(ParquetSource(str(p), filters=ds.field("id") >= 8))
    assert [r["id"] for r in rows] == [8, 9]


def test_parquet_source_legacy_filters(tmp_path):
    from datapipe import ParquetSource

    p = tmp_path / "in.parquet"
    pq.write_table(pa.table({"id": list(range(10))}), str(p))
    rows = list(ParquetSource(str(p), filters=[("id", ">=", 8)]))
    assert [r["id"] for r in rows] == [8, 9]


# ---------------------------------------------------------------------------
# Finding 7: distributed Parquet output never overwritten across ranks
# ---------------------------------------------------------------------------


def test_parquet_plain_file_ranked(tmp_path):
    from datapipe import ParquetSink

    out = str(tmp_path / "shared.parquet")
    for rank in (0, 1):
        Pipeline([GenericStage(process=lambda r: r, name="id")]).run(
            source=IterableSource([{"id": 0}, {"id": 1}]),
            sink=ParquetSink(out),
            executor=SequentialExecutor(),
            runtime=RuntimeContext(rank=rank, world_size=2),
            progress=False,
        )
    assert os.path.exists(str(tmp_path / "shared.part-00000.parquet"))
    assert os.path.exists(str(tmp_path / "shared.part-00001.parquet"))
    r0 = pq.read_table(str(tmp_path / "shared.part-00000.parquet")).to_pydict()["id"]
    r1 = pq.read_table(str(tmp_path / "shared.part-00001.parquet")).to_pydict()["id"]
    assert r0 == [0]
    assert r1 == [1]


# ---------------------------------------------------------------------------
# Finding 8: explicit Parquet schema applied to batches; no bogus flush()
# ---------------------------------------------------------------------------


def test_parquet_explicit_schema_python_ints(tmp_path):
    from datapipe import ParquetSink

    schema = pa.schema([("id", pa.int32())])
    out = str(tmp_path / "out") + "/"
    Pipeline([GenericStage(process=lambda r: r, name="id")]).run(
        source=IterableSource([{"id": 1}, {"id": 2}]),
        sink=ParquetSink(out, schema=schema),
        executor=SequentialExecutor(),
        progress=False,
    )
    t = pq.read_table(str(tmp_path / "out" / "part-00000.parquet"))
    assert t.schema.equals(schema)


def test_parquet_sink_has_no_broken_flush(tmp_path):
    """ParquetWriter has no flush(); ParquetSink.flush() must drain the
    buffer without raising AttributeError."""
    from datapipe import ParquetSink

    sink = ParquetSink(str(tmp_path / "out") + "/", batch_size=2)
    sink.open(RuntimeContext())
    sink.write({"a": 1})
    sink.flush()  # must not raise
    sink.write({"a": 2})
    sink.close()
    t = pq.read_table(str(tmp_path / "out" / "part-00000.parquet"))
    assert t.num_rows == 2


# ---------------------------------------------------------------------------
# Finding 9: source decoding failures route through error policies
# ---------------------------------------------------------------------------


def test_source_record_error_marker_flows_to_policy():
    """A source yielding SourceRecordError is reported per-policy and the
    source stays resumable."""

    class FlakySource(Source):
        def __init__(self, items):
            self.items = items

        def __iter__(self):
            for it in self.items:
                if it is ...:
                    yield SourceRecordError(ValueError("decode fail"))
                else:
                    yield it

    sink = ListSink()
    stats = Pipeline([GenericStage(process=lambda r: r, name="id")]).run(
        source=FlakySource([0, ..., 1, 2]),
        sink=sink,
        executor=SequentialExecutor(),
        errors="skip",
        progress=False,
    )
    assert sink.items == [0, 1, 2]
    assert stats.failed_records == 1


def test_jsonl_raw_false_decode_error_skip(tmp_path):
    """JsonlSource(raw=False) yields SourceRecordError on a malformed line so
    errors='skip' skips it and keeps going (finding 9)."""
    p = tmp_path / "bad.jsonl"
    with open(p, "w") as f:
        f.write('{"ok": 1}\nNOT JSON\n{"ok": 3}\n')
    sink = ListSink()
    stats = Pipeline([GenericStage(process=lambda r: r, name="id")]).run(
        source=JsonlSource(str(p)),  # raw=False default
        sink=sink,
        executor=SequentialExecutor(),
        errors="skip",
        progress=False,
    )
    assert sink.items == [{"ok": 1}, {"ok": 3}]
    assert stats.failed_records == 1


def test_source_record_error_raised_marker():
    """A source may *raise* SourceRecordError; the runner normalizes it to a
    per-record error and the policy still applies."""
    from datapipe.io.base import Source

    class RaisingSource(Source):
        def __init__(self, n, fail_at):
            self.n = n
            self.fail_at = fail_at

        def __iter__(self):
            for i in range(self.n):
                if i == self.fail_at:
                    raise SourceRecordError(ValueError("boom"), line=i)
                yield i

    sink = ListSink()
    stats = Pipeline([GenericStage(process=lambda r: r, name="id")]).run(
        source=RaisingSource(5, 2),
        sink=sink,
        executor=SequentialExecutor(),
        errors="skip",
        progress=False,
    )
    assert sink.items == [0, 1]
    assert stats.failed_records == 1

    with pytest.raises(ValueError, match="boom"):
        Pipeline([GenericStage(process=lambda r: r, name="id")]).run(
            source=RaisingSource(5, 2),
            sink=ListSink(),
            executor=SequentialExecutor(),
            errors="raise",
            progress=False,
        )


def test_source_record_error_raise_policy():
    class FlakySource(Source):
        def __iter__(self):
            yield 0
            yield SourceRecordError(ValueError("decode fail"))
            yield 1

    with pytest.raises(ValueError, match="decode fail"):
        Pipeline([GenericStage(process=lambda r: r, name="id")]).run(
            source=FlakySource(),
            sink=ListSink(),
            executor=SequentialExecutor(),
            errors="raise",
            progress=False,
        )


# ---------------------------------------------------------------------------
# Finding 10: RangeSharding obtains totals from sources
# ---------------------------------------------------------------------------


def test_range_sharding_source_total():
    """RangeSharding() with no explicit total uses the source's reported
    total."""
    world = 4
    per_rank = []
    for rank in range(world):
        sink = ListSink()
        Pipeline([GenericStage(process=lambda r: r, name="id")]).run(
            source=IterableSource(range(10)),
            sink=sink,
            executor=SequentialExecutor(),
            runtime=RuntimeContext(rank=rank, world_size=world),
            sharding=RangeSharding(),
            progress=False,
        )
        per_rank.append(set(sink.items))
    union = set().union(*per_rank)
    assert union == set(range(10))
    for i in range(world):
        for j in range(i + 1, world):
            assert not (per_rank[i] & per_rank[j])
