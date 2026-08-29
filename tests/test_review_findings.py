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
    """Records (thread_id, ctx.worker_id, ctx.record_index) per record.

    ``records``, ``setup_threads``, and ``_lock`` are shared across all
    per-thread deep copies so the coordinator can inspect collected data after
    the run.  Per-thread mutable state (if any) would live in separate attrs.
    """

    name = "probe"

    def __init__(self):
        self.records = []
        self.setup_threads = set()
        self._lock = threading.Lock()

    def __deepcopy__(self, memo):
        """Share collection state across per-thread copies."""
        import copy as _copy
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        # Share the collection infrastructure so all thread copies write to
        # the same list/set and the coordinator can read them after the run.
        result.records = self.records
        result.setup_threads = self.setup_threads
        result._lock = self._lock
        result.name = self.name
        result._name_explicit = self._name_explicit
        return result

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


# ---------------------------------------------------------------------------
# review2.md finding 1: thread teardown runs on abort too
# ---------------------------------------------------------------------------


class _TeardownTracker(Stage):
    """Counts teardown calls across all per-thread copies via shared state."""

    name = "teardown_tracker"

    def __init__(self):
        self._lock = threading.Lock()
        self.teardown_count = 0
        self._shared: "_TeardownTracker | None" = None

    def __deepcopy__(self, memo):
        # Share collection state intentionally.
        result = self.__class__.__new__(self.__class__)
        memo[id(self)] = result
        result._lock = self._lock
        result.teardown_count = 0
        result.name = self.name
        result._name_explicit = self._name_explicit
        result._shared = self
        return result

    def process(self, value, ctx):
        return value

    def teardown(self, ctx):
        with self._lock:
            self.teardown_count += 1
        # If this is a copy, also increment the original's counter.
        shared = self._shared
        if shared is not None:
            with shared._lock:
                shared.teardown_count += 1


class _FailOnRecord(Stage):
    """Raises on the first record it sees."""

    name = "fail_first"

    def process(self, value, ctx):
        raise ValueError("intentional abort")


def test_thread_teardown_on_abort():
    """Workers that completed setup must receive teardown even when the run
    aborts due to a stage error."""
    tracker = _TeardownTracker()
    with pytest.raises(Exception):
        Pipeline([tracker, _FailOnRecord()]).run(
            source=IterableSource(range(50)),
            sink=ListSink(),
            executor=ThreadExecutor(workers=4, max_in_flight=16),
            errors="raise",
            progress=False,
        )
    # At least one thread ran setup and must have received teardown.
    assert tracker.teardown_count > 0


# ---------------------------------------------------------------------------
# review2.md finding 2: locks nested inside containers deep-copy without error
# ---------------------------------------------------------------------------


class _NestedLockStage(Stage):
    """Stage holding a lock inside a dict — exercises recursive lock handling."""

    name = "nested_lock"

    def __init__(self):
        self.state = {"lock": threading.Lock(), "value": 0}

    def process(self, value, ctx):
        with self.state["lock"]:
            self.state["value"] += 1
        return value


def test_stage_deepcopy_nested_lock():
    """deepcopy of a stage with a lock inside a dict must not raise TypeError
    and the resulting copy must have an independent lock."""
    import copy
    stage = _NestedLockStage()
    copy2 = copy.deepcopy(stage)
    # Independent dict objects.
    assert copy2.state is not stage.state
    # Independent lock objects.
    assert copy2.state["lock"] is not stage.state["lock"]
    # Value is independent.
    stage.state["value"] = 99
    assert copy2.state["value"] == 0


def test_stage_deepcopy_deeply_nested_lock():
    """Locks nested more than one level deep must also deepcopy without error."""
    import copy

    class _DeepLockStage(Stage):
        name = "deep_lock"
        def __init__(self):
            self.state = {"resources": [{"lock": threading.Lock(), "v": 0}]}
        def process(self, value, ctx):
            return value

    stage = _DeepLockStage()
    copy2 = copy.deepcopy(stage)
    assert copy2.state is not stage.state
    assert copy2.state["resources"] is not stage.state["resources"]
    assert copy2.state["resources"][0]["lock"] is not stage.state["resources"][0]["lock"]
    # Value is independent.
    stage.state["resources"][0]["v"] = 42
    assert copy2.state["resources"][0]["v"] == 0


def test_stage_deepcopy_lock_as_dict_key():
    """A lock used as a dictionary key must deepcopy without TypeError."""
    import copy

    class _LockKeyStage(Stage):
        name = "lock_key"
        def __init__(self):
            lock = threading.Lock()
            self.state = {lock: "guard"}
        def process(self, value, ctx):
            return value

    stage = _LockKeyStage()
    copy2 = copy.deepcopy(stage)
    assert copy2.state is not stage.state
    # The copied dict has one key which is a fresh lock.
    orig_key = next(iter(stage.state))
    copy_key = next(iter(copy2.state))
    assert orig_key is not copy_key
    assert copy2.state[copy_key] == "guard"


def test_thread_executor_nested_lock_stage():
    """A stage with a lock nested inside a dict must run without error under
    ThreadExecutor (exercises the full deepcopy-on-clone path)."""
    sink = ListSink()
    Pipeline([_NestedLockStage()]).run(
        source=IterableSource(range(50)),
        sink=sink,
        executor=ThreadExecutor(workers=4, max_in_flight=16),
        progress=False,
    )
    assert len(sink.items) == 50


# ---------------------------------------------------------------------------
# review2.md finding 1: setup failure aborts regardless of error policy
# ---------------------------------------------------------------------------


class _FailingSetup(Stage):
    """Raises in setup() so the worker can never initialize."""

    name = "failing_setup"

    def setup(self, ctx):
        raise RuntimeError("setup always fails")

    def process(self, value, ctx):
        return value


def test_thread_setup_failure_aborts_with_skip_policy():
    """A setup() failure must abort the run even under errors='skip'."""
    from datapipe.execution.base import WorkerSetupError

    with pytest.raises(WorkerSetupError):
        Pipeline([_FailingSetup()]).run(
            source=IterableSource(range(10)),
            sink=ListSink(),
            executor=ThreadExecutor(workers=2, max_in_flight=4),
            errors="skip",
            progress=False,
        )


# ---------------------------------------------------------------------------
# review2.md finding 3 (Parquet): Hive partition column filters
# ---------------------------------------------------------------------------


def test_parquet_hive_partition_filter(tmp_path):
    """Filtering on a Hive partition column must work without ArrowInvalid."""
    from datapipe import ParquetSource

    # Write a small Hive-partitioned dataset: part=a/ and part=b/
    for part in ("a", "b"):
        d = tmp_path / f"part={part}"
        d.mkdir()
        pq.write_table(
            pa.table({"id": list(range(5))}),
            str(d / "data.parquet"),
        )

    rows = list(ParquetSource(str(tmp_path), filters=ds.field("part") == "b"))
    assert all(r["part"] == "b" for r in rows)
    assert len(rows) == 5


def test_parquet_hive_partition_filter_sharded(tmp_path):
    """Hive partition filters must work in multi-rank sharded reads.
    Per-rank results must be pairwise disjoint and cover exactly the
    selected partition.  An empty rank must return no rows without error.
    """
    from datapipe import ParquetSource

    # Three partitions: a, b, c — one file each.
    for part in ("a", "b", "c"):
        d = tmp_path / f"part={part}"
        d.mkdir()
        pq.write_table(
            pa.table({"id": list(range(4))}),
            str(d / "data.parquet"),
        )

    # world_size=4 > number of matching files (1) — exercises the empty-rank path.
    world = 4
    per_rank: list[list] = []
    for rank in range(world):
        sink = ListSink()
        Pipeline([GenericStage(process=lambda r: r, name="id")]).run(
            source=ParquetSource(str(tmp_path), filters=ds.field("part") == "b"),
            sink=sink,
            executor=SequentialExecutor(),
            runtime=RuntimeContext(rank=rank, world_size=world),
            progress=False,
        )
        per_rank.append(sink.items)

    all_rows = [r for rows in per_rank for r in rows]
    # All rows from partition b (4 rows) are returned exactly once.
    assert all(r["part"] == "b" for r in all_rows)
    assert len(all_rows) == 4
    # Rank outputs are pairwise disjoint by id.
    id_sets = [set(r["id"] for r in rows) for rows in per_rank]
    for i in range(world):
        for j in range(i + 1, world):
            assert not (id_sets[i] & id_sets[j]), f"ranks {i} and {j} overlap"
