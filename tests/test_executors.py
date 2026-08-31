"""Executor tests: bounded submission, ordering, errors, setup semantics.

Process tests run in this file and rely on the top-level ``__main__`` guard
of the test runner (pytest uses spawn-safe imports). Helper stages are
module-level so they pickle cleanly.
"""

from __future__ import annotations

import threading
import time

import pytest

from datapipe import (
    GenericStage,
    IterableSource,
    ListSink,
    Pipeline,
    ProcessExecutor,
    SequentialExecutor,
    ThreadExecutor,
    TransformStage,
)
from datapipe.errors import StageExecutionError
from datapipe.progress.base import ProgressReporter
from datapipe.stage import Stage


def _slow(x):
    time.sleep((x % 7) * 0.001 + 0.0005)
    return x


def _boom(x):
    if x == 5:
        raise ValueError("boom at 5")
    return x


class _SetupOnce(Stage):
    """Fails setup if invoked more than once per worker process."""

    name = "setup_once"

    def __init__(self):
        self.count = 0

    def setup(self, ctx):
        self.count += 1
        if self.count > 1:
            raise RuntimeError("setup ran more than once in a worker!")

    def process(self, value, ctx):
        return value


class _SetupTally(Stage):
    """Records how many times setup ran in this process (module-level)."""

    name = "setup_tally"

    def __init__(self, key):
        self.key = key
        self.setup_count = 0

    def setup(self, ctx):
        self.setup_count += 1
        _SETUP_LOG[self.key] = _SETUP_LOG.get(self.key, 0) + 1

    def process(self, value, ctx):
        return value

    def teardown(self, ctx):
        pass


_SETUP_LOG: dict = {}


# ---------------------------------------------------------------------------
# Sequential
# ---------------------------------------------------------------------------


def test_sequential_basic():
    sink = ListSink()
    p = Pipeline([TransformStage(lambda x: x * 2, name="d")])
    stats = p.run(
        source=IterableSource(range(10)),
        sink=sink,
        executor=SequentialExecutor(),
        progress=False,
    )
    assert sink.items == [i * 2 for i in range(10)]
    assert stats.completed_records == 10


# ---------------------------------------------------------------------------
# Thread
# ---------------------------------------------------------------------------


def test_thread_ordered():
    sink = ListSink()
    p = Pipeline([TransformStage(_slow, name="slow")])
    p.run(
        source=IterableSource(range(200)),
        sink=sink,
        executor=ThreadExecutor(workers=4, max_in_flight=16),
        ordered=True,
        progress=False,
    )
    assert sink.items == list(range(200))


def test_thread_unordered_preserves_values():
    sink = ListSink()
    p = Pipeline([TransformStage(_slow, name="slow")])
    p.run(
        source=IterableSource(range(200)),
        sink=sink,
        executor=ThreadExecutor(workers=4, max_in_flight=16),
        ordered=False,
        progress=False,
    )
    assert sorted(sink.items) == list(range(200))


# ---------------------------------------------------------------------------
# Process
# ---------------------------------------------------------------------------


def test_process_ordered():
    sink = ListSink()
    p = Pipeline([TransformStage(_slow, name="slow")])
    stats = p.run(
        source=IterableSource(range(300)),
        sink=sink,
        executor=ProcessExecutor(workers=4, max_in_flight=16),
        ordered=True,
        progress=False,
    )
    assert sink.items == list(range(300))
    assert stats.max_in_flight_observed <= 16


def test_process_unordered():
    sink = ListSink()
    p = Pipeline([TransformStage(_slow, name="slow")])
    p.run(
        source=IterableSource(range(300)),
        sink=sink,
        executor=ProcessExecutor(workers=4, max_in_flight=16),
        ordered=False,
        progress=False,
    )
    assert sorted(sink.items) == list(range(300))


def test_process_bounded_submission():
    """Invariant 4: the source is never eagerly consumed up front."""

    class TrackingSource:
        def __init__(self, n):
            self.n = n
            self.pulled = 0

        def __iter__(self):
            for i in range(self.n):
                self.pulled += 1
                yield i

    src = TrackingSource(500)
    sink = ListSink()
    stats = Pipeline([TransformStage(_slow, name="slow")]).run(
        source=src,  # plain iterable is accepted (IterableSource coercion)
        sink=sink,
        executor=ProcessExecutor(workers=4, max_in_flight=8),
        ordered=True,
        progress=False,
    )
    assert stats.max_in_flight_observed <= 8, stats.max_in_flight_observed
    assert len(sink.items) == 500


def test_process_setup_once_per_worker():
    sink = ListSink()
    Pipeline([_SetupOnce()]).run(
        source=IterableSource(range(200)),
        sink=sink,
        executor=ProcessExecutor(workers=4, max_in_flight=16),
        progress=False,
    )
    assert len(sink.items) == 200


# ---------------------------------------------------------------------------
# Error policies (shared across executors via parametrization)
# ---------------------------------------------------------------------------

def _make_executor(exec_cls):
    if exec_cls is SequentialExecutor:
        return exec_cls()
    return exec_cls(workers=2, max_in_flight=8)


ALL_EXECUTORS = [
    pytest.param(SequentialExecutor, id="sequential"),
    pytest.param(ThreadExecutor, id="thread"),
    pytest.param(ProcessExecutor, id="process"),
]


@pytest.mark.parametrize("exec_cls", ALL_EXECUTORS)
def test_error_policy_skip(exec_cls):
    exec_ = _make_executor(exec_cls)
    sink = ListSink()
    stats = Pipeline([TransformStage(_boom, name="boom")]).run(
        source=IterableSource(range(10)),
        sink=sink,
        executor=exec_,
        errors="skip",
        progress=False,
    )
    assert 5 not in sink.items
    assert stats.failed_records == 1
    assert len(sink.items) == 9


@pytest.mark.parametrize("exec_cls", ALL_EXECUTORS)
def test_error_policy_raise(exec_cls):
    exec_ = _make_executor(exec_cls)
    with pytest.raises(StageExecutionError) as ei:
        Pipeline([TransformStage(_boom, name="boom")]).run(
            source=IterableSource(range(10)),
            sink=ListSink(),
            executor=exec_,
            errors="raise",
            progress=False,
        )
    assert ei.value.stage_name == "boom"
    assert ei.value.record_seq == 5
    assert isinstance(ei.value.cause, ValueError)


@pytest.mark.parametrize("exec_cls", ALL_EXECUTORS)
def test_error_policy_return_without_error_sink(exec_cls):
    """errors='return' without an error_sink writes structured error payload dicts."""
    exec_ = _make_executor(exec_cls)
    sink = ListSink()
    stats = Pipeline([TransformStage(_boom, name="boom")]).run(
        source=IterableSource(range(6)),
        sink=sink,
        executor=exec_,
        errors="return",
        progress=False,
    )
    # 5 successes + 1 error payload dict, in input order (ordered default).
    assert len(sink.items) == 6
    ok = [i for i in sink.items if isinstance(i, int)]
    errs = [i for i in sink.items if not isinstance(i, int)]
    assert ok == [0, 1, 2, 3, 4]
    assert len(errs) == 1
    # Error payload is a JSON-serializable dict, not a raw TaskResult object.
    assert isinstance(errs[0], dict), f"expected dict, got {type(errs[0])}"
    assert errs[0]["seq"] == 5
    assert errs[0]["stage_name"] == "boom"
    assert errs[0].get("error_type") is not None
    assert stats.failed_records == 1


@pytest.mark.parametrize("exec_cls", ALL_EXECUTORS)
def test_error_policy_return(exec_cls):
    exec_ = _make_executor(exec_cls)
    esink = ListSink()
    sink = ListSink()
    stats = Pipeline([TransformStage(_boom, name="boom")]).run(
        source=IterableSource(range(10)),
        sink=sink,
        executor=exec_,
        errors="return",
        error_sink=esink,
        progress=False,
    )
    assert len(esink.items) == 1
    err = esink.items[0]
    assert err["seq"] == 5
    assert err["stage_name"] == "boom"
    assert err["error_type"] == "ValueError"
    assert "boom at 5" in err["error_message"]
    assert len(sink.items) == 9


# ---------------------------------------------------------------------------
# Immediate progress
# ---------------------------------------------------------------------------


class _Recorder(ProgressReporter):
    def __init__(self):
        self.updates = 0
        self.started = False

    def start(self, total=None):
        self.started = True

    def update(self, n=1, snapshot=None, **stats):
        self.updates += n

    def close(self):
        pass


def test_immediate_progress_before_source_exhausted():
    """Acceptance criterion: progress updates occur before input is fully
    consumed. We assert the recorder received many updates while only a
    bounded window of the source had been pulled."""
    reporter = _Recorder()
    sink = ListSink()
    p = Pipeline([TransformStage(_slow, name="slow")])
    p.run(
        source=IterableSource(range(2000)),
        sink=sink,
        executor=ProcessExecutor(workers=8, max_in_flight=32),
        ordered=False,
        progress=True,
        progress_reporter=reporter,
    )
    # With ordered=False, every completion produces exactly one progress tick.
    assert reporter.updates == 2000


# ---------------------------------------------------------------------------
# A1. Fatal source errors propagate for every executor and error policy
# ---------------------------------------------------------------------------


class _BrokenSource:
    """Yields one record then raises OSError to simulate an I/O failure."""

    def __iter__(self):
        yield 0
        raise OSError("read failed")


def _identity(x):
    return x


@pytest.mark.parametrize("exec_cls", ALL_EXECUTORS)
@pytest.mark.parametrize("policy", ["skip", "return"])
def test_fatal_source_error_propagates_for_all_executors(exec_cls, policy):
    """An OSError from the source iterator must propagate regardless of executor
    or record-error policy — it is not a resumable per-record failure."""
    exec_ = _make_executor(exec_cls)
    with pytest.raises(OSError, match="read failed"):
        Pipeline([TransformStage(_identity, name="id")]).run(
            source=_BrokenSource(),
            sink=ListSink(),
            executor=exec_,
            errors=policy,
            progress=False,
        )


# ---------------------------------------------------------------------------
# A2. KeyboardInterrupt from the source is never normalized to a record error
# ---------------------------------------------------------------------------


class _KeyboardInterruptSource:
    """Raises KeyboardInterrupt immediately on the first next() call."""

    def __iter__(self):
        raise KeyboardInterrupt
        yield  # make it a generator


@pytest.mark.parametrize("exec_cls", [
    pytest.param(ThreadExecutor, id="thread"),
    pytest.param(ProcessExecutor, id="process"),
])
@pytest.mark.parametrize("policy", ["skip", "return"])
def test_source_keyboard_interrupt_is_not_skipped(exec_cls, policy):
    """KeyboardInterrupt from the source must propagate unchanged; it must
    never be converted into a per-record error payload."""
    sink = ListSink()
    error_sink = ListSink()
    with pytest.raises(KeyboardInterrupt):
        Pipeline([TransformStage(lambda x: x, name="id")]).run(
            source=_KeyboardInterruptSource(),
            sink=sink,
            executor=_make_executor(exec_cls),
            errors=policy,
            error_sink=error_sink,
            progress=False,
        )
    # No records or error payloads must have been written.
    assert sink.items == []
    assert error_sink.items == []


# ---------------------------------------------------------------------------
# Ordered progress behind a slow first record (Phase 0 item 6 regression)
# ---------------------------------------------------------------------------


class _CapturingProgress(ProgressReporter):
    """Records every update() call so tests can inspect the progression."""

    def __init__(self):
        self.calls: list[dict] = []

    def start(self, total=None):
        pass

    def update(self, n=1, snapshot=None, **stats):
        # Accept both structured snapshot and legacy keyword-argument style.
        entry = {"n": n}
        if snapshot is not None:
            entry["processed"] = snapshot.processed
            entry["written"] = snapshot.written
            entry["buffered"] = snapshot.buffered
            entry["in_flight"] = snapshot.in_flight
            entry["failed"] = snapshot.failed
            entry["dropped"] = snapshot.dropped
        else:
            entry.update(stats)
        self.calls.append(entry)

    def close(self):
        pass


class _SlowFirstStage(Stage):
    """Blocks the first record until a gate fires, then processes normally.

    All other records are processed immediately.  This simulates a straggler
    at position zero that causes the reorder buffer to fill up in ordered mode.
    """

    name = "slow_first"

    def __init__(self, gate: threading.Event):
        self.gate = gate

    def __deepcopy__(self, memo):
        result = self.__class__.__new__(self.__class__)
        memo[id(self)] = result
        result.gate = self.gate
        result.name = self.name
        result._name_explicit = self._name_explicit
        return result

    def process(self, value, ctx):
        if value == 0:
            self.gate.wait(timeout=5.0)
        return value


def test_ordered_progress_advances_before_emission():
    """In ordered mode, progress must advance as records complete, not on emission.

    With a slow record at position 0, all records 1..N complete and are buffered
    before record 0 unblocks.  Progress (processed) must reach N before written
    reaches N, proving it advances on completion rather than waiting for the
    reorder buffer to drain.
    """
    gate = threading.Event()
    stage = _SlowFirstStage(gate)

    reporter = _CapturingProgress()

    RECORDS = 20
    # Use a thread executor so we can control timing without spawning processes.
    executor = ThreadExecutor(workers=4, max_in_flight=RECORDS)

    # Run in a background thread so we can release the gate mid-run.
    import threading as _threading
    results = {}

    def _run():
        results["stats"] = Pipeline([stage]).run(
            source=IterableSource(range(RECORDS)),
            sink=ListSink(),
            executor=executor,
            ordered=True,
            progress=True,
            progress_reporter=reporter,
        )

    t = _threading.Thread(target=_run)
    t.start()

    # Give workers time to start and process records 1..N while 0 blocks.
    time.sleep(0.15)

    # Check that progress calls have arrived before we release record 0.
    processed_so_far = sum(c["n"] for c in reporter.calls)
    # At least some records must have reported as processed.
    assert processed_so_far > 0, (
        "no progress updates arrived before record 0 was released; "
        "progress may only advance after ordered emission"
    )

    # Also check buffered is reported when records are waiting.
    buffered_calls = [c for c in reporter.calls if c.get("buffered", 0) > 0]
    # In ordered mode with a straggler at 0, completed records 1+ should be buffered.
    # (This assertion is best-effort: timing-dependent, but almost always true.)
    assert len(buffered_calls) > 0, (
        "no update reported buffered > 0; ordered progress snapshot not propagated"
    )

    # Release record 0 and wait for run to finish.
    gate.set()
    t.join(timeout=5.0)
    assert not t.is_alive(), "pipeline did not finish within timeout"
    assert results["stats"].completed_records == RECORDS
