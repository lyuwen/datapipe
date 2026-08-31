"""Progress model regressions (plan §12): submitted/in_flight accounting,
tqdm postfix refresh, and sequential auto-name numbering.

Every test here is deterministic: no sleeps, no timing assumptions.  Snapshots
are captured through a recording reporter and asserted structurally.
"""

from __future__ import annotations

import json

import pytest

from datapipe.execution import ProcessExecutor, SequentialExecutor, ThreadExecutor
from datapipe.io.base import Source, SourceRecordError
from datapipe.io.iterable import IterableSource, ListSink
from datapipe.pipeline import Pipeline
from datapipe.progress.base import ProgressReporter, ProgressSnapshot
from datapipe.progress.tqdm import TqdmProgress
from datapipe.stage import GenericStage, TransformStage

ALL_EXECUTORS = [
    pytest.param(SequentialExecutor, id="sequential"),
    pytest.param(ThreadExecutor, id="thread"),
    pytest.param(ProcessExecutor, id="process"),
]


def _make_executor(exec_cls):
    if exec_cls is SequentialExecutor:
        return exec_cls()
    return exec_cls(workers=2, max_in_flight=8)


class _Snapshots(ProgressReporter):
    """Records every ProgressSnapshot published during a run."""

    def __init__(self) -> None:
        self.snapshots: list[ProgressSnapshot] = []

    def start(self, total=None) -> None:
        pass

    def update(self, n=1, snapshot=None, **stats) -> None:
        if snapshot is not None:
            self.snapshots.append(snapshot)

    def close(self) -> None:
        pass


def _identity(x):
    return x


def _boom_on_five(x):
    if x == 5:
        raise ValueError("boom at 5")
    return x


class _MarkerSource(Source):
    """Yields SourceRecordError markers interleaved with good records.

    These markers consume a sequence number without ever being dispatched to a
    worker, which is precisely the case the old coordinator-side counting
    wrapper could not see.
    """

    def __init__(self, n: int, bad_positions: set[int]) -> None:
        self.n = n
        self.bad_positions = bad_positions

    def __iter__(self):
        for i in range(self.n):
            if i in self.bad_positions:
                yield SourceRecordError(ValueError(f"decode fail at {i}"))
            else:
                yield i


class _RaisingMarkerSource(Source):
    """Raises (rather than yields) SourceRecordError partway through."""

    def __init__(self, n: int, fail_at: int) -> None:
        self.n = n
        self.fail_at = fail_at

    def __iter__(self):
        for i in range(self.n):
            if i == self.fail_at:
                raise SourceRecordError(ValueError("boom"), line=i)
            yield i


class _UnnormalizedRaisingSource(Source):
    """Overrides ``iter_for_runtime`` so the raised SourceRecordError reaches
    the scheduler directly instead of being normalized into a yielded marker.

    This is the exact shape that drove ``in_flight`` negative: the scheduler's
    ``except SourceRecordError`` branch calls ``on_result`` (decrementing the
    old coordinator counter) for a record that never passed through the
    counting wrapper that would have incremented it.
    """

    def __init__(self, good_before: int = 1) -> None:
        self.good_before = good_before

    def __iter__(self):
        yield from range(self.good_before)

    def iter_for_runtime(self, runtime, sharding):
        def gen():
            yield from range(self.good_before)
            raise SourceRecordError(ValueError("decode fail"), line=self.good_before)

        return gen()


# ---------------------------------------------------------------------------
# Task 1 — ProgressSnapshot carries `submitted`
# ---------------------------------------------------------------------------


class TestSubmittedField:
    def test_snapshot_has_submitted_field(self):
        snap = ProgressSnapshot(submitted=7, processed=3)
        assert snap.submitted == 7

    def test_submitted_defaults_to_zero(self):
        assert ProgressSnapshot().submitted == 0

    def test_submitted_is_frozen(self):
        snap = ProgressSnapshot(submitted=1)
        with pytest.raises(Exception):
            snap.submitted = 99  # type: ignore[misc]

    @pytest.mark.parametrize("exec_cls", ALL_EXECUTORS)
    @pytest.mark.parametrize("ordered", [True, False])
    def test_submitted_is_populated_and_monotonic(self, exec_cls, ordered):
        rec = _Snapshots()
        Pipeline([TransformStage(_identity, name="id")]).run(
            source=IterableSource(range(20)),
            sink=ListSink(),
            executor=_make_executor(exec_cls),
            ordered=ordered,
            progress=True,
            progress_reporter=rec,
        )
        assert rec.snapshots, "no snapshots published"
        submitted = [s.submitted for s in rec.snapshots]
        assert submitted == sorted(submitted), "submitted must never decrease"
        assert max(submitted) == 20
        assert rec.snapshots[-1].submitted == 20

    @pytest.mark.parametrize("exec_cls", ALL_EXECUTORS)
    def test_submitted_never_lags_processed(self, exec_cls):
        """A record is counted as submitted before its result is delivered."""
        rec = _Snapshots()
        Pipeline([TransformStage(_identity, name="id")]).run(
            source=IterableSource(range(30)),
            sink=ListSink(),
            executor=_make_executor(exec_cls),
            progress=True,
            progress_reporter=rec,
        )
        for snap in rec.snapshots:
            assert snap.submitted >= snap.processed, snap

    @pytest.mark.parametrize("exec_cls", ALL_EXECUTORS)
    def test_submitted_counts_source_error_markers(self, exec_cls):
        """Markers consume a sequence number, so they count as submitted."""
        rec = _Snapshots()
        Pipeline([TransformStage(_identity, name="id")]).run(
            source=_MarkerSource(10, {2, 5, 7}),
            sink=ListSink(),
            executor=_make_executor(exec_cls),
            errors="skip",
            progress=True,
            progress_reporter=rec,
        )
        assert rec.snapshots[-1].submitted == 10


# ---------------------------------------------------------------------------
# Task 2 — in_flight accuracy (never negative; markers accounted for)
# ---------------------------------------------------------------------------


#: The raised-SourceRecordError-into-the-scheduler path only exists for the
#: bounded scheduler; ``SequentialExecutor`` handles yielded markers only, and
#: ``Source.iter_for_runtime`` normalizes raises into yields before either
#: executor sees them.  ``_UnnormalizedRaisingSource`` deliberately bypasses
#: that normalization to exercise the scheduler branch directly.
BOUNDED_EXECUTORS = [
    pytest.param(ThreadExecutor, id="thread"),
    pytest.param(ProcessExecutor, id="process"),
]


class TestInFlightAccuracy:
    @pytest.mark.parametrize("exec_cls", BOUNDED_EXECUTORS)
    @pytest.mark.parametrize("good_before", [1, 3])
    def test_in_flight_never_goes_negative(self, exec_cls, good_before):
        """The core regression: a source error delivered straight to the
        scheduler produced in_flight == -1 in a published snapshot."""
        rec = _Snapshots()
        Pipeline([TransformStage(_identity, name="id")]).run(
            source=_UnnormalizedRaisingSource(good_before),
            sink=ListSink(),
            executor=_make_executor(exec_cls),
            errors="skip",
            progress=True,
            progress_reporter=rec,
        )
        assert rec.snapshots
        assert all(s.in_flight >= 0 for s in rec.snapshots), (
            f"negative in_flight published: {[s.in_flight for s in rec.snapshots]}"
        )

    @pytest.mark.parametrize("exec_cls", BOUNDED_EXECUTORS)
    def test_undispatched_source_error_still_counts_as_submitted(self, exec_cls):
        """It consumed a sequence number, so submitted must include it."""
        rec = _Snapshots()
        Pipeline([TransformStage(_identity, name="id")]).run(
            source=_UnnormalizedRaisingSource(2),
            sink=ListSink(),
            executor=_make_executor(exec_cls),
            errors="skip",
            progress=True,
            progress_reporter=rec,
        )
        final = rec.snapshots[-1]
        assert final.submitted == 3
        assert final.submitted - final.processed == final.in_flight

    @pytest.mark.parametrize("exec_cls", ALL_EXECUTORS)
    @pytest.mark.parametrize("ordered", [True, False])
    def test_in_flight_never_negative_plain_run(self, exec_cls, ordered):
        rec = _Snapshots()
        Pipeline([TransformStage(_identity, name="id")]).run(
            source=IterableSource(range(25)),
            sink=ListSink(),
            executor=_make_executor(exec_cls),
            ordered=ordered,
            progress=True,
            progress_reporter=rec,
        )
        assert rec.snapshots
        for snap in rec.snapshots:
            assert snap.in_flight >= 0, f"negative in_flight: {snap}"

    @pytest.mark.parametrize("exec_cls", ALL_EXECUTORS)
    @pytest.mark.parametrize("policy", ["skip", "return"])
    def test_in_flight_never_negative_with_source_markers(self, exec_cls, policy):
        """The regression: markers were reported to on_result (decrementing
        in_flight) without ever passing through the counting wrapper that
        incremented it, so the count drifted negative."""
        rec = _Snapshots()
        Pipeline([TransformStage(_identity, name="id")]).run(
            source=_MarkerSource(12, {0, 1, 2, 3, 4, 5}),
            sink=ListSink(),
            executor=_make_executor(exec_cls),
            errors=policy,
            progress=True,
            progress_reporter=rec,
        )
        assert rec.snapshots
        for snap in rec.snapshots:
            assert snap.in_flight >= 0, f"negative in_flight: {snap}"

    @pytest.mark.parametrize("exec_cls", ALL_EXECUTORS)
    def test_in_flight_never_negative_with_raising_marker_source(self, exec_cls):
        rec = _Snapshots()
        Pipeline([TransformStage(_identity, name="id")]).run(
            source=_RaisingMarkerSource(8, 3),
            sink=ListSink(),
            executor=_make_executor(exec_cls),
            errors="skip",
            progress=True,
            progress_reporter=rec,
        )
        assert rec.snapshots
        for snap in rec.snapshots:
            assert snap.in_flight >= 0, f"negative in_flight: {snap}"

    @pytest.mark.parametrize("exec_cls", ALL_EXECUTORS)
    @pytest.mark.parametrize("ordered", [True, False])
    def test_submitted_minus_processed_equals_in_flight(self, exec_cls, ordered):
        """The accounting identity that makes the displayed numbers coherent.

        Holds even with undispatched source-error markers mixed in, because
        the scheduler counts a marker as submitted at the same moment it
        reports it as processed.
        """
        rec = _Snapshots()
        Pipeline([TransformStage(_identity, name="id")]).run(
            source=_MarkerSource(15, {3, 9}),
            sink=ListSink(),
            executor=_make_executor(exec_cls),
            ordered=ordered,
            errors="skip",
            progress=True,
            progress_reporter=rec,
        )
        assert rec.snapshots
        for snap in rec.snapshots:
            assert snap.submitted - snap.processed == snap.in_flight, snap

    @pytest.mark.parametrize("exec_cls", ALL_EXECUTORS)
    def test_in_flight_bounded_by_max_in_flight(self, exec_cls):
        rec = _Snapshots()
        limit = 4
        Pipeline([TransformStage(_identity, name="id")]).run(
            source=IterableSource(range(40)),
            sink=ListSink(),
            executor=_make_executor(exec_cls),
            max_in_flight=limit,
            progress=True,
            progress_reporter=rec,
        )
        for snap in rec.snapshots:
            assert snap.in_flight <= limit, snap

    def test_final_snapshot_has_zero_in_flight(self):
        rec = _Snapshots()
        Pipeline([TransformStage(_identity, name="id")]).run(
            source=IterableSource(range(10)),
            sink=ListSink(),
            executor=ThreadExecutor(workers=2, max_in_flight=4),
            progress=True,
            progress_reporter=rec,
        )
        assert rec.snapshots[-1].in_flight == 0

    @pytest.mark.parametrize("exec_cls", ALL_EXECUTORS)
    def test_in_flight_nonnegative_when_records_fail(self, exec_cls):
        rec = _Snapshots()
        Pipeline([TransformStage(_boom_on_five, name="boom")]).run(
            source=IterableSource(range(12)),
            sink=ListSink(),
            executor=_make_executor(exec_cls),
            errors="skip",
            progress=True,
            progress_reporter=rec,
        )
        for snap in rec.snapshots:
            assert snap.in_flight >= 0, snap

    def test_source_markers_do_not_break_output_or_counts(self):
        """Guards the behavior the accounting change rides on top of."""
        sink = ListSink()
        stats = Pipeline([TransformStage(_identity, name="id")]).run(
            source=_MarkerSource(10, {2, 5}),
            sink=sink,
            executor=SequentialExecutor(),
            errors="skip",
            progress=False,
        )
        assert sink.items == [0, 1, 3, 4, 6, 7, 8, 9]
        assert stats.failed_records == 2


# ---------------------------------------------------------------------------
# Task 3 — tqdm postfix reflects current state (and clears)
# ---------------------------------------------------------------------------


class _FakeBar:
    def __init__(self) -> None:
        self.postfix_calls: list[dict] = []
        self.updates: list[int] = []
        self.closed = False

    def update(self, n):
        self.updates.append(n)

    def set_postfix(self, d, **kwargs):
        self.postfix_calls.append(dict(d))

    def close(self):
        self.closed = True


class _FakeBarProgress(TqdmProgress):
    """TqdmProgress wired to a fake bar so postfix calls are inspectable
    without depending on tqdm being installed or on terminal rendering."""

    def __init__(self) -> None:
        super().__init__()
        self._bar = _FakeBar()

    def start(self, total=None) -> None:
        pass

    def close(self) -> None:
        pass

    @property
    def bar(self) -> _FakeBar:
        return self._bar


def _reporter_with_fake_bar() -> tuple[TqdmProgress, _FakeBar]:
    rep = _FakeBarProgress()
    return rep, rep.bar


class TestTqdmPostfix:
    def test_postfix_clears_when_counts_return_to_zero(self):
        """The regression: a non-empty postfix was latched onto the bar and
        never refreshed once the dict built empty, so buffered/in_flight stayed
        displayed after they drained."""
        rep, bar = _reporter_with_fake_bar()
        rep.update(1, snapshot=ProgressSnapshot(
            submitted=10, processed=4, written=1, buffered=3, in_flight=6
        ))
        rep.update(1, snapshot=ProgressSnapshot(
            submitted=10, processed=10, written=10, buffered=0, in_flight=0
        ))
        assert len(bar.postfix_calls) == 2
        final = bar.postfix_calls[-1]
        assert final["buffered"] == 0
        assert final["in_flight"] == 0

    def test_errors_clear_after_being_shown(self):
        rep, bar = _reporter_with_fake_bar()
        rep.update(1, snapshot=ProgressSnapshot(processed=1, failed=2))
        assert bar.postfix_calls[-1]["errors"] == 2
        rep.update(1, snapshot=ProgressSnapshot(processed=2, failed=0))
        assert bar.postfix_calls[-1]["errors"] == 0

    def test_postfix_always_published_even_when_all_zero(self):
        rep, bar = _reporter_with_fake_bar()
        rep.update(1, snapshot=ProgressSnapshot())
        assert bar.postfix_calls, "postfix must be set even when all counts are 0"

    def test_postfix_surfaces_plan_fields(self):
        """Plan §12 requires processed/written/buffered/in_flight on the bar."""
        rep, bar = _reporter_with_fake_bar()
        rep.update(1, snapshot=ProgressSnapshot(
            submitted=100, processed=82, written=80, buffered=2, in_flight=18
        ))
        post = bar.postfix_calls[-1]
        assert post["processed"] == 82
        assert post["written"] == 80
        assert post["buffered"] == 2
        assert post["in_flight"] == 18

    def test_legacy_errors_kwarg_still_supported(self):
        rep, bar = _reporter_with_fake_bar()
        rep.update(1, errors=3)
        assert bar.postfix_calls[-1]["errors"] == 3

    def test_update_without_bar_is_noop(self):
        rep = TqdmProgress()
        rep._bar = None
        rep.update(1, snapshot=ProgressSnapshot(processed=1))

    def test_end_to_end_with_real_reporter(self):
        """A full run through TqdmProgress must not raise on any snapshot."""
        rep, bar = _reporter_with_fake_bar()
        Pipeline([TransformStage(_identity, name="id")]).run(
            source=IterableSource(range(10)),
            sink=ListSink(),
            executor=SequentialExecutor(),
            progress=True,
            progress_reporter=rep,
        )
        assert bar.postfix_calls
        assert bar.postfix_calls[-1]["in_flight"] == 0


# ---------------------------------------------------------------------------
# Task 4 — auto-generated stage names are numbered sequentially
# ---------------------------------------------------------------------------


class TestAutoNameNumbering:
    def test_three_duplicates_number_sequentially(self):
        """Was: loads, loads_4, loads_5 (indices skipped by the pre-count)."""
        p = Pipeline([json.loads, json.loads, json.loads])
        assert p.stage_names() == ["loads", "loads_2", "loads_3"]

    def test_two_duplicates(self):
        p = Pipeline([json.loads, json.loads])
        assert p.stage_names() == ["loads", "loads_2"]

    def test_five_duplicates_are_contiguous(self):
        p = Pipeline([json.loads] * 5)
        assert p.stage_names() == [
            "loads", "loads_2", "loads_3", "loads_4", "loads_5"
        ]

    def test_names_remain_unique(self):
        p = Pipeline([json.loads] * 6)
        names = p.stage_names()
        assert len(set(names)) == len(names)

    def test_two_distinct_duplicate_groups_number_independently(self):
        p = Pipeline([json.loads, json.dumps, json.loads, json.dumps])
        assert p.stage_names() == ["loads", "dumps", "loads_2", "dumps_2"]

    def test_auto_suffix_avoids_colliding_with_explicit_name(self):
        """An auto suffix must not collide with a user-chosen name."""
        p = Pipeline([
            GenericStage(process=_identity, name="s"),
            GenericStage(process=_identity, name="s_2"),
            json.loads,
            json.loads,
        ])
        names = p.stage_names()
        assert len(set(names)) == len(names)

    def test_deduped_pipeline_still_runs(self):
        sink = ListSink()
        Pipeline([json.loads, json.loads]).run(
            source=IterableSource(['"[1]"', '"[2]"']),
            sink=sink,
            executor=SequentialExecutor(),
            progress=False,
        )
        assert sink.items == [[1], [2]]
