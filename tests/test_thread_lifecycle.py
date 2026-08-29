"""Thread worker lifecycle regression tests (A5-A10 from the test plan).

Tests here focus on the full setup/process/teardown contract for ThreadExecutor:
- per-thread attribute and nested-state isolation (A5, A6)
- setup failure is fatal for every error policy (A7)
- teardown runs exactly once per initialized worker, on the owning thread (A8)
- abort does not tear down a worker while it is still processing (A9)
- abort tears down every worker that completed setup (A10)

Test design follows the plan's recommendations:
- threading.Event / threading.Barrier for ordering, not time.sleep
- shared synchronized collectors separate from per-worker stage state
- bounded timeouts (5 s) on all coordination waits so regressions fail fast
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from datapipe import (
    IterableSource,
    ListSink,
    Pipeline,
    SequentialExecutor,
    ThreadExecutor,
    TransformStage,
)
from datapipe.execution.base import WorkerSetupError
from datapipe.stage import Stage

_TIMEOUT = 5  # seconds; guards all coordination waits against hanging CI


# ---------------------------------------------------------------------------
# Shared synchronized collector used by probes that must survive deep-copy
# ---------------------------------------------------------------------------


class _Collector:
    """Thread-safe store for per-worker lifecycle observations.

    Passed as a constructor argument to probe stages.  ``__deepcopy__`` returns
    ``self`` so every per-thread stage copy writes to the same instance, keeping
    all observations in one place for the test to inspect after the run.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self.setup: dict[int, int] = {}       # worker_id → thread_id at setup
        self.teardown: dict[int, int] = {}    # worker_id → thread_id at teardown
        self.process_calls: list[tuple] = []  # (worker_id, thread_id, value)
        self.mismatches: list[str] = []       # isolation violations

    def __deepcopy__(self, memo):
        # Shared intentionally: all per-thread stage copies must write here.
        return self

    def record_setup(self, worker_id: int) -> None:
        with self._lock:
            self.setup[worker_id] = threading.get_ident()

    def record_teardown(self, worker_id: int) -> None:
        with self._lock:
            self.teardown[worker_id] = threading.get_ident()

    def record_process(self, worker_id: int, value) -> None:
        with self._lock:
            self.process_calls.append((worker_id, threading.get_ident(), value))

    def record_mismatch(self, msg: str) -> None:
        with self._lock:
            self.mismatches.append(msg)


def _make_multi_thread_gate(min_threads: int, timeout: float = _TIMEOUT):
    """Return an event that fires once ``min_threads`` threads have checked in.

    Using an event+counter instead of threading.Barrier avoids BrokenBarrierError
    when fewer threads than the barrier's parties actually initialize (which can
    happen when the pool receives fewer records than configured workers).
    """
    lock = threading.Lock()
    count = [0]
    ready = threading.Event()

    def check_in():
        with lock:
            count[0] += 1
            if count[0] >= min_threads:
                ready.set()
        ready.wait(timeout=timeout)

    return check_in


# ---------------------------------------------------------------------------
# A5: direct stage attribute is isolated per thread
# ---------------------------------------------------------------------------


class _OwnerThreadStage(Stage):
    """Stores thread identity in setup; verifies it in process.

    self.owner_thread is a plain attribute — the common pattern of storing a
    model or client in setup.  Under copy.copy this would be shared; under
    deepcopy each thread copy has its own namespace.
    """

    name = "owner_thread"

    def __init__(self, collector: _Collector, check_in):
        self.collector = collector
        self.check_in = check_in
        self.owner_thread: int | None = None

    def setup(self, ctx):
        self.owner_thread = threading.get_ident()
        self.collector.record_setup(ctx.worker_id)
        self.check_in()

    def process(self, value, ctx):
        tid = threading.get_ident()
        if self.owner_thread != tid:
            self.collector.record_mismatch(
                f"record {value}: owner_thread {self.owner_thread} != current {tid}"
            )
        self.collector.record_process(ctx.worker_id, value)
        return value

    def teardown(self, ctx):
        self.collector.record_teardown(ctx.worker_id)


def test_thread_stage_attribute_state_is_worker_local():
    """setup() stores self.owner_thread; process() must always see its own thread's value."""
    collector = _Collector()
    check_in = _make_multi_thread_gate(min_threads=2)
    sink = ListSink()
    Pipeline([_OwnerThreadStage(collector, check_in)]).run(
        source=IterableSource(range(100)),
        sink=sink,
        executor=ThreadExecutor(workers=4, max_in_flight=32),
        ordered=False,
        progress=False,
    )
    assert sorted(sink.items) == list(range(100))
    assert collector.mismatches == [], collector.mismatches
    assert len(collector.setup) >= 2, "expected at least 2 worker threads to initialize"


# ---------------------------------------------------------------------------
# A6: nested mutable stage state is isolated per thread
# ---------------------------------------------------------------------------


class _NestedStateStage(Stage):
    """Stores ownership in a nested dict (self.state["owner_thread"]).

    This is the pattern that copy.copy fails to isolate because all copies
    share the same dict object.
    """

    name = "nested_state"

    def __init__(self, collector: _Collector, check_in):
        self.collector = collector
        self.check_in = check_in
        self.state: dict = {"owner_thread": None}

    def setup(self, ctx):
        self.state["owner_thread"] = threading.get_ident()
        self.collector.record_setup(ctx.worker_id)
        self.check_in()

    def process(self, value, ctx):
        tid = threading.get_ident()
        if self.state["owner_thread"] != tid:
            self.collector.record_mismatch(
                f"record {value}: nested owner {self.state['owner_thread']} != current {tid}"
            )
        self.collector.record_process(ctx.worker_id, value)
        return value

    def teardown(self, ctx):
        self.collector.record_teardown(ctx.worker_id)


def test_thread_nested_mutable_state_is_worker_local():
    """setup() writes into self.state dict; process() must see its own thread's value."""
    collector = _Collector()
    check_in = _make_multi_thread_gate(min_threads=2)
    sink = ListSink()
    Pipeline([_NestedStateStage(collector, check_in)]).run(
        source=IterableSource(range(100)),
        sink=sink,
        executor=ThreadExecutor(workers=4, max_in_flight=32),
        ordered=False,
        progress=False,
    )
    assert sorted(sink.items) == list(range(100))
    assert collector.mismatches == [], collector.mismatches
    assert len(collector.setup) >= 2


# ---------------------------------------------------------------------------
# A7: setup failure is fatal under every record-error policy
# ---------------------------------------------------------------------------


class _AlwaysFailSetup(Stage):
    """Raises unconditionally in setup()."""

    name = "always_fail_setup"

    def setup(self, ctx):
        raise RuntimeError("setup always fails")

    def process(self, value, ctx):
        return value


@pytest.mark.parametrize("policy", ["raise", "skip", "return"])
def test_thread_setup_failure_is_fatal_for_every_error_policy(policy):
    """WorkerSetupError must propagate regardless of the record-error policy."""
    sink = ListSink()
    error_sink = ListSink()
    with pytest.raises(WorkerSetupError) as ei:
        Pipeline([_AlwaysFailSetup()]).run(
            source=IterableSource(range(20)),
            sink=sink,
            executor=ThreadExecutor(workers=2, max_in_flight=4),
            errors=policy,
            error_sink=error_sink if policy == "return" else None,
            progress=False,
        )
    assert isinstance(ei.value.cause, RuntimeError)
    assert str(ei.value.cause) == "setup always fails"
    # process() must never have been called
    assert sink.items == []
    assert error_sink.items == []


# ---------------------------------------------------------------------------
# A8: teardown runs exactly once per initialized worker, on the owning thread
# ---------------------------------------------------------------------------


def test_thread_teardown_once_on_owning_thread():
    """For each worker: setup and teardown run on the same OS thread exactly once."""
    collector = _Collector()
    check_in = _make_multi_thread_gate(min_threads=2)
    Pipeline([_OwnerThreadStage(collector, check_in)]).run(
        source=IterableSource(range(100)),
        sink=ListSink(),
        executor=ThreadExecutor(workers=4, max_in_flight=32),
        ordered=False,
        progress=False,
    )
    setup_ids = set(collector.setup)
    teardown_ids = set(collector.teardown)
    assert setup_ids == teardown_ids, (
        f"setup workers {setup_ids} != teardown workers {teardown_ids}"
    )
    assert len(collector.teardown) == len(setup_ids)
    for wid in setup_ids:
        setup_tid = collector.setup[wid]
        teardown_tid = collector.teardown[wid]
        assert setup_tid == teardown_tid, (
            f"worker {wid}: setup on thread {setup_tid}, teardown on thread {teardown_tid}"
        )


# ---------------------------------------------------------------------------
# A9: abort never tears down a worker while it is still processing
# ---------------------------------------------------------------------------


class _AbortCoordStage(Stage):
    """Two-worker coordinated abort probe.

    Worker A blocks in process() until release_event is set.
    Worker B raises on abort_value to trigger abort.
    teardown() verifies no worker is still inside process() when it runs.

    _processing_lock and _active_workers are shared across all deep-copies via
    __deepcopy__ so all threads write to the same tracking structures.
    """

    name = "abort_coord"

    def __init__(
        self,
        collector: _Collector,
        entered_event: threading.Event,
        release_event: threading.Event,
        abort_value: int,
    ):
        self.collector = collector
        self.entered_event = entered_event
        self.release_event = release_event
        self.abort_value = abort_value
        self._processing_lock = threading.Lock()
        self._active_workers: set[int] = set()

    def __deepcopy__(self, memo):
        # Share coordination state across per-thread copies so all threads
        # write to the same _active_workers set and check the same events.
        result = self.__class__.__new__(self.__class__)
        memo[id(self)] = result
        result.collector = self.collector
        result.entered_event = self.entered_event
        result.release_event = self.release_event
        result.abort_value = self.abort_value
        result._processing_lock = self._processing_lock
        result._active_workers = self._active_workers
        result.name = self.name
        result._name_explicit = self._name_explicit
        return result

    def setup(self, ctx):
        self.collector.record_setup(ctx.worker_id)

    def process(self, value, ctx):
        if value == self.abort_value:
            raise ValueError(f"intentional abort on {value}")
        with self._processing_lock:
            self._active_workers.add(ctx.worker_id)
        self.entered_event.set()
        ok = self.release_event.wait(timeout=_TIMEOUT)
        with self._processing_lock:
            self._active_workers.discard(ctx.worker_id)
        if not ok:
            raise RuntimeError("release_event timed out in test")
        return value

    def teardown(self, ctx):
        with self._processing_lock:
            if ctx.worker_id in self._active_workers:
                self.collector.record_mismatch(
                    f"worker {ctx.worker_id} torn down while still processing"
                )
        self.collector.record_teardown(ctx.worker_id)


def test_thread_abort_waits_before_teardown():
    """When a run aborts, no worker is torn down while it is still in process()."""
    entered = threading.Event()
    release = threading.Event()
    collector = _Collector()

    stage = _AbortCoordStage(
        collector=collector,
        entered_event=entered,
        release_event=release,
        abort_value=0,  # first record triggers abort; others block in process()
    )

    def _run():
        with pytest.raises(Exception):
            Pipeline([stage]).run(
                source=IterableSource(range(20)),
                sink=ListSink(),
                executor=ThreadExecutor(workers=4, max_in_flight=16),
                errors="raise",
                progress=False,
            )

    t = threading.Thread(target=_run)
    t.start()
    # Wait until at least one worker has entered process() before letting abort proceed.
    entered.wait(timeout=_TIMEOUT)
    release.set()
    t.join(timeout=_TIMEOUT)
    assert not t.is_alive(), "pipeline did not finish within timeout"
    assert collector.mismatches == [], collector.mismatches


# ---------------------------------------------------------------------------
# A10: abort tears down every worker that completed setup
# ---------------------------------------------------------------------------


class _SetupCountStage(Stage):
    """Counts setup/teardown by worker_id via a shared collector."""

    name = "setup_count"

    def __init__(self, collector: _Collector, fail_on: int = -1):
        self.collector = collector
        self.fail_on = fail_on

    def setup(self, ctx):
        self.collector.record_setup(ctx.worker_id)

    def process(self, value, ctx):
        if value == self.fail_on:
            raise ValueError(f"abort on {value}")
        return value

    def teardown(self, ctx):
        self.collector.record_teardown(ctx.worker_id)


def test_thread_teardown_on_abort_covers_all_initialized_workers():
    """After a stage-error abort, every worker that completed setup must be torn down."""
    collector = _Collector()
    with pytest.raises(Exception):
        Pipeline([_SetupCountStage(collector, fail_on=10)]).run(
            source=IterableSource(range(100)),
            sink=ListSink(),
            executor=ThreadExecutor(workers=4, max_in_flight=16),
            errors="raise",
            progress=False,
        )
    setup_ids = set(collector.setup)
    teardown_ids = set(collector.teardown)
    assert len(setup_ids) >= 2, "expected at least two workers to initialize before abort"
    assert setup_ids == teardown_ids, (
        f"workers that set up {setup_ids} != workers that tore down {teardown_ids}"
    )


def test_thread_teardown_on_keyboard_interrupt_covers_all_initialized_workers():
    """After a KeyboardInterrupt abort, every initialized worker must be torn down."""
    from datapipe.io.base import Source

    collector = _Collector()
    # Enough records to initialize multiple workers before KI fires.
    check_in = _make_multi_thread_gate(min_threads=2)

    class _SetupAndCountStage(Stage):
        """Combines setup counting with gate synchronization."""

        name = "setup_and_count"

        def __init__(self, col: _Collector, gate):
            self.col = col
            self.gate = gate

        def __deepcopy__(self, memo):
            result = self.__class__.__new__(self.__class__)
            memo[id(self)] = result
            result.col = self.col    # shared collector
            result.gate = self.gate  # shared gate
            result.name = self.name
            result._name_explicit = self._name_explicit
            return result

        def setup(self, ctx):
            self.col.record_setup(ctx.worker_id)
            self.gate()

        def process(self, value, ctx):
            return value

        def teardown(self, ctx):
            self.col.record_teardown(ctx.worker_id)

    class _KISource(Source):
        def __iter__(self):
            # Yield enough records for workers to initialize, then interrupt.
            yield from range(50)
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        Pipeline([_SetupAndCountStage(collector, check_in)]).run(
            source=_KISource(),
            sink=ListSink(),
            executor=ThreadExecutor(workers=4, max_in_flight=16),
            errors="skip",
            progress=False,
        )
    setup_ids = set(collector.setup)
    teardown_ids = set(collector.teardown)
    assert len(setup_ids) >= 2, f"expected ≥2 workers to initialize, got {setup_ids}"
    assert setup_ids == teardown_ids, (
        f"workers that set up {setup_ids} != workers that tore down {teardown_ids}"
    )
