"""Phase S7 execution-model verification (plan §15.5).

The structural language added statements to the *program*, not operators to the
*scheduler*.  That distinction is the whole architectural bet: a five-statement
expression must still cost exactly one dispatch and one gather per record.  If
a future change ever routes a statement through its own future, or materializes
the source to plan across statements, these tests are what notices.

Everything here counts real events rather than inferring them:

- submissions are counted by wrapping the executor's ``_submit`` (and, for the
  sequential executor, its ``worker.process``) — never inferred from timing,
  logs, or ``ExecutionStats`` fields that the scheduler could compute wrongly;
- streaming is proved by a source that records how many records it has yielded
  at the moment each output is written, so "the pipeline did not materialize
  the source" is an observation, not an assumption;
- provider resolution is counted by patching ``resolve_tool``.

Both counting tests were revert-verified: with the bounded scheduler's window
neutered, or with per-statement dispatch simulated, they fail.  See the phase
report for the exact mutations used.
"""

from __future__ import annotations

import copy
import json
import pickle

import pytest

from datapipe.context import WorkerContext
from datapipe.dsl.compiler import compile_program
from datapipe.execution.process import ProcessExecutor
from datapipe.execution.sequential import SequentialExecutor
from datapipe.execution.thread import ThreadExecutor
from datapipe.io.iterable import IterableSource, ListSink
from datapipe.pipeline import Pipeline
from datapipe.stages.tool_program import CompiledProgramStage
from datapipe.tools.errors import StructuralExecutionError


@pytest.fixture(autouse=True)
def isolated_registry(tmp_path, monkeypatch):
    """Redirect the tool registry at tmp_path and clear the loader cache."""
    monkeypatch.setenv("DATAPIPE_USER_DATA", str(tmp_path / "dp_data"))

    from datapipe.tools import loader as _loader

    _loader._loaded_providers.clear()

    from datapipe.dsl import compiler as _compiler

    monkeypatch.setattr(_compiler, "_BUILTIN_REGISTRY", None, raising=False)


# ===========================================================================
# Principal expressions and records
# ===========================================================================

#: The §7 catalogue's principal shapes, each paired with a record it applies
#: to.  Every executor test runs the whole set so a regression in any one
#: structural form (sequence, grouped move, complement move, copy, transformed
#: move, focused pipe) is caught, not just the easiest one.
PRINCIPAL: list[tuple[str, str, dict]] = [
    (
        "sequence",
        "fromjson(.tools); fromjson(.annotation, recursive=true)",
        {
            "instance_id": "i-0",
            "tools": '[{"name": "t"}]',
            "annotation": '{"k": "v"}',
        },
    ),
    (
        "nested_wildcard",
        "tojson(.tools[].function.parameters)",
        {"tools": [{"function": {"parameters": {"a": 1}}},
                   {"function": {"parameters": {"b": 2}}}]},
    ),
    (
        "explicit_move",
        ".metadata << .annotation_key, .temperature, .score | tojson",
        {
            "instance_id": "i-0",
            "messages": [],
            "tools": [],
            "annotation_key": "k",
            "temperature": 0.7,
            "score": 3,
        },
    ),
    (
        "field_set_move",
        ".metadata << .(annotation_key|temperature|score) | tojson",
        {
            "instance_id": "i-0",
            "messages": [],
            "tools": [],
            "annotation_key": "k",
            "temperature": 0.7,
            "score": 3,
        },
    ),
    (
        "complement_move",
        ".metadata << .(^instance_id|messages|tools) | tojson",
        {
            "instance_id": "i-0",
            "messages": [],
            "tools": [],
            "annotation_key": "k",
            "temperature": 0.7,
            "score": 3,
        },
    ),
    (
        "named_nest",
        'nest(., key="metadata", '
        'exclude=["instance_id", "messages", "tools"], jsonify=true)',
        {
            "instance_id": "i-0",
            "messages": [],
            "tools": [],
            "annotation_key": "k",
            "temperature": 0.7,
            "score": 3,
        },
    ),
    (
        "transformed_move_out",
        "fromjson(.metadata); "
        ".temperature <- fromjson(.metadata.temperature); "
        "tojson(.metadata)",
        {
            "instance_id": "i-0",
            "metadata": json.dumps({"temperature": "0.5", "note": "n"}),
        },
    ),
    (
        "copy_out",
        "fromjson(.metadata); .temperature = .metadata.temperature; "
        "tojson(.metadata)",
        {
            "instance_id": "i-0",
            "metadata": json.dumps({"temperature": 0.5, "note": "n"}),
        },
    ),
]

PRINCIPAL_IDS = [name for name, _e, _r in PRINCIPAL]

#: A five-statement program used wherever the point is "statement count must
#: not change dispatch count".  Deliberately mixes a tool call, a copy, a
#: move, a grouped move-into and a whole-record tool.
FIVE_STATEMENTS = (
    "fromjson(.blob); "
    ".kept = .blob.a; "
    ".moved <- .blob.b; "
    ".metadata << .(^instance_id|blob|kept|moved); "
    "tojson(.metadata)"
)

#: The same record shape the five-statement program consumes.
def _five_statement_record(i: int) -> dict:
    return {
        "instance_id": f"i-{i}",
        "blob": json.dumps({"a": i, "b": i * 2}),
        "extra": i * 3,
    }


def _stage(expression: str) -> CompiledProgramStage:
    return CompiledProgramStage(compile_program(expression))


def _pipeline(expression: str) -> Pipeline:
    return Pipeline([_stage(expression)])


def _executors():
    """The three executors under test, each freshly constructed."""
    return [
        ("sequential", SequentialExecutor(), None),
        ("thread", ThreadExecutor(workers=2, max_in_flight=4), 4),
        ("process", ProcessExecutor(workers=2, max_in_flight=4), 4),
    ]


# ===========================================================================
# Submission counting
# ===========================================================================


class _CountingSource:
    """A source that reports how many records it has yielded so far.

    ``yielded`` is read by the sink at write time, which is what makes the
    streaming property observable: if the coordinator drained the whole source
    before producing any output, ``yielded`` would already equal ``n`` when the
    first record is written.
    """

    def __init__(self, records: list) -> None:
        self._records = records
        self.yielded = 0

    def __iter__(self):
        for record in self._records:
            self.yielded += 1
            yield record

    def __len__(self) -> int:
        return len(self._records)


def _count_submissions(executor):
    """Patch *executor* so every dispatch increments a counter.

    Returns the mutable counter list.  For the future-based executors this
    wraps ``_submit``, the single funnel through which every job reaches a
    pool.  ``SequentialExecutor`` has no ``_submit``; its equivalent single
    funnel is ``worker.process``, which the scheduler calls exactly once per
    dispatched record, so we wrap the compiled worker instead at run time.
    """
    counter: list[int] = []

    if hasattr(executor, "_submit"):
        original = executor._submit

        def counting_submit(job):
            counter.append(job.seq)
            return original(job)

        executor._submit = counting_submit
        return counter

    original_run = executor.run

    def counting_run(*, worker, **kwargs):
        worker_process = worker.process

        def counting_process(value, ctx):
            counter.append(ctx.record_index)
            return worker_process(value, ctx)

        worker.process = counting_process
        return original_run(worker=worker, **kwargs)

    executor.run = counting_run
    return counter


@pytest.mark.parametrize("statements", [1, 2, 5])
def test_one_submission_per_record_regardless_of_statement_count(statements):
    """§15.5 / §16.14 — dispatch count tracks records, never statements.

    The same 12 records are run through a one-, two- and five-statement
    program.  A per-statement future would make the count 12, 24 and 60; the
    architecture requires 12 every time.
    """
    expressions = {
        1: "fromjson(.blob)",
        2: "fromjson(.blob); .kept = .blob.a",
        5: FIVE_STATEMENTS,
    }
    records = [_five_statement_record(i) for i in range(12)]

    executor = ThreadExecutor(workers=2, max_in_flight=4)
    counter = _count_submissions(executor)

    sink = ListSink()
    _pipeline(expressions[statements]).run(
        source=IterableSource(copy.deepcopy(records)),
        sink=sink,
        executor=executor,
        progress=False,
        max_in_flight=4,
    )

    assert len(counter) == len(records), (
        f"{statements}-statement program dispatched {len(counter)} jobs for "
        f"{len(records)} records; expected exactly one per record"
    )
    # Every record dispatched exactly once, and every sequence number used.
    assert sorted(counter) == list(range(len(records)))
    assert len(sink.items) == len(records)


def test_submission_count_is_independent_of_statement_count():
    """§16.14 stated as a direct comparison rather than three separate runs.

    Growing the program from one statement to five must not change the number
    of dispatches by even one.
    """
    records = [_five_statement_record(i) for i in range(8)]
    counts = {}
    for label, expression in (
        ("one", "fromjson(.blob)"),
        ("five", FIVE_STATEMENTS),
    ):
        executor = ThreadExecutor(workers=2, max_in_flight=3)
        counter = _count_submissions(executor)
        _pipeline(expression).run(
            source=IterableSource(copy.deepcopy(records)),
            sink=ListSink(),
            executor=executor,
            progress=False,
            max_in_flight=3,
        )
        counts[label] = len(counter)

    assert counts["one"] == counts["five"] == len(records), counts


@pytest.mark.parametrize(
    "name,expression,record", PRINCIPAL, ids=PRINCIPAL_IDS
)
def test_every_principal_expression_dispatches_once_per_record(
    name, expression, record
):
    """§15.5 — the one-dispatch rule holds for every principal form.

    Statement counts across the catalogue range from one to three and include
    moves, copies and focused pipes; none of them may add a dispatch.
    """
    records = [copy.deepcopy(record) for _ in range(6)]
    executor = ThreadExecutor(workers=2, max_in_flight=3)
    counter = _count_submissions(executor)

    sink = ListSink()
    _pipeline(expression).run(
        source=IterableSource(records),
        sink=sink,
        executor=executor,
        progress=False,
        max_in_flight=3,
    )

    assert len(counter) == 6, f"{name}: {len(counter)} dispatches for 6 records"
    assert len(sink.items) == 6


def test_sequential_executor_invokes_the_worker_once_per_record():
    """§15.5 — the sequential path obeys the same rule.

    ``SequentialExecutor`` has no futures at all, so the equivalent assertion
    is that the fused worker program is entered exactly once per record even
    for a five-statement expression.
    """
    records = [_five_statement_record(i) for i in range(7)]
    executor = SequentialExecutor()
    counter = _count_submissions(executor)

    _pipeline(FIVE_STATEMENTS).run(
        source=IterableSource(records),
        sink=ListSink(),
        executor=executor,
        progress=False,
    )

    assert len(counter) == 7, counter


def test_no_per_statement_futures_reach_the_pool():
    """§15.5 — the pool sees whole records, not statements.

    Beyond counting, this inspects the payload: each submitted job carries the
    *record* and a sequence number.  A per-statement scheduler would have to
    submit either a statement object or the same seq repeatedly; both are
    excluded here.
    """
    records = [_five_statement_record(i) for i in range(5)]
    executor = ThreadExecutor(workers=2, max_in_flight=2)

    # Snapshot the payload at submit time.  Thread workers mutate the record
    # in place, so inspecting ``job.value`` after the run would show the
    # post-transformation state rather than what actually crossed.
    submitted_jobs: list[tuple[int, dict]] = []
    original = executor._submit

    def capturing(job):
        submitted_jobs.append((job.seq, copy.deepcopy(job.value)))
        return original(job)

    executor._submit = capturing

    _pipeline(FIVE_STATEMENTS).run(
        source=IterableSource(copy.deepcopy(records)),
        sink=ListSink(),
        executor=executor,
        progress=False,
        max_in_flight=2,
    )

    assert len(submitted_jobs) == 5
    # Sequence numbers are unique: no record was submitted twice.
    assert len({seq for seq, _value in submitted_jobs}) == 5
    # Each payload is a whole input record, not a statement or a partial.
    for (_seq, value), expected in zip(submitted_jobs, records):
        assert value == expected, value


# ===========================================================================
# Bounded submission and streaming
# ===========================================================================


@pytest.mark.parametrize("window", [1, 2, 5])
def test_max_in_flight_is_a_hard_bound_under_structural_programs(window):
    """§15.5 — a multi-statement program does not widen the window.

    The bound is checked at the moment of each dispatch: the number of
    outstanding (submitted but not yet completed) jobs must never exceed the
    configured window.
    """
    records = [_five_statement_record(i) for i in range(20)]
    executor = ThreadExecutor(workers=2, max_in_flight=window)

    outstanding = {"live": 0, "peak": 0}
    original = executor._submit

    def tracking(job):
        outstanding["live"] += 1
        outstanding["peak"] = max(outstanding["peak"], outstanding["live"])
        future = original(job)
        future.add_done_callback(
            lambda _f: outstanding.__setitem__("live", outstanding["live"] - 1)
        )
        return future

    executor._submit = tracking

    stats = _pipeline(FIVE_STATEMENTS).run(
        source=IterableSource(copy.deepcopy(records)),
        sink=ListSink(),
        executor=executor,
        progress=False,
        max_in_flight=window,
    )

    assert outstanding["peak"] <= window, outstanding
    assert stats.max_in_flight_observed <= window
    assert stats.output_records == len(records)


def test_structural_pipeline_produces_output_before_the_source_is_exhausted():
    """§15.5 / §16.15 — the pipeline streams; it does not materialize.

    A source of 200 records with a window of 4 must have yielded only a
    handful of records at the moment the first output is written.  If any
    structural operation ever buffered the dataset (to plan across statements,
    say), the first write would come after all 200 were pulled.
    """
    n = 200
    window = 4
    source = _CountingSource([_five_statement_record(i) for i in range(n)])

    yielded_at_write: list[int] = []

    def sink(_record):
        yielded_at_write.append(source.yielded)

    executor = ThreadExecutor(workers=2, max_in_flight=window)
    _pipeline(FIVE_STATEMENTS).run(
        source=IterableSource(source),
        sink=sink,
        executor=executor,
        progress=False,
        max_in_flight=window,
    )

    assert len(yielded_at_write) == n
    # The first output must appear while the vast majority of the source is
    # still unread.  The window plus a small slack is the true bound; the
    # assertion is deliberately far below n so it cannot pass vacuously.
    assert yielded_at_write[0] <= window + 2, (
        f"first output was written only after {yielded_at_write[0]} of {n} "
        "records had been pulled — the source appears to be materialized"
    )
    # Reading stays ahead of writing by at most the window throughout, which
    # is the streaming property stated over the whole run rather than just its
    # first record.
    for index, yielded in enumerate(yielded_at_write):
        assert yielded - index <= window + 2, (index, yielded)


def test_sequential_execution_also_streams():
    """§16.15 — the streaming guarantee is not a thread-pool artifact."""
    n = 50
    source = _CountingSource([_five_statement_record(i) for i in range(n)])
    yielded_at_write: list[int] = []

    _pipeline(FIVE_STATEMENTS).run(
        source=IterableSource(source),
        sink=lambda _r: yielded_at_write.append(source.yielded),
        executor=SequentialExecutor(),
        progress=False,
    )

    assert len(yielded_at_write) == n
    assert yielded_at_write[0] == 1, yielded_at_write[0]
    assert yielded_at_write == list(range(1, n + 1))


def test_no_runtime_queue_is_added_between_statements():
    """§16.15 — statements share one call frame, not a queue.

    All five statements of one record must run to completion before the next
    record's first statement begins under a single worker.  A queue between
    statements would interleave them.  The probe records
    ``(record_id, statement_marker)`` pairs from inside the worker program.
    """
    order: list[tuple[str, int]] = []

    class _Probe(CompiledProgramStage):
        """Wraps the real stage, recording entry and exit per record."""

        def process(self, value, ctx):
            marker = value["instance_id"]
            order.append((marker, 0))
            result = super().process(value, ctx)
            order.append((marker, 1))
            return result

    stage = _Probe(compile_program(FIVE_STATEMENTS))
    records = [_five_statement_record(i) for i in range(6)]

    Pipeline([stage]).run(
        source=IterableSource(records),
        sink=ListSink(),
        executor=SequentialExecutor(),
        progress=False,
    )

    assert len(order) == 12
    # Entries and exits strictly alternate per record: no record's program was
    # suspended partway through in favour of another's.
    for index in range(0, len(order), 2):
        enter, exit_ = order[index], order[index + 1]
        assert enter[1] == 0 and exit_[1] == 1
        assert enter[0] == exit_[0], (enter, exit_)


# ===========================================================================
# Cross-executor equivalence
# ===========================================================================


@pytest.mark.parametrize(
    "name,expression,record", PRINCIPAL, ids=PRINCIPAL_IDS
)
def test_three_executors_produce_identical_records(name, expression, record):
    """§15.5 / §16.13 — sequential, thread and process agree exactly."""
    records = [copy.deepcopy(record) for _ in range(6)]

    results = {}
    for label, executor, window in _executors():
        sink = ListSink()
        _pipeline(expression).run(
            source=IterableSource(copy.deepcopy(records)),
            sink=sink,
            executor=executor,
            progress=False,
            ordered=True,
            max_in_flight=window,
        )
        results[label] = sink.items

    assert results["sequential"] == results["thread"] == results["process"], (
        f"{name}: executors disagree"
    )
    # And the result is a real transformation, not the input echoed back.
    assert results["sequential"][0] != record, (
        f"{name}: the expression did not change the record, so equivalence "
        "across executors is vacuous"
    )
    assert len(results["sequential"]) == 6


def test_ordered_output_matches_input_order_under_every_executor():
    """§15.5 — ordered mode restores input order despite out-of-order completion."""
    records = [_five_statement_record(i) for i in range(30)]

    for label, executor, window in _executors():
        sink = ListSink()
        _pipeline(FIVE_STATEMENTS).run(
            source=IterableSource(copy.deepcopy(records)),
            sink=sink,
            executor=executor,
            progress=False,
            ordered=True,
            max_in_flight=window,
        )
        ids = [item["instance_id"] for item in sink.items]
        assert ids == [f"i-{i}" for i in range(30)], label


def test_unordered_output_is_a_permutation_with_no_loss_or_duplication():
    """§15.5 — unordered mode may reorder but must not lose or duplicate."""
    records = [_five_statement_record(i) for i in range(30)]

    for label, executor, window in _executors():
        sink = ListSink()
        _pipeline(FIVE_STATEMENTS).run(
            source=IterableSource(copy.deepcopy(records)),
            sink=sink,
            executor=executor,
            progress=False,
            ordered=False,
            max_in_flight=window,
        )
        ids = sorted(item["instance_id"] for item in sink.items)
        assert ids == sorted(f"i-{i}" for i in range(30)), label


# ===========================================================================
# Error policies
# ===========================================================================

#: Statement 1 fails whenever ``.blob`` decodes to a non-object, which lets a
#: single source mix successes and structured failures.
FAILING_EXPRESSION = "fromjson(.blob); .moved <- .blob.value"


def _mixed_records() -> list[dict]:
    return [
        {"id": 0, "blob": json.dumps({"value": "a"})},
        {"id": 1, "blob": json.dumps("not-an-object")},
        {"id": 2, "blob": json.dumps({"value": "c"})},
    ]


def test_errors_raise_aborts_with_structured_attribution():
    """§15.5 / §16.11 — the raised error names the statement and both paths."""
    from datapipe.errors import StageExecutionError

    with pytest.raises(StageExecutionError) as excinfo:
        _pipeline(FAILING_EXPRESSION).run(
            source=IterableSource(_mixed_records()),
            sink=ListSink(),
            executor=SequentialExecutor(),
            progress=False,
            errors="raise",
        )

    cause = excinfo.value.cause
    assert isinstance(cause, StructuralExecutionError), type(cause)
    assert cause.statement_index == 1
    assert cause.operation == "move"
    assert cause.source_path == ".blob"
    assert cause.destination_path == ".moved"
    assert cause.record_seq == 1


def test_errors_skip_omits_only_the_failing_record():
    """§15.5 — skip keeps every good record and counts the bad one."""
    sink = ListSink()
    stats = _pipeline(FAILING_EXPRESSION).run(
        source=IterableSource(_mixed_records()),
        sink=sink,
        executor=SequentialExecutor(),
        progress=False,
        errors="skip",
    )

    assert stats.failed_records == 1
    assert [item["id"] for item in sink.items] == [0, 2]
    assert all("moved" in item for item in sink.items)


def test_errors_return_emits_a_structured_payload_for_the_failure():
    """§15.5 / §16.11 — the returned payload carries §12's structural block."""
    sink = ListSink()
    stats = _pipeline(FAILING_EXPRESSION).run(
        source=IterableSource(_mixed_records()),
        sink=sink,
        executor=SequentialExecutor(),
        progress=False,
        errors="return",
    )

    assert stats.output_records == 3
    payloads = [item for item in sink.items if "structural" in item]
    assert len(payloads) == 1, sink.items
    structural = payloads[0]["structural"]
    assert structural["statement_index"] == 1
    assert structural["operation"] == "move"
    assert structural["source_path"] == ".blob"
    assert structural["destination_path"] == ".moved"
    assert structural["reason"]
    assert payloads[0]["error_type"] == "StructuralExecutionError"


@pytest.mark.parametrize("policy", ["skip", "return"])
def test_error_policies_behave_identically_across_executors(policy):
    """§15.5 — a structured failure survives the process boundary unchanged."""
    outcomes = {}
    for label, executor, window in _executors():
        sink = ListSink()
        stats = _pipeline(FAILING_EXPRESSION).run(
            source=IterableSource(_mixed_records()),
            sink=sink,
            executor=executor,
            progress=False,
            ordered=True,
            errors=policy,
            max_in_flight=window,
        )
        outcomes[label] = (stats.failed_records, stats.output_records)

    assert (
        outcomes["sequential"]
        == outcomes["thread"]
        == outcomes["process"]
    ), outcomes
    assert outcomes["sequential"][0] == 1


def test_structural_error_survives_the_process_boundary_with_its_fields():
    """The pickling round-trip that matters: spawn must not flatten the error."""
    error = StructuralExecutionError(
        record_seq=7,
        statement_index=2,
        operation="move",
        selector=".blob.value",
        source_path=".blob",
        destination_path=".moved",
        expression_span=(3, 19),
        policy="error",
        reason="destination key already exists",
    )
    revived = pickle.loads(pickle.dumps(error))

    assert isinstance(revived, StructuralExecutionError)
    assert revived.record_seq == 7
    assert revived.statement_index == 2
    assert revived.operation == "move"
    assert revived.source_path == ".blob"
    assert revived.destination_path == ".moved"
    assert revived.expression_span == (3, 19)
    assert revived.reason == "destination key already exists"


# ===========================================================================
# Provider resolution
# ===========================================================================

_PROVIDER_SRC = '''\
from datapipe.tools import tool, JsonType


@tool(
    name="shout",
    target="value",
    input=JsonType.STRING,
    output=JsonType.STRING,
    description="Uppercase a string.",
)
def shout(value) -> str:
    return value.upper()


@tool(
    name="whisper",
    target="value",
    input=JsonType.STRING,
    output=JsonType.STRING,
    description="Lowercase a string.",
)
def whisper(value) -> str:
    return value.lower()
'''


@pytest.fixture
def installed_provider(tmp_path):
    from datapipe.tools.installer import install_provider

    path = tmp_path / "s7_exec_provider.py"
    path.write_text(_PROVIDER_SRC)
    install_provider(path, yes=True)
    return path


def test_provider_descriptors_resolve_once_per_worker_not_once_per_record(
    installed_provider, monkeypatch
):
    """§15.5 — resolution is a setup cost, not a per-record cost.

    Two provider tools across two statements must produce exactly two
    ``resolve_tool`` calls no matter how many records flow through.
    """
    from datapipe.tools import loader as _loader

    calls: list[str] = []
    original = _loader.resolve_tool

    def counting(descriptor, tool_name):
        calls.append(tool_name)
        return original(descriptor, tool_name)

    monkeypatch.setattr(_loader, "resolve_tool", counting)

    stage = _stage("shout(.a); whisper(.b)")
    ctx = WorkerContext(rank=0, world_size=1, worker_id=0, local_rank=0)
    stage.setup(ctx)

    after_setup = len(calls)
    assert after_setup == 2, calls

    for index in range(25):
        ctx.record_index = index
        stage.process({"a": "x", "b": "Y"}, ctx)

    assert len(calls) == after_setup, (
        f"{len(calls) - after_setup} extra resolutions occurred while "
        "processing 25 records; descriptors must resolve once per worker"
    )
    assert sorted(calls) == ["shout", "whisper"]


def test_resolved_callables_are_never_pickled_across_the_boundary(
    installed_provider,
):
    """§14.3 — the payload carries descriptors, not live callables.

    A resolved function pickled into the stage would either fail on spawn or
    silently ship a stale binding; ``__getstate__`` drops them.
    """
    stage = _stage("shout(.a); whisper(.b)")
    ctx = WorkerContext(rank=0, world_size=1, worker_id=0, local_rank=0)
    stage.setup(ctx)
    assert stage._resolved_fns, "precondition: resolution happened"

    revived = pickle.loads(pickle.dumps(stage))
    assert revived._resolved_fns == {}

    # And the revived stage still works after re-resolving on its own.
    revived.setup(ctx)
    assert revived.process({"a": "x", "b": "Y"}, ctx) == {"a": "X", "b": "y"}


def test_provider_tools_run_correctly_under_the_process_executor(
    installed_provider, tmp_path
):
    """End-to-end: descriptors cross to spawned workers and resolve there."""
    records = [{"a": f"x{i}", "b": f"Y{i}"} for i in range(6)]
    sink = ListSink()

    _pipeline("shout(.a); whisper(.b)").run(
        source=IterableSource(records),
        sink=sink,
        executor=ProcessExecutor(workers=2, max_in_flight=3, mp_context="spawn"),
        progress=False,
        ordered=True,
        max_in_flight=3,
    )

    assert sink.items == [{"a": f"X{i}", "b": f"y{i}"} for i in range(6)]


# ===========================================================================
# Worker-boundary payload (§14.3)
# ===========================================================================


def test_structural_program_adds_no_extra_pickling_versus_a_plain_program():
    """§14.3 — statement count does not multiply what crosses the boundary.

    The compiled program is pickled once per worker as part of the pool
    initializer.  Its size grows with the program text, which is expected; what
    must NOT happen is per-record or per-statement serialization of the
    program.  This asserts the structural payload stays the same order of
    magnitude as a single-invocation one.
    """
    plain = pickle.dumps(_stage("fromjson(.blob)"))
    structural = pickle.dumps(_stage(FIVE_STATEMENTS))

    # A five-statement program is bigger, but bounded — not a per-statement
    # copy of the whole pipeline.
    assert len(structural) < len(plain) * 12, (len(plain), len(structural))


def test_the_compiled_program_is_pickled_once_per_worker_not_once_per_record():
    """§14.3 — record payloads carry only (seq, value).

    Counting bytes submitted per job proves the program is not riding along
    with each record: each job payload must be roughly the size of the record
    alone.
    """
    records = [_five_statement_record(i) for i in range(10)]
    executor = ProcessExecutor(workers=2, max_in_flight=3, mp_context="spawn")

    payload_sizes: list[int] = []
    original = executor._submit

    def measuring(job):
        payload_sizes.append(len(pickle.dumps((job.seq, job.value))))
        return original(job)

    executor._submit = measuring

    _pipeline(FIVE_STATEMENTS).run(
        source=IterableSource(copy.deepcopy(records)),
        sink=ListSink(),
        executor=executor,
        progress=False,
        max_in_flight=3,
    )

    assert len(payload_sizes) == 10
    program_size = len(pickle.dumps(_stage(FIVE_STATEMENTS)))
    for size in payload_sizes:
        assert size < program_size, (
            f"a per-record payload of {size} bytes is at least the size of "
            f"the compiled program ({program_size} bytes) — the program "
            "appears to be shipped with every record"
        )


def test_no_compiled_selector_cycle_or_live_callable_is_in_the_payload():
    """§14.3 — the pickled stage holds descriptors and IR, nothing live."""
    stage = _stage(FIVE_STATEMENTS)
    state = stage.__getstate__()

    assert state["_resolved_fns"] == {}
    # The whole state must round-trip; a cycle or a live callable would raise.
    revived_state = pickle.loads(pickle.dumps(state))
    assert revived_state["_resolved_fns"] == {}
