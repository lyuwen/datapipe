"""Phase S7 verification of the §16 definition of done.

One test per numbered criterion, named so the number is unmissable.  Where a
criterion is already covered by an S0–S6 test, this file does not restate that
test's fixtures; it asserts the criterion *as stated in §16*, which is usually a
narrower and blunter claim than the phase test that established the behavior.
The value is that a reader can point at criterion 9 and see a test called
criterion 9 that fails if criterion 9 stops being true.

Two criteria were treated as load-bearing rather than assumed, per the phase
brief: 14 (one submission per record) and 15 (no materialization, no runtime
queue).  Their rigorous forms live in ``test_structural_dsl_s7_execution.py``,
which counts real dispatches and real source pulls and was revert-verified
against four mutations of the scheduler.  The versions here are the §16
statements, cross-linked to those.

Finding recorded during this phase: criterion 12's migration suggestion was
lossy — it rendered only the selector, so ``fromjson(.a, recursive=true)`` was
suggested as ``fromjson(.a)``, advice that compiles but changes the output.
Fixed in ``datapipe/dsl/compiler.py``; ``test_criterion_12_...`` now asserts the
suggestion round-trips to the same record.
"""

from __future__ import annotations

import copy
import json
import warnings

import pytest

from datapipe.cli.transform import _compile_or_report, describe_compiled
from datapipe.context import WorkerContext
from datapipe.dsl.compiler import (
    CompiledProgram,
    compile_expression,
    compile_program,
)
from datapipe.execution.process import ProcessExecutor
from datapipe.execution.sequential import SequentialExecutor
from datapipe.execution.thread import ThreadExecutor
from datapipe.io.iterable import IterableSource, ListSink
from datapipe.pipeline import Pipeline
from datapipe.stages.tool_program import (
    CompiledProgramStage,
    CompiledToolProgramStage,
)
from datapipe.tools.errors import StructuralExecutionError


@pytest.fixture(autouse=True)
def isolated_registry(tmp_path, monkeypatch):
    """Redirect the tool registry at tmp_path and clear the loader cache."""
    monkeypatch.setenv("DATAPIPE_USER_DATA", str(tmp_path / "dp_data"))

    from datapipe.tools import loader as _loader

    _loader._loaded_providers.clear()

    from datapipe.dsl import compiler as _compiler

    monkeypatch.setattr(_compiler, "_BUILTIN_REGISTRY", None, raising=False)


def _stage(expression: str):
    compiled = compile_program(expression)
    return CompiledProgramStage(compiled)


def _run(expression: str, record):
    """Compile and execute *expression* against a copy of *record*."""
    return _stage(expression).process(copy.deepcopy(record), None)


def _run_via_cli_router(expression: str, record):
    """Execute through the CLI's compile router, as `datapipe transform` does."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        compiled = _compile_or_report(expression)
    assert compiled is not None, f"failed to compile: {expression}"
    stage = (
        CompiledProgramStage(compiled)
        if isinstance(compiled, CompiledProgram)
        else CompiledToolProgramStage(compiled)
    )
    return stage.process(copy.deepcopy(record), None)


# ===========================================================================
# 1. `;` sequences independent record mutations inside one worker invocation.
# ===========================================================================


def test_criterion_1_semicolon_sequences_mutations_in_one_worker_invocation():
    record = {"a": json.dumps({"x": 1}), "b": json.dumps({"y": 2}), "c": 3}
    result = _run("fromjson(.a); fromjson(.b); tojson(.c)", record)

    # All three mutations landed on the same evolving record.
    assert result == {"a": {"x": 1}, "b": {"y": 2}, "c": "3"}

    # And they ran inside a single worker call: one process() entry produced
    # every mutation, which is what "one worker invocation" means here.
    calls = []

    class _Counting(CompiledProgramStage):
        def process(self, value, ctx):
            calls.append(1)
            return super().process(value, ctx)

    stage = _Counting(compile_program("fromjson(.a); fromjson(.b); tojson(.c)"))
    assert stage.process(copy.deepcopy(record), None) == result
    assert len(calls) == 1, calls


# ===========================================================================
# 2. `|` chains transformations on a well-defined current target.
# ===========================================================================


def test_criterion_2_pipe_chains_transformations_on_the_current_target():
    # `<<` leaves .metadata focused; both bare tools then apply to .metadata,
    # and the emitted value is still the whole root record.
    record = {"instance_id": "i", "a": 1, "b": 2}
    result = _run(".metadata << .(a|b) | tojson", record)

    assert result["instance_id"] == "i"
    assert result["metadata"] == '{"a":1,"b":2}'
    assert "a" not in result and "b" not in result

    # The focus is the destination, not the root: tojson serialized only
    # .metadata, leaving the root a dict rather than a JSON string.
    assert isinstance(result, dict)


def test_criterion_2_focused_pipe_target_is_a_selector_not_the_root():
    record = {"note": "  hi  ", "other": 1}
    result = _run(".note | tojson", record)

    assert result["note"] == '"  hi  "'
    assert result["other"] == 1


# ===========================================================================
# 3. Existing `tool(path)` syntax remains in-place shorthand.
# ===========================================================================


def test_criterion_3_tool_path_syntax_remains_in_place_shorthand():
    record = {"tools": json.dumps([{"n": 1}]), "keep": "untouched"}
    result = _run("fromjson(.tools)", record)

    # The value at the path is replaced in place; nothing else moves.
    assert result == {"tools": [{"n": 1}], "keep": "untouched"}
    assert list(result) == list(record), "key order changed"


def test_criterion_3_wildcard_shorthand_applies_elementwise_in_place():
    record = {"tools": [{"p": {"a": 1}}, {"p": {"b": 2}}]}
    result = _run("tojson(.tools[].p)", record)

    assert result == {"tools": [{"p": '{"a":1}'}, {"p": '{"b":2}'}]}


# ===========================================================================
# 4. `=` copies without deleting the source.
# ===========================================================================


def test_criterion_4_equals_copies_and_retains_the_source():
    record = {"metadata": {"temperature": 0.5, "note": "n"}}
    result = _run(".temperature = .metadata.temperature", record)

    assert result["temperature"] == 0.5
    assert result["metadata"]["temperature"] == 0.5, "source was deleted by ="


def test_criterion_4_copied_container_is_not_an_alias_of_the_source():
    """The S3 aliasing bug: a copy must not share mutable state."""
    record = {"metadata": {"inner": {"k": "v"}}}
    result = _run(".copied = .metadata.inner", record)

    assert result["copied"] == {"k": "v"}
    result["copied"]["k"] = "changed"
    assert result["metadata"]["inner"]["k"] == "v", (
        "mutating the copy changed the source — the copy is an alias"
    )


# ===========================================================================
# 5. `<-` moves only after destination validation succeeds.
# ===========================================================================


def test_criterion_5_move_validates_the_destination_before_removing_the_source():
    # A destination whose parent is the wrong type makes the write impossible.
    record = {"parent": "a string", "src": {"v": 1}}
    before = copy.deepcopy(record)

    with pytest.raises(StructuralExecutionError):
        _stage(".parent.child <- .src").process(record, None)

    assert record == before, (
        "the record was modified despite the move failing — validation did "
        "not precede mutation"
    )
    assert record["src"] == {"v": 1}, "source removed before the write succeeded"


def test_criterion_5_move_removes_the_source_once_the_write_succeeds():
    record = {"metadata": {"temperature": 0.5, "note": "n"}}
    result = _run(".temperature <- .metadata.temperature", record)

    assert result["temperature"] == 0.5
    assert "temperature" not in result["metadata"]
    assert result["metadata"]["note"] == "n"


# ===========================================================================
# 6. `<<` supports explicit lists, positive field sets, and complement sets.
# ===========================================================================


def test_criterion_6_move_into_supports_all_three_source_forms():
    record = {
        "instance_id": "i",
        "messages": [],
        "tools": [],
        "annotation_key": "k",
        "temperature": 0.7,
        "score": 3,
    }
    explicit = _run(
        ".metadata << .annotation_key, .temperature, .score", record
    )
    positive = _run(".metadata << .(annotation_key|temperature|score)", record)
    complement = _run(".metadata << .(^instance_id|messages|tools)", record)

    expected_metadata = {"annotation_key": "k", "temperature": 0.7, "score": 3}
    assert explicit["metadata"] == expected_metadata
    assert positive["metadata"] == expected_metadata
    assert complement["metadata"] == expected_metadata

    # All three forms agree on the whole record, not just the destination.
    assert explicit == positive == complement
    for key in ("annotation_key", "temperature", "score"):
        assert key not in explicit


# ===========================================================================
# 7. Blanket nesting automatically excludes the destination key.
# ===========================================================================


def test_criterion_7_blanket_nesting_self_excludes_the_destination_key():
    # .metadata already exists and is NOT named in the complement's exclusions,
    # so a naive implementation would try to move .metadata into itself.
    record = {
        "instance_id": "i",
        "metadata": {"pre_existing": True},
        "a": 1,
        "b": 2,
    }
    result = _run(".metadata << .(^instance_id)", record)

    assert result["instance_id"] == "i"
    assert result["metadata"]["a"] == 1
    assert result["metadata"]["b"] == 2
    # The destination did not nest inside itself.
    assert "metadata" not in result["metadata"], "destination nested into itself"


# ===========================================================================
# 8. `nest` and `unnest` match their symbolic desugarings.
# ===========================================================================


def test_criterion_8_nest_matches_its_symbolic_desugaring():
    record = {
        "instance_id": "i",
        "messages": [],
        "tools": [],
        "annotation_key": "k",
        "temperature": 0.7,
        "score": 3,
    }
    symbolic = _run(".metadata << .(^instance_id|messages|tools) | tojson", record)
    named = _run(
        'nest(., key="metadata", '
        'exclude=["instance_id", "messages", "tools"], jsonify=true)',
        record,
    )

    assert symbolic == named
    # Non-vacuous: the operation really did something.
    assert isinstance(named["metadata"], str) and named["metadata"] != "{}"


def test_criterion_8_unnest_matches_its_symbolic_desugaring():
    record = {"instance_id": "i", "metadata": {"temperature": 0.5, "score": 3}}
    named = _run('unnest(., key="metadata")', record)
    symbolic = _run(". << .metadata.(temperature|score)", record)

    assert named == symbolic
    assert named["temperature"] == 0.5 and named["score"] == 3
    assert named["metadata"] == {}


# ===========================================================================
# 9. Moving values out of serialized metadata and reserializing it works.
# ===========================================================================


def test_criterion_9_move_out_of_serialized_metadata_and_reserialize():
    record = {
        "instance_id": "i",
        "metadata": json.dumps(
            {"temperature": "0.5", "score": 3, "note": "keep"}
        ),
    }
    result = _run(
        "fromjson(.metadata); "
        ".temperature <- fromjson(.metadata.temperature); "
        "tojson(.metadata)",
        record,
    )

    # The moved value was decoded on the way out.
    assert result["temperature"] == 0.5
    # Metadata came back as a JSON string, minus the moved key.
    assert isinstance(result["metadata"], str)
    remaining = json.loads(result["metadata"])
    assert remaining == {"score": 3, "note": "keep"}


def test_criterion_9_copy_variant_leaves_the_value_in_metadata():
    record = {
        "metadata": json.dumps({"temperature": 0.5, "note": "keep"}),
    }
    result = _run(
        "fromjson(.metadata); .temperature = .metadata.temperature; "
        "tojson(.metadata)",
        record,
    )

    assert result["temperature"] == 0.5
    assert json.loads(result["metadata"]) == {"temperature": 0.5, "note": "keep"}


# ===========================================================================
# 10. Several value transformations can precede a whole-record operation.
# ===========================================================================


def test_criterion_10_value_transformations_precede_a_whole_record_operation():
    record = {
        "instance_id": "i",
        "tools": json.dumps([{"n": 1}]),
        "extra": 5,
    }
    result = _run(
        "fromjson(.tools); "
        'nest(., key="metadata", exclude=["instance_id", "tools"])',
        record,
    )

    # The whole-record operation observed the earlier per-value mutations.
    assert result["tools"] == [{"n": 1}], "record op ran before the value ops"
    assert result["metadata"] == {"extra": 5}
    assert result["instance_id"] == "i"


# ===========================================================================
# 11. Every structural failure includes statement and concrete path attribution.
# ===========================================================================


@pytest.mark.parametrize(
    "expression,record,statement_index",
    [
        (".dst <- .missing", {"a": 1}, 0),
        ("fromjson(.blob); .x <- .blob.x", {"blob": '"str"'}, 1),
        (
            "fromjson(.blob); .a = .blob.a; .dst.child <- .a",
            {"blob": '{"a": 1}', "dst": "not-a-dict"},
            2,
        ),
    ],
)
def test_criterion_11_structural_failures_carry_statement_and_path(
    expression, record, statement_index
):
    with pytest.raises(StructuralExecutionError) as excinfo:
        _stage(expression).process(copy.deepcopy(record), None)

    error = excinfo.value
    assert error.statement_index == statement_index
    assert error.operation in ("move", "copy", "move-into")
    # Concrete paths, not the raw selector text.
    assert error.source_path is not None or error.destination_path is not None
    assert error.reason, "no reason given"
    # The rendered message names the statement and a concrete path.
    message = str(error)
    assert f"statement: {statement_index}" in message
    if error.destination_path:
        assert error.destination_path in message


# ===========================================================================
# 12. Legacy `|` sequencing has an actionable migration path.
# ===========================================================================


def test_criterion_12_legacy_pipe_suggestion_is_a_faithful_rewrite():
    """The suggestion must be actionable: following it must not change output.

    This is the criterion that failed when first tested.  The suggestion
    rendered only the selector, dropping ``recursive=true``, so a user who
    followed the advice silently stopped decoding nested payloads.
    """
    legacy = "fromjson(.tools) | fromjson(.metadata.annotation, recursive=true)"
    record = {
        "tools": "[1]",
        "metadata": {"annotation": json.dumps({"a": json.dumps({"b": 1})})},
    }

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        # The warning is raised by the library compiler, which is where a
        # caller filtering on the category receives it.  The CLI renders its
        # own `warning:` line instead of re-raising, so asserting the message
        # here keeps this criterion pinned to the source of the diagnostic.
        compile_expression(legacy)

    deprecations = [
        w for w in caught if issubclass(w.category, DeprecationWarning)
    ]
    assert deprecations, "no migration warning was emitted"
    message = str(deprecations[0].message)
    assert "use semicolons" in message

    suggested = message.split("\n")[1].strip()
    assert ";" in suggested and "|" not in suggested

    # The suggested rewrite must be a drop-in: same record, same result.
    assert _run_via_cli_router(suggested, record) == _run_via_cli_router(
        legacy, record
    )
    # Non-vacuous: the argument that was previously dropped is present.
    assert "recursive=true" in suggested


def test_criterion_12_suggestion_omits_arguments_left_at_their_defaults():
    """The rewrite stays readable: only non-default arguments are rendered."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        compile_expression("fromjson(.a) | fromjson(.b)")

    suggested = str(caught[0].message).split("\n")[1].strip()
    assert suggested == "fromjson(.a); fromjson(.b)", suggested


# ===========================================================================
# 13. Sequential, thread, and process execution produce equivalent records.
# ===========================================================================


def test_criterion_13_three_executors_produce_equivalent_records():
    expression = (
        "fromjson(.blob); .kept = .blob.a; .moved <- .blob.b; "
        ".metadata << .(^instance_id|blob|kept|moved); tojson(.metadata)"
    )
    records = [
        {
            "instance_id": f"i-{i}",
            "blob": json.dumps({"a": i, "b": i * 2}),
            "extra": i * 3,
        }
        for i in range(10)
    ]

    outputs = {}
    for label, executor, window in (
        ("sequential", SequentialExecutor(), None),
        ("thread", ThreadExecutor(workers=2, max_in_flight=4), 4),
        ("process", ProcessExecutor(workers=2, max_in_flight=4), 4),
    ):
        sink = ListSink()
        Pipeline([_stage(expression)]).run(
            source=IterableSource(copy.deepcopy(records)),
            sink=sink,
            executor=executor,
            progress=False,
            ordered=True,
            max_in_flight=window,
        )
        outputs[label] = sink.items

    assert outputs["sequential"] == outputs["thread"] == outputs["process"]
    assert len(outputs["sequential"]) == 10
    # Non-vacuous: the program transformed the records.
    assert outputs["sequential"][0] != records[0]


# ===========================================================================
# 14. A record is submitted once and gathered once regardless of statement count.
# ===========================================================================


def test_criterion_14_one_submission_and_one_gather_per_record():
    """Counts real dispatches and real gathers; see the s7_execution suite.

    ``test_structural_dsl_s7_execution.py`` holds the rigorous form of this
    (every principal expression, every executor, revert-verified).  Here the
    §16 sentence is asserted directly: a five-statement program over N records
    yields exactly N submissions and exactly N results.
    """
    expression = (
        "fromjson(.blob); .kept = .blob.a; .moved <- .blob.b; "
        ".metadata << .(^instance_id|blob|kept|moved); tojson(.metadata)"
    )
    records = [
        {"instance_id": f"i-{i}", "blob": json.dumps({"a": i, "b": i}), "e": i}
        for i in range(15)
    ]

    executor = ThreadExecutor(workers=3, max_in_flight=4)
    submissions: list[int] = []
    original_submit = executor._submit

    def counting_submit(job):
        submissions.append(job.seq)
        return original_submit(job)

    executor._submit = counting_submit

    gathers: list[int] = []
    sink = ListSink()
    pipeline = Pipeline([_stage(expression)])
    original_emit = Pipeline._emit

    def counting_emit(result, *args, **kwargs):
        gathers.append(result.seq)
        return original_emit(result, *args, **kwargs)

    pipeline._emit = staticmethod(counting_emit)
    Pipeline._emit = staticmethod(counting_emit)
    try:
        pipeline.run(
            source=IterableSource(copy.deepcopy(records)),
            sink=sink,
            executor=executor,
            progress=False,
            max_in_flight=4,
        )
    finally:
        Pipeline._emit = staticmethod(original_emit)

    assert sorted(submissions) == list(range(15)), submissions
    assert sorted(gathers) == list(range(15)), gathers
    assert len(sink.items) == 15


# ===========================================================================
# 15. No structural operation materializes the dataset or adds a runtime queue.
# ===========================================================================


def test_criterion_15_structural_operations_do_not_materialize_the_dataset():
    """Observes how much of the source was pulled when the first output landed.

    The rigorous form (200 records, per-write tracking, revert-verified against
    a materializing scheduler) lives in the s7_execution suite.
    """
    expression = (
        "fromjson(.blob); .kept = .blob.a; "
        ".metadata << .(^instance_id|blob|kept); tojson(.metadata)"
    )
    total = 120
    window = 4
    pulled = {"count": 0}

    def source_records():
        for i in range(total):
            pulled["count"] += 1
            yield {
                "instance_id": f"i-{i}",
                "blob": json.dumps({"a": i}),
                "extra": i,
            }

    pulled_at_first_write: list[int] = []

    def sink(_record):
        if not pulled_at_first_write:
            pulled_at_first_write.append(pulled["count"])

    Pipeline([_stage(expression)]).run(
        source=IterableSource(source_records()),
        sink=sink,
        executor=ThreadExecutor(workers=2, max_in_flight=window),
        progress=False,
        max_in_flight=window,
    )

    assert pulled_at_first_write, "no record was ever written"
    assert pulled_at_first_write[0] <= window + 2, (
        f"{pulled_at_first_write[0]} of {total} records were pulled before "
        "the first output — the dataset appears to be materialized"
    )


def test_criterion_15_statements_add_no_queue_between_them():
    """All statements of a record run in one uninterrupted call frame.

    A queue between statements would let another record's statements interleave.
    """
    expression = "fromjson(.blob); .kept = .blob.a; tojson(.kept)"
    trace: list[str] = []

    class _Tracing(CompiledProgramStage):
        def process(self, value, ctx):
            marker = value["instance_id"]
            trace.append(f"enter:{marker}")
            result = super().process(value, ctx)
            trace.append(f"exit:{marker}")
            return result

    records = [
        {"instance_id": f"i-{i}", "blob": json.dumps({"a": i})}
        for i in range(5)
    ]
    Pipeline([_Tracing(compile_program(expression))]).run(
        source=IterableSource(records),
        sink=ListSink(),
        executor=SequentialExecutor(),
        progress=False,
    )

    assert trace == [
        item
        for i in range(5)
        for item in (f"enter:i-{i}", f"exit:i-{i}")
    ], trace


# ===========================================================================
# 16. Inspection shows statement, focus, tool, and provider resolution.
# ===========================================================================


def test_criterion_16_inspection_shows_statement_focus_tool_and_provider():
    expression = (
        ".metadata << .(^instance_id|messages) | tojson; fromjson(.blob)"
    )
    compiled = _compile_or_report(expression)
    described = describe_compiled(compiled, expression)

    statements = described["statements"]
    assert len(statements) == 2, described

    first = statements[0]
    # Statement identity.
    assert first["index"] == 0
    # Focus.
    assert first["focus"] == ".metadata"
    # Operation and its structural detail.
    assert first["operation"]["kind"] == "move_into"
    assert first["operation"]["destination"] == ".metadata"
    assert first["operation"]["sources"][0]["complement"] is True

    # Tool and provider resolution for the focused pipe.
    pipe = first["pipes"][0]
    assert pipe["tool"] == "tojson"
    assert pipe["provider"]["provider_id"] == "builtin"
    assert pipe["provider"]["mode"] == "builtin"

    second = statements[1]
    assert second["index"] == 1
    assert second["operation"]["tool"] == "fromjson"
    assert second["operation"]["provider"]["provider_id"] == "builtin"


def test_criterion_16_inspection_names_an_installed_provider_not_just_builtins():
    """Provider resolution must be visible for real installed providers too."""
    from datapipe.tools.installer import install_provider

    import tempfile
    from pathlib import Path

    source = '''\
from datapipe.tools import tool, JsonType


@tool(
    name="shout_s7",
    target="value",
    input=JsonType.STRING,
    output=JsonType.STRING,
    description="Uppercase.",
)
def shout_s7(value) -> str:
    return value.upper()
'''
    directory = Path(tempfile.mkdtemp())
    path = directory / "s7_done_provider.py"
    path.write_text(source)
    install_provider(path, yes=True)

    # A multi-statement expression so the CLI router produces a CompiledProgram
    # (a lone invocation still compiles to the legacy single-expression form,
    # which reports "invocations" rather than "statements").
    expression = "shout_s7(.a); tojson(.b)"
    described = describe_compiled(_compile_or_report(expression), expression)
    operation = described["statements"][0]["operation"]

    assert operation["tool"] == "shout_s7"
    provider = operation["provider"]
    assert provider["provider_id"] != "builtin"
    assert provider["mode"] in ("copied", "editable")
    assert provider["alias"] == "s7_done_provider"


# ===========================================================================
# 17. Documentation includes every normative example from Section 7.
# ===========================================================================

#: The distinctive *construct* each §7 case introduces, as a regex matched
#: against user documentation.  Patterns rather than the plan's literal strings
#: because the docs legitimately use their own field names — what §16.17
#: requires is that every normative form is shown, not that the examples are
#: copied verbatim.  Each pattern is narrow enough that only that case's shape
#: satisfies it.
SECTION_7_PATTERNS = {
    "7.1 deserialize in place, with an argument": (
        r"fromjson\(\.[\w.]+, *recursive=true\)"
    ),
    "7.2 serialize a nested wildcard path": (
        r"tojson\(\.\w+\[\]\.[\w.]+\)"
    ),
    "7.3 several independent serializations": (
        r"tojson\(\.\w+\); *tojson\(\.\w+\)"
    ),
    "7.4 value operations then a whole-record operation": (
        r"\|[^'\n]*;\s*nest\(\.,"
    ),
    "7.5 explicit comma-separated move list": r"<< *\.\w+, *\.\w+",
    "7.6 positive field set": r"<< *\.\((?!\^)[\w|]+\)",
    "7.7 complement field set": r"<< *\.\(\^[\w|]+\)",
    "7.8 configurable nest form": r"nest\(\., *key=",
    "7.9 move fields out of metadata": r"\. *<< *\.metadata\.\([\w|]+\)",
    "7.10 transformed move": r"<- *fromjson\(",
    "7.11 copy rather than move": r"\.\w+ = \.\w+\.\w+",
    "7.12 move-into followed by chained bare tools": (
        # Two bare tool calls after the move-into.  The field-set's own `|`
        # is excluded by requiring the pipes to follow the closing paren.
        r"<<[^'\n]*\)\s*\|\s*\w+\s*\|\s*\w+"
    ),
    "7.13 legacy pipe form": r"fromjson\(\.\w+\) *\| *\w+\(",
}


def _documentation_text() -> str:
    from pathlib import Path

    docs = Path(__file__).resolve().parent.parent / "docs"
    names = ["cli.md", "concepts.md", "migration-guide.md"]
    return "\n".join((docs / name).read_text() for name in names)


def test_criterion_17_documentation_carries_every_section_7_construct():
    """Each §7 case's distinctive construct appears in user documentation.

    The plan document is deliberately excluded: §16.17 is about the docs a user
    reads, and matching against the plan would make this vacuous.
    """
    import re

    text = _documentation_text()
    assert len(text) > 5000, "documentation did not load; the check would be vacuous"

    missing = [
        case
        for case, pattern in SECTION_7_PATTERNS.items()
        if not re.search(pattern, text, re.M)
    ]
    assert not missing, f"§7 constructs absent from docs/: {missing}"


def test_criterion_17_the_pattern_set_is_not_trivially_satisfiable():
    """Guards the guard: these patterns must not match arbitrary prose.

    A regex loose enough to match anything would make the criterion vacuous —
    exactly the doc-guard failure mode this project has already hit once.
    """
    import re

    filler = "the quick brown fox jumps over the lazy dog. " * 200
    matched = [
        case
        for case, pattern in SECTION_7_PATTERNS.items()
        if re.search(pattern, filler, re.M)
    ]
    assert not matched, f"patterns match unrelated prose: {matched}"
    assert len(SECTION_7_PATTERNS) == 13, "one pattern per §7 case"


def test_criterion_17_the_documented_structural_examples_still_execute():
    """Documented patterns are not just present as text — they compile and run.

    Complements the S6 doc-guard, which extracts and executes every documented
    expression.  This is the §16.17 statement narrowed to the §7 catalogue.
    """
    record = {
        "instance_id": "i",
        "messages": [],
        "tools": [],
        "annotation_key": "k",
        "temperature": 0.7,
        "score": 3,
    }
    executable = [
        ".metadata << .annotation_key, .temperature, .score | tojson",
        ".metadata << .(annotation_key|temperature|score) | tojson",
        ".metadata << .(^instance_id|messages|tools) | tojson",
        'nest(., key="metadata", '
        'exclude=["instance_id", "messages", "tools"], jsonify=true)',
    ]
    results = [_run(expression, record) for expression in executable]

    for result in results:
        assert isinstance(result["metadata"], str)
        assert json.loads(result["metadata"]) == {
            "annotation_key": "k",
            "temperature": 0.7,
            "score": 3,
        }
    # All four forms are equivalent, which is the §15.4 claim §7.8 rests on.
    assert all(result == results[0] for result in results)
