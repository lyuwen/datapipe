"""Phase S3 tests: `=` copy assignment and `<-` exact move.

TDD: written before the implementation.  The core guarantees under test are
the §8.1 resolve-before-mutate ordering and the atomicity rule — a statement
that fails any precondition must leave the record completely unmodified.
"""

from __future__ import annotations

import json
import pickle

import pytest

from datapipe.dsl.ast import Assignment, AssignmentRHS, Invocation, Program, Selector
from datapipe.dsl.errors import (
    ExpressionSyntaxError,
    ToolConfigurationError,
    ToolResolutionError,
)
from datapipe.dsl.lexer import TT, tokenize
from datapipe.dsl.parser import parse_program
from datapipe.tools.errors import StructuralExecutionError, ToolExecutionError


@pytest.fixture(autouse=True)
def isolated_registry(tmp_path, monkeypatch):
    """Redirect the tool registry at tmp_path and clear the loader cache."""
    monkeypatch.setenv("DATAPIPE_USER_DATA", str(tmp_path / "dp_data"))

    from datapipe.tools import loader as _loader
    _loader._loaded_providers.clear()

    from datapipe.dsl import compiler as _compiler
    monkeypatch.setattr(_compiler, "_BUILTIN_REGISTRY", None, raising=False)


def _stage(expression: str, validate: str = "always"):
    from datapipe.dsl.compiler import compile_program
    from datapipe.stages.tool_program import CompiledProgramStage

    return CompiledProgramStage(compile_program(expression), validate=validate)


def _run_jsonl(expression, record, tmp_path, executor):
    from datapipe.io.jsonl import JsonlSink, JsonlSource
    from datapipe.pipeline import Pipeline
    from datapipe.stage import JsonDumpStage, JsonLoadStage

    tmp_path.mkdir(parents=True, exist_ok=True)
    pipeline = Pipeline([JsonLoadStage(), _stage(expression), JsonDumpStage()])
    inp = tmp_path / "in.jsonl"
    out = tmp_path / "out.jsonl"
    inp.write_text(json.dumps(record) + "\n")
    pipeline.run(
        source=JsonlSource(str(inp), raw=True),
        sink=JsonlSink(str(out), raw=True),
        executor=executor,
    )
    return json.loads(out.read_text().strip())


# ===========================================================================
# Lexer / parser
# ===========================================================================


# 1. `<-` tokenizes as one ARROW_LEFT token
def test_arrow_left_is_a_single_token():
    types = [t.type for t in tokenize(".a <- .b")]
    assert TT.ARROW_LEFT in types
    # Exactly one arrow, and no stray token between the selectors.
    assert types.count(TT.ARROW_LEFT) == 1
    assert types == [
        TT.DOT, TT.IDENT, TT.ARROW_LEFT, TT.DOT, TT.IDENT, TT.EOF,
    ]


def test_arrow_left_without_space_tokenizes():
    types = [t.type for t in tokenize(".a<-.b")]
    assert types == [TT.DOT, TT.IDENT, TT.ARROW_LEFT, TT.DOT, TT.IDENT, TT.EOF]


def test_bare_less_than_is_still_a_lex_error():
    """`<` alone has no meaning; `<<` arrives in S4, not here."""
    with pytest.raises(ExpressionSyntaxError):
        tokenize(".a < .b")


# 2. `.a = .b` parses to an Assignment with is_move=False
def test_parse_copy_assignment():
    prog = parse_program(".a = .b")
    assert isinstance(prog, Program)
    assert len(prog.statements) == 1
    stmt = prog.statements[0]
    op = stmt.operation
    assert isinstance(op, Assignment)
    assert op.is_move is False
    assert isinstance(op.destination, Selector)
    assert op.destination.parts[0].name == "a"
    assert isinstance(op.rhs, AssignmentRHS)
    assert op.rhs.transform is None
    assert op.rhs.source.parts[0].name == "b"


# 3. `.a <- .b` parses with is_move=True
def test_parse_move_assignment():
    stmt = parse_program(".a <- .b").statements[0]
    op = stmt.operation
    assert isinstance(op, Assignment)
    assert op.is_move is True
    assert op.destination.parts[0].name == "a"
    assert op.rhs.source.parts[0].name == "b"
    assert op.rhs.transform is None


# 4. `.a = fromjson(.b)` parses with transform set and source = .b
def test_parse_transformed_rhs():
    op = parse_program(".a = fromjson(.b)").statements[0].operation
    assert isinstance(op, Assignment)
    assert isinstance(op.rhs.transform, Invocation)
    assert op.rhs.transform.qualified_name.name == "fromjson"
    assert op.rhs.source.parts[0].name == "b"
    # The primary source is the invocation's own selector argument.
    assert op.rhs.source is op.rhs.transform.selector


# 5. Keyword arguments still parse (statement-level `=` must not break them)
def test_keyword_arguments_still_parse_inside_rhs():
    op = parse_program(".a = fromjson(.b, recursive=true)").statements[0].operation
    args = op.rhs.transform.arguments
    assert [(a.name, a.value.value) for a in args] == [("recursive", True)]


def test_keyword_arguments_still_parse_in_plain_invocation():
    stmt = parse_program("fromjson(.b, recursive=true)").statements[0]
    assert [(a.name, a.value.value) for a in stmt.operation.arguments] == [
        ("recursive", True)
    ]


# 6. `.a = .b; .c <- .d` parses as two statements
def test_parse_two_assignment_statements():
    prog = parse_program(".a = .b; .c <- .d")
    assert len(prog.statements) == 2
    assert prog.statements[0].operation.is_move is False
    assert prog.statements[1].operation.is_move is True


def test_assignment_mixes_with_s2_statements():
    prog = parse_program(".m | fromjson; .t <- .m.t; tojson(.m)")
    assert len(prog.statements) == 3
    assert isinstance(prog.statements[1].operation, Assignment)


def test_assignment_focus_is_the_destination():
    """Trailing pipes operate on the destination, so it is the published focus."""
    stmt = parse_program(".a = .b | tojson").statements[0]
    assert stmt.focus_selector is stmt.operation.destination
    assert len(stmt.pipes) == 1
    assert stmt.pipes[0].qualified_name.name == "tojson"


def test_rhs_must_be_a_selector_or_invocation():
    with pytest.raises(ExpressionSyntaxError):
        parse_program(".a = 42")


def test_empty_rhs_rejected():
    with pytest.raises(ExpressionSyntaxError):
        parse_program(".a =")


# ===========================================================================
# Compile-time rejection
# ===========================================================================


# 7. `.a <- .a.b` rejected (destination inside source subtree)
def test_reject_move_destination_inside_source():
    from datapipe.dsl.compiler import compile_program

    with pytest.raises(ToolConfigurationError) as exc:
        compile_program(".a <- .a.b")
    assert "overlap" in str(exc.value).lower()


# 8. `.a.b <- .a` rejected (source is an ancestor of destination)
def test_reject_move_source_ancestor_of_destination():
    from datapipe.dsl.compiler import compile_program

    with pytest.raises(ToolConfigurationError) as exc:
        compile_program(".a.b <- .a")
    assert "overlap" in str(exc.value).lower()


# 9. `.a = .a` rejected (self-copy no-op)
def test_reject_self_copy():
    from datapipe.dsl.compiler import compile_program

    with pytest.raises(ToolConfigurationError) as exc:
        compile_program(".a = .a")
    assert "overlap" in str(exc.value).lower()


def test_reject_self_copy_through_quoted_key_alias():
    """``["a"]`` and ``.a`` name the same key, so this is still a self-copy."""
    from datapipe.dsl.compiler import compile_program

    with pytest.raises(ToolConfigurationError):
        compile_program('.a = .["a"]')


def test_reject_copy_overlap_too():
    from datapipe.dsl.compiler import compile_program

    with pytest.raises(ToolConfigurationError):
        compile_program(".a = .a.b")


# 10. A move with an unidentifiable RHS source is rejected, pointing at `=`
def test_reject_move_without_identifiable_source():
    from datapipe.dsl.compiler import compile_program

    with pytest.raises(ToolConfigurationError) as exc:
        compile_program(".a <- fromjson(.)")
    message = str(exc.value)
    assert "'='" in message or "`=`" in message
    assert "source" in message.lower()


def test_copy_from_root_transform_is_allowed():
    """The identifiable-source rule is a *move* rule; `=` may compute freely."""
    from datapipe.dsl.compiler import compile_program

    compiled = compile_program(".a = tojson(.)")
    assert len(compiled.statements) == 1


def test_wildcard_source_is_not_statically_rejected():
    """A wildcard path is not statically comparable — it is a runtime check."""
    from datapipe.dsl.compiler import compile_program

    compile_program(".a = .items[]")


def test_unknown_tool_in_rhs_is_rejected():
    from datapipe.dsl.compiler import compile_program

    with pytest.raises(ToolResolutionError):
        compile_program(".a = no_such_tool(.b)")


# ---------------------------------------------------------------------------
# Compiled IR shape
# ---------------------------------------------------------------------------


def test_compiled_assignment_shape():
    from datapipe.dsl.compiler import CompiledAssignment, compile_program

    stmt = compile_program(".a = fromjson(.b)").statements[0]
    op = stmt.operation
    assert isinstance(op, CompiledAssignment)
    assert op.destination.render() == ".a"
    assert op.source.render() == ".b"
    assert op.transform is not None
    assert op.is_move is False
    assert isinstance(op.span, tuple) and len(op.span) == 2


def test_expression_indices_unique_with_assignments():
    """Transforms and pipes share the program-global numbering.

    The program below has four callables — the ``.d <- .e`` move has no
    transform and so contributes none.
    """
    from datapipe.dsl.compiler import CompiledAssignment, compile_program

    compiled = compile_program(
        ".a = fromjson(.b) | tojson; fromjson(.c) | tojson; .d <- .e"
    )
    indices = []
    for stmt in compiled.statements:
        op = stmt.operation
        if isinstance(op, CompiledAssignment):
            if op.transform is not None:
                indices.append(op.transform.expression_index)
        else:
            indices.append(op.expression_index)
        indices.extend(b.expression_index for b in stmt.pipes)
    assert len(indices) == 4
    assert len(set(indices)) == 4, f"duplicate expression_index: {indices}"


def test_stage_resolves_every_callable_in_a_mixed_program():
    """_expected_fn_count must agree with what _resolve_all populates.

    A mismatch would make _resolve_all's early return short-circuit with a
    partially populated dict, and the missing key would KeyError per record.
    """
    stage = _stage(".a = fromjson(.b) | tojson; fromjson(.c) | tojson; .d <- .e")
    stage._resolve_all()
    assert len(stage._resolved_fns) == stage._expected_fn_count() == 4


def test_transformless_assignment_needs_no_callables():
    stage = _stage(".a = .b")
    stage._resolve_all()
    assert stage._expected_fn_count() == 0
    assert stage._resolved_fns == {}
    # The early-return path must not stop a later process() from working.
    assert stage.process({"b": 1}, None) == {"a": 1, "b": 1}


# ===========================================================================
# Runtime — copy
# ===========================================================================


# 11. Copy leaves the source present
def test_copy_preserves_source():
    stage = _stage(".temperature = .metadata.temperature")
    out = stage.process({"metadata": {"temperature": 0.7}}, None)
    assert out == {"metadata": {"temperature": 0.7}, "temperature": 0.7}


# 12. Transformed copy decodes and leaves the source string untouched
def test_transformed_copy_preserves_source():
    stage = _stage(".t = fromjson(.metadata.t)")
    out = stage.process({"metadata": {"t": "[1,2]"}}, None)
    assert out == {"metadata": {"t": "[1,2]"}, "t": [1, 2]}


def test_copy_overwrites_an_existing_destination():
    """§8.4 collision=error governs `<<` in S4, not explicit-path assignment."""
    stage = _stage(".a = .b")
    assert stage.process({"a": "old", "b": "new"}, None) == {"a": "new", "b": "new"}


def test_copy_into_nested_existing_parent():
    stage = _stage(".meta.t = .t")
    out = stage.process({"meta": {}, "t": 1}, None)
    assert out == {"meta": {"t": 1}, "t": 1}


def test_copy_missing_destination_parent_is_an_error():
    stage = _stage(".meta.t = .t")
    record = {"t": 1}
    with pytest.raises(StructuralExecutionError):
        stage.process(record, None)
    assert record == {"t": 1}


def test_copy_from_a_list_index():
    stage = _stage(".first = .values[0]")
    out = stage.process({"values": [10, 20]}, None)
    assert out == {"values": [10, 20], "first": 10}


# §6.1: `=` copies, so a later write through the destination must not reach
# back into the source.
def test_copy_of_a_container_does_not_alias_the_source():
    stage = _stage(".a = .b; .a.x = .c")
    out = stage.process({"b": {"y": 1}, "c": 5}, None)
    assert out["b"] == {"y": 1}
    assert out["a"] == {"y": 1, "x": 5}
    assert out["a"] is not out["b"]


def test_copied_list_is_independent_of_its_source():
    stage = _stage(".a = .b; .a[0] = .c")
    out = stage.process({"b": [1, 2], "c": 9}, None)
    assert out["b"] == [1, 2]
    assert out["a"] == [9, 2]
    assert out["a"] is not out["b"]


def test_nested_containers_are_copied_deeply():
    stage = _stage(".a = .b; .a.inner.x = .c")
    out = stage.process({"b": {"inner": {"x": 1}}, "c": 2}, None)
    assert out["b"] == {"inner": {"x": 1}}
    assert out["a"] is not out["b"]
    assert out["a"]["inner"] is not out["b"]["inner"]


def test_scalar_copy_is_not_copied():
    """Scalars are immutable: the copy must be the identical object."""
    stage = _stage(".a = .b")
    src = {"b": "a string value"}
    out = stage.process(src, None)
    assert out["a"] == "a string value"
    assert out["a"] is out["b"]


def test_move_of_a_container_does_not_alias_a_live_value():
    stage = _stage(".a <- .b; .a.x = .c")
    out = stage.process({"b": {"y": 1}, "c": 5}, None)
    assert out == {"c": 5, "a": {"y": 1, "x": 5}}


# ===========================================================================
# Runtime — move
# ===========================================================================


# 13. Move removes the source
def test_move_removes_source():
    stage = _stage(".temperature <- .metadata.temperature")
    out = stage.process({"metadata": {"temperature": 0.7}}, None)
    assert out == {"metadata": {}, "temperature": 0.7}


# 14. Transformed move removes the source and assigns the decoded value
def test_transformed_move_removes_source():
    stage = _stage(".temperature <- fromjson(.metadata.temperature)")
    out = stage.process({"metadata": {"temperature": "0.7"}}, None)
    assert out == {"metadata": {}, "temperature": 0.7}


# 15. Atomicity: a failed transform leaves the record completely unmodified
def test_failed_move_leaves_record_unmodified():
    stage = _stage(".t <- fromjson(.metadata.t)", validate="off")
    record = {"metadata": {"t": "not json"}, "other": 1}
    with pytest.raises(ToolExecutionError):
        stage.process(record, None)
    assert record == {"metadata": {"t": "not json"}, "other": 1}
    assert "t" not in record


def test_failed_move_destination_parent_leaves_source_intact():
    stage = _stage(".missing.t <- .metadata.t")
    record = {"metadata": {"t": 1}}
    with pytest.raises(StructuralExecutionError):
        stage.process(record, None)
    assert record == {"metadata": {"t": 1}}


# 16. Missing source path → StructuralExecutionError, record unmodified
def test_missing_source_path_errors_without_mutating():
    stage = _stage(".t <- .metadata.t")
    record = {"metadata": {}}
    with pytest.raises(StructuralExecutionError) as exc:
        stage.process(record, None)
    err = exc.value
    assert err.operation == "move"
    assert err.statement_index == 0
    assert record == {"metadata": {}}


# 17. Wildcard source producing multiple refs → error, record unmodified
def test_wildcard_source_multiple_refs_errors():
    stage = _stage(".first <- .items[]")
    record = {"items": [1, 2, 3]}
    with pytest.raises(StructuralExecutionError) as exc:
        stage.process(record, None)
    assert "one" in str(exc.value).lower()
    assert record == {"items": [1, 2, 3]}


def test_wildcard_source_zero_refs_errors():
    stage = _stage(".first = .items[]")
    record = {"items": []}
    with pytest.raises(StructuralExecutionError):
        stage.process(record, None)
    assert record == {"items": []}


def test_wildcard_source_single_ref_succeeds():
    stage = _stage(".first = .items[]")
    assert stage.process({"items": [7]}, None) == {"items": [7], "first": 7}


def test_runtime_overlap_through_wildcard_is_rejected():
    """A wildcard hides the overlap from the compiler; runtime must catch it."""
    stage = _stage(".items[] <- .items")
    record = {"items": [1]}
    with pytest.raises(StructuralExecutionError) as exc:
        stage.process(record, None)
    assert "overlap" in str(exc.value).lower()
    assert record == {"items": [1]}


def test_move_from_a_list_index_removes_the_element():
    stage = _stage(".first <- .values[0]")
    out = stage.process({"values": [10, 20]}, None)
    assert out == {"values": [20], "first": 10}


# §8.9 "Array sources": a move out of an array into a different container is
# valid, and the remaining elements must survive intact and reindexed.
def test_move_from_array_index_into_another_container_reindexes():
    stage = _stage(".metadata.first <- .values[0]")
    out = stage.process({"values": [1, 2, 3], "metadata": {}}, None)
    assert out == {"values": [2, 3], "metadata": {"first": 1}}


def test_move_between_indices_of_the_same_array_is_rejected():
    """Deleting by index renumbers the destination, so this destroys data."""
    from datapipe.dsl.compiler import compile_program

    with pytest.raises(ToolConfigurationError) as exc:
        compile_program(".items[1] <- .items[0]")
    assert "same array" in str(exc.value)


def test_move_into_a_subtree_of_the_same_array_is_rejected():
    from datapipe.dsl.compiler import compile_program

    with pytest.raises(ToolConfigurationError):
        compile_program(".items[1].k <- .items[0]")


def test_move_between_indices_of_different_arrays_is_allowed():
    stage = _stage(".a[0] <- .b[0]")
    out = stage.process({"a": [0], "b": [7, 8]}, None)
    assert out == {"a": [7], "b": [8]}


def test_same_array_move_hidden_by_a_wildcard_is_rejected_at_runtime():
    """A wildcard defers the check; the runtime must still refuse to mutate."""
    stage = _stage(".a[].items[1] <- .a[0].items[0]")
    record = {"a": [{"items": [10, 20, 30]}]}
    with pytest.raises(StructuralExecutionError) as exc:
        stage.process(record, None)
    assert "same array" in str(exc.value)
    assert record == {"a": [{"items": [10, 20, 30]}]}


def test_index_destination_writes_in_place():
    """A destination ending in an index has no key to create; it must exist."""
    stage = _stage(".slots[1] = .v")
    out = stage.process({"slots": ["a", "b"], "v": "z"}, None)
    assert out == {"slots": ["a", "z"], "v": "z"}


def test_index_destination_out_of_range_errors_without_mutating():
    stage = _stage(".slots[5] = .v")
    record = {"slots": ["a"], "v": "z"}
    with pytest.raises(StructuralExecutionError):
        stage.process(record, None)
    assert record == {"slots": ["a"], "v": "z"}


def test_root_destination_is_rejected_as_an_overlap():
    """The root is an ancestor of every source, so it can never be assigned."""
    from datapipe.dsl.compiler import compile_program

    with pytest.raises(ToolConfigurationError) as exc:
        compile_program(". <- .metadata")
    assert "overlap" in str(exc.value).lower()


def test_copy_with_source_above_destination_rejected():
    """``.a.b = .a`` would assign a container into itself — a cycle, not a copy.

    Reading before writing does not save it: the read yields an alias of the
    live container, so the write nests it inside itself indefinitely.
    """
    from datapipe.dsl.compiler import compile_program

    with pytest.raises(ToolConfigurationError) as exc:
        compile_program(".a.b = .a")
    assert "contain itself" in str(exc.value)


def test_move_source_ancestor_of_destination_rejected():
    from datapipe.dsl.compiler import compile_program

    with pytest.raises(ToolConfigurationError):
        compile_program(".a.b <- .a")


def test_transformed_copy_from_an_ancestor_is_allowed():
    """A transform normally returns a fresh value, so the cycle is unprovable."""
    stage = _stage(".a.b = tojson(.a)")
    out = stage.process({"a": {"x": 1}}, None)
    assert out == {"a": {"x": 1, "b": '{"x":1}'}}


IDENTITY_PROVIDER_SRC = """\
from datapipe.tools import tool, JsonType


@tool(
    name="passthrough",
    target="value",
    input=JsonType.ANY,
    output=JsonType.ANY,
    description="Return the value unchanged.",
)
def passthrough(value):
    return value
"""


def test_identity_transform_from_an_ancestor_is_caught_at_runtime(tmp_path):
    """A transform that returns its own argument lands back in the cycle case.

    The compiler waived the overlap because a transform was present; the
    runtime re-check before the write must catch it and leave the record alone.
    """
    from datapipe.dsl.compiler import compile_program
    from datapipe.stages.tool_program import CompiledProgramStage
    from datapipe.tools.installer import install_provider

    src = tmp_path / "s3_identity.py"
    src.write_text(IDENTITY_PROVIDER_SRC)
    install_provider(src, yes=True)

    stage = CompiledProgramStage(compile_program(".a.b = passthrough(.a)"))
    record = {"a": {"x": 1}}
    with pytest.raises(StructuralExecutionError) as exc:
        stage.process(record, None)
    assert "unchanged" in str(exc.value)
    # Nothing was written, so no cycle exists and the record still serializes.
    assert record == {"a": {"x": 1}}
    json.dumps(record)


# ===========================================================================
# Error type
# ===========================================================================


def test_structural_error_pickles():
    err = StructuralExecutionError(
        record_seq=1842,
        statement_index=2,
        operation="move",
        selector=".metadata.temperature",
        source_path=".temperature",
        destination_path=".metadata.temperature",
        expression_span=(0, 10),
        policy="error",
        reason="destination key already exists",
        cause=ValueError("boom"),
    )
    revived = pickle.loads(pickle.dumps(err))
    assert isinstance(revived, StructuralExecutionError)
    assert revived.record_seq == 1842
    assert revived.statement_index == 2
    assert revived.operation == "move"
    assert revived.source_path == ".temperature"
    assert revived.destination_path == ".metadata.temperature"
    assert revived.policy == "error"
    assert str(revived) == str(err)


def test_structural_error_message_shape():
    err = StructuralExecutionError(
        record_seq=1842,
        statement_index=2,
        operation="move",
        selector=".metadata.temperature",
        source_path=".temperature",
        destination_path=".metadata.temperature",
        reason="destination key already exists",
    )
    lines = str(err).splitlines()
    assert lines[0] == "record 1842 failed in move"
    assert "statement: 2" in lines
    assert "source: .temperature" in lines
    assert "destination: .metadata.temperature" in lines
    assert "cause: destination key already exists" in lines


# ===========================================================================
# Integration
# ===========================================================================


# 18. Assignment followed by a pipe
def test_assignment_then_pipe_applies_to_destination():
    stage = _stage(".a = .b | tojson")
    out = stage.process({"b": [1, 2]}, None)
    assert out == {"b": [1, 2], "a": "[1,2]"}


def test_move_then_pipe_applies_to_destination():
    stage = _stage(".a <- .b | tojson")
    assert stage.process({"b": [1, 2]}, None) == {"a": "[1,2]"}


# 19. End-to-end under SequentialExecutor
def test_e2e_sequential(tmp_path):
    from datapipe.execution.sequential import SequentialExecutor

    result = _run_jsonl(
        ".temperature <- fromjson(.metadata.temperature)",
        {"metadata": {"temperature": "0.7"}},
        tmp_path,
        SequentialExecutor(),
    )
    assert result == {"metadata": {}, "temperature": 0.7}


def test_e2e_sequential_acceptance_expressions(tmp_path):
    """All three §S3 acceptance expressions compile and execute."""
    from datapipe.execution.sequential import SequentialExecutor

    assert _run_jsonl(
        ".temperature = .metadata.temperature",
        {"metadata": {"temperature": 0.7}},
        tmp_path / "a",
        SequentialExecutor(),
    ) == {"metadata": {"temperature": 0.7}, "temperature": 0.7}

    assert _run_jsonl(
        ".temperature <- .metadata.temperature",
        {"metadata": {"temperature": 0.7}},
        tmp_path / "b",
        SequentialExecutor(),
    ) == {"metadata": {}, "temperature": 0.7}

    assert _run_jsonl(
        ".temperature <- fromjson(.metadata.temperature)",
        {"metadata": {"temperature": "0.7"}},
        tmp_path / "c",
        SequentialExecutor(),
    ) == {"metadata": {}, "temperature": 0.7}


# 20. Same end-to-end under ProcessExecutor (IR must pickle across spawn)
def test_e2e_process_executor(tmp_path):
    from datapipe.execution.process import ProcessExecutor

    result = _run_jsonl(
        ".temperature <- fromjson(.metadata.temperature); .copy = .temperature",
        {"metadata": {"temperature": "0.7"}},
        tmp_path,
        ProcessExecutor(workers=2),
    )
    assert result == {"metadata": {}, "temperature": 0.7, "copy": 0.7}


def test_e2e_process_executor_surfaces_structural_error(tmp_path):
    """StructuralExecutionError must survive the spawn boundary intact.

    ``errors="return"`` routes the unpickled error to the error sink, so the
    structured fields prove the error crossed the boundary as itself rather
    than degrading to a generic exception.
    """
    from datapipe.execution.process import ProcessExecutor
    from datapipe.io.jsonl import JsonlSink, JsonlSource
    from datapipe.pipeline import Pipeline
    from datapipe.stage import JsonDumpStage, JsonLoadStage

    pipeline = Pipeline([JsonLoadStage(), _stage(".t <- .metadata.t"), JsonDumpStage()])
    inp = tmp_path / "in.jsonl"
    out = tmp_path / "out.jsonl"
    err = tmp_path / "err.jsonl"
    inp.write_text(json.dumps({"metadata": {}}) + "\n")

    pipeline.run(
        source=JsonlSource(str(inp), raw=True),
        sink=JsonlSink(str(out), raw=True),
        executor=ProcessExecutor(workers=1),
        errors="return",
        error_sink=JsonlSink(str(err)),
    )
    payload = json.loads(err.read_text().strip())
    assert payload["error_type"] == "StructuralExecutionError"
    assert payload["structural"]["operation"] == "move"
    assert payload["structural"]["statement_index"] == 0
    assert payload["structural"]["destination_path"] == ".t"
    assert payload["structural"]["policy"] == "error"


def test_pickle_roundtrip_preserves_assignment_ir():
    stage = _stage(".temperature <- fromjson(.metadata.temperature)")
    revived = pickle.loads(pickle.dumps(stage))
    assert revived.process({"metadata": {"temperature": "0.7"}}, None) == {
        "metadata": {},
        "temperature": 0.7,
    }


# ===========================================================================
# CLI surface
# ===========================================================================


def test_inspect_expression_describes_an_assignment(capsys):
    from datapipe.cli.transform import _compile_or_report, describe_compiled

    compiled = _compile_or_report(".temperature <- fromjson(.metadata.temperature)")
    assert compiled is not None
    doc = describe_compiled(compiled, ".temperature <- fromjson(.metadata.temperature)")
    stmt = doc["statements"][0]
    assert stmt["operation"]["kind"] == "assignment"
    assert stmt["operation"]["destination"] == ".temperature"
    assert stmt["operation"]["source"] == ".metadata.temperature"
    assert stmt["operation"]["move"] is True
    json.dumps(doc)  # must stay JSON-serializable


def test_print_compiled_handles_assignments(capsys):
    from datapipe.cli.transform import _compile_or_report, _print_compiled

    expr = ".a = .b | tojson"
    _print_compiled(_compile_or_report(expr), expr)
    out = capsys.readouterr().out
    # Anchored to the indented statement line: the bare text also appears in
    # the `Expression:` echo and the stage summary, so a substring test alone
    # would pass with the assignment-rendering branch removed entirely.
    assert "\n    copy .a = .b\n" in out


# ===========================================================================
# Regression: S1/S2 behaviour is unchanged
# ===========================================================================


def test_s2_focused_pipe_still_works():
    stage = _stage(".metadata | fromjson | tojson")
    assert stage.process({"metadata": "[1,2,3]"}, None) == {"metadata": "[1,2,3]"}


def test_s1_multi_statement_still_works():
    stage = _stage("fromjson(.a); fromjson(.b)")
    assert stage.process({"a": "[1]", "b": "[2]"}, None) == {"a": [1], "b": [2]}


def test_s2_selector_without_pipe_or_assignment_still_errors():
    with pytest.raises(ExpressionSyntaxError):
        parse_program(".metadata")


# ===========================================================================
# Impure tools must never see the live record (Finding 1)
# ===========================================================================

MUTATING_PROVIDER_SRC = """\
from datapipe.tools import tool, JsonType


@tool(
    name="popx",
    target="value",
    input=JsonType.OBJECT,
    output=JsonType.OBJECT,
    description="Remove key 'x' from the value, in place, and return it.",
)
def popx(value):
    value.pop("x", None)
    return value


@tool(
    name="popx_boom",
    target="value",
    input=JsonType.OBJECT,
    output=JsonType.OBJECT,
    description="Mutate the value in place and then fail.",
)
def popx_boom(value):
    value.pop("x", None)
    raise ValueError("popx_boom always fails")
"""


@pytest.fixture()
def mutating_tools(tmp_path):
    """Install `popx` (mutates in place) and `popx_boom` (mutates, then raises)."""
    from datapipe.tools.installer import install_provider

    src = tmp_path / "s3_mutating.py"
    src.write_text(MUTATING_PROVIDER_SRC)
    install_provider(src, yes=True)


# A tool is not required to be pure.  §6.1 guarantees `=` leaves the source
# present and unchanged, so the value handed to the tool must be detached.
def test_mutating_transform_does_not_corrupt_the_copy_source(mutating_tools):
    stage = _stage(".a = popx(.b)")
    out = stage.process({"b": {"x": 1, "y": 2}}, None)
    assert out == {"b": {"x": 1, "y": 2}, "a": {"y": 2}}


def test_mutating_bare_pipe_call_does_not_corrupt_the_copy_source(mutating_tools):
    """The trailing bare calls of a pipe chain are fed the same value."""
    stage = _stage(".a = .b | popx")
    out = stage.process({"b": {"x": 1, "y": 2}}, None)
    assert out == {"b": {"x": 1, "y": 2}, "a": {"y": 2}}


# Mutate-then-raise is the atomicity case: §8.1 says a failed statement leaves
# the record byte-for-byte as it arrived, so the whole record is compared.
def test_failing_mutating_transform_leaves_the_copy_record_untouched(mutating_tools):
    import copy as _copy

    stage = _stage(".a = popx_boom(.b)")
    record = {"b": {"x": 1, "y": 2}, "other": [{"x": 9}]}
    expected = _copy.deepcopy(record)
    with pytest.raises(ToolExecutionError) as exc:
        stage.process(record, None)
    assert isinstance(exc.value.cause, ValueError)
    assert record == expected


def test_failing_mutating_transform_leaves_the_move_record_untouched(mutating_tools):
    import copy as _copy

    stage = _stage(".a <- popx_boom(.b)")
    record = {"b": {"x": 1, "y": 2}, "other": [{"x": 9}]}
    expected = _copy.deepcopy(record)
    with pytest.raises(ToolExecutionError) as exc:
        stage.process(record, None)
    assert isinstance(exc.value.cause, ValueError)
    assert record == expected


def test_failing_mutating_bare_call_leaves_the_copy_record_untouched(mutating_tools):
    import copy as _copy

    stage = _stage(".a = .b | popx_boom")
    record = {"b": {"x": 1, "y": 2}, "other": [{"x": 9}]}
    expected = _copy.deepcopy(record)
    with pytest.raises(ToolExecutionError) as exc:
        stage.process(record, None)
    assert isinstance(exc.value.cause, ValueError)
    assert record == expected


def test_failing_mutating_bare_call_leaves_the_move_record_untouched(mutating_tools):
    import copy as _copy

    stage = _stage(".a <- .b | popx_boom")
    record = {"b": {"x": 1, "y": 2}, "other": [{"x": 9}]}
    expected = _copy.deepcopy(record)
    with pytest.raises(ToolExecutionError) as exc:
        stage.process(record, None)
    assert isinstance(exc.value.cause, ValueError)
    assert record == expected
