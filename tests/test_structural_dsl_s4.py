"""Phase S4 tests: `<<` grouped move-into, `^` complement, and field unions.

TDD: written before the implementation.  The core guarantees under test are
the §8.1 resolve-before-mutate ordering (a precondition failure on the *third*
source must leave the record completely unmodified), source-object ordering
(§6.4), destination self-exclusion (§8.7), and the two S3 carry-forward
hazards: value aliasing and multi-source deletion.
"""

from __future__ import annotations

import json
import pickle

import pytest

from datapipe.dsl.ast import FieldSet, MoveInto, Program, Selector
from datapipe.dsl.errors import ExpressionSyntaxError, ToolConfigurationError
from datapipe.dsl.lexer import TT, tokenize
from datapipe.dsl.parser import parse_program
from datapipe.tools.errors import StructuralExecutionError


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


def _run(expression: str, record):
    """Execute *expression* against *record* in-process and return the result."""
    return _stage(expression).process(record, None)


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


def _only(expression: str):
    """Parse a single-statement program and return that statement."""
    program = parse_program(expression)
    assert isinstance(program, Program)
    assert len(program.statements) == 1
    return program.statements[0]


# ===========================================================================
# 1-8. Lexer / parser
# ===========================================================================


# 1. `<<` and `^` tokenize distinctly, and `<-` still works.
def test_move_in_and_complement_tokenize_distinctly():
    types = [t.type for t in tokenize(".m << .(^a|b)")]
    assert types == [
        TT.DOT, TT.IDENT,
        TT.MOVE_IN,
        TT.DOT, TT.LPAREN, TT.COMPLEMENT, TT.IDENT, TT.PIPE, TT.IDENT, TT.RPAREN,
        TT.EOF,
    ]


def test_move_in_does_not_break_arrow_left():
    assert [t.type for t in tokenize(".a <- .b")] == [
        TT.DOT, TT.IDENT, TT.ARROW_LEFT, TT.DOT, TT.IDENT, TT.EOF,
    ]


def test_move_in_without_spaces_is_one_token():
    types = [t.type for t in tokenize(".a<<.b")]
    assert types.count(TT.MOVE_IN) == 1
    assert types == [TT.DOT, TT.IDENT, TT.MOVE_IN, TT.DOT, TT.IDENT, TT.EOF]


# 2. Single-source move-into.
def test_parse_single_source_move_into():
    stmt = _only(".metadata << .temperature")
    op = stmt.operation
    assert isinstance(op, MoveInto)
    assert len(op.sources) == 1
    assert isinstance(op.sources[0], Selector)
    assert op.sources[0].parts[0].name == "temperature"
    # The published focus is the destination, so trailing pipes hit it.
    assert stmt.focus_selector is op.destination


# 3. Three comma-separated sources.
def test_parse_three_comma_sources():
    op = _only(".metadata << .a, .b, .c").operation
    assert isinstance(op, MoveInto)
    assert [s.parts[0].name for s in op.sources] == ["a", "b", "c"]


# 4. Positive field set.
def test_parse_positive_field_set():
    op = _only(".metadata << .(a|b|c)").operation
    assert len(op.sources) == 1
    fs = op.sources[0]
    assert isinstance(fs, FieldSet)
    assert fs.complement is False
    assert fs.names == ("a", "b", "c")
    assert fs.base.is_root


# 5. Complement field set — `^` complements the whole set, not just the first name.
def test_parse_complement_field_set():
    fs = _only(".metadata << .(^a|b|c)").operation.sources[0]
    assert isinstance(fs, FieldSet)
    assert fs.complement is True
    assert fs.names == ("a", "b", "c")


# 6. Nested base.
def test_parse_nested_field_set_base():
    fs = _only(".archive << .metadata.(temperature|score)").operation.sources[0]
    assert isinstance(fs, FieldSet)
    assert fs.complement is False
    assert fs.names == ("temperature", "score")
    assert [p.name for p in fs.base.parts] == ["metadata"]


# 7. §9.1 precedence: the pipe binds to the whole move-into, not to the last source.
def test_trailing_pipe_binds_to_the_move_not_the_last_source():
    stmt = _only(".m << .a, .b | tojson")
    op = stmt.operation
    assert isinstance(op, MoveInto)
    assert len(op.sources) == 2
    assert [s.parts[0].name for s in op.sources] == ["a", "b"]
    assert len(stmt.pipes) == 1
    assert stmt.pipes[0].qualified_name.name == "tojson"


# 8. §9.2 contextual `|`: inside `.()` it unions field names, outside it pipes.
def test_contextual_pipe_inside_and_outside_parens():
    stmt = _only(".metadata << .(^instance_id|messages) | tojson")
    fs = stmt.operation.sources[0]
    assert isinstance(fs, FieldSet)
    assert fs.names == ("instance_id", "messages")
    assert len(stmt.pipes) == 1
    assert stmt.pipes[0].qualified_name.name == "tojson"


def test_two_statements_with_field_union_and_pipe():
    program = parse_program(".metadata << .(^a|b|c) | tojson; tojson(.)")
    assert len(program.statements) == 2
    assert program.statements[0].operation.sources[0].names == ("a", "b", "c")


def test_empty_field_set_is_a_syntax_error():
    with pytest.raises(ExpressionSyntaxError):
        parse_program(".m << .()")


# ===========================================================================
# 9-11. Compile-time rejection (§12)
# ===========================================================================


def _compile(expression: str):
    from datapipe.dsl.compiler import compile_program

    return compile_program(expression)


# 9. Array / wildcard sources under inferred-key `<<` (§8.9).
def test_index_source_under_move_into_is_rejected():
    with pytest.raises(ToolConfigurationError) as exc:
        _compile(".metadata << .values[0]")
    assert "cannot infer a destination key" in str(exc.value)


def test_wildcard_source_under_move_into_is_rejected():
    with pytest.raises(ToolConfigurationError):
        _compile(".metadata << .items[].name")


def test_root_source_under_move_into_is_rejected():
    with pytest.raises(ToolConfigurationError):
        _compile(".metadata << .")


def test_wildcard_field_set_base_is_rejected():
    with pytest.raises(ToolConfigurationError):
        _compile(".metadata << .items[].(a|b)")


# 10. Duplicate names inside a field set (§12).
def test_duplicate_field_names_in_a_set_are_rejected():
    with pytest.raises(ToolConfigurationError) as exc:
        _compile(".m << .(a|b|a)")
    assert "duplicate" in str(exc.value).lower()


# 11. Two sources deriving the same destination key (§8.8).
def test_two_sources_deriving_the_same_key_are_rejected():
    with pytest.raises(ToolConfigurationError) as exc:
        _compile(".m << .a, .b.a")
    assert "two sources derive the same destination key 'a'" in str(exc.value)


def test_positive_set_and_path_deriving_the_same_key_are_rejected():
    with pytest.raises(ToolConfigurationError):
        _compile(".m << .temperature, .metadata.(temperature|score)")


# §8.8 overlap: moving a field into itself.
def test_source_equal_to_derived_destination_is_rejected():
    with pytest.raises(ToolConfigurationError) as exc:
        _compile(".metadata << .metadata.temperature")
    assert "overlap" in str(exc.value).lower()


def test_source_ancestor_of_destination_is_rejected():
    with pytest.raises(ToolConfigurationError):
        _compile(".a.b << .a")


# ===========================================================================
# 12-24. Runtime
# ===========================================================================


# 12. §6.3 single source; destination auto-created (§8.2); source removed.
def test_single_source_creates_destination_and_removes_source():
    assert _run(".metadata << .temperature", {"temperature": 0.7}) == {
        "metadata": {"temperature": 0.7}
    }


def test_single_source_into_existing_destination():
    assert _run(".metadata << .temperature", {"temperature": 0.7, "metadata": {"k": 1}}) == {
        "metadata": {"k": 1, "temperature": 0.7}
    }


def test_missing_intermediate_parent_is_an_error(recwarn):
    record = {"temperature": 0.7}
    with pytest.raises(StructuralExecutionError):
        _run(".a.b << .temperature", record)
    assert record == {"temperature": 0.7}


# 13. §6.3 grouped: three sources moved.
def test_grouped_sources_all_moved():
    result = _run(
        ".metadata << .annotation_key, .temperature, .score",
        {"instance_id": "i1", "annotation_key": "k", "temperature": 0.7, "score": 5},
    )
    assert result == {
        "instance_id": "i1",
        "metadata": {"annotation_key": "k", "temperature": 0.7, "score": 5},
    }


# 14. §6.4 positive field set preserves SOURCE OBJECT order, not written order.
def test_positive_field_set_uses_source_object_order():
    record = {"zeta": 1, "alpha": 2, "mid": 3}
    # The expression names alpha first, but the record orders zeta first.
    result = _run(".meta << .(alpha|zeta)", record)
    assert list(result["meta"].keys()) == ["zeta", "alpha"]
    assert result == {"mid": 3, "meta": {"zeta": 1, "alpha": 2}}


def test_nested_positive_field_set():
    result = _run(
        ".archive << .metadata.(temperature|score)",
        {"metadata": {"temperature": 0.7, "score": 5, "note": "n"}},
    )
    assert result == {
        "metadata": {"note": "n"},
        "archive": {"temperature": 0.7, "score": 5},
    }


# 15. §6.5 complement moves everything except the named keys.
def test_complement_moves_everything_else():
    result = _run(
        ".metadata << .(^instance_id|messages)",
        {"instance_id": "i1", "messages": [1], "temperature": 0.7, "score": 5},
    )
    assert result == {
        "instance_id": "i1",
        "messages": [1],
        "metadata": {"temperature": 0.7, "score": 5},
    }


def test_complement_preserves_source_object_order():
    record = {"keep": 0, "z": 1, "a": 2, "m": 3}
    result = _run(".bag << .(^keep)", record)
    assert list(result["bag"].keys()) == ["z", "a", "m"]


# 16. §8.7 destination self-exclusion: `.metadata` must not nest inside itself.
def test_destination_is_self_excluded_from_a_complement_set():
    result = _run(
        ".metadata << .(^instance_id)",
        {"instance_id": "i1", "metadata": {"pre": 1}, "temperature": 0.7},
    )
    assert result == {
        "instance_id": "i1",
        "metadata": {"pre": 1, "temperature": 0.7},
    }
    assert "metadata" not in result["metadata"]


# 17. §8.3 an existing non-object destination is an error.
def test_non_object_destination_errors():
    record = {"metadata": '{"a": 1}', "temperature": 0.7}
    with pytest.raises(StructuralExecutionError) as exc:
        _run(".metadata << .temperature", record)
    assert "object" in str(exc.value).lower()
    assert record == {"metadata": '{"a": 1}', "temperature": 0.7}


def test_complement_on_a_non_object_base_errors():
    record = {"metadata": "not-an-object"}
    with pytest.raises(StructuralExecutionError):
        _run(".bag << .metadata.(^a)", record)
    assert record == {"metadata": "not-an-object"}


# 18. §8.4 derived-key collision errors and leaves the record unmodified.
def test_derived_key_collision_errors():
    record = {"temperature": 0.7, "metadata": {"temperature": 0.1}}
    with pytest.raises(StructuralExecutionError) as exc:
        _run(".metadata << .temperature", record)
    assert "exist" in str(exc.value).lower()
    assert record == {"temperature": 0.7, "metadata": {"temperature": 0.1}}


# 19. §8.5 positive sets are strict.
def test_positive_set_missing_field_errors():
    record = {"a": 1}
    with pytest.raises(StructuralExecutionError) as exc:
        _run(".m << .(a|b)", record)
    assert "b" in str(exc.value)
    assert record == {"a": 1}


# 20. §8.6 complement misses are harmless.
def test_complement_with_a_missing_exclusion_succeeds():
    result = _run(
        ".m << .(^b|optional_future_field)", {"a": 1, "b": 2}
    )
    assert result == {"b": 2, "m": {"a": 1}}


# 21. §6.6 root as a destination — the three-statement program from the spec.
def test_root_destination_end_to_end():
    record = {
        "instance_id": "i1",
        "metadata": json.dumps({"temperature": 0.7, "score": 5, "note": "n"}),
    }
    result = _run(
        "fromjson(.metadata); . << .metadata.(temperature|score); tojson(.metadata)",
        record,
    )
    assert result["temperature"] == 0.7
    assert result["score"] == 5
    assert json.loads(result["metadata"]) == {"note": "n"}
    assert result["instance_id"] == "i1"


def test_root_destination_single_field():
    assert _run(". << .metadata.x", {"metadata": {"x": 1, "y": 2}}) == {
        "metadata": {"y": 2},
        "x": 1,
    }


def test_root_is_a_legal_move_into_destination_but_not_a_move_destination():
    """§6.6 opens the root to `<<` without opening it to `<-`.

    `. <- .metadata` replaces the whole record with a subtree of itself, which
    S3 rejects as an overlap and still must.  `. << .metadata.x` adds a key to
    the root, which is a different operation and is required to work.
    """
    from datapipe.dsl.compiler import compile_program

    with pytest.raises(ToolConfigurationError):
        compile_program(". <- .metadata")
    compile_program(". << .metadata.x")


def test_root_destination_collision_errors():
    record = {"x": 9, "metadata": {"x": 1}}
    with pytest.raises(StructuralExecutionError):
        _run(". << .metadata.x", record)
    assert record == {"x": 9, "metadata": {"x": 1}}


# 22. ATOMICITY: a collision on the THIRD source leaves the record untouched.
def test_collision_on_third_source_leaves_record_completely_unmodified():
    record = {
        "a": 1,
        "b": 2,
        "c": 3,
        "keep": "untouched",
        "metadata": {"c": 99, "pre": "p"},
    }
    with pytest.raises(StructuralExecutionError):
        _run(".metadata << .a, .b, .c", record)
    assert record == {
        "a": 1,
        "b": 2,
        "c": 3,
        "keep": "untouched",
        "metadata": {"c": 99, "pre": "p"},
    }


def test_atomicity_when_a_later_positive_set_name_is_missing():
    record = {"a": 1, "b": 2, "metadata": {}}
    with pytest.raises(StructuralExecutionError):
        _run(".metadata << .a, .(b|missing)", record)
    assert record == {"a": 1, "b": 2, "metadata": {}}


# 23. Multi-source delete removes EXACTLY the intended fields (S3 carry-forward).
def test_multi_source_move_removes_exactly_the_intended_fields():
    record = {"f0": 0, "f1": 1, "f2": 2, "f3": 3, "f4": 4, "f5": 5}
    result = _run(".bag << .f1, .f3, .f5", record)
    assert set(result) == {"f0", "f2", "f4", "bag"}
    assert result["bag"] == {"f1": 1, "f3": 3, "f5": 5}
    # Nothing that stayed behind was renumbered or reassigned.
    assert result["f0"] == 0 and result["f2"] == 2 and result["f4"] == 4


def test_multi_source_move_out_of_a_list_element_object():
    record = {"items": [{"a": 1, "b": 2, "c": 3}], "bag": {}}
    result = _run(".bag << .items[0].a, .items[0].c", record)
    assert result == {"items": [{"b": 2}], "bag": {"a": 1, "c": 3}}


def test_complement_delete_removes_exactly_the_moved_keys():
    record = {"k1": 1, "k2": 2, "k3": 3, "k4": 4}
    result = _run(".bag << .(^k2|k4)", record)
    assert set(result) == {"k2", "k4", "bag"}
    assert result["bag"] == {"k1": 1, "k3": 3}


# 24. Aliasing (S3 carry-forward): written values share no structure with the record.
def test_moved_container_is_detached_from_the_source_object():
    payload = {"n": 1}
    record = {"payload": payload, "metadata": {}}
    result = _run(".metadata << .payload", record)
    assert result["metadata"]["payload"] == {"n": 1}
    assert result["metadata"]["payload"] is not payload


def test_moved_container_is_detached_deeply_from_caller_held_data():
    """Detachment must be deep, not just a fresh top-level container.

    A shallow copy would leave the nested object shared, so mutating the
    destination would reach back into data the caller still holds.
    """
    inner = {"n": 1}
    record = {"payload": {"deep": inner}, "bag": {}}
    result = _run(".bag << .payload", record)
    assert result["bag"]["payload"] == {"deep": {"n": 1}}
    result["bag"]["payload"]["deep"]["n"] = 999
    assert inner == {"n": 1}


def test_a_source_nested_inside_another_source_is_rejected():
    """Moving both would delete the outer container out from under the inner one."""
    record = {"outer": {"inner": {"n": 1}}, "bag": {}}
    with pytest.raises(StructuralExecutionError) as exc:
        _run(".bag << .outer, .outer.inner", record)
    assert "ancestor" in str(exc.value)
    assert record == {"outer": {"inner": {"n": 1}}, "bag": {}}


# ===========================================================================
# 25-26. Integration
# ===========================================================================


# 25. Trailing pipe applies to the assembled destination.
def test_trailing_pipe_serializes_the_destination():
    result = _run(".metadata << .a, .b | tojson", {"a": 1, "b": 2})
    assert isinstance(result["metadata"], str)
    assert json.loads(result["metadata"]) == {"a": 1, "b": 2}


def test_pipe_sees_the_destination_after_the_move_not_before():
    """A pipe must receive the post-move destination.

    When the destination is an ancestor of the source (§6.6's root move), the
    source is still physically inside the destination at assembly time.  The
    piped tool must not see it there: serializing the root before the removal
    would emit the moved field twice — once at its new key and once still
    nested in the object it was moved out of.
    """
    result = _run(". << .metadata.x | tojson", {"metadata": {"x": 1, "y": 2}})
    assert json.loads(result) == {"metadata": {"y": 2}, "x": 1}


def test_acceptance_complement_pipe_tojson():
    result = _run(
        ".metadata << .(^instance_id|messages|tools) | tojson",
        {
            "instance_id": "i1",
            "messages": [{"role": "user"}],
            "tools": [],
            "temperature": 0.7,
            "score": 5,
        },
    )
    assert json.loads(result["metadata"]) == {"temperature": 0.7, "score": 5}
    assert result["instance_id"] == "i1"


def test_acceptance_positive_set_pipe_tojson():
    result = _run(
        ".metadata << .(annotation_key|temperature|score) | tojson",
        {"annotation_key": "k", "temperature": 0.7, "score": 5, "keep": 1},
    )
    assert json.loads(result["metadata"]) == {
        "annotation_key": "k",
        "temperature": 0.7,
        "score": 5,
    }
    assert result["keep"] == 1


# 26. End-to-end under both executors.
def test_e2e_sequential_executor(tmp_path):
    from datapipe.execution.sequential import SequentialExecutor

    result = _run_jsonl(
        ".metadata << .annotation_key, .temperature, .score | tojson",
        {"instance_id": "i1", "annotation_key": "k", "temperature": 0.7, "score": 5},
        tmp_path,
        SequentialExecutor(),
    )
    assert result["instance_id"] == "i1"
    assert json.loads(result["metadata"]) == {
        "annotation_key": "k", "temperature": 0.7, "score": 5,
    }


def test_e2e_process_executor(tmp_path):
    from datapipe.execution.process import ProcessExecutor

    result = _run_jsonl(
        ".metadata << .(^instance_id|messages) | tojson",
        {"instance_id": "i1", "messages": [1], "temperature": 0.7},
        tmp_path,
        ProcessExecutor(workers=2),
    )
    assert result["instance_id"] == "i1"
    assert json.loads(result["metadata"]) == {"temperature": 0.7}


def test_e2e_process_executor_surfaces_structural_error(tmp_path):
    from datapipe.execution.process import ProcessExecutor
    from datapipe.io.jsonl import JsonlSink, JsonlSource
    from datapipe.pipeline import Pipeline
    from datapipe.stage import JsonDumpStage, JsonLoadStage

    inp = tmp_path / "in.jsonl"
    out = tmp_path / "out.jsonl"
    err = tmp_path / "err.jsonl"
    inp.write_text(json.dumps({"t": 1, "metadata": {"t": 2}}) + "\n")

    pipeline = Pipeline(
        [JsonLoadStage(), _stage(".metadata << .t"), JsonDumpStage()]
    )
    pipeline.run(
        source=JsonlSource(str(inp), raw=True),
        sink=JsonlSink(str(out), raw=True),
        executor=ProcessExecutor(workers=1),
        errors="return",
        error_sink=JsonlSink(str(err)),
    )
    payload = json.loads(err.read_text().strip())
    assert payload["error_type"] == "StructuralExecutionError"
    assert payload["structural"]["operation"] == "move-into"
    # §12 reports the effective destination — the derived key that collided.
    assert payload["structural"]["destination_path"] == ".metadata.t"
    assert payload["structural"]["source_path"] == ".t"


def test_pickle_roundtrip_preserves_move_into_ir():
    from datapipe.dsl.compiler import CompiledMoveInto

    stage = _stage(".metadata << .(^a|b), .c | tojson")
    revived = pickle.loads(pickle.dumps(stage))
    op = revived._compiled.statements[0].operation
    assert isinstance(op, CompiledMoveInto)
    assert op.destination.render() == ".metadata"
    assert len(op.sources) == 2
    assert op.sources[0].names == ("a", "b")
    assert op.sources[0].complement is True


def test_inspect_expression_renders_a_move_into(capsys):
    from datapipe.cli.transform import inspect_expression_command

    class _Args:
        expression = ".metadata << .a, .(^b|c) | tojson"
        as_json = True
        validate_tools = "always"

    assert inspect_expression_command(_Args()) == 0
    doc = json.loads(capsys.readouterr().out)
    op = doc["statements"][0]["operation"]
    assert op["kind"] == "move_into"
    assert op["destination"] == ".metadata"
    assert len(op["sources"]) == 2


def test_inspect_expression_text_output_renders_a_move_into(capsys):
    """The text renderer has its own branch; the JSON test does not cover it."""
    from datapipe.cli.transform import inspect_expression_command

    class _Args:
        expression = ".metadata << .a, .(^b|c) | tojson"
        as_json = False
        validate_tools = "always"

    assert inspect_expression_command(_Args()) == 0
    out = capsys.readouterr().out
    assert "move-into .metadata" in out
    assert "source: .a" in out
    assert "complement(b, c)" in out


def test_transform_cli_end_to_end(tmp_path):
    from datapipe.cli.main import main

    inp = tmp_path / "in.jsonl"
    out = tmp_path / "out.jsonl"
    inp.write_text(json.dumps({"instance_id": "i1", "temperature": 0.7}) + "\n")

    assert main([
        "transform",
        ".metadata << .(^instance_id) | tojson",
        str(inp),
        str(out),
    ]) == 0
    result = json.loads(out.read_text().strip())
    assert result["instance_id"] == "i1"
    assert json.loads(result["metadata"]) == {"temperature": 0.7}
