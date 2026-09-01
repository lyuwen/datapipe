"""Phase S2 tests: focused tool pipelines, selector-first statements, bare pipe calls.

TDD: these tests are written before the implementation and drive the S2 feature set.
"""

from __future__ import annotations

import json
import warnings

import pytest

from datapipe.dsl.ast import BareToolCall, Invocation, Program, Selector, Statement
from datapipe.dsl.errors import ExpressionSyntaxError
from datapipe.dsl.parser import parse_program
from datapipe.tools.errors import ToolExecutionError


@pytest.fixture(autouse=True)
def isolated_registry(tmp_path, monkeypatch):
    """Redirect the tool registry at tmp_path and clear the loader cache.

    Compilation consults the user registry, so without this the suite would
    read (and be perturbed by) the developer's real installed providers.
    """
    monkeypatch.setenv("DATAPIPE_USER_DATA", str(tmp_path / "dp_data"))

    from datapipe.tools import loader as _loader
    _loader._loaded_providers.clear()

    from datapipe.dsl import compiler as _compiler
    monkeypatch.setattr(_compiler, "_BUILTIN_REGISTRY", None, raising=False)


PROVIDER_SRC = """\
from datapipe.tools import tool, JsonType


@tool(
    name="shout",
    target="value",
    input=JsonType.STRING,
    output=JsonType.STRING,
    description="Uppercase a string and append a suffix.",
)
def shout(value, *, suffix: str = "!") -> str:
    return value.upper() + suffix
"""


# ---------------------------------------------------------------------------
# 1. parse_program(".metadata | fromjson") — selector-first, one bare op
# ---------------------------------------------------------------------------

def test_parse_selector_first_single_op():
    prog = parse_program(".metadata | fromjson")
    assert isinstance(prog, Program)
    assert len(prog.statements) == 1
    stmt = prog.statements[0]
    assert isinstance(stmt.focus_selector, Selector)
    assert stmt.focus_selector.parts[0].name == "metadata"
    assert isinstance(stmt.operation, BareToolCall)
    assert stmt.operation.qualified_name.name == "fromjson"
    assert len(stmt.pipes) == 0


# ---------------------------------------------------------------------------
# 2. parse_program(".metadata | fromjson | tojson") — focus + one pipe
# ---------------------------------------------------------------------------

def test_parse_selector_first_with_pipe():
    prog = parse_program(".metadata | fromjson | tojson")
    assert len(prog.statements) == 1
    stmt = prog.statements[0]
    assert isinstance(stmt.focus_selector, Selector)
    assert isinstance(stmt.operation, BareToolCall)
    assert stmt.operation.qualified_name.name == "fromjson"
    assert len(stmt.pipes) == 1
    assert stmt.pipes[0].qualified_name.name == "tojson"


# ---------------------------------------------------------------------------
# 3. parse_program(".metadata | fromjson; finalize(.)") — focus reset
# ---------------------------------------------------------------------------

def test_parse_semicolon_resets_focus():
    prog = parse_program(".metadata | fromjson; tojson(.)")
    assert len(prog.statements) == 2
    # First statement: focused
    s0 = prog.statements[0]
    assert isinstance(s0.focus_selector, Selector)
    assert isinstance(s0.operation, BareToolCall)
    # Second statement: invocation-first (focus_selector is None)
    s1 = prog.statements[1]
    assert s1.focus_selector is None
    assert isinstance(s1.operation, Invocation)


# ---------------------------------------------------------------------------
# 4. parse_program("fromjson(.a) | tojson") — invocation-first with pipe
# ---------------------------------------------------------------------------

def test_parse_invocation_first_with_pipe():
    prog = parse_program("fromjson(.a) | tojson")
    assert len(prog.statements) == 1
    stmt = prog.statements[0]
    assert stmt.focus_selector is None
    assert isinstance(stmt.operation, Invocation)
    assert stmt.operation.qualified_name.name == "fromjson"
    assert len(stmt.pipes) == 1
    assert stmt.pipes[0].qualified_name.name == "tojson"


# ---------------------------------------------------------------------------
# 5. compile_program(".metadata | fromjson") — CompiledStatement with focus
# ---------------------------------------------------------------------------

def test_compile_program_focused_statement():
    from datapipe.dsl.compiler import CompiledProgram, CompiledStatement, compile_program
    result = compile_program(".metadata | fromjson")
    assert isinstance(result, CompiledProgram)
    assert len(result.statements) == 1
    stmt = result.statements[0]
    assert isinstance(stmt, CompiledStatement)
    assert stmt.focus_selector is not None
    assert len(stmt.pipes) == 0


# ---------------------------------------------------------------------------
# 6. E2E: focused single op under SequentialExecutor
# ---------------------------------------------------------------------------

def test_e2e_focused_single_op(tmp_path):
    from datapipe.dsl.compiler import compile_program
    from datapipe.execution.sequential import SequentialExecutor
    from datapipe.io.jsonl import JsonlSink, JsonlSource
    from datapipe.pipeline import Pipeline
    from datapipe.stage import JsonDumpStage, JsonLoadStage
    from datapipe.stages.tool_program import CompiledProgramStage

    record = {"metadata": "[1,2,3]"}
    compiled = compile_program(".metadata | fromjson")
    stage = CompiledProgramStage(compiled)
    pipeline = Pipeline([JsonLoadStage(), stage, JsonDumpStage()])

    inp = tmp_path / "in.jsonl"
    out = tmp_path / "out.jsonl"
    inp.write_text(json.dumps(record) + "\n")

    pipeline.run(
        source=JsonlSource(str(inp), raw=True),
        sink=JsonlSink(str(out), raw=True),
        executor=SequentialExecutor(),
    )
    result = json.loads(out.read_text().strip())
    assert result == {"metadata": [1, 2, 3]}


# ---------------------------------------------------------------------------
# 7. E2E: chained focused pipes
# ---------------------------------------------------------------------------

def test_e2e_focused_chained_pipes(tmp_path):
    from datapipe.dsl.compiler import compile_program
    from datapipe.execution.sequential import SequentialExecutor
    from datapipe.io.jsonl import JsonlSink, JsonlSource
    from datapipe.pipeline import Pipeline
    from datapipe.stage import JsonDumpStage, JsonLoadStage
    from datapipe.stages.tool_program import CompiledProgramStage

    record = {"metadata": "[1,2,3]"}
    compiled = compile_program(".metadata | fromjson | tojson")
    stage = CompiledProgramStage(compiled)
    pipeline = Pipeline([JsonLoadStage(), stage, JsonDumpStage()])

    inp = tmp_path / "in.jsonl"
    out = tmp_path / "out.jsonl"
    inp.write_text(json.dumps(record) + "\n")

    pipeline.run(
        source=JsonlSource(str(inp), raw=True),
        sink=JsonlSink(str(out), raw=True),
        executor=SequentialExecutor(),
    )
    result = json.loads(out.read_text().strip())
    # fromjson decodes "[1,2,3]" → [1,2,3]; tojson re-encodes compactly.
    # The round trip proves both pipe steps fired in order.
    assert result == {"metadata": "[1,2,3]"}


# ---------------------------------------------------------------------------
# 8. E2E: multi-statement with focus reset
# ---------------------------------------------------------------------------

def test_e2e_multi_statement_focus_reset(tmp_path):
    from datapipe.dsl.compiler import compile_program
    from datapipe.execution.sequential import SequentialExecutor
    from datapipe.io.jsonl import JsonlSink, JsonlSource
    from datapipe.pipeline import Pipeline
    from datapipe.stage import JsonDumpStage, JsonLoadStage
    from datapipe.stages.tool_program import CompiledProgramStage

    record = {"a": "[1]", "b": "[2]"}
    compiled = compile_program(".a | fromjson; .b | fromjson")
    stage = CompiledProgramStage(compiled)
    pipeline = Pipeline([JsonLoadStage(), stage, JsonDumpStage()])

    inp = tmp_path / "in.jsonl"
    out = tmp_path / "out.jsonl"
    inp.write_text(json.dumps(record) + "\n")

    pipeline.run(
        source=JsonlSource(str(inp), raw=True),
        sink=JsonlSink(str(out), raw=True),
        executor=SequentialExecutor(),
    )
    result = json.loads(out.read_text().strip())
    assert result == {"a": [1], "b": [2]}


# ---------------------------------------------------------------------------
# 9. E2E: wildcard elementwise
# ---------------------------------------------------------------------------

def test_e2e_wildcard_elementwise(tmp_path):
    from datapipe.dsl.compiler import compile_program
    from datapipe.execution.sequential import SequentialExecutor
    from datapipe.io.jsonl import JsonlSink, JsonlSource
    from datapipe.pipeline import Pipeline
    from datapipe.stage import JsonDumpStage, JsonLoadStage
    from datapipe.stages.tool_program import CompiledProgramStage

    record = {"items": ["[1]", "[2]", "[3]"]}
    compiled = compile_program(".items[] | fromjson")
    stage = CompiledProgramStage(compiled)
    pipeline = Pipeline([JsonLoadStage(), stage, JsonDumpStage()])

    inp = tmp_path / "in.jsonl"
    out = tmp_path / "out.jsonl"
    inp.write_text(json.dumps(record) + "\n")

    pipeline.run(
        source=JsonlSource(str(inp), raw=True),
        sink=JsonlSink(str(out), raw=True),
        executor=SequentialExecutor(),
    )
    result = json.loads(out.read_text().strip())
    assert result == {"items": [[1], [2], [3]]}


# ---------------------------------------------------------------------------
# 10. E2E: invocation-first with bare pipe
# ---------------------------------------------------------------------------

def test_e2e_invocation_first_with_pipe(tmp_path):
    from datapipe.dsl.compiler import compile_program
    from datapipe.execution.sequential import SequentialExecutor
    from datapipe.io.jsonl import JsonlSink, JsonlSource
    from datapipe.pipeline import Pipeline
    from datapipe.stage import JsonDumpStage, JsonLoadStage
    from datapipe.stages.tool_program import CompiledProgramStage

    # fromjson(.a) decodes the JSON string, then | tojson re-encodes it
    record = {"a": '{"x":1}'}
    compiled = compile_program("fromjson(.a) | tojson")
    stage = CompiledProgramStage(compiled)
    pipeline = Pipeline([JsonLoadStage(), stage, JsonDumpStage()])

    inp = tmp_path / "in.jsonl"
    out = tmp_path / "out.jsonl"
    inp.write_text(json.dumps(record) + "\n")

    pipeline.run(
        source=JsonlSource(str(inp), raw=True),
        sink=JsonlSink(str(out), raw=True),
        executor=SequentialExecutor(),
    )
    result = json.loads(out.read_text().strip())
    # {"x": 1} re-encoded compactly by tojson
    assert result == {"a": '{"x":1}'}


# ---------------------------------------------------------------------------
# 11. ProcessExecutor: confirms pickling still works for focused statements
# ---------------------------------------------------------------------------

def test_e2e_focused_process_executor(tmp_path):
    from datapipe.dsl.compiler import compile_program
    from datapipe.execution.process import ProcessExecutor
    from datapipe.io.jsonl import JsonlSink, JsonlSource
    from datapipe.pipeline import Pipeline
    from datapipe.stage import JsonDumpStage, JsonLoadStage
    from datapipe.stages.tool_program import CompiledProgramStage

    record = {"a": "[1]", "b": "[2]"}
    compiled = compile_program(".a | fromjson; .b | fromjson")
    stage = CompiledProgramStage(compiled)
    pipeline = Pipeline([JsonLoadStage(), stage, JsonDumpStage()])

    inp = tmp_path / "in.jsonl"
    out = tmp_path / "out.jsonl"
    inp.write_text(json.dumps(record) + "\n")

    pipeline.run(
        source=JsonlSource(str(inp), raw=True),
        sink=JsonlSink(str(out), raw=True),
        executor=ProcessExecutor(workers=2),
    )
    result = json.loads(out.read_text().strip())
    assert result == {"a": [1], "b": [2]}


# ---------------------------------------------------------------------------
# 12. Legacy pipe DeprecationWarning still fires for explicit-selector chains
# ---------------------------------------------------------------------------

def test_legacy_pipe_deprecation_warning():
    from datapipe.dsl.compiler import compile_expression
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        compile_expression("fromjson(.a) | tojson(.b)")
    dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
    assert dep_warnings, "expected a DeprecationWarning for legacy | between explicit selectors"


# ---------------------------------------------------------------------------
# 13. Bare pipe call with keyword arguments binds correctly
# ---------------------------------------------------------------------------

def test_e2e_bare_call_with_keyword_args(tmp_path):
    from datapipe.dsl.compiler import compile_program
    from datapipe.execution.sequential import SequentialExecutor
    from datapipe.io.jsonl import JsonlSink, JsonlSource
    from datapipe.pipeline import Pipeline
    from datapipe.stage import JsonDumpStage, JsonLoadStage
    from datapipe.stages.tool_program import CompiledProgramStage

    record = {"metadata": "[1,2,3]"}
    # compact=false makes tojson emit spaced separators, proving the bare
    # call's keyword argument was bound rather than silently dropped.
    compiled = compile_program(".metadata | fromjson | tojson(compact=false)")
    stage = CompiledProgramStage(compiled)
    pipeline = Pipeline([JsonLoadStage(), stage, JsonDumpStage()])

    inp = tmp_path / "in.jsonl"
    out = tmp_path / "out.jsonl"
    inp.write_text(json.dumps(record) + "\n")

    pipeline.run(
        source=JsonlSource(str(inp), raw=True),
        sink=JsonlSink(str(out), raw=True),
        executor=SequentialExecutor(),
    )
    result = json.loads(out.read_text().strip())
    assert result == {"metadata": "[1, 2, 3]"}


# ---------------------------------------------------------------------------
# 14. Parser: bare call with keyword arguments
# ---------------------------------------------------------------------------

def test_parse_bare_call_with_kwargs():
    prog = parse_program(".metadata | fromjson | tojson(compact=false)")
    stmt = prog.statements[0]
    assert len(stmt.pipes) == 1
    bare = stmt.pipes[0]
    assert bare.qualified_name.name == "tojson"
    assert len(bare.arguments) == 1
    assert bare.arguments[0].name == "compact"
    assert bare.arguments[0].value.value is False


# ---------------------------------------------------------------------------
# 15. Selector-first without a following pipe is a syntax error
# ---------------------------------------------------------------------------

def test_parse_selector_without_pipe_errors():
    with pytest.raises(ExpressionSyntaxError):
        parse_program(".metadata")


# ---------------------------------------------------------------------------
# 16. Invariant: wildcard applies the WHOLE pipe chain elementwise
# ---------------------------------------------------------------------------

def test_wildcard_applies_pipes_elementwise():
    """Each element runs the full chain, and the array is never passed as a whole.

    Test 9 only covers a wildcard base op; this pins the brief's stated
    invariant that subsequent pipes are elementwise too.
    """
    from datapipe.dsl.compiler import compile_program
    from datapipe.stages.tool_program import CompiledProgramStage

    stage = CompiledProgramStage(
        compile_program(".items[] | fromjson | tojson(compact=false)")
    )
    out = stage.process({"items": ["[1,2]", "[3]"]}, None)
    # Elementwise: each element decoded then re-encoded with spaced separators.
    # Had the chain run on the array as a whole, this would be a single string.
    assert out == {"items": ["[1, 2]", "[3]"]}


# ---------------------------------------------------------------------------
# 17. Invariant: the emitted value is the ROOT record, not the focused value
# ---------------------------------------------------------------------------

def test_root_record_is_emitted_not_focus():
    from datapipe.dsl.compiler import compile_program
    from datapipe.stages.tool_program import CompiledProgramStage

    stage = CompiledProgramStage(compile_program(".a | fromjson"))
    out = stage.process({"a": "[1]", "untouched": "keep"}, None)
    # The sibling key proves the root survived rather than being replaced by
    # the focused value ([1]).
    assert out == {"a": [1], "untouched": "keep"}


# ---------------------------------------------------------------------------
# 18. Root selector as the focus
# ---------------------------------------------------------------------------

def test_root_selector_focus():
    from datapipe.dsl.compiler import compile_program
    from datapipe.stages.tool_program import CompiledProgramStage

    stage = CompiledProgramStage(compile_program(". | tojson"))
    assert stage.process({"a": 1}, None) == '{"a":1}'


# ---------------------------------------------------------------------------
# 19. Bare-call failures are attributed to the pipe, not the base operation
# ---------------------------------------------------------------------------

def test_bare_call_error_attribution():
    from datapipe.dsl.compiler import compile_program
    from datapipe.stages.tool_program import CompiledProgramStage

    # fromjson('"not json"') -> 'not json' (a str), then the bare fromjson
    # fails on it. The error must point at the pipe (index 1), not index 0.
    stage = CompiledProgramStage(
        compile_program(".a | fromjson | fromjson"), validate="off"
    )
    with pytest.raises(ToolExecutionError) as excinfo:
        stage.process({"a": '"not json"'}, None)
    err = excinfo.value
    assert err.invocation_index == 1
    assert err.tool_name == "fromjson"
    assert err.stage == "call"
    assert err.provider_id == "builtin:json"
    assert err.selector == ".a"


# ---------------------------------------------------------------------------
# 20. Deliverable 5: provider tools resolve as focused bare pipe calls
# ---------------------------------------------------------------------------

def test_provider_tool_as_bare_pipe_call(tmp_path):
    """A bare pipe call backed by an installed provider resolves per worker.

    Exercises the CompiledBareCall.descriptor branch of _resolve_all, which
    built-in-only tests never reach.
    """
    from datapipe.dsl.compiler import compile_program
    from datapipe.stages.tool_program import CompiledProgramStage
    from datapipe.tools.installer import install_provider

    src = tmp_path / "s2_prov.py"
    src.write_text(PROVIDER_SRC)
    install_provider(src, yes=True)

    compiled = compile_program(".a | shout")
    stmt = compiled.statements[0]
    # Base operation is the provider tool; it carries a descriptor, not a
    # directly-pickleable callable.
    assert stmt.operation.tool_descriptor is not None

    stage = CompiledProgramStage(compiled)
    assert stage.process({"a": "hi"}, None) == {"a": "HI!"}


def test_provider_tool_in_pipe_position(tmp_path):
    """Provider tool used as a non-leading pipe: descriptor path in a pipe slot."""
    from datapipe.dsl.compiler import compile_program
    from datapipe.stages.tool_program import CompiledProgramStage
    from datapipe.tools.installer import install_provider

    src = tmp_path / "s2_prov2.py"
    src.write_text(PROVIDER_SRC)
    install_provider(src, yes=True)

    compiled = compile_program(".a | fromjson | shout")
    bare = compiled.statements[0].pipes[0]
    assert bare.descriptor is not None
    assert bare.callable is None

    stage = CompiledProgramStage(compiled)
    # fromjson('"hi"') -> 'hi', then the provider shout -> 'HI!'
    assert stage.process({"a": '"hi"'}, None) == {"a": "HI!"}


# ---------------------------------------------------------------------------
# 21. expression_index values are unique across the whole program
# ---------------------------------------------------------------------------

def test_expression_indices_unique_across_program():
    """Resolved callables are keyed by index, so collisions would drop tools."""
    from datapipe.dsl.compiler import compile_program

    # Statement 1: fromjson + tojson + fromjson (op + 2 pipes)
    # Statement 2: fromjson + tojson            (op + 1 pipe)
    compiled = compile_program(".a | fromjson | tojson | fromjson; .b | fromjson | tojson")
    indices = []
    for stmt in compiled.statements:
        indices.append(stmt.operation.expression_index)
        indices.extend(b.expression_index for b in stmt.pipes)
    assert len(indices) == 5
    assert len(set(indices)) == 5, f"duplicate expression_index: {indices}"


# ---------------------------------------------------------------------------
# 22. Pickle round-trip preserves pipes (spawn correctness)
# ---------------------------------------------------------------------------

def test_pickle_roundtrip_preserves_pipes():
    import pickle

    from datapipe.dsl.compiler import compile_program
    from datapipe.stages.tool_program import CompiledProgramStage

    stage = CompiledProgramStage(compile_program(".metadata | fromjson | tojson"))
    revived = pickle.loads(pickle.dumps(stage))
    assert len(revived._compiled.statements[0].pipes) == 1
    assert revived.process({"metadata": "[1,2,3]"}, None) == {"metadata": "[1,2,3]"}


# ---------------------------------------------------------------------------
# 23. Bare-call argument lists use the same comma grammar as invocations
# ---------------------------------------------------------------------------

def _bare_args(source: str):
    """Return the bare tool call's arguments from a selector-first statement."""
    stmt = parse_program(source).statements[0]
    return stmt.operation.arguments


class TestBareCallArgumentCommas:
    """A missing comma between bare-call arguments must be a syntax error.

    The invocation form (``tojson(.a, compact=false indent=2)``) already
    rejects this; the bare form used to accept it and silently bind both
    arguments.  These tests pin the two paths to one grammar.
    """

    def test_missing_comma_between_bare_arguments_rejected(self):
        with pytest.raises(ExpressionSyntaxError) as exc_info:
            parse_program(".a | tojson(compact=false indent=2)")
        message = str(exc_info.value)
        assert "expected ')'" in message
        assert "indent" in message

    def test_missing_comma_matches_invocation_diagnostic(self):
        """Same defect, same diagnostic, whichever form the user wrote."""
        from datapipe.dsl.parser import parse

        with pytest.raises(ExpressionSyntaxError) as bare_exc:
            parse_program(".a | tojson(compact=false indent=2)")
        with pytest.raises(ExpressionSyntaxError) as inv_exc:
            parse("tojson(.a, compact=false indent=2)")

        assert bare_exc.value.args[0].splitlines()[0] == inv_exc.value.args[0].splitlines()[0]

    def test_correct_commas_bind_both_arguments(self):
        args = _bare_args(".a | tojson(compact=false, indent=2)")
        assert [(a.name, a.value.value) for a in args] == [
            ("compact", False),
            ("indent", 2),
        ]

    def test_single_argument_still_parses(self):
        args = _bare_args(".a | tojson(compact=false)")
        assert [(a.name, a.value.value) for a in args] == [("compact", False)]

    def test_empty_argument_list_still_parses(self):
        stmt = parse_program(".a | tojson()").statements[0]
        assert stmt.operation.qualified_name.name == "tojson"
        assert stmt.operation.arguments == ()

    def test_no_parens_form_still_parses(self):
        stmt = parse_program(".a | tojson").statements[0]
        assert stmt.operation.qualified_name.name == "tojson"
        assert stmt.operation.arguments == ()

    def test_trailing_comma_rejected_matching_invocation_form(self):
        """``_parse_invocation`` rejects a trailing comma; the bare form must too."""
        from datapipe.dsl.parser import parse

        with pytest.raises(ExpressionSyntaxError) as bare_exc:
            parse_program(".a | tojson(compact=false,)")
        with pytest.raises(ExpressionSyntaxError) as inv_exc:
            parse("tojson(.a, compact=false,)")

        assert "expected argument name" in str(bare_exc.value)
        assert bare_exc.value.args[0].splitlines()[0] == inv_exc.value.args[0].splitlines()[0]

    def test_unterminated_bare_argument_list_rejected(self):
        with pytest.raises(ExpressionSyntaxError) as exc_info:
            parse_program(".a | tojson(compact=false")
        assert "expected ')'" in str(exc_info.value)

    def test_missing_comma_rejected_in_later_pipe_stage(self):
        """The stricter loop applies to every bare call in the chain."""
        with pytest.raises(ExpressionSyntaxError):
            parse_program(".a | fromjson | tojson(compact=false indent=2)")
