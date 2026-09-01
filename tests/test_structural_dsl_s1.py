"""Phase S1 tests: SEMICOLON token, Program/Statement AST, parse_program(),
compile_program(), CompiledProgramStage, dispatch-count invariant, and
legacy-pipe deprecation warning.
"""

from __future__ import annotations

import pytest

from datapipe.dsl.lexer import TT, tokenize
from datapipe.dsl.parser import parse_program
from datapipe.dsl.ast import Program, Statement
from datapipe.dsl.errors import ExpressionSyntaxError


# ---------------------------------------------------------------------------
# 1. Tokenizer: SEMICOLON token present
# ---------------------------------------------------------------------------

def test_tokenize_semicolon_token():
    tokens = tokenize("tojson(.a); tojson(.b)")
    types = [t.type for t in tokens]
    assert TT.SEMICOLON in types


# ---------------------------------------------------------------------------
# 2. parse_program: two-statement result
# ---------------------------------------------------------------------------

def test_parse_program_two_statements():
    result = parse_program("tojson(.a); tojson(.b)")
    assert isinstance(result, Program)
    assert len(result.statements) == 2
    for stmt in result.statements:
        assert isinstance(stmt, Statement)


# ---------------------------------------------------------------------------
# 3. parse_program: trailing semicolon succeeds, one statement
# ---------------------------------------------------------------------------

def test_parse_program_trailing_semicolon():
    result = parse_program("tojson(.a);")
    assert isinstance(result, Program)
    assert len(result.statements) == 1


# ---------------------------------------------------------------------------
# 4. parse_program: double semicolon raises with "empty statement"
# ---------------------------------------------------------------------------

def test_parse_program_empty_statement_error():
    with pytest.raises(ExpressionSyntaxError, match="empty statement"):
        parse_program("tojson(.a);; tojson(.b)")


# ---------------------------------------------------------------------------
# 5. parse_program: no semicolons yields one-statement Program
# ---------------------------------------------------------------------------

def test_parse_program_no_semicolon():
    result = parse_program("tojson(.a)")
    assert isinstance(result, Program)
    assert len(result.statements) == 1


# ---------------------------------------------------------------------------
# 6. compile_program: returns CompiledProgram with two invocations
# ---------------------------------------------------------------------------

def test_compile_program_two_invocations():
    from datapipe.dsl.compiler import compile_program, CompiledProgram
    result = compile_program("tojson(.a); tojson(.b)")
    assert isinstance(result, CompiledProgram)
    assert len(result.statements) == 2


# ---------------------------------------------------------------------------
# 7. End-to-end: SequentialExecutor
# ---------------------------------------------------------------------------

def test_e2e_sequential(tmp_path):
    from datapipe.dsl.compiler import compile_program
    from datapipe.stages.tool_program import CompiledProgramStage
    from datapipe.pipeline import Pipeline
    from datapipe.execution.sequential import SequentialExecutor
    from datapipe.io.jsonl import JsonlSource, JsonlSink
    from datapipe.stage import JsonLoadStage, JsonDumpStage
    import json

    record = {"a": "[1]", "b": "[2]"}
    compiled = compile_program("fromjson(.a); fromjson(.b)")
    stage = CompiledProgramStage(compiled)
    pipeline = Pipeline([JsonLoadStage(), stage, JsonDumpStage()])

    input_path = tmp_path / "input.jsonl"
    output_path = tmp_path / "output.jsonl"
    input_path.write_text(json.dumps(record) + "\n")

    source = JsonlSource(str(input_path), raw=True)
    sink = JsonlSink(str(output_path), raw=True)
    pipeline.run(source=source, sink=sink, executor=SequentialExecutor())

    out = json.loads(output_path.read_text().strip())
    assert out == {"a": [1], "b": [2]}


# ---------------------------------------------------------------------------
# 8. End-to-end: ProcessExecutor (proves cross-process pickling)
# ---------------------------------------------------------------------------

def test_e2e_process(tmp_path):
    from datapipe.dsl.compiler import compile_program
    from datapipe.stages.tool_program import CompiledProgramStage
    from datapipe.pipeline import Pipeline
    from datapipe.execution.process import ProcessExecutor
    from datapipe.io.jsonl import JsonlSource, JsonlSink
    from datapipe.stage import JsonLoadStage, JsonDumpStage
    import json

    record = {"a": "[1]", "b": "[2]"}
    compiled = compile_program("fromjson(.a); fromjson(.b)")
    stage = CompiledProgramStage(compiled)
    pipeline = Pipeline([JsonLoadStage(), stage, JsonDumpStage()])

    input_path = tmp_path / "input.jsonl"
    output_path = tmp_path / "output.jsonl"
    input_path.write_text(json.dumps(record) + "\n")

    source = JsonlSource(str(input_path), raw=True)
    sink = JsonlSink(str(output_path), raw=True)
    pipeline.run(
        source=source, sink=sink,
        executor=ProcessExecutor(workers=1),
    )

    out = json.loads(output_path.read_text().strip())
    assert out == {"a": [1], "b": [2]}


# ---------------------------------------------------------------------------
# 9. Dispatch count: two statements → exactly one process() call per record
# ---------------------------------------------------------------------------

def test_dispatch_count_one_per_record():
    """Two statements on one record must produce exactly one process() call."""
    from datapipe.dsl.compiler import compile_program
    from datapipe.stages.tool_program import CompiledProgramStage
    from datapipe.pipeline import Pipeline
    from datapipe.execution.sequential import SequentialExecutor
    from datapipe.io.jsonl import JsonlSource, JsonlSink
    from datapipe.stage import JsonLoadStage, JsonDumpStage
    import json
    import tempfile
    import os
    from unittest.mock import patch

    record = {"a": "[1]", "b": "[2]"}
    compiled = compile_program("fromjson(.a); fromjson(.b)")
    stage = CompiledProgramStage(compiled)
    pipeline = Pipeline([JsonLoadStage(), stage, JsonDumpStage()])

    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
        f.write(json.dumps(record) + "\n")
        input_path = f.name
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
        output_path = f.name

    call_count = []
    original_process = CompiledProgramStage.process

    def counting_process(self, value, ctx):
        call_count.append(1)
        return original_process(self, value, ctx)

    try:
        with patch.object(CompiledProgramStage, "process", counting_process):
            source = JsonlSource(input_path, raw=True)
            sink = JsonlSink(output_path, raw=True)
            pipeline.run(source=source, sink=sink, executor=SequentialExecutor())

        # One record → exactly one process() call, despite two statements
        assert len(call_count) == 1
    finally:
        os.unlink(input_path)
        os.unlink(output_path)


# ---------------------------------------------------------------------------
# 10. Legacy | diagnostic: emits DeprecationWarning for multi-invocation
# ---------------------------------------------------------------------------

def test_legacy_pipe_warning_emitted():
    from datapipe.dsl.compiler import compile_expression
    with pytest.warns(DeprecationWarning, match="semicolons"):
        compile_expression("fromjson(.a) | tojson(.b)")


# ---------------------------------------------------------------------------
# 11. Legacy | diagnostic: NOT emitted for single invocation
# ---------------------------------------------------------------------------

def test_legacy_pipe_warning_not_emitted_single():
    import warnings
    from datapipe.dsl.compiler import compile_expression
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        # Should not raise
        compile_expression("fromjson(.a)")


# ---------------------------------------------------------------------------
# 12. Routing: semicolon inside a string literal does not trigger compile_program
# ---------------------------------------------------------------------------

def test_semicolon_in_string_literal_not_detected():
    """A semicolon inside a quoted string must not be seen as a statement separator."""
    from datapipe.dsl.lexer import tokenize, TT
    tokens = tokenize('fromjson(.a, key="x;y")')
    assert not any(tok.type is TT.SEMICOLON for tok in tokens)


def test_has_semicolons_false_for_string_with_semicolon():
    """_has_semicolons returns False when the only semicolon is inside a string."""
    from datapipe.cli.transform import _has_semicolons
    assert _has_semicolons('fromjson(.a, key="x;y")') is False


def test_has_semicolons_true_for_real_separator():
    """_has_semicolons returns True when a top-level semicolon is present."""
    from datapipe.cli.transform import _has_semicolons
    assert _has_semicolons("fromjson(.a); fromjson(.b)") is True
