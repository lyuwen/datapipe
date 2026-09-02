"""Regression tests for the three findings in review-structural-transform-dsl-3.

Two P1s and one P3, all traceable to the literal-RHS work in 2ecd076:

1. ``. = <literal>`` silently dropped the assignment.  ``Reference.replace``
   documents root replacement as the caller's job (return the new value); the
   assignment write path called it anyway and returned the untouched record.
2. Literal assignments crashed ``inspect-expression`` and ``--dry-run``:
   ``_describe_assignment`` called ``op.source.render()``, and ``source`` is
   ``None`` for a literal RHS.
3. The legacy-``|`` migration diagnostic never reached a real user, because
   Python's default filter hides ``DeprecationWarning`` outside ``__main__``.

Finding 2 is the reason this file exercises the *inspection* surfaces and not
only the execution one: the feature was tested on the happy path it added and
never against the surfaces that read the field it introduced.  Finding 3 is
tested through the real CLI, since ``pytest.warns`` re-enables the category and
is exactly what masked the defect.
"""

from __future__ import annotations

import json
import subprocess
import sys
import warnings

import pytest

from datapipe.cli.transform import (
    _compile_or_report,
    describe_compiled,
    inspect_expression_command,
    transform_command,
)
from datapipe.dsl.compiler import compile_program
from datapipe.execution.process import ProcessExecutor
from datapipe.execution.sequential import SequentialExecutor
from datapipe.io.jsonl import JsonlSink, JsonlSource
from datapipe.pipeline import Pipeline
from datapipe.stage import JsonDumpStage, JsonLoadStage
from datapipe.stages.tool_program import CompiledProgramStage


def _run(expression: str, record):
    """Execute *expression* against *record* through the program stage."""
    stage = CompiledProgramStage(compile_program(expression), validate="off")
    return stage.process(record, None)


def _run_cli(expression: str, record, tmp_path, executor=None):
    """Execute *expression* over one record through the real pipeline path."""
    inp = tmp_path / "in.jsonl"
    out = tmp_path / "out.jsonl"
    inp.write_text(json.dumps(record) + "\n")

    Pipeline([
        JsonLoadStage(),
        CompiledProgramStage(compile_program(expression), validate="off"),
        JsonDumpStage(),
    ]).run(
        source=JsonlSource(str(inp), raw=True),
        sink=JsonlSink(str(out), raw=True),
        executor=executor or SequentialExecutor(),
        progress=False,
    )
    return json.loads(out.read_text().strip())


class _Args:
    def __init__(self, expression, *, as_json=False, dry_run=False, validate_tools="always"):
        self.expression = expression
        self.as_json = as_json
        self.dry_run = dry_run
        self.validate_tools = validate_tools


# ---------------------------------------------------------------------------
# Finding 1: `. = <literal>` must replace the whole record
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "expression,expected",
    [
        (". = 5", 5),
        ('. = "done"', "done"),
        (". = true", True),
        (". = null", None),
        ('. = {"x": 1}', {"x": 1}),
        (". = [1, 2]", [1, 2]),
        (". = 5 | tojson", "5"),
        ('. = {"x": 1} | tojson', '{"x":1}'),
    ],
)
def test_root_literal_replaces_the_record(expression, expected):
    """The pre-fix code returned the untouched record for every one of these."""
    assert _run(expression, {"old": 1}) == expected


@pytest.mark.parametrize(
    "expression,expected",
    [
        (". = 5", 5),
        ('. = {"x": 1}', {"x": 1}),
        (". = 5 | tojson", "5"),
    ],
)
def test_root_literal_replaces_the_record_through_the_pipeline(
    expression, expected, tmp_path
):
    """The same three cases through JsonLoad → stage → JsonDump and a sink."""
    assert _run_cli(expression, {"old": 1}, tmp_path) == expected


def test_root_literal_replaces_the_record_under_process_executor(tmp_path):
    """Root replacement survives the spawn boundary (everything stays pickleable)."""
    assert _run_cli(
        '. = {"x": 1}', {"old": 1}, tmp_path, executor=ProcessExecutor(workers=1)
    ) == {"x": 1}


def test_root_literal_is_visible_to_later_statements():
    """`;` commits the replacement, so the next statement sees the new record."""
    assert _run('. = {"x": 1}; .y = 2', {"old": 1}) == {"x": 1, "y": 2}


def test_root_container_literal_is_not_shared_between_records():
    """Each record gets its own copy, never the one held by the compiled program."""
    stage = CompiledProgramStage(compile_program('. = {"x": 1}'), validate="off")
    first = stage.process({"r": 1}, None)
    second = stage.process({"r": 2}, None)

    assert first is not second
    first["x"] = 99
    assert second == {"x": 1}
    # And the program itself was not mutated, so a third record is still clean.
    assert stage.process({"r": 3}, None) == {"x": 1}


def test_root_selector_source_assignment_is_still_rejected():
    """A root destination with a *selector* source stays an overlap error.

    This is why the literal form is honored rather than rejected: the reason
    `. = .meta` fails is overlap (the root is an ancestor of every path), and a
    constant has no path to overlap with.  Pinning the distinction keeps a
    later "make it symmetric" change from silently enabling a self-destructive
    root copy.
    """
    from datapipe.dsl.errors import ToolConfigurationError

    with pytest.raises(ToolConfigurationError) as excinfo:
        compile_program(". = .meta")
    assert "the destination is an ancestor of the source" in str(excinfo.value)


def test_root_scalar_literal_then_field_write_fails_cleanly():
    """Assigning a field on a now-scalar root is a structural error, not a crash."""
    from datapipe.stages.tool_program import StructuralExecutionError

    with pytest.raises(StructuralExecutionError) as excinfo:
        _run(". = 5; .y = 2", {"old": 1})
    assert excinfo.value.statement_index == 1


# ---------------------------------------------------------------------------
# Finding 2: literal assignments must describe, not crash
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "expression,value",
    [
        (".a = 5", 5),
        ('.a = {"x": [1, 2]}', {"x": [1, 2]}),
        (".a = 5 | tojson", 5),
        (". = 5", 5),
    ],
)
def test_describe_renders_a_literal_rhs_as_source_null_plus_literal(
    expression, value
):
    """The pre-fix code raised AttributeError on `op.source.render()` here."""
    doc = describe_compiled(compile_program(expression), expression)
    op = doc["statements"][0]["operation"]

    assert op["kind"] == "assignment"
    assert op["source"] is None
    assert op["literal"] == {"value": value}
    # The document must survive the round trip the --json surface performs.
    assert json.loads(json.dumps(doc))["statements"][0]["operation"] == op


def test_describe_keeps_source_populated_and_literal_null_for_a_path_rhs():
    """Both keys are always present, so a consumer switches on which is null."""
    expression = ".a = .b.c"
    op = describe_compiled(compile_program(expression), expression)[
        "statements"
    ][0]["operation"]

    assert op["source"] == ".b.c"
    assert op["literal"] is None


@pytest.mark.parametrize(
    "expression,rendered",
    [
        (".a = 5", "copy .a = 5"),
        ('.a = {"x": [1, 2]}', 'copy .a = {"x": [1, 2]}'),
        (".a = 5 | tojson", "copy .a = 5"),
        (". = 5", "copy . = 5"),
    ],
)
def test_text_inspection_renders_a_literal_rhs(expression, rendered, capsys):
    """Text output shows the constant where a path would go, and exits 0."""
    assert inspect_expression_command(_Args(expression)) == 0
    assert rendered in capsys.readouterr().out


@pytest.mark.parametrize(
    "expression", [".a = 5", '.a = {"x": [1, 2]}', ".a = 5 | tojson"]
)
def test_json_inspection_of_a_literal_assignment_exits_zero(expression, capsys):
    assert inspect_expression_command(_Args(expression, as_json=True)) == 0
    doc = json.loads(capsys.readouterr().out)
    assert doc["statements"][0]["operation"]["literal"] is not None


@pytest.mark.parametrize(
    "expression", [".a = 5", '.a = {"x": [1, 2]}', ".a = 5 | tojson"]
)
def test_dry_run_of_a_literal_assignment_exits_zero(expression, tmp_path, capsys):
    """`--dry-run` shares describe_compiled, so it died the same way."""
    inp = tmp_path / "in.jsonl"
    inp.write_text('{"a": 1}\n')
    args = _Args(expression, dry_run=True)
    args.input = str(inp)
    args.output = str(tmp_path / "out.jsonl")

    assert transform_command(args) == 0
    assert "Statements: 1" in capsys.readouterr().out


def test_text_and_json_inspection_describe_the_same_literal(capsys):
    """The text surface renders *from* the JSON document — keep that property."""
    expression = '.a = {"x": [1, 2]}'

    assert inspect_expression_command(_Args(expression, as_json=True)) == 0
    doc = json.loads(capsys.readouterr().out)

    assert inspect_expression_command(_Args(expression)) == 0
    text = capsys.readouterr().out

    literal = doc["statements"][0]["operation"]["literal"]["value"]
    assert json.dumps(literal) in text


# ---------------------------------------------------------------------------
# Finding 3: the legacy-pipe deprecation must reach a real user's stderr
# ---------------------------------------------------------------------------

LEGACY = "fromjson(.a) | tojson(.b)"

#: Invokes the real console-script entry point.  ``python -m datapipe`` is not
#: available (the package has no ``__main__``), and the installed ``datapipe``
#: script is not guaranteed to be on PATH in every test environment.
_CLI_MAIN = "import sys; from datapipe.cli.main import main; sys.exit(main())"


def test_legacy_pipe_warning_reaches_stderr_through_the_real_cli(tmp_path):
    """A subprocess run under Python's *default* filters — no pytest.warns.

    ``pytest.warns`` re-enables DeprecationWarning, which is what let this ship
    broken, so the assertion has to come from a process that never touches the
    warning filters.
    """
    inp = tmp_path / "in.jsonl"
    inp.write_text(json.dumps({"a": "[1]", "b": {"k": 1}}) + "\n")
    out = tmp_path / "out.jsonl"

    proc = subprocess.run(
        [
            sys.executable, "-c", _CLI_MAIN, "transform",
            LEGACY, str(inp), str(out), "--no-progress",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )

    assert proc.returncode == 0, proc.stderr
    assert "`|` between explicit record mutations is deprecated" in proc.stderr
    assert "fromjson(.a); tojson(.b)" in proc.stderr
    # Not fatal: the records still went through.
    assert json.loads(out.read_text().strip()) == {"a": [1], "b": '{"k":1}'}


def test_legacy_pipe_warning_is_printed_once_not_once_per_record(tmp_path):
    """Compilation happens once in the coordinator; 50 records must not warn 50x."""
    inp = tmp_path / "in.jsonl"
    inp.write_text(
        "".join(json.dumps({"a": "[1]", "b": {"k": i}}) + "\n" for i in range(50))
    )
    out = tmp_path / "out.jsonl"

    proc = subprocess.run(
        [
            sys.executable, "-c", _CLI_MAIN, "transform",
            LEGACY, str(inp), str(out), "--no-progress",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )

    assert proc.returncode == 0, proc.stderr
    assert proc.stderr.count("is deprecated") == 1
    assert len(out.read_text().strip().splitlines()) == 50


def test_cli_stderr_stays_clean_for_the_recommended_form(tmp_path):
    """The semicolon rewrite the diagnostic suggests must itself warn nothing."""
    inp = tmp_path / "in.jsonl"
    inp.write_text(json.dumps({"a": "[1]", "b": {"k": 1}}) + "\n")
    out = tmp_path / "out.jsonl"

    proc = subprocess.run(
        [
            sys.executable, "-c", _CLI_MAIN, "transform",
            "fromjson(.a); tojson(.b)", str(inp), str(out), "--no-progress",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )

    assert proc.returncode == 0, proc.stderr
    assert "deprecated" not in proc.stderr
    assert "warning:" not in proc.stderr


def test_cli_compile_still_re_issues_the_warning_for_library_callers():
    """Printing to stderr must not consume the warning a caller filters on."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        compiled = _compile_or_report(LEGACY)

    assert compiled is not None
    deprecations = [
        w for w in caught if issubclass(w.category, DeprecationWarning)
    ]
    assert len(deprecations) == 1
    assert "use semicolons" in str(deprecations[0].message)


def test_cli_compile_of_a_clean_expression_terminates_and_warns_nothing():
    """Guards the re-issue loop: appending into the recorded list would hang."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert _compile_or_report(".a = 5") is not None
    assert not [w for w in caught if issubclass(w.category, DeprecationWarning)]
