"""Phase S6 tests: CLI help, inspection output, diagnostics, and doc examples.

The doc-example section is the anti-rot guard.  Every expression printed in
`docs/cli.md`, `docs/migration-guide.md` and `docs/tools-authoring.md` is
extracted and compiled, and every documented input/output pair is executed and
compared.  A doc example that drifts away from what the code does fails here
rather than being discovered by a reader.
"""

from __future__ import annotations

import copy
import json
import re
import warnings
from pathlib import Path

import pytest

from datapipe.cli.transform import (
    EXPRESSION_EPILOG,
    EXPRESSION_LANGUAGE_VERSION,
    _compile_or_report,
    describe_compiled,
    inspect_expression_command,
)
from datapipe.dsl.compiler import CompiledProgram
from datapipe.dsl.errors import ExpressionSyntaxError
from datapipe.stages.tool_program import (
    CompiledProgramStage,
    CompiledToolProgramStage,
)

DOCS = Path(__file__).resolve().parent.parent / "docs"


def _compile(expression: str):
    compiled = _compile_or_report(expression)
    assert compiled is not None, f"failed to compile: {expression}"
    return compiled


def _run(expression: str, record):
    compiled = _compile(expression)
    stage = (
        CompiledProgramStage(compiled)
        if isinstance(compiled, CompiledProgram)
        else CompiledToolProgramStage(compiled)
    )
    return stage.process(copy.deepcopy(record), None)


class _Args:
    def __init__(self, expression, as_json=False, validate_tools="always"):
        self.expression = expression
        self.as_json = as_json
        self.validate_tools = validate_tools


# ===========================================================================
# 1. Expression-language version
# ===========================================================================


def test_text_inspection_reports_language_version_two(capsys):
    assert inspect_expression_command(_Args("fromjson(.a)")) == 0
    assert "expression-language: 2" in capsys.readouterr().out


def test_json_inspection_reports_language_version_two(capsys):
    assert inspect_expression_command(_Args("fromjson(.a)", as_json=True)) == 0
    doc = json.loads(capsys.readouterr().out)
    assert doc["expression_language"] == 2


def test_language_version_is_reported_for_programs_too(capsys):
    assert inspect_expression_command(_Args(".a << .b", as_json=True)) == 0
    assert json.loads(capsys.readouterr().out)["expression_language"] == 2


def test_version_constant_matches_what_is_emitted():
    """The constant is the single source of both output paths."""
    assert EXPRESSION_LANGUAGE_VERSION == 2
    for expression in ("fromjson(.a)", ".a << .b"):
        doc = describe_compiled(_compile(expression), expression)
        assert doc["expression_language"] == EXPRESSION_LANGUAGE_VERSION


def test_the_planned_not_yet_active_note_is_gone(capsys):
    """S0's parenthetical described extensions that are now shipped."""
    inspect_expression_command(_Args("fromjson(.a)"))
    out = capsys.readouterr().out
    assert "planned" not in out
    assert "not yet active" not in out


# ===========================================================================
# 2. §13.4 inspection shape — text path
# ===========================================================================

#: The plan's §13.4 example program: a complement move-into with a trailing
#: pipe, then a whole-record call.
S13_4_EXPRESSION = (
    ".metadata << .(^instance_id|messages|tools) | tojson; nest(., key=\"m\")"
)


def test_text_inspection_matches_the_plan_shape(capsys):
    assert inspect_expression_command(_Args(S13_4_EXPRESSION)) == 0
    out = capsys.readouterr().out

    # Statement boundaries are numbered headers, not bracketed indexes.
    assert "  Statement 0" in out
    assert "  Statement 1" in out
    # Operation kind and destination.
    assert "    move-into .metadata" in out
    # Sources on one line, complement rendered as such.
    assert "      sources: complement(instance_id, messages, tools)" in out
    # Pipes are labelled as pipes.
    assert "pipe: tojson" in out
    # A whole-record call reads "call <tool> at <selector>".
    assert "call nest at ." in out


def test_focus_is_shown_on_the_statement_header(capsys):
    inspect_expression_command(_Args(".metadata | fromjson | tojson"))
    assert "Statement 0  (focus: .metadata)" in capsys.readouterr().out


def test_assignment_statements_name_their_verb(capsys):
    inspect_expression_command(_Args(".a = .b; .c <- .d"))
    out = capsys.readouterr().out
    assert "    copy .a = .b" in out
    assert "    move .c <- .d" in out


def test_multiple_move_sources_render_on_one_sources_line(capsys):
    inspect_expression_command(_Args(".metadata << .a, .(^b|c)"))
    assert "      sources: .a, complement(b, c)" in capsys.readouterr().out


# ===========================================================================
# 3. JSON inspection exposes the same structure
# ===========================================================================


def test_json_inspection_exposes_statements_focus_operation_and_pipes(capsys):
    assert inspect_expression_command(_Args(S13_4_EXPRESSION, as_json=True)) == 0
    doc = json.loads(capsys.readouterr().out)

    assert doc["expression_language"] == 2
    assert [s["index"] for s in doc["statements"]] == [0, 1]

    first, second = doc["statements"]
    assert first["focus"] == ".metadata"
    assert first["operation"]["kind"] == "move_into"
    assert first["operation"]["destination"] == ".metadata"
    source = first["operation"]["sources"][0]
    assert source["kind"] == "field_set"
    assert source["complement"] is True
    assert source["names"] == ["instance_id", "messages", "tools"]
    assert [p["tool"] for p in first["pipes"]] == ["tojson"]

    assert second["focus"] is None
    assert second["operation"]["kind"] == "invocation"
    assert second["operation"]["tool"] == "nest"
    assert second["operation"]["selector"] == "."
    assert second["pipes"] == []


def test_json_and_text_describe_the_same_statements(capsys):
    """Every fact the text path prints is present in the JSON document."""
    inspect_expression_command(_Args(S13_4_EXPRESSION))
    text = capsys.readouterr().out
    inspect_expression_command(_Args(S13_4_EXPRESSION, as_json=True))
    doc = json.loads(capsys.readouterr().out)

    assert text.count("Statement ") == len(doc["statements"])
    for stmt in doc["statements"]:
        if stmt["focus"] is not None:
            assert f"(focus: {stmt['focus']})" in text
        op = stmt["operation"]
        if op["kind"] == "move_into":
            assert f"move-into {op['destination']}" in text
        else:
            assert f"call {op['tool']} at {op['selector']}" in text
        for pipe in stmt["pipes"]:
            assert f"pipe: {pipe['tool']}" in text


# ===========================================================================
# 4. --dry-run and inspect-expression agree
# ===========================================================================


@pytest.mark.parametrize(
    "expression",
    [
        "fromjson(.a)",
        S13_4_EXPRESSION,
        ".a = .b; .c <- fromjson(.d)",
        ".metadata << .(temperature|score) | tojson",
    ],
)
def test_dry_run_and_inspect_expression_agree(expression, tmp_path, capsys):
    from datapipe.cli.main import main

    inspect_expression_command(_Args(expression, as_json=True))
    inspected = json.loads(capsys.readouterr().out)

    inp = tmp_path / "in.jsonl"
    inp.write_text("{}\n")
    assert main([
        "transform", "--dry-run", "--json",
        expression, str(inp), str(tmp_path / "out.jsonl"),
    ]) == 0
    dry_run = json.loads(capsys.readouterr().out)

    assert dry_run == inspected


# ===========================================================================
# 5. §13.3 legacy pipe deprecation
# ===========================================================================


def test_legacy_pipe_deprecation_matches_the_plan_wording():
    from datapipe.dsl.compiler import compile_expression

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        compile_expression("fromjson(.tools) | tojson(.metadata)")

    assert len(caught) == 1
    assert caught[0].category is DeprecationWarning
    message = str(caught[0].message)
    # The plan's wording, and a rewrite the user can paste.
    assert message.startswith(
        "`|` between explicit record mutations is deprecated; use semicolons:"
    )
    assert "fromjson(.tools); tojson(.metadata)" in message


def test_legacy_pipe_still_produces_the_documented_result():
    """Deprecated, not broken — the compatibility window is still open."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        result = _run("fromjson(.a) | tojson(.b)", {"a": "[1]", "b": {"k": 1}})
    assert result == {"a": [1], "b": '{"k":1}'}


# ===========================================================================
# 6. §13.3 ambiguity: do not guess, suggest a rewrite
# ===========================================================================

AMBIGUOUS = [
    ("fromjson(.a) | tojson | tojson(.b)", "fromjson(.a) | tojson; tojson(.b)"),
    ("fromjson(.a) | tojson(.b) | tojson", "fromjson(.a); tojson(.b) | tojson"),
    (".a | fromjson | tojson(.b)", ".a | fromjson; tojson(.b)"),
]


@pytest.mark.parametrize("expression,rewrite", AMBIGUOUS, ids=[a[0] for a in AMBIGUOUS])
def test_ambiguous_focus_and_explicit_target_mix_is_rejected(expression, rewrite):
    """§13.3: 'Do not guess when either side uses bare focus semantics.'"""
    from datapipe.dsl.parser import parse_program

    with pytest.raises(ExpressionSyntaxError) as exc:
        parse_program(expression)

    # Anchored on the diagnostic's own words, not on text that also appears in
    # the echoed expression.
    assert exc.value.base_message.startswith("ambiguous `|`:")
    assert "cannot mean both" in exc.value.base_message
    assert rewrite in exc.value.base_message


def test_ambiguous_expression_fails_the_cli_with_a_rewrite(capsys):
    assert _compile_or_report("fromjson(.a) | tojson | tojson(.b)") is None
    err = capsys.readouterr().err
    assert "ambiguous `|`" in err
    assert "fromjson(.a) | tojson; tojson(.b)" in err


@pytest.mark.parametrize(
    "expression",
    [
        "fromjson(.a) | tojson",          # focused pipe, unambiguous
        "fromjson(.a); tojson(.b)",       # canonical sequencing
        ".metadata | fromjson | tojson",  # selector-first focused chain
        ".a << .(b|c) | tojson",          # field-set union inside `.( )`
    ],
)
def test_unambiguous_expressions_are_unaffected(expression):
    """The new rejection must not catch any legitimate use of `|`."""
    from datapipe.dsl.parser import parse_program

    parse_program(expression)


# ===========================================================================
# 7. Documentation examples are executable
# ===========================================================================

DOC_FILES = ["cli.md", "migration-guide.md", "tools-authoring.md"]

#: Expressions the docs deliberately show as *rejected* — they must not be
#: asserted to compile.  Keyed by the exact text as it appears in the doc.
DOC_NEGATIVE_EXAMPLES = {
    "fromjson(.a) | tojson | tojson(.b)",
}

#: Tools the docs mention that are not installed in a test environment.
DOC_UNINSTALLED_TOOLS = ("normalize_text", "recursive_decode", "nest_config")


def _fenced_blocks(text: str) -> "list[tuple[str, str]]":
    return re.findall(r"```(\w*)\n(.*?)```", text, re.S)


def _transform_expressions(block: str) -> "list[str]":
    """Extract single-quoted expressions from `datapipe transform`/`inspect` lines."""
    joined = block.replace("\\\n", " ")
    found = []
    for line in joined.splitlines():
        if "datapipe transform" not in line and "inspect-expression" not in line:
            continue
        found.extend(re.findall(r"'([^']*)'", line))
    return found


def _doc_expressions() -> "list[tuple[str, str]]":
    out = []
    for name in DOC_FILES:
        for lang, block in _fenced_blocks((DOCS / name).read_text()):
            if lang not in ("bash", "zsh"):
                continue
            for expression in _transform_expressions(block):
                out.append((name, expression))
    return out


DOC_EXPRESSIONS = _doc_expressions()


def test_the_docs_actually_contain_structural_examples():
    """Guards the extractor: a broken regex must not vacuously pass §7."""
    expressions = [e for _f, e in DOC_EXPRESSIONS]
    assert len(expressions) >= 20, expressions
    for operator in (";", "<<", "^", "<-", "="):
        assert any(operator in e for e in expressions), (
            f"no documented example uses {operator!r}"
        )
    assert any("nest(" in e for e in expressions)
    assert any("unnest(" in e for e in expressions)


@pytest.mark.parametrize(
    "doc,expression",
    DOC_EXPRESSIONS,
    ids=[f"{d}:{e[:48]}" for d, e in DOC_EXPRESSIONS],
)
def test_every_documented_expression_compiles(doc, expression):
    if expression in DOC_NEGATIVE_EXAMPLES:
        pytest.skip("documented as an error example")
    if any(t in expression for t in DOC_UNINSTALLED_TOOLS):
        pytest.skip("references a tool the doc only describes installing")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        from datapipe.dsl.compiler import compile_expression, compile_program
        from datapipe.dsl.parser import parse_program

        try:
            parse_program(expression)
            compile_program(expression)
        except ExpressionSyntaxError:
            # Legacy `a(.x) | b(.y)` parses only under the deprecated grammar.
            compile_expression(expression)


def _doc_io_cases() -> "list[tuple[str, str, dict, dict]]":
    """Pair each documented expression with the input/output block after it.

    A ```json block following a transform command is a result block: two lines
    are (input, expected output); one line is the expected output for the most
    recently declared input.  A ```json block that does not follow a transform
    command declares an input for the examples that follow it.

    A declared input stays current across intervening command fences: the docs
    routinely show one input record and then several commands that each
    transform it.  Clearing it at the fence silently dropped three of the
    migration guide's examples -- including both headline nesting examples --
    from verification, so a documented output could be corrupted without
    failing any test.
    """
    cases = []
    for name in DOC_FILES:
        blocks = _fenced_blocks((DOCS / name).read_text())
        current_input = None
        pending = None
        for lang, block in blocks:
            if lang in ("bash", "zsh"):
                found = _transform_expressions(block)
                pending = found[-1] if found else None
                continue
            if lang != "json":
                pending = None
                continue

            lines = [ln for ln in block.strip().splitlines() if ln.strip()]
            if pending is None:
                if len(lines) == 1:
                    current_input = json.loads(lines[0])
                continue

            if len(lines) == 2:
                record, expected = json.loads(lines[0]), json.loads(lines[1])
                current_input = record
            elif len(lines) == 1 and current_input is not None:
                record, expected = current_input, json.loads(lines[0])
            elif len(lines) == 1:
                # No input declared yet, so this single-line block is the
                # section's input rather than a result -- a prose section can
                # open with a command and only then show the record it applies
                # to.  Adopting it here is what keeps the examples that follow
                # verifiable; discarding it silently dropped three of the
                # migration guide's examples, both headline ones among them.
                current_input = json.loads(lines[0])
                pending = None
                continue
            else:
                pending = None
                continue

            cases.append((name, pending, record, expected))
            pending = None
    return cases


DOC_IO_CASES = _doc_io_cases()

#: Minimum verified input/output pairs per file.  A global floor alone let
#: migration-guide.md lose three examples while cli.md's contribution kept the
#: total above the bar.
DOC_IO_FLOOR_PER_FILE = {"cli.md": 13, "migration-guide.md": 7}


def test_the_docs_contain_verifiable_input_output_pairs():
    """Guards the extractor for the execution cases below."""
    assert len(DOC_IO_CASES) >= 15, [c[1] for c in DOC_IO_CASES]
    assert any("<<" in c[1] for c in DOC_IO_CASES)
    assert any(";" in c[1] for c in DOC_IO_CASES)


@pytest.mark.parametrize(
    "doc,expression,record,expected",
    DOC_IO_CASES,
    ids=[f"{d}:{e[:44]}" for d, e, _r, _x in DOC_IO_CASES],
)
def test_documented_outputs_are_what_the_code_produces(doc, expression, record, expected):
    if any(t in expression for t in DOC_UNINSTALLED_TOOLS):
        pytest.skip("references a tool the doc only describes installing")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        result = _run(expression, record)

    assert result == expected, (
        f"{doc} documents {expression!r} as producing {expected!r}, "
        f"but it produces {result!r}"
    )
    # Key order is part of the documented output for structural moves.
    assert list(result) == list(expected)


def test_epilog_examples_compile():
    """The CLI's own --help examples are held to the same standard."""
    text = EXPRESSION_EPILOG.replace("\\\n", " ")
    expressions = []
    for line in text.splitlines():
        if "datapipe transform" in line or line.strip().startswith("'"):
            expressions.extend(re.findall(r"'([^']*)'", line))
    # Continuation lines carry the expression on their own line.
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("'") and stripped.endswith("' \\"):
            expressions.append(stripped[1:-3])

    compilable = [e for e in expressions if e.startswith((".", "n", "u", "t", "f"))]
    assert len(compilable) >= 5, compilable
    for expression in compilable:
        _compile(expression)


def test_epilog_documents_shell_quoting_for_every_metacharacter():
    for token in ("<<", "<-", ";", "|", "(", ")", "^", "[]"):
        assert token in EXPRESSION_EPILOG
    assert "single quote" in EXPRESSION_EPILOG.lower()


# ===========================================================================
# 8. Carried-forward findings
# ===========================================================================


def test_complement_under_a_root_destination_now_compiles():
    """S4 over-rejected this; S6 narrowed the check to base == destination."""
    from datapipe.dsl.compiler import compile_program

    compile_program(". << .metadata.(^note)")

    assert _run(". << .metadata.(^note)", {
        "id": "i1", "metadata": {"temperature": 0.7, "score": 9, "note": "keep"},
    }) == {"id": "i1", "metadata": {"note": "keep"}, "temperature": 0.7, "score": 9}


def test_a_complement_whose_base_is_the_destination_is_still_rejected():
    """The one case that is provably wrong for every record stays static."""
    from datapipe.dsl.compiler import compile_program
    from datapipe.dsl.errors import ToolConfigurationError

    with pytest.raises(ToolConfigurationError) as exc:
        compile_program(".m << .m.(^a)")
    assert "the source is the destination itself" in exc.value.base_message


def test_a_complement_member_that_would_collide_still_fails_at_runtime():
    """Narrowing the static check must not weaken the guarantee."""
    from datapipe.tools.errors import StructuralExecutionError

    record = {"metadata": {"metadata": {"deep": 1}, "note": "n", "x": 2}}
    before = copy.deepcopy(record)
    with pytest.raises(StructuralExecutionError) as exc:
        _run(". << .metadata.(^note)", record)
    assert "overlapping source and destination" in exc.value.reason
    assert record == before


def test_unnest_exclude_emits_a_complement_not_an_expansion():
    """The workaround the over-rejection forced is gone."""
    import datapipe.tools.builtins.structural as structural

    assert not hasattr(structural, "_expand_complement")

    stage = structural._unnest_program("m", ("note",), True, False)
    operation = stage._compiled.statements[0].operation
    assert operation.sources[0].complement is True
    assert operation.sources[0].names == ("note",)


def test_unnest_exclude_still_matches_its_symbolic_form():
    from datapipe.tools.builtins.structural import unnest

    record = {"id": "x", "m": {"a": 1, "b": 2, "c": 3}}
    assert unnest(copy.deepcopy(record), key="m", exclude=["b"]) == _run(
        ". << .m.(^b)", record
    )


# --- validate-mode propagation ---------------------------------------------


def _modes_seen(expression: str, validate: str) -> "list[tuple[str, bool]]":
    """Record the (mode, checking) decision of every stage that runs."""
    seen: "list[tuple[str, bool]]" = []
    original = CompiledProgramStage._should_validate_record

    def spy(self):
        decision = original(self)
        seen.append((self.validate, decision))
        return decision

    CompiledProgramStage._should_validate_record = spy
    try:
        from datapipe.dsl.compiler import compile_program

        stage = CompiledProgramStage(compile_program(expression), validate=validate)
        stage.process({"m": '{"a": 1, "b": 2}'}, None)
    finally:
        CompiledProgramStage._should_validate_record = original
    return seen


UNNEST_EXPRESSION = 'unnest(., key="m", include=["a"], parse=true)'


def test_validate_off_reaches_the_desugared_inner_program():
    seen = _modes_seen(UNNEST_EXPRESSION, "off")
    # The outer stage plus the two inner stages unnest builds.
    assert len(seen) >= 3
    assert all(mode == "off" for mode, _ in seen), seen
    assert not any(checking for _m, checking in seen)


def test_validate_always_still_validates_the_inner_program():
    seen = _modes_seen(UNNEST_EXPRESSION, "always")
    assert len(seen) >= 3
    assert all(mode == "always" and checking for mode, checking in seen), seen


def test_nest_also_honours_the_outer_mode():
    seen = _modes_seen('nest(., key="bag", exclude=["m"])', "off")
    assert len(seen) >= 2
    assert all(mode == "off" for mode, _ in seen), seen


def test_validation_mode_does_not_change_results():
    """Propagation is a cost decision, never a semantics one."""
    from datapipe.dsl.compiler import compile_program

    record = {"m": '{"a": 1, "b": 2}'}
    results = [
        CompiledProgramStage(
            compile_program(UNNEST_EXPRESSION), validate=mode
        ).process(copy.deepcopy(record), None)
        for mode in ("always", "sample", "off")
    ]
    assert results[0] == results[1] == results[2]
    assert results[0] == {"m": {"b": 2}, "a": 1}


def test_with_validate_shares_resolution_and_resets_the_sample_counter():
    from datapipe.dsl.compiler import compile_program

    stage = CompiledProgramStage(compile_program("fromjson(.m)"), validate="sample")
    stage.process({"m": "1"}, None)
    clone = stage.with_validate("off")

    assert clone.validate == "off"
    assert stage.validate == "sample"
    assert clone._resolved_fns is stage._resolved_fns
    assert clone._validated_records == 0


def test_structural_module_documents_its_degenerate_cases():
    """S5 finding: 'equivalence by construction' has two exceptions."""
    import datapipe.tools.builtins.structural as structural
    from datapipe.dsl.errors import ExpressionSyntaxError as _SyntaxError

    doc = structural.__doc__
    assert "no symbolic counterpart" in doc
    assert "include=[]" in doc

    # The reason there is no counterpart: the grammar has no empty field set.
    from datapipe.dsl.parser import parse_program

    with pytest.raises(_SyntaxError):
        parse_program(".m << .()")

    # And the documented fallback behaviour holds.
    assert structural.nest({"a": 1, "b": 2}, key="m", include=[]) == {
        "m": {"a": 1, "b": 2}
    }
