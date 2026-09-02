"""Phase S2 CLI tests: expression routing and --dry-run description.

Two defects are covered here:

1. Focused pipes were unreachable from the CLI.  ``_compile_or_report`` routed
   on the presence of a semicolon, so ``.metadata | fromjson`` (selector-first)
   and ``fromjson(.a) | tojson`` (invocation-first with a bare pipe) both fell
   through to ``compile_expression``, which cannot parse either form.
2. ``describe_compiled`` iterated ``compiled.invocations`` unconditionally, so
   ``--dry-run`` / ``inspect-expression`` raised ``AttributeError`` on any
   ``CompiledProgram`` (which only has ``statements``).
"""

from __future__ import annotations

import json
import warnings

import pytest

from datapipe.cli.main import main
from datapipe.cli.transform import _compile_or_report, describe_compiled
from datapipe.dsl.compiler import (
    CompiledExpression,
    CompiledProgram,
    compile_expression,
    compile_program,
)


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


def _compile_quietly(expression):
    """Compile via the CLI router with DeprecationWarnings suppressed."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        return _compile_or_report(expression)


# ---------------------------------------------------------------------------
# Defect 1 — routing.  Each of the six cases reaches the expected compiler.
# ---------------------------------------------------------------------------


class TestExpressionRouting:
    @pytest.mark.parametrize("expression", [
        ".metadata | fromjson",          # selector-first focused statement
        "fromjson(.a) | tojson",         # invocation-first with a bare pipe
        "fromjson(.a); tojson(.b)",      # multi-statement program
    ])
    def test_program_syntax_routes_to_compile_program(self, expression):
        """Forms only ``compile_program`` can parse must reach it."""
        compiled = _compile_quietly(expression)
        assert isinstance(compiled, CompiledProgram), (
            f"{expression!r} did not reach the compile_program path"
        )

    def test_legacy_pipe_routes_to_compile_expression(self):
        """``invocation | invocation`` is only parseable by the legacy path."""
        compiled = _compile_quietly("fromjson(.a) | tojson(.b)")
        assert isinstance(compiled, CompiledExpression)

    def test_syntax_error_reports_cleanly(self, capsys):
        """A genuine syntax error yields a diagnostic and no traceback."""
        assert _compile_or_report("fromjson(") is None
        assert "error" in capsys.readouterr().err.lower()

    @pytest.mark.parametrize("expression, expected", [
        # parse_program: "requires '|', '=', or '<-' after selector"
        # legacy:        "expected tool name, got DOT"
        (".a", "after selector"),
        # parse_program: "expected ';' or end of expression after a statement"
        # legacy:        "unexpected token IDENT 'tojson'"
        ("fromjson(.a) tojson", "expected ';'"),
    ])
    def test_unparseable_reports_the_program_diagnostic(
        self, expression, expected, capsys,
    ):
        """When neither grammar parses, the parse_program error is reported.

        The multi-statement form is the canonical language, so its diagnostic
        wins.  Both grammars reject these inputs with *different* messages, so
        this genuinely distinguishes which error surfaced.
        """
        assert _compile_or_report(expression) is None
        err = capsys.readouterr().err
        assert expected in err, f"reported the legacy diagnostic instead: {err!r}"

    def test_single_invocation_keeps_the_legacy_shape(self):
        """``fromjson(.a)`` deliberately stays on the legacy path.

        Either path is functionally acceptable, but the legacy one holds the
        public ``--json`` output shape (top-level ``invocations``) stable.
        """
        assert isinstance(_compile_quietly("fromjson(.a)"), CompiledExpression)

    def test_selector_first_is_reachable_at_all(self):
        """Regression guard: the defect made this return None via a parse error."""
        assert _compile_quietly(".metadata | fromjson") is not None

    def test_legacy_pipe_still_warns(self, capsys):
        """The deprecation diagnostic for ``a | b`` survives the routing change.

        The CLI delivers it as a printed `warning:` line; the library-level
        ``compile_expression`` still raises the ``DeprecationWarning`` that
        callers filter on.
        """
        _compile_or_report("fromjson(.a) | tojson(.b)")
        assert "deprecated" in capsys.readouterr().err

        with pytest.warns(DeprecationWarning, match="deprecated"):
            compile_expression("fromjson(.a) | tojson(.b)")

    def test_semicolon_inside_string_stays_on_legacy_path(self):
        """Routing is grammar-driven, not a semicolon-character scan.

        Uses a real ``fromjson`` argument so the expression actually compiles —
        a bogus argument name would fail binding and make the assertion vacuous.
        """
        compiled = _compile_quietly('fromjson(.a, containers_only=true)')
        assert isinstance(compiled, CompiledExpression)

    def test_no_dead_has_semicolons_helper(self):
        """``_has_semicolons`` is gone; nothing should reference it."""
        import datapipe.cli.transform as transform_mod

        assert not hasattr(transform_mod, "_has_semicolons")


# ---------------------------------------------------------------------------
# Defect 2 — describe_compiled must handle CompiledProgram.
# ---------------------------------------------------------------------------


class TestDescribeCompiled:
    def test_multi_statement_program_describes_two_statements(self):
        """The AttributeError case: a ``;`` program has no ``.invocations``."""
        expression = "fromjson(.a); fromjson(.b)"
        doc = describe_compiled(compile_program(expression), expression)

        assert isinstance(doc, dict)
        assert len(doc["statements"]) == 2
        assert [s["index"] for s in doc["statements"]] == [0, 1]
        assert [s["operation"]["tool"] for s in doc["statements"]] == [
            "fromjson", "fromjson",
        ]
        assert [s["operation"]["selector"] for s in doc["statements"]] == [".a", ".b"]
        # Invocation-first statements carry no focus.
        assert all(s["focus"] is None for s in doc["statements"])
        assert doc["expression"] == expression

    def test_focused_program_includes_rendered_focus_selector(self):
        """Selector-first statements report their rendered focus."""
        expression = ".metadata | fromjson"
        doc = describe_compiled(compile_program(expression), expression)

        assert len(doc["statements"]) == 1
        assert doc["statements"][0]["focus"] == ".metadata"

    def test_program_pipes_are_described_without_a_selector(self):
        """``CompiledBareCall`` has no ``selector`` field, so none is rendered."""
        expression = ".metadata | fromjson | tojson"
        doc = describe_compiled(compile_program(expression), expression)

        pipes = doc["statements"][0]["pipes"]
        assert len(pipes) == 1
        assert pipes[0]["tool"] == "tojson"
        assert "selector" not in pipes[0]
        assert pipes[0]["provider"]["mode"] == "builtin"
        assert pipes[0]["contract"]["cardinality"] == "one_to_one"

    def test_program_description_is_json_serializable(self):
        """``--dry-run --json`` must be able to dump the program document."""
        expression = ".metadata | fromjson | tojson; tojson(.b)"
        doc = describe_compiled(compile_program(expression), expression)
        raw = json.dumps(doc)
        assert json.loads(raw) == doc
        assert "True" not in raw and "False" not in raw

    def test_legacy_expression_description_unchanged(self):
        """``CompiledExpression`` keeps the historical ``invocations`` shape."""
        expression = "fromjson(.a)"
        doc = describe_compiled(compile_expression(expression), expression)

        assert "statements" not in doc
        inv = doc["invocations"][0]
        assert inv["tool"] == "fromjson"
        assert inv["selector"] == ".a"
        assert inv["contract"]["deterministic"] is True
        assert inv["arguments"]["recursive"] is False
        assert [s["type"] for s in doc["stages"]] == [
            "JsonLoadStage", "CompiledToolProgramStage", "JsonDumpStage",
        ]

    def test_provider_backed_pipe_recovers_its_contract(self, tmp_path):
        """A provider bare call holds only a descriptor, so the contract is
        recovered by resolving the tool.  Built-in tools skip that branch."""
        from datapipe.tools.installer import install_provider

        src = tmp_path / "prov.py"
        src.write_text(PROVIDER_SRC)
        install_provider(src, yes=True)

        expression = ".msg | shout | shout"
        doc = describe_compiled(compile_program(expression), expression)
        pipe = doc["statements"][0]["pipes"][0]

        assert pipe["tool"] == "shout"
        assert pipe["provider"]["mode"] == "copied"
        assert pipe["contract"]["output"] == "string"
        assert pipe["arguments"] == {"suffix": "!"}
        # Must stay dumpable for --dry-run --json.
        assert json.loads(json.dumps(doc)) == doc

    def test_program_reports_the_program_stage(self):
        """A program routes through ``CompiledProgramStage``, not the legacy stage."""
        expression = "fromjson(.a); fromjson(.b)"
        doc = describe_compiled(compile_program(expression), expression)
        assert [s["type"] for s in doc["stages"]] == [
            "JsonLoadStage", "CompiledProgramStage", "JsonDumpStage",
        ]


# ---------------------------------------------------------------------------
# End-to-end CLI surfaces.
# ---------------------------------------------------------------------------


class TestCliEndToEnd:
    def test_inspect_expression_accepts_focused_pipe(self, capsys):
        """The headline defect: this exited non-zero with a parse error."""
        rc = main(["inspect-expression", ".metadata | fromjson"])
        out, err = capsys.readouterr()
        assert rc == 0, err
        assert "error" not in err.lower()
        assert ".metadata" in out

    def test_inspect_expression_json_for_focused_pipe(self, capsys):
        rc = main(["inspect-expression", "--json", ".metadata | fromjson | tojson"])
        assert rc == 0
        doc = json.loads(capsys.readouterr().out)
        assert doc["statements"][0]["focus"] == ".metadata"
        assert doc["statements"][0]["operation"]["tool"] == "fromjson"
        assert [p["tool"] for p in doc["statements"][0]["pipes"]] == ["tojson"]

    def test_dry_run_on_multi_statement_program(self, capsys):
        """Previously an AttributeError on ``compiled.invocations``."""
        rc = main([
            "transform", "--dry-run",
            "fromjson(.a); tojson(.b)", "in.jsonl", "out.jsonl",
        ])
        out, err = capsys.readouterr()
        assert rc == 0, err
        assert "Statements: 2" in out
        assert "CompiledProgramStage" in out

    def test_dry_run_json_on_multi_statement_program(self, capsys):
        rc = main([
            "transform", "--dry-run", "--json",
            "fromjson(.a); tojson(.b)", "in.jsonl", "out.jsonl",
        ])
        assert rc == 0
        doc = json.loads(capsys.readouterr().out)
        assert len(doc["statements"]) == 2

    def test_dry_run_text_shows_focus(self, capsys):
        rc = main([
            "transform", "--dry-run",
            ".metadata | fromjson", "in.jsonl", "out.jsonl",
        ])
        assert rc == 0
        assert "focus: .metadata" in capsys.readouterr().out

    def test_transform_runs_focused_pipe_over_records(self, tmp_path, capsys):
        """Full data path for a focused statement, end to end."""
        src = tmp_path / "in.jsonl"
        src.write_text('{"metadata": "[1, 2, 3]"}\n')
        dst = tmp_path / "out.jsonl"

        rc = main([
            "transform", ".metadata | fromjson",
            str(src), str(dst),
            "--executor", "sequential", "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err
        assert json.loads(dst.read_text().strip()) == {"metadata": [1, 2, 3]}

    def test_transform_runs_multi_statement_program(self, tmp_path, capsys):
        src = tmp_path / "in.jsonl"
        src.write_text('{"a": "[1]", "b": "[2]"}\n')
        dst = tmp_path / "out.jsonl"

        rc = main([
            "transform", "fromjson(.a); fromjson(.b)",
            str(src), str(dst),
            "--executor", "sequential", "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err
        assert json.loads(dst.read_text().strip()) == {"a": [1], "b": [2]}

    def test_bad_expression_exits_nonzero(self, capsys):
        rc = main(["inspect-expression", "fromjson("])
        assert rc == 1
        assert "error" in capsys.readouterr().err.lower()


class TestRouterExceptionContainment:
    """``_compile_or_report`` must not leak non-syntax exceptions as tracebacks.

    The first ``parse_program`` call used to sit outside the ``try`` block that
    reports "error compiling expression:", so anything other than an
    ``ExpressionSyntaxError`` escaped to the caller as a raw traceback.
    """

    def test_non_syntax_error_in_first_parse_is_reported(self, capsys, monkeypatch):
        import datapipe.dsl.parser as parser_mod

        def boom(expression):
            raise RuntimeError("parser exploded")

        monkeypatch.setattr(parser_mod, "parse_program", boom)

        result = _compile_or_report("fromjson(.a)")

        assert result is None
        err = capsys.readouterr().err
        assert "error compiling expression:" in err
        assert "parser exploded" in err

    def test_non_syntax_error_propagates_to_cli_exit_code(self, capsys, monkeypatch):
        import datapipe.dsl.parser as parser_mod

        def boom(expression):
            raise RuntimeError("parser exploded")

        monkeypatch.setattr(parser_mod, "parse_program", boom)

        rc = main(["inspect-expression", "fromjson(.a)"])

        assert rc == 1
        assert "parser exploded" in capsys.readouterr().err


class TestRoutingUnchangedAfterContainmentFix:
    """The six routing cases must behave identically after the try-block move."""

    def test_selector_first_routes_to_program(self):
        assert isinstance(_compile_or_report(".metadata | fromjson"), CompiledProgram)

    def test_invocation_with_bare_pipe_routes_to_program(self):
        assert isinstance(_compile_or_report("fromjson(.a) | tojson"), CompiledProgram)

    def test_multi_statement_routes_to_program(self):
        assert isinstance(
            _compile_or_report("fromjson(.a); tojson(.b)"), CompiledProgram
        )

    def test_legacy_invocation_pipe_routes_to_expression_with_warning(self, capsys):
        compiled = _compile_or_report("fromjson(.a) | tojson(.b)")

        assert isinstance(compiled, CompiledExpression)
        # The CLI renders the deprecation as its own `warning:` line rather
        # than re-raising it, so exactly one notice reaches the user whatever
        # their warning filters are.
        assert "deprecated" in capsys.readouterr().err

    def test_single_invocation_keeps_legacy_expression_shape(self):
        assert isinstance(_compile_or_report("fromjson(.a)"), CompiledExpression)

    def test_syntax_error_reports_cleanly_and_returns_none(self, capsys):
        result = _compile_or_report("fromjson(")

        assert result is None
        err = capsys.readouterr().err
        assert err.startswith("error:")
        assert "Traceback" not in err
