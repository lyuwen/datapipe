"""Phase 2 DSL tests: lexer, AST, parser, selector, compiler."""

from __future__ import annotations

import pytest

from datapipe.dsl import (
    CompiledExpression,
    CompiledSelector,
    Each,
    Expression,
    ExpressionSyntaxError,
    Field,
    Index,
    Invocation,
    Literal,
    QualifiedName,
    QuotedKey,
    Reference,
    Selector,
    SelectorResolutionError,
    Span,
    ToolConfigurationError,
    ToolResolutionError,
    compile_expression,
    parse,
)
from datapipe.dsl.lexer import TT, Token, tokenize


# ---------------------------------------------------------------------------
# Lexer tests
# ---------------------------------------------------------------------------


class TestLexer:
    def test_empty_gives_only_eof(self):
        toks = tokenize("")
        assert len(toks) == 1
        assert toks[0].type is TT.EOF

    def test_single_dot(self):
        toks = tokenize(".")
        assert toks[0].type is TT.DOT

    def test_pipe(self):
        toks = tokenize("|")
        assert toks[0].type is TT.PIPE

    def test_brackets(self):
        toks = tokenize("[]")
        assert toks[0].type is TT.LBRACKET
        assert toks[1].type is TT.RBRACKET

    def test_integer(self):
        toks = tokenize("42")
        assert toks[0].type is TT.INTEGER
        assert toks[0].value == 42

    def test_negative_integer(self):
        toks = tokenize("-3")
        assert toks[0].type is TT.INTEGER
        assert toks[0].value == -3

    def test_float(self):
        toks = tokenize("3.14")
        assert toks[0].type is TT.FLOAT
        assert abs(toks[0].value - 3.14) < 1e-10

    def test_double_quoted_string(self):
        toks = tokenize('"hello world"')
        assert toks[0].type is TT.STRING
        assert toks[0].value == "hello world"

    def test_single_quoted_string(self):
        toks = tokenize("'hello'")
        assert toks[0].type is TT.STRING
        assert toks[0].value == "hello"

    def test_string_escape(self):
        toks = tokenize(r'"line\nnewline"')
        assert toks[0].value == "line\nnewline"

    def test_unterminated_string_raises(self):
        with pytest.raises(ExpressionSyntaxError, match="unterminated"):
            tokenize('"not closed')

    def test_identifier(self):
        toks = tokenize("fromjson")
        assert toks[0].type is TT.IDENT
        assert toks[0].value == "fromjson"

    def test_keywords_as_idents(self):
        for kw in ("true", "false", "null", "True", "False", "None"):
            toks = tokenize(kw)
            assert toks[0].type is TT.IDENT
            assert toks[0].value == kw

    def test_span_positions(self):
        toks = tokenize("ab cd")
        assert toks[0].span == Span(0, 2)
        assert toks[1].span == Span(3, 5)

    def test_unexpected_character_raises(self):
        with pytest.raises(ExpressionSyntaxError, match="unexpected character"):
            tokenize("@bad")

    def test_full_invocation_tokens(self):
        toks = tokenize("fromjson(.tools)")
        types = [t.type for t in toks]
        assert TT.IDENT in types
        assert TT.LPAREN in types
        assert TT.DOT in types
        assert TT.RPAREN in types

    def test_whitespace_ignored(self):
        a = tokenize("fromjson( .tools )")
        b = tokenize("fromjson(.tools)")
        assert [t.type for t in a] == [t.type for t in b]


# ---------------------------------------------------------------------------
# Parser tests
# ---------------------------------------------------------------------------


class TestParser:
    def test_simple_invocation(self):
        expr = parse("fromjson(.tools)")
        assert isinstance(expr, Expression)
        assert len(expr.invocations) == 1
        inv = expr.invocations[0]
        assert inv.qualified_name.name == "fromjson"
        assert inv.qualified_name.namespace is None
        assert len(inv.selector.parts) == 1
        assert isinstance(inv.selector.parts[0], Field)
        assert inv.selector.parts[0].name == "tools"

    def test_root_selector(self):
        expr = parse("fromjson(.)")
        inv = expr.invocations[0]
        assert inv.selector.is_root

    def test_nested_selector(self):
        expr = parse("fromjson(.metadata.annotation)")
        parts = expr.invocations[0].selector.parts
        assert len(parts) == 2
        assert isinstance(parts[0], Field) and parts[0].name == "metadata"
        assert isinstance(parts[1], Field) and parts[1].name == "annotation"

    def test_array_index_selector(self):
        expr = parse("fromjson(.items[0])")
        parts = expr.invocations[0].selector.parts
        assert isinstance(parts[1], Index)
        assert parts[1].index == 0

    def test_wildcard_selector(self):
        expr = parse("fromjson(.tools[])")
        parts = expr.invocations[0].selector.parts
        assert isinstance(parts[1], Each)

    def test_quoted_key_selector(self):
        expr = parse('fromjson(.["key.with.dots"])')
        parts = expr.invocations[0].selector.parts
        assert isinstance(parts[0], QuotedKey)
        assert parts[0].key == "key.with.dots"

    def test_argument_bool_true(self):
        expr = parse("fromjson(., recursive=true)")
        arg = expr.invocations[0].arguments[0]
        assert arg.name == "recursive"
        assert arg.value.value is True

    def test_argument_bool_false(self):
        expr = parse("fromjson(., recursive=false)")
        assert expr.invocations[0].arguments[0].value.value is False

    def test_argument_python_aliases(self):
        expr = parse("fromjson(., recursive=True)")
        assert expr.invocations[0].arguments[0].value.value is True
        expr2 = parse("fromjson(., recursive=None)")
        assert expr2.invocations[0].arguments[0].value.value is None

    def test_argument_string(self):
        expr = parse('tojson(., sep=",")')
        assert expr.invocations[0].arguments[0].value.value == ","

    def test_argument_integer(self):
        expr = parse("tojson(., indent=2)")
        assert expr.invocations[0].arguments[0].value.value == 2

    def test_argument_float(self):
        expr = parse("tojson(., x=1.5)")
        assert abs(expr.invocations[0].arguments[0].value.value - 1.5) < 1e-10

    def test_argument_array_literal(self):
        expr = parse("tojson(., keys=[1, 2, 3])")
        assert expr.invocations[0].arguments[0].value.value == [1, 2, 3]

    def test_pipe_two_invocations(self):
        expr = parse("fromjson(.tools) | tojson(.tools[].name)")
        assert len(expr.invocations) == 2
        assert expr.invocations[0].qualified_name.name == "fromjson"
        assert expr.invocations[1].qualified_name.name == "tojson"

    def test_pipe_three_invocations(self):
        expr = parse("fromjson(.a) | fromjson(.b, recursive=true) | tojson(.c)")
        assert len(expr.invocations) == 3

    def test_qualified_name(self):
        expr = parse("my_tools.normalize(.text)")
        inv = expr.invocations[0]
        assert inv.qualified_name.namespace == "my_tools"
        assert inv.qualified_name.name == "normalize"

    def test_missing_lparen_raises(self):
        with pytest.raises(ExpressionSyntaxError):
            parse("fromjson.tools")

    def test_missing_dot_in_selector_raises(self):
        with pytest.raises(ExpressionSyntaxError):
            parse("fromjson(tools)")

    def test_missing_rparen_raises(self):
        with pytest.raises(ExpressionSyntaxError):
            parse("fromjson(.tools")

    def test_unknown_literal_identifier_raises(self):
        with pytest.raises(ExpressionSyntaxError, match="unknown identifier"):
            parse("fromjson(., x=foo)")

    def test_extra_tokens_raises(self):
        with pytest.raises(ExpressionSyntaxError, match="unexpected token"):
            parse("fromjson(.tools) extra")

    def test_span_recorded(self):
        expr = parse("fromjson(.tools)")
        assert expr.span.start == 0
        assert expr.span.end > 0
        inv = expr.invocations[0]
        assert inv.span.start == 0

    def test_wildcard_then_fields(self):
        expr = parse("tojson(.tools[].function.parameters)")
        parts = expr.invocations[0].selector.parts
        assert isinstance(parts[0], Field) and parts[0].name == "tools"
        assert isinstance(parts[1], Each)
        assert isinstance(parts[2], Field) and parts[2].name == "function"
        assert isinstance(parts[3], Field) and parts[3].name == "parameters"

    def test_multiple_arguments(self):
        expr = parse("fromjson(., recursive=true, containers_only=false)")
        args = expr.invocations[0].arguments
        assert len(args) == 2
        assert args[0].name == "recursive"
        assert args[1].name == "containers_only"


# ---------------------------------------------------------------------------
# CompiledSelector / selector runtime tests
# ---------------------------------------------------------------------------


class TestCompiledSelector:
    def _selector(self, expression: str) -> CompiledSelector:
        inv = parse(expression).invocations[0]
        return CompiledSelector(inv.selector)

    def test_root_selector(self):
        sel = self._selector("f(.)")
        refs = sel.resolve({"a": 1})
        assert len(refs) == 1
        assert refs[0].value == {"a": 1}
        assert refs[0].parent is None

    def test_field_selector(self):
        sel = self._selector("f(.name)")
        refs = sel.resolve({"name": "Alice"})
        assert len(refs) == 1
        assert refs[0].value == "Alice"
        assert refs[0].path == ".name"

    def test_nested_field_selector(self):
        sel = self._selector("f(.meta.tag)")
        refs = sel.resolve({"meta": {"tag": "x"}})
        assert refs[0].value == "x"
        assert refs[0].path == ".meta.tag"

    def test_index_selector(self):
        sel = self._selector("f(.items[1])")
        refs = sel.resolve({"items": ["a", "b", "c"]})
        assert refs[0].value == "b"

    def test_wildcard_selector(self):
        sel = self._selector("f(.items[])")
        refs = sel.resolve({"items": [1, 2, 3]})
        assert len(refs) == 3
        assert [r.value for r in refs] == [1, 2, 3]

    def test_wildcard_empty_list(self):
        sel = self._selector("f(.items[])")
        refs = sel.resolve({"items": []})
        assert refs == []

    def test_wildcard_on_non_list_raises(self):
        sel = self._selector("f(.items[])")
        with pytest.raises(SelectorResolutionError, match="list"):
            sel.resolve({"items": "not a list"})

    def test_missing_field_raises(self):
        sel = self._selector("f(.name)")
        with pytest.raises(SelectorResolutionError, match="missing field"):
            sel.resolve({"other": 1})

    def test_out_of_range_index_raises(self):
        sel = self._selector("f(.items[5])")
        with pytest.raises(SelectorResolutionError, match="out of range"):
            sel.resolve({"items": [1, 2]})

    def test_quoted_key_selector(self):
        sel = self._selector('f(.["key.with.dots"])')
        refs = sel.resolve({"key.with.dots": 42})
        assert refs[0].value == 42

    def test_replace_field(self):
        sel = self._selector("f(.name)")
        record = {"name": "old"}
        refs = sel.resolve(record)
        result = sel.apply(record, refs, ["new"])
        assert result["name"] == "new"

    def test_replace_wildcard(self):
        sel = self._selector("f(.items[])")
        record = {"items": [1, 2, 3]}
        refs = sel.resolve(record)
        result = sel.apply(record, refs, [10, 20, 30])
        assert result["items"] == [10, 20, 30]

    def test_replace_root(self):
        sel = self._selector("f(.)")
        record = {"a": 1}
        refs = sel.resolve(record)
        result = sel.apply(record, refs, [{"b": 2}])
        assert result == {"b": 2}

    def test_path_string(self):
        sel = self._selector("f(.a[0].b)")
        refs = sel.resolve({"a": [{"b": "x"}]})
        assert refs[0].path == ".a[0].b"


# ---------------------------------------------------------------------------
# Compiler tests
# ---------------------------------------------------------------------------


class TestCompiler:
    def test_compile_fromjson_root(self):
        ce = compile_expression("fromjson(.)")
        assert isinstance(ce, CompiledExpression)
        assert len(ce.invocations) == 1
        inv = ce.invocations[0]
        assert inv.tool_name == "fromjson"
        assert inv.expression_index == 0

    def test_compile_tojson(self):
        ce = compile_expression("tojson(.tools)")
        assert ce.invocations[0].tool_name == "tojson"

    def test_default_arguments_filled(self):
        ce = compile_expression("fromjson(.)")
        args = ce.invocations[0].arguments
        assert "recursive" in args
        assert args["recursive"] is False
        assert "containers_only" in args
        assert args["containers_only"] is True

    def test_explicit_argument_overrides_default(self):
        ce = compile_expression("fromjson(., recursive=true)")
        assert ce.invocations[0].arguments["recursive"] is True

    def test_two_invocations(self):
        ce = compile_expression("fromjson(.tools) | tojson(.tools[].name)")
        assert len(ce.invocations) == 2
        assert ce.invocations[0].tool_name == "fromjson"
        assert ce.invocations[1].tool_name == "tojson"

    def test_unknown_tool_raises(self):
        with pytest.raises(ToolResolutionError, match="unknown tool"):
            compile_expression("no_such_tool(.x)")

    def test_unknown_argument_raises(self):
        with pytest.raises(ToolConfigurationError, match="unknown argument"):
            compile_expression("fromjson(., recusive=true)")

    def test_unknown_argument_diagnostic(self):
        try:
            compile_expression("fromjson(., recusive=true)")
        except ToolConfigurationError as exc:
            # The error message should contain a caret diagnostic
            assert "^" in str(exc)

    def test_duplicate_argument_raises(self):
        with pytest.raises(ToolConfigurationError, match="duplicate"):
            compile_expression("fromjson(., recursive=true, recursive=false)")

    def test_namespaced_tool_raises_in_phase2(self):
        with pytest.raises(ToolResolutionError, match="Phase 2"):
            compile_expression("ns.mytool(.x)")

    def test_record_tool_requires_root_selector(self):
        # Build a fake record-scoped tool for this test
        from datapipe.tools import tool, JsonType
        from datapipe.dsl.compiler import _BUILTIN_REGISTRY, _build_builtin_registry
        from datapipe.dsl.compiler import compile_expression as ce_fn

        @tool(name="rec_tool", target="record",
              input=JsonType.OBJECT, output=JsonType.OBJECT)
        def rec_tool(record): return record

        import datapipe.dsl.compiler as _comp
        old = _comp._BUILTIN_REGISTRY
        _comp._BUILTIN_REGISTRY = {"rec_tool": rec_tool}
        try:
            with pytest.raises(ToolConfigurationError, match="root selector"):
                ce_fn("rec_tool(.field)")
        finally:
            _comp._BUILTIN_REGISTRY = old

    def test_source_preserved(self):
        src = "fromjson(.tools) | tojson(.tools[])"
        ce = compile_expression(src)
        assert ce.source == src

    def test_compile_motivating_expression(self):
        """The motivating use case from the CLI plan must compile cleanly."""
        expr = (
            "fromjson(.tools) | "
            "fromjson(.metadata.annotation, recursive=true) | "
            "tojson(.tools[].function.parameters)"
        )
        ce = compile_expression(expr)
        assert len(ce.invocations) == 3
        assert ce.invocations[0].tool_name == "fromjson"
        assert ce.invocations[1].arguments["recursive"] is True
        assert ce.invocations[2].selector._parts[-1].name == "parameters"  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# End-to-end execution through compiled invocations
# ---------------------------------------------------------------------------


class TestEndToEnd:
    """Execute compiled invocations on example records to verify correctness."""

    def _run(self, expression: str, record):
        """Compile *expression* and apply it to *record*.

        Drives the invocations directly without going through
        CompiledToolProgramStage.  Uses builtin_fn because phase 2 tests
        only exercise built-in tools, which always have a live callable.
        """
        ce = compile_expression(expression)
        for inv in ce.invocations:
            refs = inv.selector.resolve(record)
            if not refs:
                continue
            fn = inv.builtin_fn
            assert fn is not None, (
                f"test helper does not support provider tools; "
                f"inv {inv.tool_name!r} has no builtin_fn"
            )
            new_values = [fn(r.value, **inv.arguments) for r in refs]
            record = inv.selector.apply(record, refs, new_values)
        return record

    def test_fromjson_field(self):
        record = {"data": '{"a": 1}'}
        result = self._run("fromjson(.data)", record)
        assert result["data"] == {"a": 1}

    def test_tojson_field(self):
        record = {"data": {"a": 1}}
        result = self._run("tojson(.data)", record)
        assert result["data"] == '{"a":1}'

    def test_fromjson_then_tojson(self):
        record = {"x": '{"k": "v"}'}
        result = self._run("fromjson(.x) | tojson(.x)", record)
        assert result["x"] == '{"k":"v"}'

    def test_wildcard_tojson(self):
        record = {"items": [{"v": 1}, {"v": 2}]}
        result = self._run("tojson(.items[])", record)
        assert result["items"] == ['{"v":1}', '{"v":2}']

    def test_motivating_expression(self):
        """Simplified version of the CLI plan's motivating example."""
        record = {
            "tools": '[{"name": "foo"}]',
            "metadata": {"annotation": '{"label": "bar"}'},
        }
        result = self._run(
            "fromjson(.tools) | fromjson(.metadata.annotation, recursive=true)",
            record,
        )
        assert result["tools"] == [{"name": "foo"}]
        assert result["metadata"]["annotation"] == {"label": "bar"}
