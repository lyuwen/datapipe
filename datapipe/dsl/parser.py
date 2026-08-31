"""Recursive-descent parser for the datapipe DSL expression language.

Produces an ``Expression`` AST from a flat token list. Every AST node
carries source spans so diagnostics can render caret annotations.

Grammar::

    expression   := invocation (PIPE invocation)*
    invocation   := qualified_name LPAREN selector (COMMA argument)* RPAREN
    argument     := IDENT EQUALS literal
    selector     := DOT selector_part*
    selector_part :=
        DOT IDENT
      | LBRACKET STRING RBRACKET
      | LBRACKET INTEGER RBRACKET
      | LBRACKET RBRACKET
    literal      := STRING | INTEGER | FLOAT | boolean | null | array | object
    boolean      := IDENT("true"|"false"|"True"|"False")
    null         := IDENT("null"|"None")
    array        := LBRACKET (literal (COMMA literal)*)? RBRACKET
    object       := LBRACE (STRING COLON literal (COMMA STRING COLON literal)*)? RBRACE
    qualified_name := IDENT (DOT IDENT)?
"""

from __future__ import annotations

from typing import Any

from datapipe.dsl import ast as _ast
from datapipe.dsl.errors import ExpressionSyntaxError, Span
from datapipe.dsl.lexer import TT, Token, tokenize


def parse(expression: str) -> "_ast.Expression":
    """Parse *expression* and return an :class:`~datapipe.dsl.ast.Expression`.

    Raises :class:`~datapipe.dsl.errors.ExpressionSyntaxError` on any syntax
    problem, including source-position diagnostics.
    """
    tokens = tokenize(expression)
    p = _Parser(tokens, expression)
    return p.parse_expression()


class _Parser:
    """Stateful recursive-descent parser over a flat token list."""

    def __init__(self, tokens: list[Token], expression: str) -> None:
        self._tokens = tokens
        self._expr = expression
        self._pos = 0

    # ------------------------------------------------------------------ #
    # Token navigation helpers                                             #
    # ------------------------------------------------------------------ #

    def _peek(self) -> Token:
        return self._tokens[self._pos]

    def _advance(self) -> Token:
        tok = self._tokens[self._pos]
        if tok.type is not TT.EOF:
            self._pos += 1
        return tok

    def _expect(self, tt: TT, description: str | None = None) -> Token:
        tok = self._peek()
        if tok.type is not tt:
            expected = description or tt.name
            raise ExpressionSyntaxError(
                f"expected {expected}, got {tok.type.name}"
                + (f" {tok.value!r}" if tok.value is not None else ""),
                expression=self._expr,
                span=tok.span,
            )
        return self._advance()

    def _error(self, message: str, span: Span | None = None) -> ExpressionSyntaxError:
        return ExpressionSyntaxError(
            message,
            expression=self._expr,
            span=span or self._peek().span,
        )

    # ------------------------------------------------------------------ #
    # Grammar rules                                                        #
    # ------------------------------------------------------------------ #

    def parse_expression(self) -> "_ast.Expression":
        start = self._peek().span.start
        invocations: list["_ast.Invocation"] = []
        invocations.append(self._parse_invocation())

        while self._peek().type is TT.PIPE:
            self._advance()  # consume |
            invocations.append(self._parse_invocation())

        # Must be at end of input
        if self._peek().type is not TT.EOF:
            tok = self._peek()
            raise self._error(
                f"unexpected token {tok.type.name}"
                + (f" {tok.value!r}" if tok.value is not None else ""),
                tok.span,
            )

        end = invocations[-1].span.end
        return _ast.Expression(
            invocations=tuple(invocations),
            span=Span(start, end),
        )

    def _parse_invocation(self) -> "_ast.Invocation":
        name = self._parse_qualified_name()
        self._expect(TT.LPAREN, "'('")
        selector = self._parse_selector()

        arguments: list["_ast.Argument"] = []
        while self._peek().type is TT.COMMA:
            self._advance()  # consume ,
            arg = self._parse_argument()
            arguments.append(arg)

        close = self._expect(TT.RPAREN, "')'")
        span = Span(name.span.start, close.span.end)
        return _ast.Invocation(
            qualified_name=name,
            selector=selector,
            arguments=tuple(arguments),
            span=span,
        )

    def _parse_qualified_name(self) -> "_ast.QualifiedName":
        ident_tok = self._expect(TT.IDENT, "tool name")
        first = str(ident_tok.value)
        span_start = ident_tok.span.start

        # Check for optional namespace: identifier "." identifier where the
        # second identifier is *not* followed by "(" — that would be a
        # selector, not a qualified name. We look ahead two tokens.
        if (
            self._peek().type is TT.DOT
            and self._pos + 1 < len(self._tokens)
            and self._tokens[self._pos + 1].type is TT.IDENT
            and self._pos + 2 < len(self._tokens)
            and self._tokens[self._pos + 2].type is TT.LPAREN
        ):
            self._advance()  # consume .
            second_tok = self._advance()  # consume identifier
            return _ast.QualifiedName(
                namespace=first,
                name=str(second_tok.value),
                span=Span(span_start, second_tok.span.end),
            )

        return _ast.QualifiedName(
            namespace=None,
            name=first,
            span=ident_tok.span,
        )

    def _parse_selector(self) -> "_ast.Selector":
        dot_tok = self._expect(TT.DOT, "'.'")
        start = dot_tok.span.start
        parts: list["_ast.SelectorPart"] = []

        # The first selector part may immediately follow the leading dot as an
        # identifier (e.g. ".tools") or as a bracket (e.g. ".[0]" or ".[]").
        # Subsequent parts are always introduced by another dot or bracket.
        # We handle the first part and then loop for the rest.
        tok = self._peek()
        if tok.type is TT.IDENT:
            # .field — the identifier directly after the leading dot
            self._advance()
            parts.append(_ast.Field(
                name=str(tok.value),
                span=Span(dot_tok.span.start, tok.span.end),
            ))
        elif tok.type is TT.LBRACKET:
            # .[...] — bracket immediately after leading dot
            part, i = self._parse_bracket_part()
            parts.append(part)
        # else: root selector "." — parts stays empty

        # Consume additional selector parts.
        while True:
            tok = self._peek()

            if tok.type is TT.DOT:
                self._advance()  # consume the dot
                field_tok = self._expect(TT.IDENT, "field name after '.'")
                parts.append(_ast.Field(
                    name=str(field_tok.value),
                    span=Span(tok.span.start, field_tok.span.end),
                ))

            elif tok.type is TT.LBRACKET:
                part, _ = self._parse_bracket_part()
                parts.append(part)

            else:
                break  # no more selector parts

        end = parts[-1].span.end if parts else dot_tok.span.end
        return _ast.Selector(parts=tuple(parts), span=Span(start, end))

    def _parse_bracket_part(self) -> "tuple[_ast.SelectorPart, int]":
        """Consume a bracket selector part ``[...]`` and return (part, pos)."""
        open_tok = self._advance()  # consume [
        inner = self._peek()

        if inner.type is TT.RBRACKET:
            close = self._advance()
            return _ast.Each(span=Span(open_tok.span.start, close.span.end)), self._pos

        if inner.type is TT.STRING:
            val_tok = self._advance()
            close = self._expect(TT.RBRACKET, "']'")
            return _ast.QuotedKey(
                key=str(val_tok.value),
                span=Span(open_tok.span.start, close.span.end),
            ), self._pos

        if inner.type is TT.INTEGER:
            val_tok = self._advance()
            close = self._expect(TT.RBRACKET, "']'")
            return _ast.Index(
                index=int(val_tok.value),  # type: ignore[arg-type]
                span=Span(open_tok.span.start, close.span.end),
            ), self._pos

        raise self._error(
            f"expected string, integer, or ']' inside '[', got {inner.type.name}",
            inner.span,
        )

    def _parse_argument(self) -> "_ast.Argument":
        name_tok = self._expect(TT.IDENT, "argument name")
        self._expect(TT.EQUALS, "'='")
        value = self._parse_literal()
        span = Span(name_tok.span.start, value.span.end)
        return _ast.Argument(
            name=str(name_tok.value),
            value=value,
            span=span,
        )

    def _parse_literal(self) -> "_ast.Literal":
        tok = self._peek()

        if tok.type is TT.STRING:
            self._advance()
            return _ast.Literal(value=str(tok.value), span=tok.span)

        if tok.type is TT.INTEGER:
            self._advance()
            return _ast.Literal(value=int(tok.value), span=tok.span)  # type: ignore[arg-type]

        if tok.type is TT.FLOAT:
            self._advance()
            return _ast.Literal(value=float(tok.value), span=tok.span)  # type: ignore[arg-type]

        if tok.type is TT.LBRACKET:
            return self._parse_array_literal()

        if tok.type is TT.LBRACE:
            return self._parse_object_literal()

        if tok.type is TT.IDENT:
            raw = str(tok.value)
            if raw in ("true", "True"):
                self._advance()
                return _ast.Literal(value=True, span=tok.span)
            if raw in ("false", "False"):
                self._advance()
                return _ast.Literal(value=False, span=tok.span)
            if raw in ("null", "None"):
                self._advance()
                return _ast.Literal(value=None, span=tok.span)
            raise self._error(
                f"unknown identifier {raw!r}; expected a literal value "
                "(string, number, true, false, null)",
                tok.span,
            )

        raise self._error(
            f"expected a literal value, got {tok.type.name}"
            + (f" {tok.value!r}" if tok.value is not None else ""),
            tok.span,
        )

    def _parse_array_literal(self) -> "_ast.Literal":
        open_tok = self._advance()  # consume [
        items: list[Any] = []

        if self._peek().type is not TT.RBRACKET:
            items.append(self._parse_literal().value)
            while self._peek().type is TT.COMMA:
                self._advance()
                items.append(self._parse_literal().value)

        close = self._expect(TT.RBRACKET, "']'")
        return _ast.Literal(
            value=items,
            span=Span(open_tok.span.start, close.span.end),
        )

    def _parse_object_literal(self) -> "_ast.Literal":
        """Parse an object literal ``{"key": value, ...}`` as a plain dict."""
        open_tok = self._advance()  # consume {
        items: dict[str, Any] = {}

        if self._peek().type is not TT.RBRACE:
            key_tok = self._expect(TT.STRING, "string key")
            self._expect(TT.COLON, "':'")
            val = self._parse_literal()
            items[str(key_tok.value)] = val.value
            while self._peek().type is TT.COMMA:
                self._advance()
                key_tok = self._expect(TT.STRING, "string key")
                self._expect(TT.COLON, "':'")
                val = self._parse_literal()
                items[str(key_tok.value)] = val.value

        close = self._expect(TT.RBRACE, "'}'")
        return _ast.Literal(
            value=items,
            span=Span(open_tok.span.start, close.span.end),
        )
