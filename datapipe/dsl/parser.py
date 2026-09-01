"""Recursive-descent parser for the datapipe DSL expression language.

Produces an ``Expression`` AST from a flat token list. Every AST node
carries source spans so diagnostics can render caret annotations.

Grammar::

    expression   := invocation (PIPE invocation)*
    program      := statement (SEMICOLON statement)* SEMICOLON?
    statement    := assignment | move_into | focused | invocation (PIPE bare_call)*
    assignment   := selector (EQUALS | ARROW_LEFT) rhs (PIPE bare_call)*
    move_into    := selector MOVE_IN move_source (COMMA move_source)*
                        (PIPE bare_call)*
    move_source  := selector | field_set
    field_set    := selector DOT LPAREN COMPLEMENT? IDENT (PIPE IDENT)* RPAREN
    rhs          := selector | invocation
    focused      := selector PIPE bare_call (PIPE bare_call)*
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


def parse_program(expression: str) -> "_ast.Program":
    """Parse a multi-statement program (``;``-separated invocations).

    A single statement with no ``;`` is still valid and returns a
    ``Program`` with one ``Statement``.

    Raises :class:`~datapipe.dsl.errors.ExpressionSyntaxError` on any syntax
    problem, including an empty statement (two consecutive ``;``).
    """
    tokens = tokenize(expression)
    p = _Parser(tokens, expression)
    return p.parse_program()


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

    def parse_program(self) -> "_ast.Program":
        """Parse a ``;``-separated sequence of statements into a ``Program``.

        Each statement can be:
        - An invocation-first statement: ``name(selector, ...)`` optionally
          followed by ``| bare_tool | bare_tool ...``
        - A selector-first focused statement: ``.field | bare_tool | ...``
          where the selector establishes the focus and each bare tool applies to it.
        """
        start = self._peek().span.start
        statements: list["_ast.Statement"] = []

        # Parse the first statement (may be empty input → error at EOF below)
        while True:
            tok = self._peek()

            # Skip leading/trailing/between semicolons but detect empty statements
            if tok.type is TT.SEMICOLON:
                if not statements:
                    raise self._error("empty statement", tok.span)
                self._advance()  # consume ;
                # After consuming ';', check for another ';' (empty statement)
                if self._peek().type is TT.SEMICOLON:
                    raise self._error("empty statement", self._peek().span)
                # Trailing semicolon at EOF is allowed
                if self._peek().type is TT.EOF:
                    break
                # Parse next statement
                stmt = self._parse_statement()
                statements.append(stmt)
                continue

            if tok.type is TT.EOF:
                break

            stmt = self._parse_statement()
            statements.append(stmt)

        if not statements:
            tok = self._peek()
            raise self._error("expected an invocation", tok.span)

        # Must be at end of input
        if self._peek().type is not TT.EOF:
            tok = self._peek()
            raise self._error(
                f"unexpected token {tok.type.name}"
                + (f" {tok.value!r}" if tok.value is not None else ""),
                tok.span,
            )

        end = statements[-1].span.end
        return _ast.Program(
            statements=tuple(statements),
            span=Span(start, end),
        )

    def _parse_statement(self) -> "_ast.Statement":
        """Parse one statement.

        Assignment form:     ``.dest = rhs`` / ``.dest <- rhs`` (+ optional pipes)
        Selector-first form: ``.field | bare_tool | bare_tool ...``
        Invocation-first form: ``name(selector, ...) | bare_tool | ...``
        """
        tok = self._peek()

        # Selector-first: starts with DOT not immediately followed by IDENT+LPAREN
        # (which would be the start of an invocation's qualified name, not a selector).
        # A selector starts with DOT; an invocation starts with IDENT.
        if tok.type is TT.DOT:
            # Peek further to confirm this is a standalone selector and not
            # something else: after the DOT we expect IDENT, LBRACKET, or EOF/PIPE/SEMI
            # — basically anything that _parse_selector handles.
            selector = self._parse_selector()

            nxt = self._peek().type
            if nxt is TT.EQUALS or nxt is TT.ARROW_LEFT:
                return self._parse_assignment(selector, is_move=nxt is TT.ARROW_LEFT)

            if nxt is TT.MOVE_IN:
                return self._parse_move_into(selector)

            # Now we must see PIPE to make it a focused statement.
            if nxt is not TT.PIPE:
                raise self._error(
                    "selector-first statement requires '|', '=', '<-', or '<<' "
                    "after selector",
                    self._peek().span,
                )
            self._advance()  # consume |
            # First tool after the selector is the operation (as BareToolCall)
            op_bare = self._parse_bare_tool_call()
            pipes: list["_ast.BareToolCall"] = []
            while self._peek().type is TT.PIPE:
                self._advance()  # consume |
                pipes.append(self._parse_bare_tool_call())
            span = Span(selector.span.start, (pipes[-1] if pipes else op_bare).span.end)
            return _ast.Statement(
                operation=op_bare,
                pipes=tuple(pipes),
                focus_selector=selector,
                span=span,
            )

        # Invocation-first: starts with IDENT
        inv = self._parse_invocation()
        pipes_inv: list["_ast.BareToolCall"] = []
        while self._peek().type is TT.PIPE:
            self._advance()  # consume |
            pipes_inv.append(self._parse_bare_tool_call())
        span = Span(inv.span.start, (pipes_inv[-1] if pipes_inv else inv).span.end)
        return _ast.Statement(
            operation=inv,
            pipes=tuple(pipes_inv),
            focus_selector=None,
            span=span,
        )

    def _parse_assignment(
        self, destination: "_ast.Selector", *, is_move: bool
    ) -> "_ast.Statement":
        """Parse ``= rhs`` / ``<- rhs`` after *destination*, plus any focused pipes.

        The published focus is the destination, so trailing pipes operate on
        the value that was just written there.
        """
        self._advance()  # consume '=' or '<-'
        rhs = self._parse_assignment_rhs()

        assignment = _ast.Assignment(
            destination=destination,
            rhs=rhs,
            is_move=is_move,
            span=Span(destination.span.start, rhs.span.end),
        )

        pipes: list["_ast.BareToolCall"] = []
        while self._peek().type is TT.PIPE:
            self._advance()  # consume |
            pipes.append(self._parse_bare_tool_call())

        end = (pipes[-1].span.end if pipes else assignment.span.end)
        return _ast.Statement(
            operation=assignment,
            pipes=tuple(pipes),
            focus_selector=destination,
            span=Span(destination.span.start, end),
        )

    def _parse_assignment_rhs(self) -> "_ast.AssignmentRHS":
        """Parse an assignment right-hand side: a selector or a transform of one.

        ``.b``               → source ``.b``, no transform
        ``fromjson(.b)``     → source ``.b``, transform the invocation
        """
        tok = self._peek()

        if tok.type is TT.DOT:
            selector = self._parse_selector()
            return _ast.AssignmentRHS(
                source=selector, transform=None, span=selector.span
            )

        if tok.type is TT.IDENT:
            inv = self._parse_invocation()
            # The primary source is the invocation's own selector argument;
            # sharing the node keeps the two views of it from drifting.
            return _ast.AssignmentRHS(
                source=inv.selector, transform=inv, span=inv.span
            )

        raise self._error(
            f"expected a selector or a tool invocation on the right-hand side "
            f"of an assignment, got {tok.type.name}"
            + (f" {tok.value!r}" if tok.value is not None else ""),
            tok.span,
        )

    def _parse_bare_tool_call(self) -> "_ast.BareToolCall":
        """Parse a bare tool reference: ``name`` or ``name(key=val, ...)`` (no selector).

        A bare call must be an IDENT (optionally namespace-qualified) not followed
        by ``(`` with a selector argument.  If ``(`` is present, only keyword
        arguments (no positional selector) are allowed.
        """
        name = self._parse_qualified_name_for_bare()
        arguments: list["_ast.Argument"] = []

        if self._peek().type is TT.LPAREN:
            self._advance()  # consume (
            # Keyword arguments only — no selector.  Comma-driven like
            # _parse_invocation so a missing comma is a syntax error.
            if self._peek().type is not TT.RPAREN:
                arguments.append(self._parse_argument())
                while self._peek().type is TT.COMMA:
                    self._advance()  # consume ,
                    arguments.append(self._parse_argument())
            close = self._expect(TT.RPAREN, "')'")
            span = Span(name.span.start, close.span.end)
        else:
            span = name.span

        return _ast.BareToolCall(
            qualified_name=name,
            arguments=tuple(arguments),
            span=span,
        )

    def _parse_qualified_name_for_bare(self) -> "_ast.QualifiedName":
        """Like _parse_qualified_name but without requiring LPAREN lookahead for namespace."""
        ident_tok = self._expect(TT.IDENT, "tool name")
        first = str(ident_tok.value)
        span_start = ident_tok.span.start

        # Check for optional namespace: identifier "." identifier
        # For bare calls we check DOT + IDENT ahead (regardless of what follows IDENT).
        if (
            self._peek().type is TT.DOT
            and self._pos + 1 < len(self._tokens)
            and self._tokens[self._pos + 1].type is TT.IDENT
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

    def _parse_move_into(self, destination: "_ast.Selector") -> "_ast.Statement":
        """Parse ``<< source_list`` after *destination*, plus any focused pipes.

        Per §9.1 the comma binds tighter than the pipe, so the source list is
        consumed first and any trailing ``|`` attaches to the whole statement.
        The published focus is the destination, so a trailing pipe operates on
        the assembled object rather than on the last source.
        """
        self._advance()  # consume '<<'

        sources: list["_ast.Selector | _ast.FieldSet"] = [self._parse_move_source()]
        while self._peek().type is TT.COMMA:
            self._advance()  # consume ,
            sources.append(self._parse_move_source())

        move = _ast.MoveInto(
            destination=destination,
            sources=tuple(sources),
            span=Span(destination.span.start, sources[-1].span.end),
        )

        pipes: list["_ast.BareToolCall"] = []
        while self._peek().type is TT.PIPE:
            self._advance()  # consume |
            pipes.append(self._parse_bare_tool_call())

        end = pipes[-1].span.end if pipes else move.span.end
        return _ast.Statement(
            operation=move,
            pipes=tuple(pipes),
            focus_selector=destination,
            span=Span(destination.span.start, end),
        )

    def _parse_move_source(self) -> "_ast.Selector | _ast.FieldSet":
        """Parse one ``<<`` source: a plain selector or a field set."""
        return self._parse_selector(allow_field_set=True)

    def _parse_field_set(
        self, base: "_ast.Selector", dot_start: int
    ) -> "_ast.FieldSet":
        """Parse ``(^? name (| name)*)`` after the dot that introduced it.

        The opening ``(`` is the current token.  Inside these parentheses ``|``
        joins exact field names (§9.2); the closing ``)`` ends that context, so
        a ``|`` after it belongs to the statement's pipe chain.
        """
        self._advance()  # consume (

        complement = False
        if self._peek().type is TT.COMPLEMENT:
            self._advance()  # consume ^
            complement = True

        names: list[str] = []
        name_tok = self._expect(TT.IDENT, "field name inside '.('")
        names.append(str(name_tok.value))
        while self._peek().type is TT.PIPE:
            self._advance()  # consume | — a field-name union, not a pipe
            name_tok = self._expect(TT.IDENT, "field name after '|' inside '.('")
            names.append(str(name_tok.value))

        close = self._expect(TT.RPAREN, "')' to close a field set")
        return _ast.FieldSet(
            base=base,
            names=tuple(names),
            complement=complement,
            span=Span(dot_start, close.span.end),
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

    def _parse_selector(
        self, *, allow_field_set: bool = False
    ) -> "_ast.Selector | _ast.FieldSet":
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
        elif allow_field_set and tok.type is TT.LPAREN:
            # .(a|b|c) — a field set rooted at the current object
            base = _ast.Selector(parts=(), span=Span(start, dot_tok.span.end))
            return self._parse_field_set(base, start)
        # else: root selector "." — parts stays empty

        # Consume additional selector parts.
        while True:
            tok = self._peek()

            if tok.type is TT.DOT:
                if (
                    allow_field_set
                    and self._pos + 1 < len(self._tokens)
                    and self._tokens[self._pos + 1].type is TT.LPAREN
                ):
                    # .base.(a|b|c) — the dot introduces a field set, not a field
                    self._advance()  # consume the dot
                    end = parts[-1].span.end if parts else dot_tok.span.end
                    base = _ast.Selector(
                        parts=tuple(parts), span=Span(start, end)
                    )
                    return self._parse_field_set(base, start)

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
