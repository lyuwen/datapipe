"""Tokenizer for the datapipe DSL expression language.

Produces a flat list of typed tokens from an expression string, with every
token carrying its start and end positions for diagnostic rendering.

Token types
-----------
IDENT       identifier or keyword: fromjson, true, false, null, True, False, None
DOT         .
PIPE        |
LPAREN      (
RPAREN      )
LBRACKET    [
RBRACKET    ]
EQUALS      =
COMMA       ,
STRING      quoted string literal (single or double quotes)
INTEGER     integer literal (no leading zeros except 0 itself)
FLOAT       floating-point literal
EOF         synthetic end-of-input marker
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto

from datapipe.dsl.errors import ExpressionSyntaxError, Span


class TT(Enum):
    """Token type enum."""
    IDENT = auto()
    DOT = auto()
    PIPE = auto()
    LPAREN = auto()
    RPAREN = auto()
    LBRACKET = auto()
    RBRACKET = auto()
    EQUALS = auto()
    COMMA = auto()
    STRING = auto()
    INTEGER = auto()
    FLOAT = auto()
    EOF = auto()


@dataclass(frozen=True)
class Token:
    """A single lexed token."""
    type: TT
    value: object          # str for IDENT/STRING, int for INTEGER, float for FLOAT, None for punctuation
    span: Span

    def __repr__(self) -> str:
        if self.value is not None:
            return f"Token({self.type.name}, {self.value!r}, {self.span})"
        return f"Token({self.type.name}, {self.span})"


def tokenize(expr: str) -> list[Token]:
    """Return all tokens for *expr*, ending with an EOF token.

    Raises ``ExpressionSyntaxError`` on any unrecognised character or
    unterminated string literal.
    """
    tokens: list[Token] = []
    i = 0
    n = len(expr)

    while i < n:
        ch = expr[i]

        # Whitespace
        if ch in " \t\n\r":
            i += 1
            continue

        # Single-character punctuation
        if ch == ".":
            tokens.append(Token(TT.DOT, None, Span(i, i + 1)))
            i += 1
            continue
        if ch == "|":
            tokens.append(Token(TT.PIPE, None, Span(i, i + 1)))
            i += 1
            continue
        if ch == "(":
            tokens.append(Token(TT.LPAREN, None, Span(i, i + 1)))
            i += 1
            continue
        if ch == ")":
            tokens.append(Token(TT.RPAREN, None, Span(i, i + 1)))
            i += 1
            continue
        if ch == "[":
            tokens.append(Token(TT.LBRACKET, None, Span(i, i + 1)))
            i += 1
            continue
        if ch == "]":
            tokens.append(Token(TT.RBRACKET, None, Span(i, i + 1)))
            i += 1
            continue
        if ch == "=":
            tokens.append(Token(TT.EQUALS, None, Span(i, i + 1)))
            i += 1
            continue
        if ch == ",":
            tokens.append(Token(TT.COMMA, None, Span(i, i + 1)))
            i += 1
            continue

        # String literals
        if ch in ('"', "'"):
            tok, i = _read_string(expr, i, n)
            tokens.append(tok)
            continue

        # Numbers
        if ch.isdigit() or (ch == "-" and i + 1 < n and expr[i + 1].isdigit()):
            tok, i = _read_number(expr, i, n)
            tokens.append(tok)
            continue

        # Identifiers and keywords
        if ch.isalpha() or ch == "_":
            tok, i = _read_ident(expr, i, n)
            tokens.append(tok)
            continue

        raise ExpressionSyntaxError(
            f"unexpected character {ch!r}",
            expression=expr,
            span=Span(i, i + 1),
        )

    tokens.append(Token(TT.EOF, None, Span(n, n)))
    return tokens


# ---------------------------------------------------------------------------
# Lexing helpers
# ---------------------------------------------------------------------------


def _read_string(expr: str, start: int, n: int) -> tuple[Token, int]:
    """Consume a quoted string literal and return (Token, new_position)."""
    quote = expr[start]
    i = start + 1
    buf: list[str] = []

    while i < n:
        ch = expr[i]
        if ch == "\\":
            if i + 1 >= n:
                raise ExpressionSyntaxError(
                    "unterminated escape sequence in string",
                    expression=expr,
                    span=Span(i, i + 1),
                )
            esc = expr[i + 1]
            buf.append(_ESCAPES.get(esc, esc))
            i += 2
            continue
        if ch == quote:
            return Token(TT.STRING, "".join(buf), Span(start, i + 1)), i + 1
        buf.append(ch)
        i += 1

    raise ExpressionSyntaxError(
        "unterminated string literal",
        expression=expr,
        span=Span(start, n),
    )


_ESCAPES: dict[str, str] = {
    "n": "\n", "t": "\t", "r": "\r",
    "\\": "\\", '"': '"', "'": "'",
}


def _read_number(expr: str, start: int, n: int) -> tuple[Token, int]:
    """Consume a number literal and return (Token, new_position)."""
    i = start
    if expr[i] == "-":
        i += 1
    while i < n and expr[i].isdigit():
        i += 1
    is_float = False
    if i < n and expr[i] == ".":
        # Make sure the next char is a digit to avoid consuming '.' as decimal
        if i + 1 < n and expr[i + 1].isdigit():
            is_float = True
            i += 1
            while i < n and expr[i].isdigit():
                i += 1
    if i < n and expr[i] in ("e", "E"):
        is_float = True
        i += 1
        if i < n and expr[i] in ("+", "-"):
            i += 1
        while i < n and expr[i].isdigit():
            i += 1
    raw = expr[start:i]
    try:
        value: int | float = float(raw) if is_float else int(raw)
    except ValueError:
        raise ExpressionSyntaxError(
            f"invalid number literal {raw!r}",
            expression=expr,
            span=Span(start, i),
        )
    tt = TT.FLOAT if is_float else TT.INTEGER
    return Token(tt, value, Span(start, i)), i


def _read_ident(expr: str, start: int, n: int) -> tuple[Token, int]:
    """Consume an identifier and return (Token, new_position)."""
    i = start
    while i < n and (expr[i].isalnum() or expr[i] == "_"):
        i += 1
    return Token(TT.IDENT, expr[start:i], Span(start, i)), i
