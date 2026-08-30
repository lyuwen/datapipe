"""DSL error types with source-position diagnostics.

All errors carry the original expression string and a span so callers can
render a caret diagnostic like::

    unknown argument `recusive` for fromjson
      fromjson(.metadata.annotation, recusive=true)
                                          ^^^^^^^^^
"""

from __future__ import annotations

from typing import NamedTuple


class Span(NamedTuple):
    """Half-open byte range [start, end) within an expression string."""

    start: int
    end: int

    def __repr__(self) -> str:
        return f"Span({self.start}, {self.end})"


def render_diagnostic(expression: str, span: Span, message: str) -> str:
    """Return a multi-line diagnostic string with a caret pointing at *span*.

    Example output::

        unknown argument `recusive` for fromjson
          fromjson(.metadata.annotation, recusive=true)
                                              ^^^^^^^^^
    """
    lines = [message]
    if expression:
        lines.append(f"  {expression}")
        start = max(0, span.start)
        end = max(start + 1, min(span.end, len(expression)))
        lines.append("  " + " " * start + "^" * (end - start))
    return "\n".join(lines)


class ExpressionSyntaxError(Exception):
    """Raised when the expression cannot be tokenized or parsed."""

    def __init__(
        self,
        message: str,
        expression: str = "",
        span: Span | None = None,
    ) -> None:
        self.expression = expression
        self.span = span or Span(0, 0)
        diagnostic = render_diagnostic(expression, self.span, message)
        super().__init__(diagnostic)
        self.base_message = message


class ToolResolutionError(Exception):
    """Raised when a tool name cannot be resolved against the registry."""

    def __init__(
        self,
        message: str,
        expression: str = "",
        span: Span | None = None,
    ) -> None:
        self.expression = expression
        self.span = span or Span(0, 0)
        diagnostic = render_diagnostic(expression, self.span, message)
        super().__init__(diagnostic)
        self.base_message = message


class ToolConfigurationError(Exception):
    """Raised when an argument binding or default validation fails."""

    def __init__(
        self,
        message: str,
        expression: str = "",
        span: Span | None = None,
    ) -> None:
        self.expression = expression
        self.span = span or Span(0, 0)
        diagnostic = render_diagnostic(expression, self.span, message)
        super().__init__(diagnostic)
        self.base_message = message


class SelectorResolutionError(Exception):
    """Raised at runtime when a selector path cannot be resolved."""

    def __init__(
        self,
        message: str,
        path: str = "",
    ) -> None:
        self.path = path
        super().__init__(message)
