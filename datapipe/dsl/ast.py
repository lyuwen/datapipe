"""AST node types for the datapipe DSL expression language.

Every node carries a ``Span`` so diagnostics can render a caret pointing at
the offending token range.  The AST is syntax-only: it holds names and
literals but not resolved tool references or bound configurations.

Grammar summary::

    expression  := invocation ("|" invocation)*
    invocation  := qualified_name "(" selector ("," argument)* ")"
    argument    := identifier "=" literal
    selector    := "." selector_part*
    selector_part :=
        identifier           -- .field
      | "[" string "]"       -- ["key.with.dots"]
      | "[" integer "]"      -- [0]
      | "[]"                 -- []  (wildcard / each)
    literal     := string | number | boolean | null | array | object
    qualified_name := identifier ("." identifier)?
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from datapipe.dsl.errors import Span


# ---------------------------------------------------------------------------
# Selector nodes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Field:
    """``.name`` selector part — access an object field by identifier."""
    name: str
    span: Span


@dataclass(frozen=True)
class QuotedKey:
    """``["key"]`` selector part — access an object field by quoted string."""
    key: str
    span: Span


@dataclass(frozen=True)
class Index:
    """``[0]`` selector part — access an array element by integer index."""
    index: int
    span: Span


@dataclass(frozen=True)
class Each:
    """``[]`` selector part — match every element of an array (wildcard)."""
    span: Span


SelectorPart = Field | QuotedKey | Index | Each


@dataclass(frozen=True)
class Selector:
    """A complete selector starting with ``.``.

    ``parts`` is empty for the root selector ``.``.
    """
    parts: tuple[SelectorPart, ...]
    span: Span

    @property
    def is_root(self) -> bool:
        return len(self.parts) == 0

    @property
    def has_wildcard(self) -> bool:
        return any(isinstance(p, Each) for p in self.parts)


# ---------------------------------------------------------------------------
# Literal node
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Literal:
    """A literal value in an argument: string, number, bool, null, list, dict."""
    value: Any
    span: Span


# ---------------------------------------------------------------------------
# Invocation node
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Argument:
    """``name=literal`` in an invocation argument list."""
    name: str
    value: Literal
    span: Span


@dataclass(frozen=True)
class QualifiedName:
    """Tool name, optionally namespace-qualified: ``name`` or ``ns.name``."""
    namespace: str | None   # e.g. "my_tools" or None for unqualified
    name: str               # e.g. "normalize_text"
    span: Span

    @property
    def display(self) -> str:
        if self.namespace:
            return f"{self.namespace}.{self.name}"
        return self.name


@dataclass(frozen=True)
class Invocation:
    """A single tool call in the pipeline: ``name(selector, key=val, ...)``."""
    qualified_name: QualifiedName
    selector: Selector
    arguments: tuple[Argument, ...]
    span: Span


# ---------------------------------------------------------------------------
# Top-level expression
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Expression:
    """A complete pipeline expression: one or more ``|``-separated invocations."""
    invocations: tuple[Invocation, ...]
    span: Span


@dataclass(frozen=True)
class Statement:
    """One record-mutation statement: a base operation with optional focused pipes.

    For Phase S1 the base operation is always an ``Invocation``;
    ``pipes`` is always empty until Phase S2.
    """
    operation: Invocation          # use existing Invocation for now
    pipes: tuple[()]               # empty until S2
    span: Span


@dataclass(frozen=True)
class Program:
    """A complete multi-statement program: one or more ``;``-separated statements."""
    statements: tuple[Statement, ...]
    span: Span
