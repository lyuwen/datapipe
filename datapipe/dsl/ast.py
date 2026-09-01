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


@dataclass(frozen=True)
class BareToolCall:
    """A bare tool reference in a focused pipe: just a name, optionally with args.

    Used in ``statement.pipes`` after the base operation establishes a target.
    The selector is implied by the current focus.
    """
    qualified_name: QualifiedName
    arguments: tuple[Argument, ...]
    span: Span


# ---------------------------------------------------------------------------
# Assignment nodes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AssignmentRHS:
    """Right-hand side of an assignment.

    Exactly one of ``source`` and ``literal`` is set, matching the plan §9
    grammar ``value_expression := path | literal | invocation``:

    - ``source`` is the primary source selector, shared with ``transform``'s
      own selector when a transform is present.  A move (``<-``) requires it,
      because there is nothing else to remove.
    - ``literal`` is a constant right-hand side (``.a = 5``).  It has no path,
      so it never carries a transform and is rejected for ``<-``.
    """
    source: "Selector | None"
    transform: "Invocation | BareToolCall | None"
    span: Span
    literal: "Literal | None" = None


@dataclass(frozen=True)
class Assignment:
    """``.dest = rhs`` (copy) or ``.dest <- rhs`` (move)."""
    destination: Selector
    rhs: AssignmentRHS
    is_move: bool
    span: Span


# ---------------------------------------------------------------------------
# Move-into nodes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FieldSet:
    """``.base.(a|b|c)`` / ``.base.(^a|b|c)`` — a named set of sibling fields.

    ``complement`` inverts the whole parenthesized set: every field of the
    object at ``base`` *except* ``names``.
    """
    base: Selector
    names: tuple[str, ...]
    complement: bool
    span: Span


@dataclass(frozen=True)
class MoveInto:
    """``.dest << src, src, ...`` — move each source under the destination object.

    Each source contributes one destination key, derived from its final object
    field name.  A ``FieldSet`` source contributes one key per expanded field.
    """
    destination: Selector
    sources: tuple["Selector | FieldSet", ...]
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
    """One record-mutation statement.

    For invocation-first statements (existing form), ``focus_selector`` is None
    and ``operation`` is an ``Invocation``.

    For selector-first focused statements, ``focus_selector`` is the leading
    selector, ``operation`` is the first bare tool call, and ``pipes`` has
    further bare tool calls.

    For assignments, ``operation`` is an ``Assignment`` and ``focus_selector``
    is the assignment's destination — trailing pipes apply to the value that
    was just written there.

    For move-intos, ``operation`` is a ``MoveInto`` and ``focus_selector`` is
    its destination, so a trailing pipe applies to the assembled object.
    """
    operation: "Invocation | BareToolCall | Assignment | MoveInto"
    pipes: "tuple[BareToolCall, ...]"
    focus_selector: "Selector | None"   # None for invocation-first statements
    span: Span


@dataclass(frozen=True)
class Program:
    """A complete multi-statement program: one or more ``;``-separated statements."""
    statements: tuple[Statement, ...]
    span: Span
