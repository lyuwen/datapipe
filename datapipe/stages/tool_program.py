"""CompiledToolProgramStage: a Stage that executes a compiled DSL expression.

This is the bridge between the DSL compiler and the datapipe execution engine.
A compiled expression is stored in this stage; ``setup()`` is a no-op (tools
are simple functions in Phase 1), and ``process()`` executes the expression
against each record.

The stage is pickleable — it holds a ``CompiledExpression`` which contains
only frozen dataclasses and callables decorated with ``@tool``.  Worker
processes receive the compiled stage and resolve tool functions from it
without re-running the compiler.

Architecture (§10 of the CLI plan)
-------------------------------------
  setup(ctx)    — no-op for function tools in Phase 1
  process(value, ctx)
      for each invocation:
          resolve selector references
          call tool function with bound configuration
          replace selected values
      return updated record
  teardown(ctx) — no-op for function tools in Phase 1
"""

from __future__ import annotations

from typing import Any

from datapipe.context import WorkerContext
from datapipe.dsl.compiler import CompiledExpression
from datapipe.dsl.errors import SelectorResolutionError
from datapipe.stage import Stage


class CompiledToolProgramStage(Stage):
    """Pipeline stage that executes a compiled DSL expression per record.

    Parameters
    ----------
    compiled:
        The output of ``datapipe.dsl.compiler.compile_expression()``.
    name:
        Stage name used in error attribution and pipeline inspection.
        Defaults to a short excerpt of the expression.
    """

    def __init__(
        self,
        compiled: CompiledExpression,
        name: str | None = None,
    ) -> None:
        self._compiled = compiled
        # Truncate long expressions for the stage name.
        self.name = name or _short_name(compiled.source)
        self._name_explicit = name is not None

    def process(self, value: Any, ctx: WorkerContext) -> Any:
        """Execute all invocations in sequence against *value*."""
        record = value
        for inv in self._compiled.invocations:
            refs = inv.selector.resolve(record)
            if not refs:
                # Zero matches from an empty wildcard — no-op.
                continue
            new_values = [
                inv.tool_fn(ref.value, **inv.arguments)
                for ref in refs
            ]
            record = inv.selector.apply(record, refs, new_values)
        return record

    def __repr__(self) -> str:
        return f"CompiledToolProgramStage({self.name!r})"


def _short_name(expression: str, max_len: int = 40) -> str:
    """Return a display-friendly name derived from the expression string."""
    clean = " ".join(expression.split())
    if len(clean) > max_len:
        return clean[:max_len - 1] + "…"
    return clean
