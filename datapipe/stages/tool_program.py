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
from datapipe.dsl.compiler import CompiledExpression, ToolInvocation
from datapipe.dsl.errors import SelectorResolutionError
from datapipe.stage import Stage
from datapipe.tools.errors import ToolExecutionError
from datapipe.tools.types import describe, infer_json_type, matches

#: Number of records each worker validates in ``sample`` mode before it starts
#: trusting the provider.
SAMPLE_LIMIT = 100

#: Accepted runtime validation modes (§10.3 of the CLI plan).
VALIDATE_MODES = ("always", "sample", "off")


class CompiledToolProgramStage(Stage):
    """Pipeline stage that executes a compiled DSL expression per record.

    Parameters
    ----------
    compiled:
        The output of ``datapipe.dsl.compiler.compile_expression()``.
    name:
        Stage name used in error attribution and pipeline inspection.
        Defaults to a short excerpt of the expression.
    validate:
        Runtime contract validation mode — ``"always"`` (default),
        ``"sample"``, or ``"off"``.  Validated at construction time so an
        invalid mode fails fast in the coordinator rather than once per record
        inside every worker.
    """

    def __init__(
        self,
        compiled: CompiledExpression,
        name: str | None = None,
        validate: str = "always",
    ) -> None:
        if validate not in VALIDATE_MODES:
            raise ValueError(
                f"invalid validate mode {validate!r}; "
                f"expected one of {', '.join(repr(m) for m in VALIDATE_MODES)}"
            )
        self._compiled = compiled
        self._validate = validate
        # Records validated so far in "sample" mode.  This counter is per
        # worker process (each worker gets its own unpickled copy of the
        # stage), so the total number of validated records scales with the
        # worker count.  That is intended: sampling is a cheap smoke test of
        # the provider's contract, not a global budget, and keeping it local
        # avoids any cross-worker coordination.
        self._validated_records = 0
        # Truncate long expressions for the stage name.
        self.name = name or _short_name(compiled.source)
        self._name_explicit = name is not None

    @property
    def validate(self) -> str:
        return self._validate

    def process(self, value: Any, ctx: WorkerContext) -> Any:
        """Execute all invocations in sequence against *value*."""
        record = value
        record_seq = ctx.record_index if ctx is not None else None

        checking = self._should_validate_record()
        if checking:
            self._validated_records += 1

        for inv in self._compiled.invocations:
            refs = inv.selector.resolve(record)
            if not refs:
                # Zero matches from an empty wildcard — no-op.
                continue

            # Only meaningful for wildcard selectors; a fixed path has a single
            # match and no ordinal to report.
            wildcard = inv.selector.has_wildcard

            new_values = []
            for ordinal, ref in enumerate(refs):
                match_ordinal = ordinal if wildcard else None

                if checking and not matches(ref.value, inv.contract.input_type):
                    raise self._mismatch(
                        inv,
                        record_seq=record_seq,
                        stage="input",
                        expected=inv.contract.input_type,
                        value=ref.value,
                        matched_path=ref.path,
                        match_ordinal=match_ordinal,
                    )

                try:
                    result = inv.tool_fn(ref.value, **inv.arguments)
                except ToolExecutionError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    raise ToolExecutionError(
                        record_seq=record_seq,
                        invocation_index=inv.expression_index,
                        tool_name=inv.tool_name,
                        provider_id=_provider_id(inv),
                        expression_span=inv.expression_span,
                        selector=inv.selector.render(),
                        matched_path=ref.path,
                        match_ordinal=match_ordinal,
                        stage="call",
                        cause=exc,
                    ) from exc

                if checking and not matches(result, inv.contract.output_type):
                    raise self._mismatch(
                        inv,
                        record_seq=record_seq,
                        stage="output",
                        expected=inv.contract.output_type,
                        value=result,
                        matched_path=ref.path,
                        match_ordinal=match_ordinal,
                    )

                new_values.append(result)

            record = inv.selector.apply(record, refs, new_values)
        return record

    # -- internals ---------------------------------------------------------

    def _should_validate_record(self) -> bool:
        """Return True when contract checks apply to the current record."""
        if self._validate == "always":
            return True
        if self._validate == "off":
            return False
        # "sample": deterministic — the first SAMPLE_LIMIT records this worker
        # sees are validated, and every record after that is trusted.
        return self._validated_records < SAMPLE_LIMIT

    def _mismatch(
        self,
        inv: ToolInvocation,
        *,
        record_seq: int | None,
        stage: str,
        expected: Any,
        value: Any,
        matched_path: str | None,
        match_ordinal: int | None,
    ) -> ToolExecutionError:
        """Build a ``ToolExecutionError`` for a contract type mismatch."""
        actual = infer_json_type(value)
        actual_name = actual.value if actual is not None else type(value).__name__
        return ToolExecutionError(
            record_seq=record_seq,
            invocation_index=inv.expression_index,
            tool_name=inv.tool_name,
            provider_id=_provider_id(inv),
            expression_span=inv.expression_span,
            selector=inv.selector.render(),
            matched_path=matched_path,
            match_ordinal=match_ordinal,
            expected_type=describe(expected),
            actual_type=actual_name,
            stage=stage,
        )

    def __repr__(self) -> str:
        return f"CompiledToolProgramStage({self.name!r})"


#: Module path of the built-in JSON provider.
_BUILTIN_JSON_MODULE = "datapipe.tools.builtins.json"


def _provider_id(inv: ToolInvocation) -> str:
    """Return the provider identity string for *inv*.

    ``ToolInvocation`` does not carry a provider descriptor yet, so the value
    is derived from the tool function's defining module.  Kept in one place so
    it can be replaced with a real descriptor lookup once provider identity is
    threaded through the compiler.
    """
    module = getattr(inv.tool_fn, "__module__", None) or "<unknown>"
    if module == _BUILTIN_JSON_MODULE:
        return "builtin:json"
    return f"provider:{module}"


def _short_name(expression: str, max_len: int = 40) -> str:
    """Return a display-friendly name derived from the expression string."""
    clean = " ".join(expression.split())
    if len(clean) > max_len:
        return clean[:max_len - 1] + "…"
    return clean
