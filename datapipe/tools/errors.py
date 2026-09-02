"""Structured runtime errors for tool execution.

``ToolExecutionError`` carries the full diagnostic context required by §11 of
the CLI plan: record sequence, invocation index, tool and provider identity,
source expression span, configured selector, concrete matched path and
wildcard ordinal, expected/actual JSON types, and the original exception.

``StructuralExecutionError`` is its sibling for structural statements (``=``,
``<-``), carrying the §12 context from the structural-transform plan.

Both errors are raised inside a worker and must survive pickling back to the
coordinator, so they follow the same ``__reduce__`` + module-level factory
pattern as :class:`datapipe.errors.StageExecutionError`.  Exceptions with
keyword-only constructors do not round-trip through the default
``BaseException.__reduce__`` (which replays ``args`` positionally), hence the
explicit factories.
"""

from __future__ import annotations


class ToolExecutionError(Exception):
    """Raised when a tool invocation fails on a record.

    ``stage`` distinguishes the three failure sites:

    - ``"input"``  — the selected value violated the tool's input contract;
    - ``"output"`` — the returned value violated the output contract;
    - ``"call"``   — the tool body itself raised (``cause`` is set, and there
      is no type mismatch to report).
    """

    def __init__(
        self,
        *,
        record_seq: int | None,
        invocation_index: int,
        tool_name: str,
        provider_id: str,
        selector: str,
        stage: str,
        expression_span: tuple[int, int] | None = None,
        matched_path: str | None = None,
        match_ordinal: int | None = None,
        expected_type: str | None = None,
        actual_type: str | None = None,
        cause: BaseException | None = None,
    ) -> None:
        self.record_seq = record_seq
        self.invocation_index = invocation_index
        self.tool_name = tool_name
        self.provider_id = provider_id
        self.expression_span = (
            tuple(expression_span) if expression_span is not None else None
        )
        self.selector = selector
        self.matched_path = matched_path
        self.match_ordinal = match_ordinal
        self.expected_type = expected_type
        self.actual_type = actual_type
        self.stage = stage
        self.cause = cause
        super().__init__(self._build_message())

    # -- message rendering -------------------------------------------------

    def _build_message(self) -> str:
        """Render the multi-line diagnostic from §11 of the CLI plan."""
        where = self.matched_path or self.selector
        record = self.record_seq if self.record_seq is not None else "?"
        lines = [
            f"record {record} failed in {self.tool_name} at {where}",
            f"provider: {self.provider_id}",
            f"invocation: {self.invocation_index}",
        ]

        # A "call" failure has no type mismatch, so those lines are omitted.
        label = "output" if self.stage == "output" else "input"
        if self.expected_type is not None:
            lines.append(f"expected {label}: {self.expected_type}")
        if self.actual_type is not None:
            lines.append(f"actual {label}: {self.actual_type}")

        if self.cause is not None:
            lines.append(f"cause: {type(self.cause).__name__}: {self.cause}")

        return "\n".join(lines)

    def __str__(self) -> str:
        return self._build_message()

    # -- pickling ----------------------------------------------------------

    def __reduce__(self):
        return (
            _rebuild_tool_execution_error,
            (
                self.record_seq,
                self.invocation_index,
                self.tool_name,
                self.provider_id,
                self.expression_span,
                self.selector,
                self.matched_path,
                self.match_ordinal,
                self.expected_type,
                self.actual_type,
                self.stage,
                self.cause,
            ),
        )


def _rebuild_tool_execution_error(
    record_seq: int | None,
    invocation_index: int,
    tool_name: str,
    provider_id: str,
    expression_span: tuple[int, int] | None,
    selector: str,
    matched_path: str | None,
    match_ordinal: int | None,
    expected_type: str | None,
    actual_type: str | None,
    stage: str,
    cause: BaseException | None,
) -> ToolExecutionError:
    return ToolExecutionError(
        record_seq=record_seq,
        invocation_index=invocation_index,
        tool_name=tool_name,
        provider_id=provider_id,
        expression_span=expression_span,
        selector=selector,
        matched_path=matched_path,
        match_ordinal=match_ordinal,
        expected_type=expected_type,
        actual_type=actual_type,
        stage=stage,
        cause=cause,
    )


class StructuralExecutionError(Exception):
    """Raised when a structural statement (``=`` / ``<-``) fails on a record.

    Carries the §12 diagnostic context: record sequence, statement index,
    operation type, source span, configured selector, concrete source and
    destination paths, the collision/missing-path policy in force, and the
    original cause.

    Pickling follows the same ``__reduce__`` + module-level factory pattern as
    :class:`ToolExecutionError`, because the keyword-only constructor does not
    round-trip through the default ``BaseException.__reduce__``.
    """

    def __init__(
        self,
        *,
        record_seq: int | None,
        statement_index: int,
        operation: str,
        selector: str,
        source_path: str | None = None,
        destination_path: str | None = None,
        expression_span: tuple[int, int] | None = None,
        policy: str | None = None,
        reason: str | None = None,
        cause: BaseException | None = None,
    ) -> None:
        self.record_seq = record_seq
        self.statement_index = statement_index
        self.operation = operation
        self.selector = selector
        self.source_path = source_path
        self.destination_path = destination_path
        self.expression_span = (
            tuple(expression_span) if expression_span is not None else None
        )
        self.policy = policy
        self.reason = reason
        self.cause = cause
        super().__init__(self._build_message())

    def _build_message(self) -> str:
        """Render the multi-line diagnostic from §12 of the structural plan."""
        record = self.record_seq if self.record_seq is not None else "?"
        lines = [
            f"record {record} failed in {self.operation}",
            f"statement: {self.statement_index}",
        ]
        if self.source_path is not None:
            lines.append(f"source: {self.source_path}")
        if self.destination_path is not None:
            lines.append(f"destination: {self.destination_path}")
        if self.policy is not None:
            lines.append(f"policy: {self.policy}")

        if self.reason is not None:
            lines.append(f"cause: {self.reason}")
        elif self.cause is not None:
            lines.append(f"cause: {type(self.cause).__name__}: {self.cause}")

        return "\n".join(lines)

    def __str__(self) -> str:
        return self._build_message()

    def __reduce__(self):
        return (
            _rebuild_structural_execution_error,
            (
                self.record_seq,
                self.statement_index,
                self.operation,
                self.selector,
                self.source_path,
                self.destination_path,
                self.expression_span,
                self.policy,
                self.reason,
                self.cause,
            ),
        )


def _rebuild_structural_execution_error(
    record_seq: int | None,
    statement_index: int,
    operation: str,
    selector: str,
    source_path: str | None,
    destination_path: str | None,
    expression_span: tuple[int, int] | None,
    policy: str | None,
    reason: str | None,
    cause: BaseException | None,
) -> StructuralExecutionError:
    return StructuralExecutionError(
        record_seq=record_seq,
        statement_index=statement_index,
        operation=operation,
        selector=selector,
        source_path=source_path,
        destination_path=destination_path,
        expression_span=expression_span,
        policy=policy,
        reason=reason,
        cause=cause,
    )
