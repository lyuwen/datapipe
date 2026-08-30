"""Structured runtime errors for tool execution.

``ToolExecutionError`` carries the full diagnostic context required by §11 of
the CLI plan: record sequence, invocation index, tool and provider identity,
source expression span, configured selector, concrete matched path and
wildcard ordinal, expected/actual JSON types, and the original exception.

The error is raised inside a worker and must survive pickling back to the
coordinator, so it follows the same ``__reduce__`` + module-level factory
pattern as :class:`datapipe.errors.StageExecutionError`.  Exceptions with
keyword-only constructors do not round-trip through the default
``BaseException.__reduce__`` (which replays ``args`` positionally), hence the
explicit factory.
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
