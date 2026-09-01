"""CompiledToolProgramStage: a Stage that executes a compiled DSL expression.

This is the bridge between the DSL compiler and the datapipe execution engine.
A compiled expression is stored in this stage; ``setup()`` resolves per-worker
callables from ``ToolDescriptor`` objects for installed providers and stores
them in ``self._resolved_fns``.  ``process()`` uses those resolved callables.

Provider callables cannot be pickled across ``spawn`` process boundaries
because their ``__module__`` is a synthetic name that does not exist in worker
processes.  Only the ``ToolDescriptor`` (a frozen dataclass of primitives)
crosses the boundary; workers import the provider source and extract the
callable in ``setup()``.

Architecture (§10 of the CLI plan)
-------------------------------------
  setup(ctx)    — resolve provider callables per worker; no-op for built-ins
  process(value, ctx)
      for each invocation:
          look up resolved callable
          resolve selector references
          call tool function with bound configuration
          replace selected values
      return updated record
  teardown(ctx) — no-op
"""

from __future__ import annotations

from typing import Any, Callable

from datapipe.context import WorkerContext
from datapipe.dsl.compiler import CompiledExpression, ToolInvocation, overlap_reason
from datapipe.dsl.errors import SelectorResolutionError
from datapipe.stage import Stage
from datapipe.tools.errors import StructuralExecutionError, ToolExecutionError
from datapipe.tools.types import describe, infer_json_type, matches

# Forward reference for CompiledProgram; imported locally in CompiledProgramStage
# to avoid circular imports at module load time.
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from datapipe.dsl.compiler import CompiledBareCall, CompiledProgram
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
        # Resolved callables keyed by expression_index, populated in setup().
        # Built-in callables come straight from the ToolInvocation; provider
        # callables are resolved here from the ToolDescriptor.
        self._resolved_fns: dict[int, Callable] = {}
        # Truncate long expressions for the stage name.
        self.name = name or _short_name(compiled.source)
        self._name_explicit = name is not None

    @property
    def validate(self) -> str:
        return self._validate

    def __getstate__(self) -> dict:
        """Exclude resolved callables from pickling.

        ``_resolved_fns`` holds live provider callables whose ``__module__``
        is a synthetic loader name that does not exist in spawned worker
        processes.  Workers resolve callables fresh in ``setup()``, so the
        dict must be absent from the pickle payload.
        """
        state = self.__dict__.copy()
        state["_resolved_fns"] = {}
        return state

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        # Ensure the dict exists even if this object was pickled before the
        # attribute was introduced.
        if "_resolved_fns" not in self.__dict__:
            self._resolved_fns = {}

    def setup(self, ctx: WorkerContext) -> None:
        """Resolve provider callables once per worker before any records arrive."""
        self._resolve_all()

    def _resolve_all(self) -> None:
        """Resolve all tool callables into ``_resolved_fns`` if not already done."""
        if len(self._resolved_fns) == len(self._compiled.invocations):
            return  # already resolved (e.g. called twice or setup() was explicit)
        from datapipe.tools.loader import resolve_tool
        for inv in self._compiled.invocations:
            if inv.expression_index in self._resolved_fns:
                continue
            if inv.tool_descriptor is not None:
                fn = resolve_tool(inv.tool_descriptor.provider, inv.tool_descriptor.tool_name)
            else:
                assert inv.builtin_fn is not None, (
                    f"ToolInvocation {inv.tool_name!r}: both tool_descriptor and "
                    "builtin_fn are None — exactly one must be set"
                )
                fn = inv.builtin_fn
            self._resolved_fns[inv.expression_index] = fn

    def process(self, value: Any, ctx: WorkerContext) -> Any:
        """Execute all invocations in sequence against *value*."""
        # Lazy resolution: if setup() was not called (e.g. sequential executor
        # or direct stage usage in tests), resolve on the first process() call.
        if len(self._resolved_fns) < len(self._compiled.invocations):
            self._resolve_all()

        record = value
        record_seq = ctx.record_index if ctx is not None else None

        checking = self._should_validate_record()
        if checking:
            self._validated_records += 1

        for inv in self._compiled.invocations:
            tool_fn = self._resolved_fns[inv.expression_index]

            try:
                refs = inv.selector.resolve(record)
            except SelectorResolutionError as exc:
                raise ToolExecutionError(
                    record_seq=record_seq,
                    invocation_index=inv.expression_index,
                    tool_name=inv.tool_name,
                    provider_id=_provider_id(inv),
                    expression_span=inv.expression_span,
                    selector=inv.selector.render(),
                    matched_path=exc.path if exc.path else None,
                    match_ordinal=None,
                    stage="selector",
                    cause=exc,
                ) from exc

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
                    result = tool_fn(ref.value, **inv.arguments)
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
    """Return the provider identity string for *inv*."""
    if inv.tool_descriptor is not None:
        return inv.tool_descriptor.provider.provider_id
    # Built-in or test-local callable stored directly in builtin_fn.
    module = getattr(inv.builtin_fn, "__module__", None) or "<unknown>"
    if module == _BUILTIN_JSON_MODULE:
        return "builtin:json"
    return f"provider:{module}"


def _bare_contract(bare_fn: Callable) -> Any:
    """Return the ``ToolContract`` attached to *bare_fn*, or None if absent."""
    from datapipe.tools.decorator import get_contract
    return get_contract(bare_fn)


def _fn_name(bare_fn: Callable) -> str:
    """Best-effort display name for a bare call whose contract is unavailable."""
    return getattr(bare_fn, "__name__", None) or "<unknown>"


def _bare_provider_id(bare: "CompiledBareCall", bare_fn: Callable) -> str:
    """Return the provider identity string for a focused bare pipe call."""
    if bare.descriptor is not None:
        return bare.descriptor.provider.provider_id
    module = getattr(bare_fn, "__module__", None) or "<unknown>"
    if module == _BUILTIN_JSON_MODULE:
        return "builtin:json"
    return f"provider:{module}"


def _short_name(expression: str, max_len: int = 40) -> str:
    """Return a display-friendly name derived from the expression string."""
    clean = " ".join(expression.split())
    if len(clean) > max_len:
        return clean[:max_len - 1] + "…"
    return clean


def _join_path(parts: "tuple[str | int, ...]") -> str:
    """Render concrete path parts as selector text, e.g. ``.tools[0].name``."""
    if not parts:
        return "."
    return "".join(f"[{p}]" if isinstance(p, int) else f".{p}" for p in parts)


def _ref_path(refs: list) -> str | None:
    """Return the single reference's concrete path, or None when it is ambiguous."""
    return refs[0].path if len(refs) == 1 else None


# ---------------------------------------------------------------------------
# CompiledProgramStage: multi-statement sibling of CompiledToolProgramStage
# ---------------------------------------------------------------------------


class CompiledProgramStage(Stage):
    """Pipeline stage that executes a compiled multi-statement program per record.

    Semantics: each statement executes in sequence on the evolving root record.
    The record after all statements is the output.

    Parameters
    ----------
    compiled:
        The output of ``datapipe.dsl.compiler.compile_program()``.
    name:
        Stage name used in error attribution and pipeline inspection.
        Defaults to a short excerpt of the expression.
    validate:
        Runtime contract validation mode — ``"always"`` (default),
        ``"sample"``, or ``"off"``.
    """

    def __init__(
        self,
        compiled: "CompiledProgram",
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
        self._validated_records = 0
        self._resolved_fns: dict[int, Callable] = {}
        self.name = name or _short_name(compiled.source)
        self._name_explicit = name is not None

    @property
    def validate(self) -> str:
        return self._validate

    def __getstate__(self) -> dict:
        """Exclude resolved callables from pickling."""
        state = self.__dict__.copy()
        state["_resolved_fns"] = {}
        return state

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        if "_resolved_fns" not in self.__dict__:
            self._resolved_fns = {}

    def setup(self, ctx: WorkerContext) -> None:
        """Resolve provider callables once per worker before any records arrive."""
        self._resolve_all()

    def _expected_fn_count(self) -> int:
        """Total callables to resolve across the program.

        One per tool base operation, one per assignment transform (an
        assignment with no transform contributes none), plus one per bare pipe.
        """
        from datapipe.dsl.compiler import CompiledAssignment

        total = 0
        for stmt in self._compiled.statements:
            op = stmt.operation
            if isinstance(op, CompiledAssignment):
                total += 1 if op.transform is not None else 0
            else:
                total += 1
            total += len(stmt.pipes)
        return total

    def _resolve_all(self) -> None:
        """Resolve all tool callables into ``_resolved_fns`` if not already done.

        Base operations, assignment transforms, and focused bare pipe calls are
        all resolved, keyed by their ``expression_index`` (unique across the
        whole program).
        """
        if len(self._resolved_fns) == self._expected_fn_count():
            return
        from datapipe.dsl.compiler import CompiledAssignment
        from datapipe.tools.loader import resolve_tool

        def _resolve_invocation(inv) -> None:
            if inv.expression_index in self._resolved_fns:
                return
            if inv.tool_descriptor is not None:
                fn = resolve_tool(
                    inv.tool_descriptor.provider, inv.tool_descriptor.tool_name
                )
            else:
                assert inv.builtin_fn is not None, (
                    f"ToolInvocation {inv.tool_name!r}: both tool_descriptor and "
                    "builtin_fn are None — exactly one must be set"
                )
                fn = inv.builtin_fn
            self._resolved_fns[inv.expression_index] = fn

        for stmt in self._compiled.statements:
            op = stmt.operation
            if isinstance(op, CompiledAssignment):
                if op.transform is not None:
                    _resolve_invocation(op.transform)
            else:
                _resolve_invocation(op)

            for bare in stmt.pipes:
                if bare.expression_index in self._resolved_fns:
                    continue
                if bare.descriptor is not None:
                    bfn = resolve_tool(
                        bare.descriptor.provider, bare.descriptor.tool_name
                    )
                else:
                    assert bare.callable is not None, (
                        "CompiledBareCall: both descriptor and callable are None — "
                        "exactly one must be set"
                    )
                    bfn = bare.callable
                self._resolved_fns[bare.expression_index] = bfn

    def process(self, value: Any, ctx: WorkerContext) -> Any:
        """Execute all statements in sequence against *value*.

        Each statement resolves its target selector against the evolving root
        record, applies the base operation, then feeds the result through any
        focused bare pipe calls before writing back to the same location.
        A wildcard selector applies the whole chain elementwise to each match.
        The value returned is always the root record, never the focused value.

        An assignment statement instead resolves its source and destination,
        writes the (optionally transformed) value, and — for a move — removes
        the source only once the write has succeeded.
        """
        from datapipe.dsl.compiler import CompiledAssignment

        if len(self._resolved_fns) < self._expected_fn_count():
            self._resolve_all()

        record = value
        record_seq = ctx.record_index if ctx is not None else None

        checking = self._should_validate_record()
        if checking:
            self._validated_records += 1

        for stmt_index, stmt in enumerate(self._compiled.statements):
            if isinstance(stmt.operation, CompiledAssignment):
                record = self._apply_assignment(
                    stmt,
                    stmt_index,
                    record,
                    record_seq=record_seq,
                    checking=checking,
                )
                continue

            inv = stmt.operation
            tool_fn = self._resolved_fns[inv.expression_index]

            try:
                refs = inv.selector.resolve(record)
            except SelectorResolutionError as exc:
                raise ToolExecutionError(
                    record_seq=record_seq,
                    invocation_index=inv.expression_index,
                    tool_name=inv.tool_name,
                    provider_id=_provider_id(inv),
                    expression_span=inv.expression_span,
                    selector=inv.selector.render(),
                    matched_path=exc.path if exc.path else None,
                    match_ordinal=None,
                    stage="selector",
                    cause=exc,
                ) from exc

            if not refs:
                # Zero matches from an empty wildcard — no-op.
                continue

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
                    result = tool_fn(ref.value, **inv.arguments)
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

                # Focused pipes: feed the value through each bare call in order,
                # keeping the same location as the focus.
                for bare in stmt.pipes:
                    result = self._run_bare_call(
                        bare,
                        result,
                        record_seq=record_seq,
                        selector_text=inv.selector.render(),
                        matched_path=ref.path,
                        match_ordinal=match_ordinal,
                        checking=checking,
                    )

                new_values.append(result)

            record = inv.selector.apply(record, refs, new_values)
        return record

    def _apply_assignment(
        self,
        stmt: Any,
        stmt_index: int,
        record: Any,
        *,
        record_seq: int | None,
        checking: bool,
    ) -> Any:
        """Execute one ``=`` / ``<-`` statement, in the §8.1 binding order.

        1. resolve the source
        2. resolve/prepare the destination parent
        3. validate source cardinality
        4. detect runtime overlap
        5. compute the transformed value (and run any focused pipes)
        6. write the destination
        7. remove the move source
        8. publish focus = destination

        Nothing is written until steps 1-5 have all succeeded, so a failure at
        any precondition leaves the record byte-for-byte unmodified.
        """
        op = stmt.operation
        fail = self._structural_failure(op, stmt_index, record_seq)

        # -- 1. resolve the source ----------------------------------------
        try:
            src_refs = op.source.resolve(record)
        except SelectorResolutionError as exc:
            raise fail(
                source_path=exc.path or op.source.render(),
                reason=f"source path cannot be resolved: {exc}",
                cause=exc,
            ) from exc

        # -- 2. resolve / prepare the destination --------------------------
        # Only the final field is created by assignment; a missing intermediate
        # parent is an error (§8.2 auto-creation is scoped to `<<` in S4).
        dest_parent_container: Any = None
        dest_key: Any = None
        dest_ref = None
        if op.dest_parent is not None:
            try:
                parent_refs = op.dest_parent.resolve(record)
            except SelectorResolutionError as exc:
                raise fail(
                    source_path=_ref_path(src_refs),
                    reason=f"destination parent cannot be resolved: {exc}",
                    cause=exc,
                ) from exc
            if len(parent_refs) != 1:
                raise fail(
                    source_path=_ref_path(src_refs),
                    reason=(
                        f"destination parent must resolve to exactly one "
                        f"location, got {len(parent_refs)}"
                    ),
                )
            parent_value = parent_refs[0].value
            if not isinstance(parent_value, dict):
                raise fail(
                    source_path=_ref_path(src_refs),
                    reason=(
                        f"destination parent {parent_refs[0].path} is a "
                        f"{type(parent_value).__name__}, not an object"
                    ),
                )
            dest_parent_container = parent_value
            dest_key = op.dest_key
            dest_parts = parent_refs[0].path_parts + (op.dest_key,)
        else:
            # Root, index, or wildcard destination: it must already exist.
            try:
                dest_refs = op.destination.resolve(record)
            except SelectorResolutionError as exc:
                raise fail(
                    source_path=_ref_path(src_refs),
                    reason=f"destination cannot be resolved: {exc}",
                    cause=exc,
                ) from exc
            if len(dest_refs) != 1:
                raise fail(
                    source_path=_ref_path(src_refs),
                    reason=(
                        f"destination must resolve to exactly one location, "
                        f"got {len(dest_refs)}"
                    ),
                )
            dest_ref = dest_refs[0]
            dest_parts = dest_ref.path_parts

        # -- 3. validate source cardinality --------------------------------
        if len(src_refs) != 1:
            raise fail(
                destination_path=_join_path(dest_parts),
                reason=(
                    f"source must resolve to exactly one reference, got "
                    f"{len(src_refs)}"
                ),
            )
        src_ref = src_refs[0]

        # -- 4. detect runtime overlap -------------------------------------
        detail = overlap_reason(
            dest_parts,
            src_ref.path_parts,
            is_move=op.is_move,
            has_transform=op.transform is not None,
        )
        if detail is not None:
            raise fail(
                source_path=src_ref.path,
                destination_path=_join_path(dest_parts),
                reason=f"overlapping source and destination: {detail}",
            )

        # -- 5. compute the transformed value ------------------------------
        new_value = src_ref.value
        if op.transform is not None:
            new_value = self._run_transform(
                op.transform,
                new_value,
                record_seq=record_seq,
                matched_path=src_ref.path,
                checking=checking,
            )
        for bare in stmt.pipes:
            new_value = self._run_bare_call(
                bare,
                new_value,
                record_seq=record_seq,
                selector_text=op.destination.render(),
                matched_path=_join_path(dest_parts),
                match_ordinal=None,
                checking=checking,
            )

        # -- 6. apply the destination write --------------------------------
        # Step 4 waives the source-above-destination overlap when a transform
        # is present, because a transform normally yields a fresh value.  One
        # that returned its own argument puts us back in exactly the rejected
        # case, so re-run the check now that the value is known.
        if op.transform is not None and new_value is src_ref.value:
            detail = overlap_reason(
                dest_parts,
                src_ref.path_parts,
                is_move=op.is_move,
                has_transform=False,
            )
            if detail is not None:
                raise fail(
                    source_path=src_ref.path,
                    destination_path=_join_path(dest_parts),
                    reason=(
                        f"transform returned its own argument unchanged, so "
                        f"the source and destination overlap: {detail}"
                    ),
                )

        # A root destination is unreachable here: step 4 rejects it, because
        # the root path is a prefix of every source path.
        if dest_parent_container is not None:
            dest_parent_container[dest_key] = new_value
        else:
            dest_ref.replace(new_value)

        # -- 7. remove the move source (only now) --------------------------
        if op.is_move:
            del src_ref.parent[src_ref.key]

        # -- 8. focus is the destination; the record is what we return -----
        return record

    def _structural_failure(
        self, op: Any, stmt_index: int, record_seq: int | None
    ) -> Callable[..., StructuralExecutionError]:
        """Return a builder for this statement's ``StructuralExecutionError``."""
        operation = "move" if op.is_move else "copy"

        def build(
            *,
            source_path: str | None = None,
            destination_path: str | None = None,
            reason: str,
            cause: BaseException | None = None,
        ) -> StructuralExecutionError:
            return StructuralExecutionError(
                record_seq=record_seq,
                statement_index=stmt_index,
                operation=operation,
                selector=op.source.render(),
                source_path=source_path or op.source.render(),
                destination_path=destination_path or op.destination.render(),
                expression_span=op.span,
                policy="error",
                reason=reason,
                cause=cause,
            )

        return build

    def _run_transform(
        self,
        inv: ToolInvocation,
        value: Any,
        *,
        record_seq: int | None,
        matched_path: str | None,
        checking: bool,
    ) -> Any:
        """Apply an assignment's RHS transform to the already-resolved source value."""
        tool_fn = self._resolved_fns[inv.expression_index]

        if checking and not matches(value, inv.contract.input_type):
            raise self._mismatch(
                inv,
                record_seq=record_seq,
                stage="input",
                expected=inv.contract.input_type,
                value=value,
                matched_path=matched_path,
                match_ordinal=None,
            )

        try:
            result = tool_fn(value, **inv.arguments)
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
                matched_path=matched_path,
                match_ordinal=None,
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
                matched_path=matched_path,
                match_ordinal=None,
            )

        return result

    def _run_bare_call(
        self,
        bare: "CompiledBareCall",
        value: Any,
        *,
        record_seq: int | None,
        selector_text: str,
        matched_path: str | None,
        match_ordinal: int | None,
        checking: bool,
    ) -> Any:
        """Apply one focused bare pipe call to *value* and return the result."""
        bare_fn = self._resolved_fns[bare.expression_index]
        contract = _bare_contract(bare_fn)
        tool_name = contract.name if contract is not None else _fn_name(bare_fn)

        if checking and contract is not None and not matches(value, contract.input_type):
            raise self._bare_mismatch(
                bare,
                tool_name=tool_name,
                bare_fn=bare_fn,
                record_seq=record_seq,
                stage="input",
                expected=contract.input_type,
                value=value,
                selector_text=selector_text,
                matched_path=matched_path,
                match_ordinal=match_ordinal,
            )

        try:
            result = bare_fn(value, **bare.bound_args)
        except ToolExecutionError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise ToolExecutionError(
                record_seq=record_seq,
                invocation_index=bare.expression_index,
                tool_name=tool_name,
                provider_id=_bare_provider_id(bare, bare_fn),
                expression_span=bare.span,
                selector=selector_text,
                matched_path=matched_path,
                match_ordinal=match_ordinal,
                stage="call",
                cause=exc,
            ) from exc

        if checking and contract is not None and not matches(result, contract.output_type):
            raise self._bare_mismatch(
                bare,
                tool_name=tool_name,
                bare_fn=bare_fn,
                record_seq=record_seq,
                stage="output",
                expected=contract.output_type,
                value=result,
                selector_text=selector_text,
                matched_path=matched_path,
                match_ordinal=match_ordinal,
            )

        return result

    def _bare_mismatch(
        self,
        bare: "CompiledBareCall",
        *,
        tool_name: str,
        bare_fn: Callable,
        record_seq: int | None,
        stage: str,
        expected: Any,
        value: Any,
        selector_text: str,
        matched_path: str | None,
        match_ordinal: int | None,
    ) -> ToolExecutionError:
        """Build a ``ToolExecutionError`` for a bare pipe call type mismatch."""
        actual = infer_json_type(value)
        actual_name = actual.value if actual is not None else type(value).__name__
        return ToolExecutionError(
            record_seq=record_seq,
            invocation_index=bare.expression_index,
            tool_name=tool_name,
            provider_id=_bare_provider_id(bare, bare_fn),
            expression_span=bare.span,
            selector=selector_text,
            matched_path=matched_path,
            match_ordinal=match_ordinal,
            expected_type=describe(expected),
            actual_type=actual_name,
            stage=stage,
        )

    def _should_validate_record(self) -> bool:
        if self._validate == "always":
            return True
        if self._validate == "off":
            return False
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
        return f"CompiledProgramStage({self.name!r})"
