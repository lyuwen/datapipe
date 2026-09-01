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

import contextvars
from copy import deepcopy as _deepcopy
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

#: The validation mode the enclosing stage is running under, published for the
#: duration of each ``process()`` call.
#:
#: A ``target="record"`` tool receives only ``(record, **arguments)`` — the
#: contract deliberately keeps tool functions ignorant of the runtime.  The
#: built-in ``nest``/``unnest`` are the one case where that hurts: they desugar
#: into a nested ``CompiledProgramStage`` of their own, and without this channel
#: that inner stage would validate on every record even under ``--validate off``.
#: A ContextVar carries the mode without widening the tool signature, and is
#: correct under threads because each thread gets its own value.
_ACTIVE_VALIDATE: "contextvars.ContextVar[str]" = contextvars.ContextVar(
    "datapipe_active_validate", default="always"
)


def active_validate_mode() -> str:
    """Return the validation mode of the innermost running stage."""
    return _ACTIVE_VALIDATE.get()


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
        token = _ACTIVE_VALIDATE.set(self._validate)
        try:
            return self._process(value, ctx)
        finally:
            _ACTIVE_VALIDATE.reset(token)

    def _process(self, value: Any, ctx: WorkerContext) -> Any:
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


#: Module paths of the built-in providers, mapped to their provider identity.
_BUILTIN_MODULES = {
    "datapipe.tools.builtins.json": "builtin:json",
    "datapipe.tools.builtins.structural": "builtin:structural",
}


def builtin_provider_id(fn: Callable) -> str:
    """Return the provider identity string for a built-in or local callable."""
    module = getattr(fn, "__module__", None) or "<unknown>"
    builtin = _BUILTIN_MODULES.get(module)
    return builtin if builtin is not None else f"provider:{module}"


def _provider_id(inv: ToolInvocation) -> str:
    """Return the provider identity string for *inv*."""
    if inv.tool_descriptor is not None:
        return inv.tool_descriptor.provider.provider_id
    # Built-in or test-local callable stored directly in builtin_fn.
    return builtin_provider_id(inv.builtin_fn)


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
    return builtin_provider_id(bare_fn)


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


def _detached(value: Any) -> Any:
    """Return *value* with no shared structure, so writing it creates no alias.

    Only containers are copied: JSON scalars are immutable, so copying them
    would be pure per-record cost on the hot path for no correctness gain.
    """
    if isinstance(value, (dict, list)):
        return _deepcopy(value)
    return value


def _without(
    target: dict,
    entries: "list[tuple[str, Any]]",
    dest_parts: "tuple[str | int, ...]",
) -> dict:
    """Return a detached copy of *target* with any nested move sources removed.

    A trailing pipe must see the destination as it will be once the move is
    complete.  When the destination is an ancestor of a source — §6.6's
    ``. << .metadata.x`` — the source is still physically inside *target*, so a
    plain copy would hand the tool a value that still contains what is about to
    move out of it.
    """
    result = _deepcopy(target)
    depth = len(dest_parts)
    nested = sorted(
        (ref.path_parts[depth:] for _k, ref in entries if len(ref.path_parts) > depth
         and ref.path_parts[:depth] == dest_parts),
        key=len,
        reverse=True,
    )
    for rel in nested:
        container = result
        for part in rel[:-1]:
            container = container[part]
        del container[rel[-1]]
    return result


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

    def with_validate(self, validate: str) -> "CompiledProgramStage":
        """Return a view of this stage running under *validate*.

        The compiled program and any resolved callables are shared, so this
        costs one small object and no re-resolution.  Its own sample counter
        starts at zero, which is the same per-worker semantics a stage
        constructed directly at that mode would have.
        """
        clone = CompiledProgramStage(
            self._compiled,
            name=self.name if self._name_explicit else None,
            validate=validate,
        )
        clone._resolved_fns = self._resolved_fns
        return clone

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
        from datapipe.dsl.compiler import CompiledAssignment, CompiledMoveInto

        total = 0
        for stmt in self._compiled.statements:
            op = stmt.operation
            if isinstance(op, CompiledAssignment):
                total += 1 if op.transform is not None else 0
            elif isinstance(op, CompiledMoveInto):
                pass  # a move-into calls no tool of its own
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
        from datapipe.dsl.compiler import CompiledAssignment, CompiledMoveInto
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
            elif not isinstance(op, CompiledMoveInto):
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
        token = _ACTIVE_VALIDATE.set(self._validate)
        try:
            return self._process(value, ctx)
        finally:
            _ACTIVE_VALIDATE.reset(token)

    def _process(self, value: Any, ctx: WorkerContext) -> Any:
        from datapipe.dsl.compiler import CompiledAssignment, CompiledMoveInto

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

            if isinstance(stmt.operation, CompiledMoveInto):
                record = self._apply_move_into(
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
        # Containers are deep-copied so the destination cannot be mutated
        # through the source (or vice versa) by a later statement; §6.1 says
        # `=` copies.  Scalars are immutable and skip the per-record cost.
        written = _detached(new_value)
        if dest_parent_container is not None:
            dest_parent_container[dest_key] = written
        else:
            dest_ref.replace(written)

        # -- 7. remove the move source (only now) --------------------------
        if op.is_move:
            del src_ref.parent[src_ref.key]

        # -- 8. focus is the destination; the record is what we return -----
        return record

    def _apply_move_into(
        self,
        stmt: Any,
        stmt_index: int,
        record: Any,
        *,
        record_seq: int | None,
        checking: bool,
    ) -> Any:
        """Execute one ``<<`` statement, in the §8.1 binding order.

        1. locate the destination object (created only in step 6, never here)
        2. resolve and expand every source
        3. validate destination type and source cardinality
        4. detect self-references, duplicate keys, and key collisions
        5. assemble the destination value and run any focused pipes
        6. write the destination
        7. remove every source
        8. publish focus = the destination

        Nothing in the record is written until steps 1-5 have all succeeded, so
        a collision on the third of three sources leaves the record
        byte-for-byte unmodified.
        """
        op = stmt.operation
        fail = self._structural_failure(op, stmt_index, record_seq)

        # -- 1. locate the destination -------------------------------------
        target, dest_parts, place = self._locate_move_destination(record, op, fail)

        # -- 2/3. resolve and expand every source --------------------------
        entries = self._expand_move_sources(record, op, dest_parts, fail)

        # -- 4. reject self-references, duplicates, and collisions ---------
        self._check_move_entries(entries, target, dest_parts, fail)

        # -- 5. assemble the destination value ------------------------------
        # Values are detached so the destination shares no structure with
        # anything still live in the record (the S3 aliasing rule); this also
        # makes the deletions in step 7 harmless to what was just written.
        additions = {key: _detached(ref.value) for key, ref in entries}

        if not stmt.pipes:
            record = place(record, additions, None)
        else:
            # A pipe sees the destination as it will be *after* the move, so a
            # source that lives inside the destination subtree (§6.6's root
            # move) must already be gone from the value handed to the tool.
            final = _without(target, entries, dest_parts)
            final.update(additions)
            for bare in stmt.pipes:
                final = self._run_bare_call(
                    bare,
                    final,
                    record_seq=record_seq,
                    selector_text=op.destination.render(),
                    matched_path=_join_path(dest_parts),
                    match_ordinal=None,
                    checking=checking,
                )
            # -- 6. apply the destination write ------------------------------
            record = place(record, additions, final)

        # -- 7. remove the sources (only now) -------------------------------
        # Every source's immediate parent is an object: `<<` derives its keys
        # from final object fields, so no deletion can renumber a sibling the
        # way removing a list element would.  Step 4 has already rejected a
        # source nested inside another, so no deletion can remove the container
        # another pending reference points into either.
        for _key, ref in entries:
            del ref.parent[ref.key]

        # -- 8. focus is the destination; the record is what we return ------
        return record

    def _locate_move_destination(
        self, record: Any, op: Any, fail: Callable[..., StructuralExecutionError]
    ) -> "tuple[dict, tuple[str | int, ...], Callable]":
        """Locate a ``<<`` destination and return (target, path_parts, placer).

        ``target`` is the object the moved fields join — the existing
        destination, or an empty dict standing in for one that §8.2 will create.
        Nothing is written here: ``placer`` performs the whole write later, once
        every precondition has passed.
        """
        if op.dest_parent is not None:
            try:
                parent_refs = op.dest_parent.resolve(record)
            except SelectorResolutionError as exc:
                raise fail(
                    reason=f"destination parent cannot be resolved: {exc}",
                    cause=exc,
                ) from exc
            if len(parent_refs) != 1:
                raise fail(reason=(
                    f"destination parent must resolve to exactly one location, "
                    f"got {len(parent_refs)}"
                ))
            parent = parent_refs[0].value
            if not isinstance(parent, dict):
                raise fail(reason=(
                    f"destination parent {parent_refs[0].path} is a "
                    f"{type(parent).__name__}, not an object"
                ))

            key = op.dest_key
            dest_parts = parent_refs[0].path_parts + (key,)
            existing = parent.get(key)
            if key in parent and not isinstance(existing, dict):
                # §8.3: a serialized destination must be decoded first.
                raise fail(
                    destination_path=_join_path(dest_parts),
                    reason=(
                        f"destination {_join_path(dest_parts)} is a "
                        f"{type(existing).__name__}, not an object; decode it "
                        f"first (`fromjson({_join_path(dest_parts)})`)"
                    ),
                )
            target = existing if key in parent else {}

            def place(rec, additions, final, _p=parent, _k=key, _t=target):
                if final is not None:
                    _p[_k] = final
                    return rec
                _t.update(additions)
                _p[_k] = _t      # a no-op when the destination already existed
                return rec

            return target, dest_parts, place

        # Root, index, or wildcard destination: it must already exist (§8.2
        # creates only a missing *final field*, which has a parent selector).
        try:
            dest_refs = op.destination.resolve(record)
        except SelectorResolutionError as exc:
            raise fail(
                reason=f"destination cannot be resolved: {exc}", cause=exc
            ) from exc
        if len(dest_refs) != 1:
            raise fail(reason=(
                f"destination must resolve to exactly one location, "
                f"got {len(dest_refs)}"
            ))
        dest_ref = dest_refs[0]
        if not isinstance(dest_ref.value, dict):
            raise fail(
                destination_path=dest_ref.path,
                reason=(
                    f"destination {dest_ref.path} is a "
                    f"{type(dest_ref.value).__name__}, not an object"
                ),
            )

        def place(rec, additions, final, _ref=dest_ref):
            if final is None:
                # In-place update keeps the caller's record identity, which
                # matters when the destination is the root itself.
                _ref.value.update(additions)
                return rec
            if _ref.parent is None:
                return final   # a pipe replaced the whole record
            _ref.replace(final)
            return rec

        return dest_ref.value, dest_ref.path_parts, place

    def _expand_move_sources(
        self,
        record: Any,
        op: Any,
        dest_parts: "tuple[str | int, ...]",
        fail: Callable[..., StructuralExecutionError],
    ) -> "list[tuple[str, Any]]":
        """Resolve every ``<<`` source into ``(derived_key, Reference)`` pairs.

        Field sets expand in the **source object's** key order (§6.4), not the
        order the names were written.  A positive set is strict (§8.5); a
        complement ignores names the record does not have (§8.6) and drops the
        destination itself (§8.7).
        """
        from datapipe.dsl.compiler import CompiledFieldSet
        from datapipe.dsl.selector import Reference

        entries: list[tuple[str, Any]] = []

        for source in op.sources:
            if not isinstance(source, CompiledFieldSet):
                try:
                    refs = source.resolve(record)
                except SelectorResolutionError as exc:
                    raise fail(
                        source_path=exc.path or source.render(),
                        reason=f"source path cannot be resolved: {exc}",
                        cause=exc,
                    ) from exc
                if len(refs) != 1:
                    raise fail(
                        source_path=source.render(),
                        reason=(
                            f"source {source.render()} must resolve to exactly "
                            f"one reference, got {len(refs)}"
                        ),
                    )
                entries.append((str(refs[0].path_parts[-1]), refs[0]))
                continue

            try:
                base_refs = source.base.resolve(record)
            except SelectorResolutionError as exc:
                raise fail(
                    source_path=exc.path or source.base.render(),
                    reason=f"field-set base cannot be resolved: {exc}",
                    cause=exc,
                ) from exc
            if len(base_refs) != 1:
                raise fail(
                    source_path=source.base.render(),
                    reason=(
                        f"field-set base {source.base.render()} must resolve to "
                        f"exactly one object, got {len(base_refs)}"
                    ),
                )
            base = base_refs[0]
            if not isinstance(base.value, dict):
                raise fail(
                    source_path=base.path,
                    reason=(
                        f"field set selects from {base.path}, which is a "
                        f"{type(base.value).__name__}, not an object"
                    ),
                )

            named = set(source.names)
            if source.complement:
                # §8.6: names the record does not have are simply not excluded.
                selected = [k for k in base.value if k not in named]
            else:
                missing = [n for n in source.names if n not in base.value]
                if missing:
                    # §8.5: positive sets are strict.
                    raise fail(
                        source_path=base.path,
                        reason=(
                            f"field set requires "
                            f"{', '.join(repr(m) for m in missing)} at "
                            f"{base.path}, which is missing"
                        ),
                    )
                # Source object order, not the order the names were written.
                selected = [k for k in base.value if k in named]

            for name in selected:
                parts = base.path_parts + (name,)
                if source.complement and parts == dest_parts:
                    continue  # §8.7: never move the destination into itself
                entries.append((name, Reference(
                    parent=base.value,
                    key=name,
                    value=base.value[name],
                    path=f"{'' if base.path == '.' else base.path}.{name}",
                    path_parts=parts,
                )))

        return entries

    def _check_move_entries(
        self,
        entries: "list[tuple[str, Any]]",
        target: dict,
        dest_parts: "tuple[str | int, ...]",
        fail: Callable[..., StructuralExecutionError],
    ) -> None:
        """Reject every §8.4/§8.8 problem before a single byte is written."""
        by_key: dict[str, str] = {}

        for key, ref in entries:
            # `<<` desugars to `dest.<key> <- source`, so overlap is judged
            # against that effective destination — the destination object itself
            # is legitimately an ancestor of the source in §6.6's root move.
            detail = overlap_reason(
                dest_parts + (key,), ref.path_parts, is_move=True
            )
            if detail is not None:
                raise fail(
                    source_path=ref.path,
                    destination_path=_join_path(dest_parts + (key,)),
                    reason=f"overlapping source and destination: {detail}",
                )

            previous = by_key.get(key)
            if previous is not None:
                raise fail(
                    source_path=ref.path,
                    destination_path=_join_path(dest_parts),
                    reason=(
                        f"two sources derive the destination key {key!r}: "
                        f"{previous} and {ref.path}"
                    ),
                )
            by_key[key] = ref.path

            if key in target:
                # §8.4: `<<` is collision="error".
                raise fail(
                    source_path=ref.path,
                    destination_path=_join_path(dest_parts + (key,)),
                    reason=(
                        f"destination key {key!r} already exists at "
                        f"{_join_path(dest_parts)}"
                    ),
                )

        # A source nested inside another would have its container deleted out
        # from under it in step 7, so the pair can never both be moved.
        paths = sorted((ref.path_parts, ref.path) for _k, ref in entries)
        for (outer, outer_path), (inner, inner_path) in zip(paths, paths[1:]):
            if inner[: len(outer)] == outer:
                raise fail(
                    source_path=inner_path,
                    destination_path=_join_path(dest_parts),
                    reason=(
                        f"source {outer_path} is an ancestor of source "
                        f"{inner_path}; moving both is not well defined"
                    ),
                )

    def _structural_failure(
        self, op: Any, stmt_index: int, record_seq: int | None
    ) -> Callable[..., StructuralExecutionError]:
        """Return a builder for this statement's ``StructuralExecutionError``."""
        from datapipe.dsl.compiler import CompiledMoveInto

        if isinstance(op, CompiledMoveInto):
            operation = "move-into"
        else:
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
