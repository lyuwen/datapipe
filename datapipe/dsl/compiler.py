"""Semantic compiler: turns a parsed DSL expression into ToolInvocation descriptors.

Compilation is a sequence of explicit passes (§9 of the CLI plan):

  1. Tokenize and parse → AST (done by ``datapipe.dsl.parser.parse``).
  2. Resolve each tool name against the built-in registry.
  3. Validate selector compatibility with tool target scope.
  4. Bind configuration literals to ``ParameterSpec`` definitions.
  5. Apply defaults and produce normalized argument values.
  6. Produce ``ToolInvocation`` descriptors.
  7. Perform statically provable input/output compatibility checks between
     adjacent invocations on identical concrete paths.

The output is a ``CompiledExpression`` containing a list of ``ToolInvocation``
instances.  Built-in tools carry the live callable directly (they live in a
proper importable module and pickle fine under spawn).  Provider tools carry
a ``ToolDescriptor`` instead; ``CompiledToolProgramStage.setup()`` resolves
the callable per-worker so that only pickleable primitives cross the process
boundary.
"""

from __future__ import annotations

import hashlib
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import warnings

from datapipe.dsl import ast as _ast
from datapipe.dsl.errors import (
    Span,
    ToolConfigurationError,
    ToolResolutionError,
)
from datapipe.dsl.parser import parse, parse_program
from datapipe.dsl.selector import CompiledSelector
from datapipe.tools.contract import ParameterSpec, ToolContract
from datapipe.tools.decorator import get_contract
from datapipe.tools.descriptor import ProviderDescriptor, ToolDescriptor
from datapipe.tools.types import TypeSpec, matches as _matches


# ---------------------------------------------------------------------------
# Decoding registry metadata back into live type objects
#
# ``datapipe.tools.validation`` runs providers in a subprocess and stores a
# structured encoding of each contract's TypeSpecs and parameter annotations
# in the registry JSON.  These decoders are the exact inverse.  A registry
# entry written before the structured encoding existed has only the human
# ``describe()`` string, so both decoders accept that as a fallback.
# ---------------------------------------------------------------------------

_BASE_ANNOTATIONS: dict[str, Any] = {
    "str": str, "int": int, "float": float, "bool": bool,
    "list": list, "dict": dict, "None": type(None),
}


class UnsupportedAnnotation:
    """Marker for an annotation the provider subprocess could not encode.

    Carries the reason so ``_validate_argument_type`` can reject the argument
    with a message naming what went wrong, instead of silently accepting any
    value (which is what a plain ``None`` annotation would do).
    """

    __slots__ = ("reason",)

    def __init__(self, reason: str) -> None:
        self.reason = reason

    def __eq__(self, other: object) -> bool:
        return isinstance(other, UnsupportedAnnotation) and self.reason == other.reason

    def __hash__(self) -> int:
        return hash(self.reason)

    def __repr__(self) -> str:
        return f"UnsupportedAnnotation({self.reason!r})"


def decode_type_spec(spec: Any, description: str | None = None) -> TypeSpec:
    """Rebuild a ``TypeSpec`` from its structured registry encoding.

    Falls back to parsing *description* (the ``describe()`` string) when
    *spec* is absent, and finally to ``JsonType.ANY``.
    """
    from datapipe.tools.types import JsonType, OneOf, as_type_spec

    if isinstance(spec, dict):
        kind = spec.get("kind")
        if kind == "json_type":
            try:
                return as_type_spec(JsonType[str(spec.get("name"))])
            except KeyError:
                pass
        elif kind == "one_of":
            members = [decode_type_spec(m) for m in spec.get("members", [])]
            if len(members) >= 2:
                return OneOf(*members)
            if len(members) == 1:
                return members[0]

    return _type_spec_from_description(description)


def _type_spec_from_description(description: str | None) -> TypeSpec:
    """Best-effort parse of a ``describe()`` string, for legacy registry entries."""
    from datapipe.tools.types import JsonType, OneOf, as_type_spec

    if not description:
        return as_type_spec(JsonType.ANY)

    parts = [p.strip() for p in description.split("|")]
    resolved: list[JsonType] = []
    for part in parts:
        for jt in JsonType:
            if jt.value == part or jt.name.lower() == part.lower():
                resolved.append(jt)
                break
        else:
            return as_type_spec(JsonType.ANY)

    if len(resolved) == 1:
        return as_type_spec(resolved[0])
    return OneOf(*resolved)


def decode_annotation(spec: Any, legacy_name: str | None = None) -> Any:
    """Rebuild a parameter annotation from its structured registry encoding.

    Returns a live type (``str``, ``list[int]``, a synthesized enum, ...), an
    :class:`UnsupportedAnnotation` marker, or ``None`` when the parameter was
    genuinely unannotated.
    """
    import typing

    if spec is None:
        return _BASE_ANNOTATIONS.get(legacy_name) if legacy_name else None

    if not isinstance(spec, dict):
        return UnsupportedAnnotation(f"malformed annotation encoding {spec!r}")

    kind = spec.get("kind")

    if kind == "base":
        name = spec.get("name")
        if name in _BASE_ANNOTATIONS:
            return _BASE_ANNOTATIONS[name]
        return UnsupportedAnnotation(f"unknown base type {name!r}")

    if kind == "any":
        return typing.Any

    if kind == "enum":
        values = spec.get("values")
        if not isinstance(values, list) or not values:
            return UnsupportedAnnotation("enum encoding has no member values")
        return EnumValues(str(spec.get("name") or "enum"), tuple(values))

    if kind == "literal":
        values = spec.get("values")
        if not isinstance(values, list) or not values:
            return UnsupportedAnnotation("Literal encoding has no values")
        return EnumValues(spec.get("name") or "Literal", tuple(values))

    if kind == "union":
        members = [decode_annotation(m) for m in spec.get("members", [])]
        if not members:
            return UnsupportedAnnotation("union encoding has no members")
        bad = next((m for m in members if isinstance(m, UnsupportedAnnotation)), None)
        if bad is not None:
            return UnsupportedAnnotation(f"union member unsupported: {bad.reason}")
        return UnionAnnotation(tuple(members))

    if kind == "container":
        origin = _BASE_ANNOTATIONS.get(spec.get("origin"))
        if origin not in (list, dict):
            return UnsupportedAnnotation(
                f"container origin {spec.get('origin')!r} is not JSON-representable"
            )
        args = [decode_annotation(a) for a in spec.get("args", [])]
        bad = next((a for a in args if isinstance(a, UnsupportedAnnotation)), None)
        if bad is not None:
            return UnsupportedAnnotation(f"container element unsupported: {bad.reason}")
        return ContainerAnnotation(origin, tuple(args))

    if kind == "unresolved":
        return UnsupportedAnnotation(
            f"annotation {spec.get('text')!r} could not be resolved to a type"
        )

    if kind == "unsupported":
        return UnsupportedAnnotation(str(spec.get("reason") or "unsupported annotation"))

    return UnsupportedAnnotation(f"unknown annotation kind {kind!r}")


class UnionAnnotation:
    """A decoded ``Union[...]`` / ``X | None`` annotation."""

    __slots__ = ("members",)

    def __init__(self, members: tuple[Any, ...]) -> None:
        self.members = members

    @property
    def __name__(self) -> str:
        return " | ".join(_annotation_name(m) for m in self.members)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, UnionAnnotation) and self.members == other.members

    def __hash__(self) -> int:
        return hash(("union", self.members))

    def __repr__(self) -> str:
        return f"UnionAnnotation({self.__name__})"


class EnumValues:
    """A decoded ``enum.Enum`` subclass or ``Literal``, reduced to allowed values."""

    __slots__ = ("name", "values")

    def __init__(self, name: str, values: tuple[Any, ...]) -> None:
        self.name = name
        self.values = values

    @property
    def __name__(self) -> str:
        return self.name

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, EnumValues)
            and self.name == other.name
            and self.values == other.values
        )

    def __hash__(self) -> int:
        return hash(("enum", self.name, self.values))

    def __repr__(self) -> str:
        return f"EnumValues({self.name!r}, {self.values!r})"


class ContainerAnnotation:
    """A decoded typed container annotation such as ``list[int]``/``dict[str, int]``."""

    __slots__ = ("origin", "args")

    def __init__(self, origin: type, args: tuple[Any, ...]) -> None:
        self.origin = origin
        self.args = args

    @property
    def __name__(self) -> str:
        if not self.args:
            return self.origin.__name__
        inner = ", ".join(_annotation_name(a) for a in self.args)
        return f"{self.origin.__name__}[{inner}]"

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, ContainerAnnotation)
            and self.origin is other.origin
            and self.args == other.args
        )

    def __hash__(self) -> int:
        return hash(("container", self.origin, self.args))

    def __repr__(self) -> str:
        return f"ContainerAnnotation({self.__name__})"


def _annotation_name(annotation: Any) -> str:
    """Render an annotation for an error message."""
    import typing

    if annotation is typing.Any:
        return "Any"
    if annotation is type(None):
        return "None"
    return getattr(annotation, "__name__", None) or repr(annotation)


# ---------------------------------------------------------------------------
# Built-in registry
# ---------------------------------------------------------------------------

# Names that are permanently reserved and cannot be shadowed by providers.
_BUILTIN_NAMES: frozenset[str] = frozenset({"fromjson", "tojson"})


def _build_builtin_registry() -> dict[str, Callable]:
    """Return the canonical mapping of tool name → function for built-ins."""
    from datapipe.tools.builtins.json import fromjson, tojson
    return {
        "fromjson": fromjson,
        "tojson": tojson,
    }


# Module-level singleton; built-ins are immutable so this is safe.
_BUILTIN_REGISTRY: dict[str, Callable] | None = None


def _get_builtin_registry() -> dict[str, Callable]:
    global _BUILTIN_REGISTRY
    if _BUILTIN_REGISTRY is None:
        _BUILTIN_REGISTRY = _build_builtin_registry()
    return _BUILTIN_REGISTRY


def _build_full_registry() -> dict[str, tuple[Callable, ToolDescriptor | None]]:
    """Return a registry mapping tool name → (callable, descriptor_or_None).

    Built-in names are reserved and cannot be shadowed by provider tools.
    The descriptor is None for built-ins (they live in real importable modules
    and pickle fine) and a ToolDescriptor for provider tools (which must be
    resolved per-worker from the descriptor rather than pickled directly).

    Entries are keyed by:
      - Unqualified name (e.g. ``"my_tool"``) — for provider tools whose name
        does not clash with a built-in.
      - Qualified name (e.g. ``"my_provider.my_tool"``) — for all provider tools,
        always available alongside the unqualified form.
    """
    registry: dict[str, tuple[Callable, ToolDescriptor | None]] = {
        name: (fn, None) for name, fn in _get_builtin_registry().items()
    }

    try:
        from datapipe.tools.registry import load_registry as _load_reg, add_provider as _add_provider
        from datapipe.tools.validation import validate_dynamic
    except ImportError:
        return registry

    try:
        reg_data = _load_reg()
    except Exception:
        return registry

    for entry in reg_data.providers.values():
        # For copied providers an empty tool list means there is nothing to
        # load.  Editable providers are re-read from the user's file on every
        # run, so the file -- not the registry snapshot -- is the source of
        # truth and may have gained tools since install.
        if not entry.tools and entry.mode != "editable":
            continue
        try:
            # For editable providers, recompute the current file digest.  The
            # plan (§7.5) requires that on every expression compilation we
            # re-read and hash the file; if the hash changed we re-validate,
            # update the registry, and embed the *current* digest in the
            # descriptor so every worker verifies the same snapshot that was
            # used during compilation.
            sha256 = entry.digest
            if entry.mode == "editable":
                current_bytes = Path(entry.source_path).read_bytes()
                current_digest = "sha256:" + hashlib.sha256(current_bytes).hexdigest()
                if current_digest != entry.digest:
                    metadata = validate_dynamic(Path(entry.source_path), current_bytes)
                    entry.digest = current_digest
                    entry.tools = {t["name"]: t for t in metadata.tools}
                    _add_provider(entry)
                sha256 = current_digest
            else:
                # For copied providers, verify the snapshot on disk still
                # matches the registry digest.  A mismatch means the snapshot
                # was tampered with after installation; warn now so the user
                # gets a clear message rather than a cryptic worker failure.
                try:
                    current_bytes = Path(entry.source_path).read_bytes()
                    current_digest = "sha256:" + hashlib.sha256(current_bytes).hexdigest()
                    if current_digest != entry.digest:
                        print(
                            f"warning: provider {entry.provider_id!r} snapshot "
                            f"digest mismatch (expected {entry.digest!r}, "
                            f"got {current_digest!r}); workers will reject it",
                            file=sys.stderr,
                        )
                except OSError as exc:
                    print(
                        f"warning: provider {entry.provider_id!r} snapshot "
                        f"could not be read: {exc}; workers will reject it",
                        file=sys.stderr,
                    )

            desc = ProviderDescriptor(
                provider_id=entry.provider_id,
                alias=entry.alias,
                mode=entry.mode,
                source_path=entry.source_path,
                sha256=sha256,
                api_version=entry.datapipe_api,
            )
            # Build the tool list from registry JSON metadata rather than
            # executing the provider source in the coordinator.  The compiler
            # only needs tool names and descriptors to produce ToolInvocation
            # objects; the actual callable is resolved per-worker in setup().
            # Calling load_provider() here would exec() provider source in the
            # control-plane process, introducing side effects, hangs, and
            # import failures that should only be surfaced at worker startup.
            tool_names_in_registry = list(entry.tools.keys())
        except Exception as exc:  # noqa: BLE001
            # Never let one broken provider block tools from every other
            # provider, but do not fail silently either: without this warning
            # the user only sees "unknown tool" and has no way to discover
            # that the provider failed to load or why.
            print(
                f"warning: provider {entry.provider_id!r} could not be loaded "
                f"and its tools are unavailable: {exc}",
                file=sys.stderr,
            )
            continue

        # Register a sentinel callable with the ToolContract attached.
        # The callable is never invoked in the coordinator; the real
        # implementation is resolved per-worker from the descriptor.
        # Attaching the contract lets the compiler's _resolve_tool() call
        # get_contract(stub) just as it does for real @tool callables.
        def _make_stub_with_contract(
            tool_name_: str,
            tool_meta_: dict,
        ) -> Callable:
            from datapipe.tools.contract import (
                Cardinality as _Cardinality,
                ParameterSpec as _ParameterSpec,
                ToolContract as _ToolContract,
            )

            # Build parameters from registry metadata, restoring annotation
            # from the structured ``annotation_spec`` when present (which
            # round-trips Optional/Union/enum/container annotations) and
            # falling back to the legacy type-name string for registry
            # entries written before that field existed.
            params = []
            for p in tool_meta_.get("parameters", []):
                annotation = decode_annotation(
                    p.get("annotation_spec"), p.get("annotation")
                )
                params.append(_ParameterSpec(
                    name=p["name"],
                    default=p.get("default"),
                    required=p.get("required", False),
                    annotation=annotation,
                ))

            try:
                cardinality = _Cardinality(
                    tool_meta_.get("cardinality", "one_to_one")
                )
            except ValueError:
                cardinality = _Cardinality.ONE_TO_ONE

            contract = _ToolContract(
                name=tool_name_,
                api_version=1,
                target=tool_meta_.get("target", "value"),
                input_type=decode_type_spec(
                    tool_meta_.get("input_spec"), tool_meta_.get("input")
                ),
                output_type=decode_type_spec(
                    tool_meta_.get("output_spec"), tool_meta_.get("output")
                ),
                cardinality=cardinality,
                deterministic=bool(tool_meta_.get("deterministic", True)),
                description=tool_meta_.get("description", ""),
                parameters=tuple(params),
            )

            def _stub(*args, **kwargs):  # pragma: no cover
                raise RuntimeError(
                    f"provider tool stub for {tool_name_!r} called in coordinator; "
                    "this is a bug — provider tools must only be called in workers"
                )
            _stub.__name__ = tool_name_
            _stub.__tool_contract__ = contract  # type: ignore[attr-defined]
            return _stub

        for tool_name in tool_names_in_registry:
            tool_meta = entry.tools.get(tool_name, {})
            tool_desc = ToolDescriptor(provider=desc, tool_name=tool_name)
            stub_fn = _make_stub_with_contract(tool_name, tool_meta)
            # Always register the qualified form: alias.tool_name.
            qualified = f"{entry.alias}.{tool_name}"
            registry[qualified] = (stub_fn, tool_desc)
            # Register unqualified only when the name is not a reserved built-in
            # and is unambiguous.  When two providers export the same tool name,
            # the plan requires neither to win — both must be accessed by their
            # qualified alias.tool_name form.  This prevents silent wrong-tool
            # dispatch under first-wins behavior.
            if tool_name not in _BUILTIN_NAMES:
                if tool_name in registry:
                    # A collision: both providers define the same unqualified name.
                    # Remove the existing entry so neither wins unqualified.
                    existing_desc = registry[tool_name][1]
                    existing_pid = (
                        existing_desc.provider.provider_id
                        if existing_desc is not None
                        else "builtin"
                    )
                    del registry[tool_name]
                    print(
                        f"warning: tool name {tool_name!r} is provided by both "
                        f"{existing_pid!r} and {entry.provider_id!r}; "
                        f"unqualified {tool_name!r} is not available — use the "
                        f"qualified form e.g. {entry.alias}.{tool_name!r}",
                        file=sys.stderr,
                    )
                else:
                    registry[tool_name] = (stub_fn, tool_desc)

    return registry


# ---------------------------------------------------------------------------
# Output types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ToolInvocation:
    """A fully resolved, bound tool invocation ready for per-record execution.

    Attributes
    ----------
    tool_descriptor:
        For installed provider tools: the ``ToolDescriptor`` used to resolve
        the callable per-worker in ``setup()``.  ``None`` for built-in tools,
        which live in real importable modules and can be pickled directly.
    builtin_fn:
        For built-in tools only: the live callable.  ``None`` for provider
        tools (they cannot be pickled across spawn boundaries because their
        ``__module__`` is a synthetic name that does not exist in worker
        processes).
    tool_name:
        Canonical tool name (for diagnostics and logging).
    contract:
        The tool's ``ToolContract``.
    selector:
        Compiled selector ready for ``resolve()`` / ``apply()``.
    arguments:
        Fully bound configuration dict (defaults filled in).
    expression_index:
        Zero-based position of this invocation in the pipeline expression.
    expression_span:
        ``(start, end)`` character offsets of this invocation within the source
        expression, for diagnostics.  Stored as a plain tuple rather than a
        ``Span`` so it stays trivially pickleable.  Optional: ``None`` when the
        invocation was constructed without span information.
    """
    tool_descriptor: ToolDescriptor | None   # None for built-ins
    builtin_fn: Callable | None              # None for provider tools
    tool_name: str
    contract: ToolContract
    selector: CompiledSelector
    arguments: dict[str, Any]
    expression_index: int
    expression_span: tuple[int, int] | None = None


@dataclass(frozen=True)
class CompiledExpression:
    """The result of compiling a DSL expression string.

    Contains one ``ToolInvocation`` per ``|``-separated operation.
    """
    invocations: tuple[ToolInvocation, ...]
    source: str  # original expression string, for diagnostics


# ---------------------------------------------------------------------------
# Multi-statement program IR (Phase S2)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CompiledBareCall:
    """A resolved bare tool call for use in focused pipe execution."""
    expression_index: int
    callable: Callable | None        # None for provider tools
    descriptor: ToolDescriptor | None  # set for provider tools
    bound_args: dict[str, Any]
    span: tuple[int, int]


@dataclass(frozen=True)
class CompiledAssignment:
    """A compiled ``=`` / ``<-`` statement operation.

    ``dest_parent`` and ``dest_key`` are precomputed for the common case where
    the destination ends in a field: the parent must already exist, but the
    final key may be absent (that is what assignment creates).  They are
    ``None`` when the final part is an index or wildcard, in which case the
    whole destination selector is resolved instead.
    """
    destination: CompiledSelector
    source: CompiledSelector
    transform: "ToolInvocation | CompiledBareCall | None"
    is_move: bool
    span: tuple[int, int]
    dest_parent: CompiledSelector | None = None
    dest_key: str | None = None


@dataclass(frozen=True)
class CompiledFieldSet:
    """A compiled ``.base.(a|b|c)`` / ``.base.(^a|b|c)`` move-into source.

    Expansion happens per record: the named fields (or, for a complement, every
    other field) are read from the object at ``base`` **in that object's own key
    order**, which is what fixes the destination key order (§6.4).
    """
    base: CompiledSelector
    names: tuple[str, ...]
    complement: bool
    span: tuple[int, int]


@dataclass(frozen=True)
class CompiledMoveInto:
    """A compiled ``.dest << src, ...`` statement operation.

    ``dest_parent`` / ``dest_key`` are precomputed exactly as for
    ``CompiledAssignment``: ``<<`` creates a missing final field as ``{}`` when
    the parent exists (§8.2), so only the parent must already resolve.  They are
    ``None`` for a root, index, or wildcard destination, which must exist.
    """
    destination: CompiledSelector
    sources: tuple["CompiledSelector | CompiledFieldSet", ...]
    span: tuple[int, int]
    dest_parent: CompiledSelector | None = None
    dest_key: str | None = None

    @property
    def source(self) -> CompiledSelector:
        """The first source, for diagnostics that report a single selector."""
        first = self.sources[0]
        return first.base if isinstance(first, CompiledFieldSet) else first


@dataclass(frozen=True)
class CompiledStatement:
    """One compiled statement: a base operation plus optional focused pipes."""
    operation: "ToolInvocation | CompiledBareCall | CompiledAssignment | CompiledMoveInto"
    pipes: tuple[CompiledBareCall, ...]
    focus_selector: CompiledSelector | None  # None for invocation-first
    span: tuple[int, int]


@dataclass(frozen=True)
class CompiledProgram:
    """Compiled representation of a multi-statement program."""
    statements: tuple[CompiledStatement, ...]
    source: str


# ---------------------------------------------------------------------------
# Compiler entry point
# ---------------------------------------------------------------------------


def compile_expression(expression: str) -> CompiledExpression:
    """Parse and semantically compile *expression*.

    Returns a :class:`CompiledExpression` ready for execution.

    Raises :class:`~datapipe.dsl.errors.ExpressionSyntaxError`,
    :class:`~datapipe.dsl.errors.ToolResolutionError`, or
    :class:`~datapipe.dsl.errors.ToolConfigurationError` on any problem.
    Everything is validated before any data is read.
    """
    ast = parse(expression)
    registry = _build_full_registry()
    invocations: list[ToolInvocation] = []

    for i, inv_node in enumerate(ast.invocations):
        tool_fn, tool_desc = _resolve_tool(inv_node.qualified_name, registry, expression)
        contract = get_contract(tool_fn)
        if contract is None:
            raise ToolResolutionError(
                f"tool {inv_node.qualified_name.display!r} has no contract; "
                "only @tool-decorated functions are supported",
                expression=expression,
                span=inv_node.qualified_name.span,
            )

        selector = _compile_selector(inv_node.selector, contract, expression)
        arguments = _bind_arguments(
            inv_node.arguments, contract, expression, inv_node.span
        )

        # Built-in callables live in a real importable module and pickle fine.
        # Provider callables have a synthetic module name that does not exist
        # in spawned worker processes, so they must never be pickled.  Workers
        # resolve them from the ToolDescriptor in setup() instead.
        invocations.append(ToolInvocation(
            tool_descriptor=tool_desc,
            builtin_fn=tool_fn if tool_desc is None else None,
            tool_name=contract.name,
            contract=contract,
            selector=selector,
            arguments=arguments,
            expression_index=i,
            expression_span=(inv_node.span.start, inv_node.span.end),
        ))

    # Legacy | migration diagnostic: warn when multiple invocations all use
    # explicit (non-root, non-wildcard) selectors.  Single-invocation
    # expressions and any expression with a root or wildcard selector are
    # silent so we do not warn on legitimate uses.
    if len(invocations) > 1 and all(
        not inv.selector.has_wildcard and not inv.selector.is_root
        for inv in invocations
    ):
        suggested = "; ".join(
            f"{inv.tool_name}({inv.selector.render()})"
            for inv in invocations
        )
        warnings.warn(
            f"`|` between explicit record mutations is deprecated; use semicolons:\n"
            f"  {suggested}",
            DeprecationWarning,
            stacklevel=2,
        )

    _check_static_compatibility(invocations, expression)

    return CompiledExpression(
        invocations=tuple(invocations),
        source=expression,
    )


# ---------------------------------------------------------------------------
# Multi-statement program compiler
# ---------------------------------------------------------------------------


def compile_program(expression: str) -> CompiledProgram:
    """Compile a multi-statement program expression.

    Calls ``parse_program`` then compiles each statement into a
    ``CompiledStatement``.  Invocation-first statements have
    ``focus_selector=None``; selector-first focused statements carry the
    compiled focus selector and use it as the base operation's selector.
    Assignment statements compile to a ``CompiledAssignment`` whose focus is
    the destination.

    ``expression_index`` values are unique across every operation, assignment
    transform, and bare call in the whole program so the executing stage can
    key resolved callables by index.
    """
    program = parse_program(expression)
    registry = _build_full_registry()
    statements: list[CompiledStatement] = []

    # Monotonic counter shared by base operations, assignment transforms, and
    # bare pipe calls so every resolved callable has a distinct key in the
    # stage's _resolved_fns dict.
    index = 0

    for stmt in program.statements:
        op_node = stmt.operation

        if isinstance(op_node, _ast.Assignment):
            operation, focus, index = _compile_assignment(
                op_node, registry, expression, index
            )
        elif isinstance(op_node, _ast.MoveInto):
            operation, focus = _compile_move_into(op_node, expression)
        else:
            operation, focus, index = _compile_tool_operation(
                stmt, op_node, registry, expression, index
            )

        # Compile each bare pipe call against the same focus.
        pipes: list[CompiledBareCall] = []
        for bare_node in stmt.pipes:
            bare_fn, bare_desc = _resolve_tool(
                bare_node.qualified_name, registry, expression
            )
            bare_contract = get_contract(bare_fn)
            if bare_contract is None:
                raise ToolResolutionError(
                    f"tool {bare_node.qualified_name.display!r} has no contract; "
                    "only @tool-decorated functions are supported",
                    expression=expression,
                    span=bare_node.qualified_name.span,
                )
            bare_args = _bind_arguments(
                bare_node.arguments, bare_contract, expression, bare_node.span
            )
            pipes.append(CompiledBareCall(
                expression_index=index,
                callable=bare_fn if bare_desc is None else None,
                descriptor=bare_desc,
                bound_args=bare_args,
                span=(bare_node.span.start, bare_node.span.end),
            ))
            index += 1

        statements.append(CompiledStatement(
            operation=operation,
            pipes=tuple(pipes),
            focus_selector=focus,
            span=(stmt.span.start, stmt.span.end),
        ))

    # _check_static_compatibility is not called here: statements operate on
    # independent selectors with no output-to-input type flow between them.
    return CompiledProgram(
        statements=tuple(statements),
        source=expression,
    )


def _compile_tool_operation(
    stmt: "_ast.Statement",
    op_node: "_ast.Invocation | _ast.BareToolCall",
    registry: dict[str, tuple[Callable, ToolDescriptor | None]],
    expression: str,
    index: int,
) -> tuple[ToolInvocation, CompiledSelector | None, int]:
    """Compile an invocation-first or focused statement's base operation."""
    tool_fn, tool_desc = _resolve_tool(op_node.qualified_name, registry, expression)
    contract = get_contract(tool_fn)
    if contract is None:
        raise ToolResolutionError(
            f"tool {op_node.qualified_name.display!r} has no contract; "
            "only @tool-decorated functions are supported",
            expression=expression,
            span=op_node.qualified_name.span,
        )

    # Invocation-first statements carry their own selector; focused
    # statements take the leading focus selector as the operation target.
    if stmt.focus_selector is None:
        selector_node = op_node.selector
    else:
        selector_node = stmt.focus_selector

    selector = _compile_selector(selector_node, contract, expression)
    arguments = _bind_arguments(
        op_node.arguments, contract, expression, op_node.span
    )

    operation = ToolInvocation(
        tool_descriptor=tool_desc,
        builtin_fn=tool_fn if tool_desc is None else None,
        tool_name=contract.name,
        contract=contract,
        selector=selector,
        arguments=arguments,
        expression_index=index,
        expression_span=(op_node.span.start, op_node.span.end),
    )
    focus = selector if stmt.focus_selector is not None else None
    return operation, focus, index + 1


def _compile_assignment(
    node: "_ast.Assignment",
    registry: dict[str, tuple[Callable, ToolDescriptor | None]],
    expression: str,
    index: int,
) -> tuple[CompiledAssignment, CompiledSelector, int]:
    """Compile a ``=`` / ``<-`` statement, applying the §12 static rejections."""
    op_label = "move" if node.is_move else "copy"

    # A move must have a statically identifiable primary source to delete.
    # The root selector names no removable field, so it is only ever a
    # computation — that is a copy, not a move.
    if node.is_move and node.rhs.source.is_root:
        raise ToolConfigurationError(
            f"move (`<-`) requires a statically identifiable source path, but "
            f"the right-hand side has no source to remove; use `=` to assign "
            f"the computed value without deleting anything",
            expression=expression,
            span=node.rhs.span,
        )

    _reject_static_overlap(node, expression, op_label)

    transform: ToolInvocation | None = None
    if node.rhs.transform is not None:
        inv_node = node.rhs.transform
        tool_fn, tool_desc = _resolve_tool(
            inv_node.qualified_name, registry, expression
        )
        contract = get_contract(tool_fn)
        if contract is None:
            raise ToolResolutionError(
                f"tool {inv_node.qualified_name.display!r} has no contract; "
                "only @tool-decorated functions are supported",
                expression=expression,
                span=inv_node.qualified_name.span,
            )
        transform = ToolInvocation(
            tool_descriptor=tool_desc,
            builtin_fn=tool_fn if tool_desc is None else None,
            tool_name=contract.name,
            contract=contract,
            selector=_compile_selector(node.rhs.source, contract, expression),
            arguments=_bind_arguments(
                inv_node.arguments, contract, expression, inv_node.span
            ),
            expression_index=index,
            expression_span=(inv_node.span.start, inv_node.span.end),
        )
        index += 1

    dest_parent, dest_key = _split_destination(node.destination)

    operation = CompiledAssignment(
        destination=CompiledSelector(node.destination),
        source=CompiledSelector(node.rhs.source),
        transform=transform,
        is_move=node.is_move,
        span=(node.span.start, node.span.end),
        dest_parent=dest_parent,
        dest_key=dest_key,
    )
    return operation, operation.destination, index


def _compile_move_into(
    node: "_ast.MoveInto", expression: str
) -> tuple[CompiledMoveInto, CompiledSelector]:
    """Compile a ``<<`` statement, applying the §12 static rejections.

    Rejected here, before any record is read:

    - a source whose key cannot be inferred — an index, a wildcard, or the root
      (§8.9): ``<<`` derives destination keys from object fields only;
    - duplicate exact names inside one field set;
    - two sources that provably derive the same destination key (§8.8);
    - a statically provable source/destination overlap (§8.8).

    A ``FieldSet`` source contributes keys that are only known per record when
    it is a complement, so its duplicate and overlap checks are completed at
    runtime; the positive form is fully static and checked here.
    """
    dest_path = _static_path(node.destination)

    # Derived keys that are knowable at compile time, mapped to the source text
    # that produced them, so a collision names both sides.
    derived: dict[str, str] = {}

    for source in node.sources:
        if isinstance(source, _ast.FieldSet):
            _check_field_set(source, node, expression)
            base_path = _static_path(source.base)
            if source.complement:
                _check_complement_base(dest_path, base_path, source, node, expression)
                continue
            # A positive set is fully static: each name is an ordinary source
            # rooted at the base, so it gets the ordinary key and overlap checks.
            for name in source.names:
                rendered = f"{_render_field_set(source)} → {name}"
                _record_derived_key(derived, name, rendered, node, expression)
                _check_move_overlap(
                    dest_path,
                    None if base_path is None else base_path + (name,),
                    rendered, node, expression,
                )
            continue

        key = _inferred_key(source)
        if key is None:
            raise ToolConfigurationError(
                f"cannot infer a destination key for source "
                f"{_render_selector(source)!r} in move-into "
                f"{_render_selector(node.destination)} << ...: `<<` derives the "
                f"key from a final object field, so an array index, a wildcard, "
                f"or the root needs an explicit destination "
                f"(`{_render_selector(node.destination)}.<key> <- "
                f"{_render_selector(source)}`)",
                expression=expression,
                span=source.span,
            )
        _record_derived_key(
            derived, key, _render_selector(source), node, expression
        )
        _check_move_overlap(
            dest_path, _static_path(source), _render_selector(source),
            node, expression,
        )

    dest_parent, dest_key = _split_destination(node.destination)
    operation = CompiledMoveInto(
        destination=CompiledSelector(node.destination),
        sources=tuple(_compile_move_source(s) for s in node.sources),
        span=(node.span.start, node.span.end),
        dest_parent=dest_parent,
        dest_key=dest_key,
    )
    return operation, operation.destination


def _compile_move_source(
    source: "_ast.Selector | _ast.FieldSet",
) -> "CompiledSelector | CompiledFieldSet":
    """Compile one ``<<`` source into its runtime form."""
    if isinstance(source, _ast.FieldSet):
        return CompiledFieldSet(
            base=CompiledSelector(source.base),
            names=source.names,
            complement=source.complement,
            span=(source.span.start, source.span.end),
        )
    return CompiledSelector(source)


def _inferred_key(source: "_ast.Selector") -> str | None:
    """Return the destination key ``<<`` derives from *source*, or None (§8.9).

    Only a final object field yields a key.  An index, a wildcard, or the root
    yields ``None``, which the caller turns into a compile error.  A wildcard
    anywhere in the path also yields ``None``: it matches many values that would
    all claim the one derived key.  An index *before* the final field is fine —
    ``.bag << .items[0].a`` names exactly one value and derives ``a``.
    """
    if not source.parts:
        return None
    if source.has_wildcard:
        return None
    last = source.parts[-1]
    if isinstance(last, _ast.Field):
        return last.name
    if isinstance(last, _ast.QuotedKey):
        return last.key
    return None


def _record_derived_key(
    derived: dict[str, str],
    key: str,
    rendered: str,
    node: "_ast.MoveInto",
    expression: str,
) -> None:
    """Register *key* as derived by *rendered*, rejecting a second claim (§8.8)."""
    previous = derived.get(key)
    if previous is not None:
        raise ToolConfigurationError(
            f"two sources derive the same destination key {key!r} in move-into "
            f"{_render_selector(node.destination)} << ...: {previous} and "
            f"{rendered}; `<<` uses collision=\"error\", so give one an explicit "
            f"destination with `<-`",
            expression=expression,
            span=node.span,
        )
    derived[key] = rendered


def _check_field_set(
    source: "_ast.FieldSet", node: "_ast.MoveInto", expression: str
) -> None:
    """Reject duplicate names inside one field set, and a wildcard base (§12)."""
    seen: set[str] = set()
    for name in source.names:
        if name in seen:
            raise ToolConfigurationError(
                f"duplicate field name {name!r} in field set "
                f"{_render_field_set(source)}",
                expression=expression,
                span=source.span,
            )
        seen.add(name)

    if source.base.has_wildcard or _static_path(source.base) is None:
        raise ToolConfigurationError(
            f"field set {_render_field_set(source)} selects from a wildcard "
            f"base; a field set must name one object",
            expression=expression,
            span=source.span,
        )


def _check_move_overlap(
    dest_path: "tuple[str | int, ...] | None",
    src_path: "tuple[str | int, ...] | None",
    rendered: str,
    node: "_ast.MoveInto",
    expression: str,
) -> None:
    """Reject a statically provable source/destination overlap for ``<<`` (§8.8).

    ``<<`` desugars to ``dest.<key> <- source``, so the overlap that matters is
    between the source and that *effective* destination — not between the source
    and the destination object.  Checking the object itself would reject §6.6's
    ``. << .metadata.x``, where the destination is legitimately an ancestor of
    the source.
    """
    if dest_path is None or src_path is None:
        return

    effective = dest_path + (src_path[-1],)
    detail = overlap_reason(effective, src_path, is_move=True)
    if detail is None:
        return

    raise ToolConfigurationError(
        f"overlapping source and destination in move-into "
        f"{_render_selector(node.destination)} << {rendered}: {detail}",
        expression=expression,
        span=node.span,
    )


def _check_complement_base(
    dest_path: "tuple[str | int, ...] | None",
    base_path: "tuple[str | int, ...] | None",
    source: "_ast.FieldSet",
    node: "_ast.MoveInto",
    expression: str,
) -> None:
    """Reject a complement whose base sits at or inside the destination (§8.8).

    A complement's members are only known per record, so only the base can be
    checked statically.  The base being at or under the destination would move
    the destination's own subtree into itself.  The reverse — the destination
    inside the base, as in ``.metadata << .(^instance_id)`` — is the intended
    blanket move, kept safe by the §8.7 self-exclusion at runtime.
    """
    if dest_path is None or base_path is None:
        return
    if base_path[: len(dest_path)] != dest_path:
        return
    raise ToolConfigurationError(
        f"overlapping source and destination in move-into "
        f"{_render_selector(node.destination)} << {_render_field_set(source)}: "
        f"the destination is an ancestor of the source",
        expression=expression,
        span=node.span,
    )


def _render_field_set(source: "_ast.FieldSet") -> str:
    """Render a field set back to its source syntax for diagnostics."""
    base = _render_selector(source.base)
    prefix = "" if base == "." else base
    caret = "^" if source.complement else ""
    return f"{prefix}.({caret}{'|'.join(source.names)})"


def _split_destination(
    destination: "_ast.Selector",
) -> tuple[CompiledSelector | None, str | None]:
    """Split ``.a.b`` into the parent selector ``.a`` and the final key ``"b"``.

    Assignment creates the final key when it is absent, so only the parent has
    to already exist.  Returns ``(None, None)`` when the final part is not an
    object key (root, index, or wildcard) — those destinations must resolve as
    a whole because there is no key to create.
    """
    parts = destination.parts
    if not parts:
        return None, None
    last = parts[-1]
    if isinstance(last, _ast.Field):
        key = last.name
    elif isinstance(last, _ast.QuotedKey):
        key = last.key
    else:
        return None, None
    parent = _ast.Selector(parts=parts[:-1], span=destination.span)
    return CompiledSelector(parent), key


def _static_path(selector: "_ast.Selector") -> tuple[str | int, ...] | None:
    """Return a comparable canonical path for *selector*, or None if dynamic.

    ``.a`` and ``.["a"]`` name the same key and canonicalize identically.  A
    wildcard makes the path unknowable at compile time, so those are deferred
    to the runtime overlap check.  Keys canonicalize to ``str`` and indices to
    ``int``, matching ``Reference.path_parts`` so the two checks compare the
    same shape.
    """
    canonical: list[str | int] = []
    for part in selector.parts:
        if isinstance(part, _ast.Field):
            canonical.append(part.name)
        elif isinstance(part, _ast.QuotedKey):
            canonical.append(part.key)
        elif isinstance(part, _ast.Index):
            canonical.append(part.index)
        else:
            return None  # Each / unknown — not statically comparable
    return tuple(canonical)


def overlap_reason(
    dest: tuple[str | int, ...],
    source: tuple[str | int, ...],
    *,
    is_move: bool,
    has_transform: bool = False,
) -> str | None:
    """Return why *dest* and *source* overlap illegally, or None if they are fine.

    A move is rejected for any overlap (§8.8): whichever direction it runs, the
    write lands in the subtree the source is about to be removed from.

    A copy is rejected where the overlap is *provably* self-destructive:

    - the same path, or the destination above the source — the write drops the
      source subtree, and ``=`` must never delete;
    - the source above the destination with no transform — the assigned value
      is an alias of the container it is written into, so ``.a.b = .a`` builds
      a self-referential record that no serializer can emit.

    A transform makes the last case unprovable (its result is normally a fresh
    value), so it compiles; the runtime identity guard in the stage catches a
    transform that happens to return its input unchanged.

    A move whose source is an array element additionally conflicts with any
    destination inside that same array: removing the element renumbers every
    later one, so the destination resolved before the delete no longer names
    the slot it was resolved against.  A move out of an array into a *different*
    container (§8.9, ``.metadata.first <- .values[0]``) is unaffected and stays
    legal.
    """
    if dest == source:
        return "the source and destination are the same path"

    if source[: len(dest)] == dest:
        return "the destination is an ancestor of the source"

    if dest[: len(source)] == source:
        if is_move:
            return "the source is an ancestor of the destination"
        if not has_transform:
            return (
                "the source is an ancestor of the destination, so the assigned "
                "value would contain itself"
            )

    if is_move and _same_array_indices(dest, source):
        return (
            "the source and destination are elements of the same array, and "
            "removing the source would renumber the destination"
        )

    return None


def _same_array_indices(
    dest: tuple[str | int, ...], source: tuple[str | int, ...]
) -> bool:
    """True when *source* is an array element and *dest* lives in that array.

    ``source`` ending in an index means the move deletes by position, which
    shifts every later element of the enclosing array.  Any destination that
    reaches into the same array through its own index is therefore unstable.
    """
    if not source or not isinstance(source[-1], int):
        return False
    container = source[:-1]
    if len(dest) <= len(container) or dest[: len(container)] != container:
        return False
    return isinstance(dest[len(container)], int)


def _reject_static_overlap(
    node: "_ast.Assignment", expression: str, op_label: str
) -> None:
    """Reject a provably self-destructive source/destination pair (§8.8).

    Only paths that are fully static are compared; a wildcard defers the same
    check to runtime, where the concrete paths are known.
    """
    dest = _static_path(node.destination)
    src = _static_path(node.rhs.source)
    if dest is None or src is None:
        return

    detail = overlap_reason(
        dest,
        src,
        is_move=node.is_move,
        has_transform=node.rhs.transform is not None,
    )
    if detail is None:
        return

    raise ToolConfigurationError(
        f"overlapping source and destination in {op_label} "
        f"{_render_selector(node.destination)} "
        f"{'<-' if node.is_move else '='} {_render_selector(node.rhs.source)}: "
        f"{detail}",
        expression=expression,
        span=node.span,
    )


def _render_selector(selector: "_ast.Selector") -> str:
    """Render an AST selector for a diagnostic (mirrors CompiledSelector.render)."""
    return CompiledSelector(selector).render()


# ---------------------------------------------------------------------------
# Pass 7: statically provable input/output compatibility
# ---------------------------------------------------------------------------

# Concrete values used to probe whether an output TypeSpec and the next
# input TypeSpec can ever agree.  A pair is only reported as incompatible
# when no probe satisfies both, which keeps the pass conservative: an
# unfamiliar TypeSpec subclass simply fails to prove anything and stays silent.
_PROBES: tuple[Any, ...] = (
    None, True, False, 0, 1, -1, 1.5, "", "x", [], [1], {}, {"k": 1},
)


def _check_static_compatibility(
    invocations: list[ToolInvocation],
    expression: str,
) -> None:
    """Reject consecutive invocations that provably cannot agree on a type.

    Static propagation is necessarily conservative — JSONL has no schema, so
    the only sound inference is between two adjacent invocations writing and
    reading the exact same concrete path.  A pair is flagged only when no
    JSON value at all satisfies both the producer's declared output and the
    consumer's declared input.  Wildcards, differing paths, and anything
    involving ``ANY`` are left to runtime validation.
    """
    from datapipe.tools.types import JsonType, as_type_spec

    any_spec = as_type_spec(JsonType.ANY)

    for producer, consumer in zip(invocations, invocations[1:]):
        if producer.selector.has_wildcard or consumer.selector.has_wildcard:
            continue
        if producer.selector.render() != consumer.selector.render():
            continue
        # A record-target tool rewrites the whole row, so its declared output
        # says nothing about what sits at the consumer's path.
        if producer.contract.target != consumer.contract.target:
            continue

        out_spec = producer.contract.output_type
        in_spec = consumer.contract.input_type
        if out_spec == any_spec or in_spec == any_spec:
            continue

        if any(
            _matches(v, out_spec) and _matches(v, in_spec) for v in _PROBES
        ):
            continue
        # No probe matched the producer's output at all: we cannot prove the
        # output is inhabited, so we cannot prove a contradiction either.
        if not any(_matches(v, out_spec) for v in _PROBES):
            continue

        from datapipe.tools.types import describe as _describe
        raise ToolConfigurationError(
            f"tool {producer.tool_name!r} outputs "
            f"{_describe(out_spec)} at {producer.selector.render()}, but "
            f"{consumer.tool_name!r} accepts only {_describe(in_spec)} there; "
            "no value can satisfy both",
            expression=expression,
            span=Span(
                consumer.expression_span[0],
                consumer.expression_span[1],
            ) if consumer.expression_span else None,
        )


# ---------------------------------------------------------------------------
# Resolution helpers
# ---------------------------------------------------------------------------


def _resolve_tool(
    qname: "_ast.QualifiedName",
    registry: dict[str, tuple[Callable, ToolDescriptor | None]],
    expression: str,
) -> tuple[Callable, ToolDescriptor | None]:
    """Look up *qname* in *registry* and return (callable, descriptor_or_None)."""
    if qname.namespace is not None:
        full = f"{qname.namespace}.{qname.name}"
        if full in registry:
            return registry[full]
        raise ToolResolutionError(
            f"namespaced tool {full!r} cannot be resolved; "
            "install the provider with 'datapipe tools install' "
            "(namespaced tools were not available in Phase 2)",
            expression=expression,
            span=qname.span,
        )

    name = qname.name
    if name not in registry:
        available = sorted(registry)
        raise ToolResolutionError(
            f"unknown tool {name!r}; available tools: "
            + ", ".join(repr(n) for n in available),
            expression=expression,
            span=qname.span,
        )
    return registry[name]


def _compile_selector(
    selector_node: "_ast.Selector",
    contract: ToolContract,
    expression: str,
) -> CompiledSelector:
    """Validate selector against the contract's target scope and compile it."""
    if contract.target == "record" and not selector_node.is_root:
        raise ToolConfigurationError(
            f"tool {contract.name!r} has target='record' and requires the "
            "root selector '.', but a field selector was supplied",
            expression=expression,
            span=selector_node.span,
        )
    return CompiledSelector(selector_node)


def _bind_arguments(
    arg_nodes: "tuple[_ast.Argument, ...]",
    contract: ToolContract,
    expression: str,
    _invocation_span: Span,
) -> dict[str, Any]:
    """Bind expression arguments to the contract's ParameterSpec list.

    Returns a complete configuration dict with defaults filled in.
    Validates each supplied value against the parameter's Python annotation
    so type errors (e.g. passing a string where a bool is expected) are caught
    at compile time rather than silently accepted.
    """
    param_map: dict[str, ParameterSpec] = {p.name: p for p in contract.parameters}
    bound: dict[str, Any] = {}

    # Check for unknown or duplicate arguments.
    seen: set[str] = set()
    for arg in arg_nodes:
        if arg.name in seen:
            raise ToolConfigurationError(
                f"duplicate argument {arg.name!r} for {contract.name!r}",
                expression=expression,
                span=arg.span,
            )
        seen.add(arg.name)
        if arg.name not in param_map:
            available = sorted(param_map)
            hint = (
                f"; available: {', '.join(repr(n) for n in available)}"
                if available
                else " (tool takes no configuration arguments)"
            )
            raise ToolConfigurationError(
                f"unknown argument {arg.name!r} for {contract.name!r}{hint}",
                expression=expression,
                span=arg.span,
            )
        value = arg.value.value
        param = param_map[arg.name]
        _validate_argument_type(value, param, contract.name, expression, arg.span)
        bound[arg.name] = value

    # Fill in defaults for unspecified parameters and validate them.
    for param in contract.parameters:
        if param.name not in bound:
            default = param.default
            bound[param.name] = default
            if param.annotation is None:
                continue
            # An annotation that cannot be validated is a broken contract
            # regardless of the value, so check it even when the default is
            # None; that error is already self-explanatory and passes through.
            if isinstance(param.annotation, (UnsupportedAnnotation, str)):
                _validate_argument_type(
                    default, param, contract.name, expression, _invocation_span
                )
                continue
            # Check default against its annotation so that a badly declared
            # default (e.g. annotation=bool but default="yes") is caught at
            # compile time rather than silently passing through to the worker.
            if default is not None:
                try:
                    _validate_argument_type(default, param, contract.name, expression, _invocation_span)
                except ToolConfigurationError:
                    # Re-raise with a clearer message about it being a default.
                    raise ToolConfigurationError(
                        f"default value {default!r} for argument {param.name!r} of "
                        f"{contract.name!r} does not match its annotation "
                        f"{_annotation_name(param.annotation)!r}",
                        expression=expression,
                        span=_invocation_span,
                    )

    return bound


# Mapping from Python annotation types to the set of Python types that are
# acceptable values for that annotation.  Only the types the @tool decorator
# permits as annotations are listed here.
_ANNOTATION_TYPES: dict[type, tuple[type, ...]] = {
    str:        (str,),
    int:        (int,),
    float:      (float,),   # bool and int literals are not valid float args
    bool:       (bool,),
    list:       (list,),
    dict:       (dict,),
    type(None): (type(None),),
}


def _matches_annotation(value: Any, annotation: Any) -> bool:
    """Return True when *value* satisfies *annotation*.

    Bool is deliberately not accepted where ``int``/``float`` is declared:
    ``bool`` is a subclass of both in Python, but a boolean literal in an
    expression is never what a numeric parameter meant.
    """
    import typing

    if annotation is typing.Any:
        return True

    if isinstance(annotation, UnionAnnotation):
        return any(_matches_annotation(value, m) for m in annotation.members)

    if isinstance(annotation, EnumValues):
        return any(v == value and type(v) is type(value) for v in annotation.values)

    if isinstance(annotation, ContainerAnnotation):
        if not isinstance(value, annotation.origin):
            return False
        if annotation.origin is list and len(annotation.args) == 1:
            return all(_matches_annotation(item, annotation.args[0]) for item in value)
        if annotation.origin is dict and len(annotation.args) == 2:
            key_ann, val_ann = annotation.args
            return all(
                _matches_annotation(k, key_ann) and _matches_annotation(v, val_ann)
                for k, v in value.items()
            )
        return True

    expected = _ANNOTATION_TYPES.get(annotation)
    if expected is None:
        return False
    if annotation in (int, float) and isinstance(value, bool):
        return False
    return isinstance(value, expected)


def _validate_argument_type(
    value: Any,
    param: ParameterSpec,
    tool_name: str,
    expression: str,
    span: Span,
) -> None:
    """Raise ``ToolConfigurationError`` when *value* does not match *param*'s annotation.

    Every annotation the encoder can represent is checked deterministically
    before any data is read (§2.4).  An annotation that cannot be checked is
    rejected rather than skipped: silently accepting an unvalidated value is
    indistinguishable from having no contract at all.
    """
    import typing

    annotation = param.annotation
    if annotation is None:
        return  # no annotation → no static check

    if isinstance(annotation, UnsupportedAnnotation):
        raise ToolConfigurationError(
            f"argument {param.name!r} for {tool_name!r} has an annotation that "
            f"cannot be validated at compile time ({annotation.reason}); "
            "use str, int, float, bool, list, dict, None, Optional/Union of "
            "those, an enum, or a typed container",
            expression=expression,
            span=span,
        )

    if isinstance(annotation, str):
        raise ToolConfigurationError(
            f"argument {param.name!r} for {tool_name!r} has the unresolved "
            f"string annotation {annotation!r}; it must resolve to a type at "
            "import time so configuration can be validated before data is read",
            expression=expression,
            span=span,
        )

    if isinstance(annotation, EnumValues) and not _matches_annotation(value, annotation):
        allowed = ", ".join(repr(v) for v in annotation.values)
        raise ToolConfigurationError(
            f"argument {param.name!r} for {tool_name!r}: {value!r} is not one of "
            f"the allowed values for {annotation.__name__} ({allowed})",
            expression=expression,
            span=span,
        )

    known = (
        annotation is typing.Any
        or annotation in _ANNOTATION_TYPES
        or isinstance(annotation, (UnionAnnotation, EnumValues, ContainerAnnotation))
    )
    if not known:
        ann_name = _annotation_name(annotation)
        raise ToolConfigurationError(
            f"argument {param.name!r} for {tool_name!r} has annotation "
            f"{ann_name} which is not supported for compile-time validation; "
            "use str, int, float, bool, list, dict, None, Optional/Union of "
            "those, an enum, or a typed container",
            expression=expression,
            span=span,
        )

    if not _matches_annotation(value, annotation):
        raise ToolConfigurationError(
            f"argument {param.name!r} for {tool_name!r}: expected "
            f"{_annotation_name(annotation)}, got {type(value).__name__} ({value!r})",
            expression=expression,
            span=span,
        )
