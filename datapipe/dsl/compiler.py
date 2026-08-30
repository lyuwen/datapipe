"""Semantic compiler: turns a parsed DSL expression into ToolInvocation descriptors.

Compilation is a sequence of explicit passes (§9 of the CLI plan):

  1. Tokenize and parse → AST (done by ``datapipe.dsl.parser.parse``).
  2. Resolve each tool name against the built-in registry.
  3. Validate selector compatibility with tool target scope.
  4. Bind configuration literals to ``ParameterSpec`` definitions.
  5. Apply defaults and produce normalized argument values.
  6. Perform statically provable input/output compatibility checks.
  7. Produce ``ToolInvocation`` descriptors.

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

from datapipe.dsl import ast as _ast
from datapipe.dsl.errors import (
    Span,
    ToolConfigurationError,
    ToolResolutionError,
)
from datapipe.dsl.parser import parse
from datapipe.dsl.selector import CompiledSelector
from datapipe.tools.contract import ParameterSpec, ToolContract
from datapipe.tools.decorator import get_contract
from datapipe.tools.descriptor import ProviderDescriptor, ToolDescriptor


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
        from datapipe.tools.loader import load_provider
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

            desc = ProviderDescriptor(
                provider_id=entry.provider_id,
                alias=entry.alias,
                mode=entry.mode,
                source_path=entry.source_path,
                sha256=sha256,
                api_version=entry.datapipe_api,
            )
            provider_entry = load_provider(desc)
            tools: dict[str, Callable] = provider_entry["tools"]
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

        for tool_name, fn in tools.items():
            tool_desc = ToolDescriptor(provider=desc, tool_name=tool_name)
            # Always register the qualified form: alias.tool_name.
            qualified = f"{entry.alias}.{tool_name}"
            registry[qualified] = (fn, tool_desc)
            # Register unqualified only when the name is not a reserved built-in.
            if tool_name not in _BUILTIN_NAMES:
                registry.setdefault(tool_name, (fn, tool_desc))

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

    return CompiledExpression(
        invocations=tuple(invocations),
        source=expression,
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

    # Fill in defaults for unspecified parameters.
    for param in contract.parameters:
        if param.name not in bound:
            bound[param.name] = param.default

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


def _validate_argument_type(
    value: Any,
    param: ParameterSpec,
    tool_name: str,
    expression: str,
    span: Span,
) -> None:
    """Raise ``ToolConfigurationError`` when *value* does not match *param*'s annotation.

    Bool must be checked before int/float because ``bool`` is a subclass of
    both in Python, so ``isinstance(True, int)`` and ``isinstance(True, float)``
    are both True.
    """
    annotation = param.annotation
    if annotation is None:
        return  # no annotation → no static check

    expected = _ANNOTATION_TYPES.get(annotation)
    if expected is None:
        return  # unsupported annotation type → skip (e.g. Optional, Union)

    # Special-case bool: True/False must not be accepted where int or float is
    # declared — the caller likely meant a numeric literal, not a boolean.
    if annotation in (int, float) and isinstance(value, bool):
        raise ToolConfigurationError(
            f"argument {param.name!r} for {tool_name!r}: expected "
            f"{annotation.__name__}, got bool ({value!r})",
            expression=expression,
            span=span,
        )

    if not isinstance(value, expected):
        actual_type = type(value).__name__
        raise ToolConfigurationError(
            f"argument {param.name!r} for {tool_name!r}: expected "
            f"{annotation.__name__}, got {actual_type} ({value!r})",
            expression=expression,
            span=span,
        )
