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
instances, each holding a reference to the tool function, the compiled
selector, and the bound configuration.  These are then passed to
``CompiledToolProgramStage`` for execution.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Any, Callable

from datapipe.dsl import ast as _ast
from datapipe.dsl.errors import (
    ExpressionSyntaxError,
    Span,
    ToolConfigurationError,
    ToolResolutionError,
)
from datapipe.dsl.parser import parse
from datapipe.dsl.selector import CompiledSelector
from datapipe.tools.contract import ParameterSpec, ToolContract
from datapipe.tools.decorator import get_contract


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


def _build_full_registry() -> dict[str, Callable]:
    """Return a registry of all available tools: built-ins plus installed providers.

    Built-in names are reserved and cannot be shadowed by provider tools.

    Entries are keyed by:
      - Unqualified name (e.g. ``"my_tool"``) — for provider tools whose name
        does not clash with a built-in.
      - Qualified name (e.g. ``"my_provider.my_tool"``) — for all provider tools,
        always available alongside the unqualified form.
    """
    registry: dict[str, Callable] = dict(_get_builtin_registry())

    try:
        from datapipe.tools.registry import load_registry as _load_reg
        from datapipe.tools.loader import load_provider
        from datapipe.tools.descriptor import ProviderDescriptor
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
            desc = ProviderDescriptor(
                provider_id=entry.provider_id,
                alias=entry.alias,
                mode=entry.mode,
                source_path=entry.source_path,
                sha256=entry.digest,
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
            # Always register the qualified form: alias.tool_name.
            qualified = f"{entry.alias}.{tool_name}"
            registry[qualified] = fn
            # Register unqualified only when the name is not a reserved built-in.
            if tool_name not in _BUILTIN_NAMES:
                registry.setdefault(tool_name, fn)

    return registry


# ---------------------------------------------------------------------------
# Output types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ToolInvocation:
    """A fully resolved, bound tool invocation ready for per-record execution.

    Attributes
    ----------
    tool_fn:
        The resolved callable.  Do not store this in a pickleable descriptor;
        it is resolved at compilation time in the coordinator process.
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
    """
    tool_fn: Callable
    tool_name: str
    contract: ToolContract
    selector: CompiledSelector
    arguments: dict[str, Any]
    expression_index: int


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
        tool_fn = _resolve_tool(inv_node.qualified_name, registry, expression)
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

        invocations.append(ToolInvocation(
            tool_fn=tool_fn,
            tool_name=contract.name,
            contract=contract,
            selector=selector,
            arguments=arguments,
            expression_index=i,
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
    registry: dict[str, Callable],
    expression: str,
) -> Callable:
    """Look up *qname* in *registry* and return the callable."""
    if qname.namespace is not None:
        # Namespaced lookup: alias.tool_name for installed providers (Phase 4+).
        # Unknown namespaced names still raise a helpful error mentioning Phase 2
        # to preserve the expectation set by the Phase 2 test suite.
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
    invocation_span: Span,
) -> dict[str, Any]:
    """Bind expression arguments to the contract's ParameterSpec list.

    Returns a complete configuration dict with defaults filled in.
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
        bound[arg.name] = arg.value.value

    # Fill in defaults for unspecified parameters.
    for param in contract.parameters:
        if param.name not in bound:
            bound[param.name] = param.default

    return bound
