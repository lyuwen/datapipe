"""``@tool`` decorator: attach a ToolContract to a callable.

Usage::

    from datapipe.tools import JsonType, tool

    @tool(
        name="normalize_text",
        target="value",
        input=JsonType.STRING,
        output=JsonType.STRING,
        description="Normalize whitespace in a string.",
    )
    def normalize_text(value: str, *, lowercase: bool = False) -> str:
        return value.strip().lower() if lowercase else value.strip()

The decorator validates the function signature immediately at import time:

- exactly one positional parameter (the value or record);
- all remaining parameters must be keyword-only;
- no ``*args`` or ``**kwargs``;
- all keyword-only parameters must have JSON-serializable defaults;
- the declared tool ``input`` type must not directly contradict an explicit
  Python annotation on the first parameter.

The resulting callable is unchanged; the contract is stored as the
``__tool_contract__`` attribute.
"""

from __future__ import annotations

import inspect
import json
import typing
from typing import Any, Callable

from datapipe.tools.contract import (
    Cardinality,
    ParameterSpec,
    ToolContract,
    ToolExample,
    make_contract,
)
from datapipe.tools.types import JsonType, TypeSpec, as_type_spec


class ToolDecoratorError(Exception):
    """Raised when ``@tool`` detects a contract or signature problem."""


# Annotation types that are allowed on keyword-only config parameters.
_ALLOWED_ANNOTATIONS = {str, int, float, bool, type(None), list, dict}


def tool(
    *,
    name: str,
    api_version: int = 1,
    target: str,
    input: "JsonType | TypeSpec",
    output: "JsonType | TypeSpec",
    cardinality: "str | Cardinality" = "one_to_one",
    deterministic: bool = True,
    description: str = "",
    examples: "list[ToolExample]" = [],  # noqa: B006 — immutable default is fine: never mutated
) -> Callable:
    """Attach a ``ToolContract`` to the decorated function.

    Parameters
    ----------
    name:
        Tool name used in DSL expressions and inspection.
    api_version:
        Must be 1.
    target:
        ``"value"`` or ``"record"``.
    input:
        Acceptable input type: a ``JsonType`` or ``TypeSpec``.
    output:
        Expected output type: a ``JsonType`` or ``TypeSpec``.
    cardinality:
        ``"one_to_one"`` (only supported value in Phase 1).
    deterministic:
        True when output depends only on input and configuration.
    description:
        Human-readable description of what the tool does.
    examples:
        Optional list of ``ToolExample`` for smoke tests.
    """

    def decorator(fn: Callable) -> Callable:
        params = _validate_signature(fn, name, target)
        contract = make_contract(
            name=name,
            api_version=api_version,
            target=target,
            input=input,
            output=output,
            cardinality=cardinality,
            deterministic=deterministic,
            description=description,
            parameters=params,
            examples=list(examples),
        )
        fn.__tool_contract__ = contract  # type: ignore[attr-defined]
        return fn

    return decorator


# ---------------------------------------------------------------------------
# Signature validation helpers
# ---------------------------------------------------------------------------


def _validate_signature(
    fn: Callable,
    tool_name: str,
    target: str,
) -> tuple[ParameterSpec, ...]:
    """Inspect *fn* and return a tuple of ``ParameterSpec`` for its config params.

    Raises ``ToolDecoratorError`` for any disallowed signature pattern.
    """
    sig = inspect.signature(fn)
    params = list(sig.parameters.values())

    # Resolve string annotations (PEP 563 / from __future__ import annotations)
    # so that ParameterSpec.annotation holds the actual type object, not a string.
    try:
        hints = typing.get_type_hints(fn)
    except Exception:  # noqa: BLE001
        hints = {}

    if not params:
        raise ToolDecoratorError(
            f"tool {tool_name!r}: function must have at least one positional "
            "parameter (the value or record)"
        )

    # --- First parameter: the value/record ----------------------------------
    first = params[0]
    if first.kind not in (
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.POSITIONAL_ONLY,
    ):
        raise ToolDecoratorError(
            f"tool {tool_name!r}: first parameter must be positional, "
            f"got {first.kind.name}"
        )
    if first.default is not inspect.Parameter.empty:
        raise ToolDecoratorError(
            f"tool {tool_name!r}: first parameter {first.name!r} must not "
            "have a default value"
        )

    # --- Remaining parameters: keyword-only config --------------------------
    rest = params[1:]
    param_specs: list[ParameterSpec] = []

    for p in rest:
        if p.kind == inspect.Parameter.VAR_POSITIONAL:
            raise ToolDecoratorError(
                f"tool {tool_name!r}: *args is not allowed; all configuration "
                "parameters must be keyword-only"
            )
        if p.kind == inspect.Parameter.VAR_KEYWORD:
            raise ToolDecoratorError(
                f"tool {tool_name!r}: **kwargs is not allowed; all "
                "configuration parameters must be keyword-only"
            )
        if p.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
            raise ToolDecoratorError(
                f"tool {tool_name!r}: parameter {p.name!r} is positional; "
                "configuration parameters must be keyword-only (add * before "
                "the first configuration parameter)"
            )
        if p.kind != inspect.Parameter.KEYWORD_ONLY:
            raise ToolDecoratorError(
                f"tool {tool_name!r}: unexpected parameter kind {p.kind.name} "
                f"for {p.name!r}"
            )
        if p.default is inspect.Parameter.empty:
            raise ToolDecoratorError(
                f"tool {tool_name!r}: keyword-only parameter {p.name!r} has "
                "no default value; all configuration parameters must have "
                "JSON-serializable defaults in Phase 1"
            )
        _validate_default(p.default, p.name, tool_name)
        # Use the resolved type hint when available; fall back to the raw
        # annotation from inspect (which may be a string if PEP 563 is active)
        # or None when no annotation is present at all.
        annotation = hints.get(p.name)
        if annotation is None and p.annotation is not inspect.Parameter.empty:
            annotation = p.annotation  # keep as-is (string or type)
        param_specs.append(
            ParameterSpec(
                name=p.name,
                default=p.default,
                annotation=annotation,
                required=False,
            )
        )

    return tuple(param_specs)


def _validate_default(value: Any, param_name: str, tool_name: str) -> None:
    """Raise ``ToolDecoratorError`` if *value* is not JSON-serializable."""
    try:
        json.dumps(value)
    except (TypeError, ValueError) as exc:
        raise ToolDecoratorError(
            f"tool {tool_name!r}: default for {param_name!r} is not "
            f"JSON-serializable: {exc}"
        ) from exc


def get_contract(fn: Callable) -> "ToolContract | None":
    """Return the ``ToolContract`` attached to *fn*, or None."""
    return getattr(fn, "__tool_contract__", None)


def is_tool(fn: object) -> bool:
    """True when *fn* has been decorated with ``@tool``."""
    return hasattr(fn, "__tool_contract__")
