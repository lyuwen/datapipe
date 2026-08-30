"""Tool contract model: ToolContract, ParameterSpec, Cardinality.

A ``ToolContract`` is an immutable, pickleable description of a tool's:

  - name and API version;
  - target scope (value or record);
  - input and output type specifications;
  - cardinality (one_to_one only in Phase 1);
  - determinism declaration;
  - keyword-only configuration parameters.

``ParameterSpec`` describes one keyword-only configuration parameter.

These objects travel across process boundaries as part of compiled tool
descriptors, so they must be pickleable and must not hold live Python
callables.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any

from datapipe.tools.types import JsonType, TypeSpec, as_type_spec


class Cardinality(enum.Enum):
    """Output cardinality of a tool invocation.

    Only ``ONE_TO_ONE`` is executable in Phase 1.  ``ONE_TO_ZERO`` and
    ``ONE_TO_MANY`` are reserved for future phases.
    """

    ONE_TO_ONE = "one_to_one"
    ONE_TO_ZERO = "one_to_zero"
    ONE_TO_MANY = "one_to_many"


_SUPPORTED_CARDINALITIES = frozenset({Cardinality.ONE_TO_ONE})

# Python types that are allowed as keyword-only configuration annotations.
_ALLOWED_PARAM_TYPES: frozenset[type] = frozenset(
    {str, int, float, bool, type(None), list, dict}
)


@dataclass(frozen=True)
class ParameterSpec:
    """Description of one keyword-only configuration parameter.

    Attributes
    ----------
    name:
        The parameter name as it appears in the function signature.
    default:
        The JSON-serializable default value.  Required (keyword-only
        parameters without defaults are rejected by the decorator).
    annotation:
        The Python annotation for the parameter, stored for documentation
        and conflict-detection purposes.  May be ``None`` when unannotated.
    required:
        True when the parameter has no default and must be supplied by the
        expression.  Phase 1 requires all parameters to have defaults, so
        this will always be False in the initial release.
    """

    name: str
    default: Any
    annotation: Any = None
    required: bool = False

    def __post_init__(self) -> None:
        if not self.name.isidentifier():
            raise ValueError(f"ParameterSpec name {self.name!r} is not a valid identifier")


@dataclass(frozen=True)
class ToolContract:
    """Immutable, pickleable metadata for a registered tool.

    Attributes
    ----------
    name:
        The tool name used in DSL expressions and inspection output.
    api_version:
        The tool API version; currently must be 1.
    target:
        ``"value"`` — invoked once per selector match.
        ``"record"`` — invoked once on the complete row; requires selector ``.``.
    input_type:
        TypeSpec for the acceptable input.  Runtime validation checks selected
        values against this before calling the tool.
    output_type:
        TypeSpec for the acceptable output.  Runtime validation checks the
        return value against this after calling the tool.
    cardinality:
        Output cardinality; only ``ONE_TO_ONE`` is executable in Phase 1.
    deterministic:
        True when the tool always returns the same output for the same input
        and configuration.  Informational; used for caching decisions later.
    description:
        Human-readable description.  Empty string when not provided.
    parameters:
        Ordered tuple of ``ParameterSpec`` for keyword-only configuration.
    examples:
        Optional tuple of ``ToolExample`` instances for smoke tests.
    """

    name: str
    api_version: int
    target: str
    input_type: TypeSpec
    output_type: TypeSpec
    cardinality: Cardinality
    deterministic: bool
    description: str
    parameters: tuple[ParameterSpec, ...]
    examples: tuple["ToolExample", ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("ToolContract.name must not be empty")
        if self.api_version != 1:
            raise ValueError(
                f"unsupported api_version {self.api_version!r}; only 1 is supported"
            )
        if self.target not in ("value", "record"):
            raise ValueError(
                f"target must be 'value' or 'record', got {self.target!r}"
            )
        if self.cardinality not in _SUPPORTED_CARDINALITIES:
            raise ValueError(
                f"cardinality {self.cardinality!r} is not supported in Phase 1; "
                f"only {Cardinality.ONE_TO_ONE!r} is executable"
            )
        if not isinstance(self.input_type, TypeSpec):
            raise TypeError(
                f"input_type must be a TypeSpec, got {type(self.input_type).__name__}"
            )
        if not isinstance(self.output_type, TypeSpec):
            raise TypeError(
                f"output_type must be a TypeSpec, got {type(self.output_type).__name__}"
            )

    def parameter_defaults(self) -> dict[str, Any]:
        """Return a mapping of parameter names to their default values."""
        return {p.name: p.default for p in self.parameters}


@dataclass(frozen=True)
class ToolExample:
    """One example input/output pair for a tool, used in smoke tests.

    Attributes
    ----------
    input:
        A JSON-representable input value.
    arguments:
        A mapping of keyword argument names to values.
    output:
        The expected JSON-representable output value.
    description:
        Optional human-readable label for the example.
    """

    input: Any
    output: Any
    arguments: dict[str, Any] = field(default_factory=dict)
    description: str = ""


def make_contract(
    *,
    name: str,
    api_version: int = 1,
    target: str,
    input: "JsonType | TypeSpec",
    output: "JsonType | TypeSpec",
    cardinality: "str | Cardinality" = "one_to_one",
    deterministic: bool = True,
    description: str = "",
    parameters: "tuple[ParameterSpec, ...] | list[ParameterSpec]" = (),
    examples: "tuple[ToolExample, ...] | list[ToolExample]" = (),
) -> ToolContract:
    """Convenience constructor that coerces ``JsonType`` values to ``TypeSpec``."""
    if isinstance(cardinality, str):
        try:
            cardinality = Cardinality(cardinality)
        except ValueError:
            raise ValueError(
                f"unknown cardinality {cardinality!r}; expected one of "
                f"{[c.value for c in Cardinality]}"
            )
    return ToolContract(
        name=name,
        api_version=api_version,
        target=target,
        input_type=as_type_spec(input),
        output_type=as_type_spec(output),
        cardinality=cardinality,
        deterministic=deterministic,
        description=description,
        parameters=tuple(parameters),
        examples=tuple(examples),
    )
