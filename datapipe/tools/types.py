"""JSON-oriented type system for tool contracts.

Tools declare their input and output types using the JSON type vocabulary
rather than arbitrary Python types, because the DSL operates on JSON-
representable values and cross-process validation needs pickleable tokens.

The hierarchy is:

  ANY
  ├── NULL
  ├── BOOLEAN
  ├── NUMBER
  │   ├── INTEGER
  │   └── (non-integer float)
  ├── STRING
  ├── ARRAY
  └── OBJECT

  SCALAR  = NULL | BOOLEAN | NUMBER | STRING
  CONTAINER = ARRAY | OBJECT

Runtime matching rules follow JSON semantics:
  - bool is checked before int (bool is a subclass of int in Python).
  - INTEGER matches int values that are not bool.
  - NUMBER matches int-and-not-bool OR float-and-not-bool (NaN/Inf rejected
    unless explicitly allowed).
  - ANY matches any Python value that can be JSON-serialized.
"""

from __future__ import annotations

import enum
import math
from typing import Any


class JsonType(enum.Enum):
    """Enumeration of JSON-representable types used in tool contracts."""

    NULL = "null"
    BOOLEAN = "boolean"
    INTEGER = "integer"
    NUMBER = "number"
    STRING = "string"
    ARRAY = "array"
    OBJECT = "object"
    SCALAR = "scalar"          # NULL | BOOLEAN | NUMBER | STRING
    CONTAINER = "container"    # ARRAY | OBJECT
    ANY = "any"

    def __repr__(self) -> str:
        return f"JsonType.{self.name}"


class TypeSpec:
    """Base class for all type specifications.

    All concrete subclasses must be immutable and pickleable so that compiled
    tool descriptors can cross process boundaries under the spawn start method.
    """

    def matches(self, value: Any) -> bool:
        """Return True when *value* satisfies this type specification."""
        raise NotImplementedError

    def __repr__(self) -> str:  # pragma: no cover
        raise NotImplementedError


class _SimpleTypeSpec(TypeSpec):
    """TypeSpec wrapping a single JsonType."""

    __slots__ = ("_json_type",)

    def __init__(self, json_type: JsonType) -> None:
        self._json_type = json_type

    @property
    def json_type(self) -> JsonType:
        return self._json_type

    def matches(self, value: Any) -> bool:
        return _matches_json_type(value, self._json_type)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _SimpleTypeSpec) and self._json_type is other._json_type

    def __hash__(self) -> int:
        return hash(self._json_type)

    def __repr__(self) -> str:
        return repr(self._json_type)


class OneOf(TypeSpec):
    """TypeSpec that accepts any value matching at least one of its members.

    Example::

        OneOf(JsonType.STRING, JsonType.ARRAY)

    Members may be ``JsonType`` values or other ``TypeSpec`` instances.
    ``OneOf`` is flattened: ``OneOf(A, OneOf(B, C))`` is equivalent to
    ``OneOf(A, B, C)``.
    """

    __slots__ = ("_members",)

    def __init__(self, *members: "JsonType | TypeSpec") -> None:
        if len(members) < 2:
            raise ValueError("OneOf requires at least two members")
        specs: list[TypeSpec] = []
        for m in members:
            if isinstance(m, JsonType):
                specs.append(_SimpleTypeSpec(m))
            elif isinstance(m, OneOf):
                specs.extend(m._members)
            elif isinstance(m, TypeSpec):
                specs.append(m)
            else:
                raise TypeError(f"expected JsonType or TypeSpec, got {type(m).__name__}")
        self._members: tuple[TypeSpec, ...] = tuple(specs)

    @property
    def members(self) -> tuple[TypeSpec, ...]:
        return self._members

    def matches(self, value: Any) -> bool:
        return any(m.matches(value) for m in self._members)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, OneOf) and self._members == other._members

    def __hash__(self) -> int:
        return hash(self._members)

    def __repr__(self) -> str:
        inner = ", ".join(repr(m) for m in self._members)
        return f"OneOf({inner})"


# ---------------------------------------------------------------------------
# Module-level TypeSpec singletons for every JsonType.
# These are the values tool authors reference in contracts, e.g.
#     from datapipe.tools.types import AnyType, StringType
# The preferred public API is via JsonType together with the as_type_spec()
# helper, but named singletons are more ergonomic in practice.
# ---------------------------------------------------------------------------

_SINGLETONS: dict[JsonType, _SimpleTypeSpec] = {
    jt: _SimpleTypeSpec(jt) for jt in JsonType
}


def as_type_spec(value: "JsonType | TypeSpec") -> TypeSpec:
    """Coerce a ``JsonType`` or ``TypeSpec`` to a canonical ``TypeSpec``.

    ``JsonType`` values become the corresponding singleton ``_SimpleTypeSpec``.
    ``TypeSpec`` instances are returned as-is.
    """
    if isinstance(value, JsonType):
        return _SINGLETONS[value]
    if isinstance(value, TypeSpec):
        return value
    raise TypeError(f"expected JsonType or TypeSpec, got {type(value).__name__!r}")


# ---------------------------------------------------------------------------
# Runtime matching
# ---------------------------------------------------------------------------

def _matches_json_type(value: Any, jt: JsonType) -> bool:  # noqa: C901 (intentional)
    """Return True when *value* matches *jt* according to JSON semantics."""
    if jt is JsonType.ANY:
        # Accept only JSON-representable values: None, bool, int (not bool),
        # finite float, str, list, dict.  Sets, tuples, arbitrary objects, and
        # non-finite floats are not JSON-representable and must be rejected so
        # that tool output failures are attributed to the tool rather than
        # surfacing later as a JsonDumpStage error.
        return infer_json_type(value) is not None
    if jt is JsonType.NULL:
        return value is None
    if jt is JsonType.BOOLEAN:
        return isinstance(value, bool)
    if jt is JsonType.INTEGER:
        return isinstance(value, int) and not isinstance(value, bool)
    if jt is JsonType.NUMBER:
        if isinstance(value, bool):
            return False
        if isinstance(value, int):
            return True
        if isinstance(value, float):
            return not (math.isnan(value) or math.isinf(value))
        return False
    if jt is JsonType.STRING:
        return isinstance(value, str)
    if jt is JsonType.ARRAY:
        return isinstance(value, list)
    if jt is JsonType.OBJECT:
        return isinstance(value, dict)
    if jt is JsonType.SCALAR:
        return (
            value is None
            or isinstance(value, bool)
            or (isinstance(value, int) and not isinstance(value, bool))
            or (isinstance(value, float) and not (math.isnan(value) or math.isinf(value)))
            or isinstance(value, str)
        )
    if jt is JsonType.CONTAINER:
        return isinstance(value, (list, dict))
    return False  # pragma: no cover


def matches(value: Any, spec: "JsonType | TypeSpec") -> bool:
    """Check *value* against a ``JsonType`` or ``TypeSpec``."""
    return as_type_spec(spec).matches(value)


# ---------------------------------------------------------------------------
# Type-name inference and human-readable descriptions (for diagnostics)
# ---------------------------------------------------------------------------


def infer_json_type(value: Any) -> "JsonType | None":
    """Return the most specific ``JsonType`` describing *value*.

    Returns the narrowest concrete member of the JSON type vocabulary:
    ``NULL``, ``BOOLEAN``, ``INTEGER``, ``NUMBER`` (non-integer finite float),
    ``STRING``, ``ARRAY``, or ``OBJECT``.  The umbrella types (``ANY``,
    ``SCALAR``, ``CONTAINER``) are never returned because they are not
    specific.

    Returns ``None`` when *value* is not JSON-representable at all — an
    arbitrary object, a set, or a non-finite float.  Callers rendering an
    "actual type" for an error message should treat ``None`` as "unknown"
    and fall back to the Python type name.

    ``bool`` is checked before ``int`` because ``bool`` is an ``int`` subclass.
    """
    if value is None:
        return JsonType.NULL
    # bool must precede int: isinstance(True, int) is True.
    if isinstance(value, bool):
        return JsonType.BOOLEAN
    if isinstance(value, int):
        return JsonType.INTEGER
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            # Not representable in strict JSON.
            return None
        # A float holding an exact integral value is still a NUMBER, not an
        # INTEGER: INTEGER is reserved for Python ints so that round-tripping
        # through JSON preserves the distinction.
        return JsonType.NUMBER
    if isinstance(value, str):
        return JsonType.STRING
    if isinstance(value, list):
        return JsonType.ARRAY
    if isinstance(value, dict):
        return JsonType.OBJECT
    return None


def describe(spec: "JsonType | TypeSpec") -> str:
    """Return a human-readable type name for *spec*, for error messages.

    A ``JsonType`` renders as its value (``"string"``).  A ``OneOf`` renders
    its members joined with ``" | "`` (``"string | array | object"``).
    """
    if isinstance(spec, JsonType):
        return spec.value
    if isinstance(spec, _SimpleTypeSpec):
        return spec.json_type.value
    if isinstance(spec, OneOf):
        return " | ".join(describe(m) for m in spec.members)
    if isinstance(spec, TypeSpec):
        # Unknown third-party TypeSpec subclass: fall back to its repr rather
        # than raising, since this is only ever used to build a message.
        return repr(spec)
    raise TypeError(f"expected JsonType or TypeSpec, got {type(spec).__name__!r}")
