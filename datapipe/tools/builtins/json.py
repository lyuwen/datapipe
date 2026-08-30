"""Built-in JSON tools: ``fromjson`` and ``tojson``.

Both tools follow the same ``@tool`` contract as user-installed tools and
are registered in the built-in provider.  The DSL compiler resolves them
by name through the registry; they do not need special-case treatment.

``fromjson`` semantics (§6.1 of the CLI plan)
---------------------------------------------
Without ``recursive``:
  - The selected value must be a string.
  - It is decoded once with ``json.loads``.
  - Failure to decode is an error (not silently ignored).

With ``recursive=True``:
  - The root value is decoded first (must be a string).
  - The decoded result is then traversed depth-first.
  - Strings in the traversal are decoded only when decoding succeeds AND
    produces an array or object (when ``containers_only=True``, the default).
  - With ``containers_only=False``, decoded scalars (booleans, integers,
    null) also replace the source string.
  - Strings that are not valid JSON remain unchanged during traversal.

``tojson`` semantics (§6.2 of the CLI plan)
-------------------------------------------
- Every selected match is serialized independently.
- An already-string value is *re*-serialized as a JSON string literal —
  it is not treated as already serialized.
- ``compact=True`` uses ``separators=(",", ":")``.
- Non-finite floats raise ``ValueError`` by default.
"""

from __future__ import annotations

import json
import math
from typing import Any

from datapipe.tools.decorator import tool
from datapipe.tools.types import JsonType
from datapipe.tools.contract import ToolExample
from datapipe.tools.types import OneOf


@tool(
    name="fromjson",
    api_version=1,
    target="value",
    input=OneOf(JsonType.STRING, JsonType.ARRAY, JsonType.OBJECT),
    output=JsonType.ANY,
    cardinality="one_to_one",
    deterministic=True,
    description=(
        "Decode a JSON-encoded string to a Python value. "
        "With recursive=True, traverses arrays and objects and decodes "
        "nested JSON-encoded strings."
    ),
    examples=[
        ToolExample(
            input='{"a": 1}',
            output={"a": 1},
            description="simple object decode",
        ),
        ToolExample(
            input='[1, 2, 3]',
            output=[1, 2, 3],
            description="array decode",
        ),
    ],
)
def fromjson(
    value: Any,
    *,
    recursive: bool = False,
    containers_only: bool = True,
) -> Any:
    """Decode a JSON-encoded string.

    Parameters
    ----------
    value:
        The selected value.  Must be a string without ``recursive``.
        Arrays and objects are also accepted but passed through unchanged
        unless they contain nested JSON-encoded strings (with ``recursive``).
    recursive:
        When True, traverse the decoded value depth-first and decode any
        string that contains a valid JSON array or object (or any valid
        JSON value when ``containers_only=False``).
    containers_only:
        When True (the default), recursive traversal only replaces strings
        whose decoded form is a list or dict.  Scalars such as ``true``,
        ``null``, and ``42`` are left as-is.
    """
    if not recursive:
        if not isinstance(value, str):
            raise TypeError(
                f"fromjson: expected a string without recursive=True, "
                f"got {type(value).__name__!r}"
            )
        return json.loads(value)

    # Recursive mode: if the root is a string, decode it first (failure is a
    # hard error), then traverse the result.  If the root is already a dict or
    # list (a valid contract input), traverse it directly.
    if isinstance(value, str):
        value = json.loads(value)  # hard error on invalid JSON
    elif not isinstance(value, (dict, list)):
        raise TypeError(
            f"fromjson: with recursive=True the root value must be a string, "
            f"dict, or list, got {type(value).__name__!r}"
        )

    return _recursive_decode(value, containers_only=containers_only)


def _recursive_decode(obj: Any, *, containers_only: bool) -> Any:
    """Depth-first traversal: decode JSON strings, recurse into containers."""
    if isinstance(obj, dict):
        return {k: _recursive_decode(v, containers_only=containers_only) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_recursive_decode(v, containers_only=containers_only) for v in obj]
    if isinstance(obj, str):
        try:
            decoded = json.loads(obj)
        except (json.JSONDecodeError, ValueError):
            return obj  # not valid JSON — leave unchanged
        # Decide whether to keep the decoded value.
        if containers_only and not isinstance(decoded, (dict, list)):
            return obj  # scalar result — keep the original string
        return _recursive_decode(decoded, containers_only=containers_only)
    return obj


@tool(
    name="tojson",
    api_version=1,
    target="value",
    input=JsonType.ANY,
    output=JsonType.STRING,
    cardinality="one_to_one",
    deterministic=True,
    description=(
        "Serialize a value to a JSON string. "
        "An already-string value is re-serialized as a JSON string literal."
    ),
    examples=[
        ToolExample(
            input={"a": 1},
            output='{"a":1}',
            arguments={"compact": True},
            description="compact object serialization",
        ),
        ToolExample(
            input="hello",
            output='"hello"',
            description="string is re-serialized as a JSON string literal",
        ),
    ],
)
def tojson(
    value: Any,
    *,
    ensure_ascii: bool = False,
    compact: bool = True,
    sort_keys: bool = False,
) -> str:
    """Serialize *value* to a JSON string.

    Parameters
    ----------
    value:
        Any JSON-representable Python value.  Non-finite floats raise
        ``ValueError``.
    ensure_ascii:
        When True, all non-ASCII characters are escaped.
    compact:
        When True (the default), output uses ``separators=(",", ":")``
        with no extra whitespace.
    sort_keys:
        When True, dict keys are sorted alphabetically.
    """
    # Reject non-finite floats explicitly for strict JSON compliance.
    _check_finite(value)

    separators = (",", ":") if compact else (", ", ": ")
    return json.dumps(
        value,
        ensure_ascii=ensure_ascii,
        separators=separators,
        sort_keys=sort_keys,
    )


def _check_finite(obj: Any) -> None:
    """Raise ValueError when *obj* contains a non-finite float."""
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        raise ValueError(
            f"tojson: non-finite float {obj!r} is not representable in JSON; "
            "convert or replace it before serialization"
        )
    if isinstance(obj, dict):
        for v in obj.values():
            _check_finite(v)
    elif isinstance(obj, list):
        for v in obj:
            _check_finite(v)
