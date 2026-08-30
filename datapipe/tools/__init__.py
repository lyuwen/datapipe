"""datapipe.tools: public authoring API for tool contracts and built-ins.

Phase 1 — tool contracts and built-ins:

    from datapipe.tools import JsonType, OneOf, tool, ToolContract, ParameterSpec
    from datapipe.tools import fromjson, tojson

Phase 2-4 (registry, installer, loader) — not yet implemented.
"""

from datapipe.tools.types import JsonType, OneOf, TypeSpec, as_type_spec, matches
from datapipe.tools.contract import (
    Cardinality,
    ParameterSpec,
    ToolContract,
    ToolExample,
    make_contract,
)
from datapipe.tools.decorator import (
    ToolDecoratorError,
    get_contract,
    is_tool,
    tool,
)
from datapipe.tools.builtins.json import fromjson, tojson

__all__ = [
    # Type system
    "JsonType",
    "OneOf",
    "TypeSpec",
    "as_type_spec",
    "matches",
    # Contract model
    "Cardinality",
    "ParameterSpec",
    "ToolContract",
    "ToolExample",
    "make_contract",
    # Decorator
    "ToolDecoratorError",
    "get_contract",
    "is_tool",
    "tool",
    # Built-ins
    "fromjson",
    "tojson",
]
