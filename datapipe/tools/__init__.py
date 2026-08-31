"""datapipe.tools: public authoring API for tool contracts and built-ins.

Usage::

    from datapipe.tools import JsonType, OneOf, tool, ToolContract, ParameterSpec
    from datapipe.tools import fromjson, tojson
    from datapipe.tools.installer import install_provider, remove_provider
    from datapipe.tools.registry import load_registry, get_provider
"""

from datapipe.tools.types import (
    JsonType,
    OneOf,
    TypeSpec,
    as_type_spec,
    describe,
    infer_json_type,
    matches,
)
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
    "describe",
    "infer_json_type",
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
