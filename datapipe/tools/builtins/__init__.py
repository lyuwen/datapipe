"""Built-in tool providers for datapipe.

Built-in tools are registered via the same ``@tool`` decorator API as
user-installed tools.  They are discoverable through the same inspection
mechanisms.
"""

from datapipe.tools.builtins.json import fromjson, tojson

__all__ = ["fromjson", "tojson"]
