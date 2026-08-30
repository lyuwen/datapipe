"""Pickleable provider and tool descriptor models.

Descriptors are the currency that crosses process boundaries: the
coordinator compiles an expression into ``ToolInvocation`` objects each
carrying a ``ToolDescriptor``, which itself carries a ``ProviderDescriptor``.
Workers receive these frozen objects, verify the source digest, import the
provider once per worker, and bind each invocation to its resolved callable.

Design constraints
------------------
- Every field must be a primitive (str, int, bool) or a tuple/frozen dataclass
  composed of primitives so that ``pickle`` works under the ``spawn``
  multiprocessing start method without any import side effects.
- No live Python callables, no open file handles, no registry references.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ProviderDescriptor:
    """Immutable identity of an installed provider.

    Attributes
    ----------
    provider_id:
        Canonical provider ID, e.g. ``"local:my-tools"``.
    alias:
        Short namespace for DSL expressions, e.g. ``"my_tools"``.
        Must be a valid Python identifier.
    mode:
        ``"copied"`` or ``"editable"``.
    source_path:
        Absolute path to the source file.  For copied installations this
        points to the snapshot inside the registry directory; for editable
        installations it points to the user's original file.
    sha256:
        Hex SHA-256 digest of the source bytes at install time (copied) or
        at last successful validation (editable).  Workers verify this
        before importing.
    api_version:
        Tool API version declared by the provider.  Currently always 1.
    """

    provider_id: str
    alias: str
    mode: str           # "copied" | "editable"
    source_path: str
    sha256: str
    api_version: int = 1


@dataclass(frozen=True)
class ToolDescriptor:
    """Immutable identity of one tool within a provider.

    Attributes
    ----------
    provider:
        The descriptor of the provider that contains this tool.
    tool_name:
        Unqualified tool name as it appears in the ``@tool`` decorator.
    """

    provider: ProviderDescriptor
    tool_name: str

    @property
    def qualified_name(self) -> str:
        return f"{self.provider.alias}.{self.tool_name}"
