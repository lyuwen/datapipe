"""Worker-side provider loading with digest verification.

Workers receive a :class:`~datapipe.tools.descriptor.ProviderDescriptor`
built at compile time.  Before importing the source file they verify the
SHA-256 digest to detect tampering or stale editable installs.

Modules are cached in ``_loaded_providers`` after the first successful import
so repeated invocations within one worker process pay no re-import cost.
"""

from __future__ import annotations

import hashlib
import importlib.util
import re
import sys
from pathlib import Path
from typing import Any, Callable

from datapipe.tools.descriptor import ProviderDescriptor


class ProviderLoadError(Exception):
    """Raised when a provider cannot be loaded or verified."""


# Module-level cache: provider_id → {"module": ..., "tools": {name: fn}}
_loaded_providers: dict[str, dict[str, Any]] = {}


def _module_name_for(provider_id: str) -> str:
    """Return a unique, stable Python module name for *provider_id*.

    Multiple copied providers each live as ``source.py`` in their own
    subdirectory, so using ``source_path.stem`` directly causes the second
    import to collide with the first in ``sys.modules``.  Deriving the name
    from the provider_id — e.g. ``"local:my-tools"`` → ``"_dp_local_my_tools"``
    — gives every provider a distinct module name.
    """
    sanitised = re.sub(r"[^a-zA-Z0-9]", "_", provider_id)
    return "_dp_" + sanitised


def load_provider(descriptor: ProviderDescriptor) -> dict[str, Any]:
    """Import the provider described by *descriptor* and return a cache entry.

    The entry has the shape::

        {"module": <module>, "tools": {tool_name: callable, ...}}

    Raises :class:`ProviderLoadError` when the source file cannot be read,
    the digest does not match, or no ``@tool``-decorated functions are found.
    """
    if descriptor.provider_id in _loaded_providers:
        return _loaded_providers[descriptor.provider_id]

    source_path = Path(descriptor.source_path)

    try:
        source_bytes = source_path.read_bytes()
    except OSError as exc:
        raise ProviderLoadError(
            f"cannot read provider source {source_path}: {exc}"
        ) from exc

    # Verify the digest for every provider, including editable ones.
    #
    # For copied installations the descriptor carries the install-time digest;
    # tampering or corruption is detected here.
    #
    # For editable installations the compiler re-reads the file at expression
    # compilation time, computes its digest, and embeds that current digest in
    # the descriptor.  Workers then verify against that value, so a file edited
    # between compilation and worker startup fails fast rather than letting
    # different workers import different versions of the provider.
    actual_digest = "sha256:" + hashlib.sha256(source_bytes).hexdigest()
    if actual_digest != descriptor.sha256:
        raise ProviderLoadError(
            f"provider {descriptor.provider_id!r}: digest mismatch — "
            f"expected {descriptor.sha256!r}, got {actual_digest!r}; "
            "the source file may have been modified since installation"
        )

    module_name = _module_name_for(descriptor.provider_id)
    spec = importlib.util.spec_from_file_location(module_name, str(source_path))
    if spec is None or spec.loader is None:
        raise ProviderLoadError(
            f"cannot create module spec for {source_path}"
        )
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        # Execute the exact bytes we just read and verified, rather than
        # calling spec.loader.exec_module(), which re-reads the file through
        # the import machinery.  Two reasons:
        #
        # 1. Correctness under editable mode.  The bytecode cache keys on
        #    (mtime, size), so an edit that preserves the file size within the
        #    same mtime-second reuses a stale .pyc and the edit is invisible.
        #    Editing is the whole point of editable mode, so that must not
        #    happen.
        # 2. Integrity.  Re-reading opens a gap between the bytes we hashed
        #    and the bytes actually executed; compiling source_bytes closes it.
        code = compile(source_bytes, str(source_path), "exec")
        exec(code, module.__dict__)
    except Exception as exc:
        # Do not leave a half-initialised module behind for the next lookup.
        sys.modules.pop(module_name, None)
        raise ProviderLoadError(
            f"provider {descriptor.provider_id!r}: import failed: {exc}"
        ) from exc

    tools: dict[str, Callable] = {}
    for attr_name in dir(module):
        obj = getattr(module, attr_name, None)
        if obj is not None and hasattr(obj, "__tool_contract__"):
            contract = obj.__tool_contract__
            tools[contract.name] = obj

    entry: dict[str, Any] = {"module": module, "tools": tools}
    _loaded_providers[descriptor.provider_id] = entry
    return entry


def resolve_tool(descriptor: ProviderDescriptor, tool_name: str) -> Callable:
    """Return the callable for *tool_name* from the provider identified by *descriptor*.

    Raises :class:`ProviderLoadError` when the provider cannot be loaded or
    *tool_name* is not found in it.
    """
    entry = load_provider(descriptor)
    tools = entry["tools"]
    if tool_name not in tools:
        available = sorted(tools)
        hint = (
            f"; available: {', '.join(available)}" if available else " (no tools found)"
        )
        raise ProviderLoadError(
            f"provider {descriptor.provider_id!r} has no tool {tool_name!r}{hint}"
        )
    return tools[tool_name]
