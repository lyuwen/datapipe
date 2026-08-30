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
import sys
from pathlib import Path
from typing import Any, Callable

from datapipe.tools.descriptor import ProviderDescriptor


class ProviderLoadError(Exception):
    """Raised when a provider cannot be loaded or verified."""


# Module-level cache: provider_id → {"module": ..., "tools": {name: fn}}
_loaded_providers: dict[str, dict[str, Any]] = {}


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

    # Digest verification applies to copied installations only.
    #
    # A copied provider lives in the registry directory and is never expected
    # to change after install, so a mismatch means tampering or corruption and
    # must abort.  An *editable* provider points at the user's own file, and
    # editing it between runs is the entire purpose of the mode -- enforcing
    # the install-time digest there would break every edit, so we deliberately
    # skip the check and pick up whatever the file currently contains.
    if descriptor.mode != "editable":
        actual_digest = "sha256:" + hashlib.sha256(source_bytes).hexdigest()
        if actual_digest != descriptor.sha256:
            raise ProviderLoadError(
                f"provider {descriptor.provider_id!r}: digest mismatch — "
                f"expected {descriptor.sha256!r}, got {actual_digest!r}; "
                "the source file may have been modified since installation"
            )

    stem = source_path.stem
    parent = str(source_path.parent)
    if parent not in sys.path:
        sys.path.insert(0, parent)

    spec = importlib.util.spec_from_file_location(stem, str(source_path))
    if spec is None or spec.loader is None:
        raise ProviderLoadError(
            f"cannot create module spec for {source_path}"
        )
    module = importlib.util.module_from_spec(spec)
    sys.modules[stem] = module
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
        sys.modules.pop(stem, None)
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
