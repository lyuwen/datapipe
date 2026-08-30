"""Persistent JSON registry for installed tool providers.

The registry lives at ``<data_dir>/registry.json`` where *data_dir* is
resolved from ``DATAPIPE_USER_DATA`` or ``~/.local/share/datapipe``.
Provider source snapshots for copied installations are stored under
``<data_dir>/providers/<provider-id>/``.

All writes are atomic (write-to-tmp → fsync → rename) and protected against
concurrent corruption via ``fcntl.flock``.
"""

from __future__ import annotations

import fcntl
import json
import os
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path


# ---------------------------------------------------------------------------
# Directory resolution
# ---------------------------------------------------------------------------

def _registry_dir() -> Path:
    """Return the user data directory (not created here)."""
    base = os.environ.get("DATAPIPE_USER_DATA")
    if base:
        return Path(base)
    return Path.home() / ".local" / "share" / "datapipe"


def _registry_path() -> Path:
    return _registry_dir() / "registry.json"


def provider_dir(provider_id: str) -> Path:
    """Return ``<data_dir>/providers/<provider-id>/``."""
    return _registry_dir() / "providers" / provider_id


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

_SCHEMA_VERSION = 1


@dataclass
class ProviderEntry:
    """Metadata for one installed provider."""

    provider_id: str
    alias: str
    mode: str                       # "copied" | "editable"
    source_path: str
    digest: str                     # "sha256:<hexdigest>"
    installed_at: str               # ISO-8601 UTC
    datapipe_api: int
    tools: dict[str, dict] = field(default_factory=dict)


@dataclass
class RegistryData:
    """Top-level registry document."""

    schema_version: int = _SCHEMA_VERSION
    providers: dict[str, ProviderEntry] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------

def _entry_to_dict(entry: ProviderEntry) -> dict:
    return {
        "provider_id": entry.provider_id,
        "alias": entry.alias,
        "mode": entry.mode,
        "source_path": entry.source_path,
        "digest": entry.digest,
        "installed_at": entry.installed_at,
        "datapipe_api": entry.datapipe_api,
        "tools": entry.tools,
    }


def _entry_from_dict(d: dict) -> ProviderEntry:
    return ProviderEntry(
        provider_id=d["provider_id"],
        alias=d["alias"],
        mode=d["mode"],
        source_path=d["source_path"],
        digest=d["digest"],
        installed_at=d["installed_at"],
        datapipe_api=d["datapipe_api"],
        tools=d.get("tools", {}),
    )


def _registry_to_dict(data: RegistryData) -> dict:
    return {
        "schema_version": data.schema_version,
        "providers": {k: _entry_to_dict(v) for k, v in data.providers.items()},
    }


def _registry_from_dict(d: dict) -> RegistryData:
    providers = {k: _entry_from_dict(v) for k, v in d.get("providers", {}).items()}
    return RegistryData(
        schema_version=d.get("schema_version", _SCHEMA_VERSION),
        providers=providers,
    )


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------

def load_registry() -> RegistryData:
    """Load the registry from disk; return an empty registry if absent.

    Emits a warning to stderr when the file exists but cannot be parsed, so
    the user knows the registry is being treated as empty rather than silently
    losing all installed providers.
    """
    path = _registry_path()
    if not path.exists():
        return RegistryData()
    try:
        text = path.read_text(encoding="utf-8")
        return _registry_from_dict(json.loads(text))
    except (json.JSONDecodeError, KeyError, TypeError, OSError) as exc:
        print(
            f"warning: registry at {path} is corrupt or unreadable ({exc}); "
            "treating as empty — reinstall providers if needed",
            file=sys.stderr,
        )
        return RegistryData()


def save_registry(data: RegistryData) -> None:
    """Write *data* atomically to the registry file.

    Uses a lock file in the same directory to serialise concurrent writers,
    writes to a sibling temp file, fsyncs, then renames into place.
    """
    reg_path = _registry_path()
    reg_path.parent.mkdir(parents=True, exist_ok=True)

    lock_path = reg_path.with_suffix(".lock")
    payload = json.dumps(_registry_to_dict(data), indent=2, ensure_ascii=False)

    with open(lock_path, "a", encoding="utf-8") as lock_fh:
        fcntl.flock(lock_fh, fcntl.LOCK_EX)
        try:
            fd, tmp_path = tempfile.mkstemp(
                dir=reg_path.parent, prefix=".registry-", suffix=".tmp"
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as fh:
                    fh.write(payload)
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(tmp_path, reg_path)
            except BaseException:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        finally:
            fcntl.flock(lock_fh, fcntl.LOCK_UN)


# ---------------------------------------------------------------------------
# High-level accessors
# ---------------------------------------------------------------------------

def get_provider(provider_id: str) -> ProviderEntry | None:
    """Return the entry for *provider_id*, or None if not installed."""
    return load_registry().providers.get(provider_id)


def list_providers() -> list[ProviderEntry]:
    """Return all installed providers sorted by provider_id."""
    return sorted(load_registry().providers.values(), key=lambda e: e.provider_id)


def add_provider(entry: ProviderEntry) -> None:
    """Add or replace *entry* in the registry atomically.

    The read-modify-write is performed entirely inside the exclusive file lock
    so two concurrent installers cannot each read the same old registry and
    then silently overwrite each other's update.
    """
    reg_path = _registry_path()
    reg_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = reg_path.with_suffix(".lock")

    with open(lock_path, "a", encoding="utf-8") as lock_fh:
        fcntl.flock(lock_fh, fcntl.LOCK_EX)
        try:
            # Read while holding the lock so no concurrent writer can sneak in.
            if reg_path.exists():
                try:
                    text = reg_path.read_text(encoding="utf-8")
                    data = _registry_from_dict(json.loads(text))
                except (json.JSONDecodeError, KeyError, TypeError, OSError):
                    data = RegistryData()
            else:
                data = RegistryData()

            data.providers[entry.provider_id] = entry
            payload = json.dumps(_registry_to_dict(data), indent=2, ensure_ascii=False)

            fd, tmp_path = tempfile.mkstemp(
                dir=reg_path.parent, prefix=".registry-", suffix=".tmp"
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as fh:
                    fh.write(payload)
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(tmp_path, reg_path)
            except BaseException:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        finally:
            fcntl.flock(lock_fh, fcntl.LOCK_UN)


def remove_provider(provider_id: str) -> None:
    """Remove *provider_id* from the registry atomically.

    The read-modify-write is performed entirely inside the exclusive file lock.
    Raises ``KeyError`` if the provider is not installed.
    """
    reg_path = _registry_path()
    reg_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = reg_path.with_suffix(".lock")

    with open(lock_path, "a", encoding="utf-8") as lock_fh:
        fcntl.flock(lock_fh, fcntl.LOCK_EX)
        try:
            if reg_path.exists():
                try:
                    text = reg_path.read_text(encoding="utf-8")
                    data = _registry_from_dict(json.loads(text))
                except (json.JSONDecodeError, KeyError, TypeError, OSError):
                    data = RegistryData()
            else:
                data = RegistryData()

            if provider_id not in data.providers:
                raise KeyError(provider_id)
            del data.providers[provider_id]
            payload = json.dumps(_registry_to_dict(data), indent=2, ensure_ascii=False)

            fd, tmp_path = tempfile.mkstemp(
                dir=reg_path.parent, prefix=".registry-", suffix=".tmp"
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as fh:
                    fh.write(payload)
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(tmp_path, reg_path)
            except BaseException:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        finally:
            fcntl.flock(lock_fh, fcntl.LOCK_UN)
