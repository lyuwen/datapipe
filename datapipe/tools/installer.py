"""Provider installation and removal.

``install_provider`` validates, optionally copies, and registers a .py file
as an installable tool provider.  ``remove_provider`` reverses that.
"""

from __future__ import annotations

import json
import re
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

from datapipe.tools.registry import (
    ProviderEntry,
    add_provider,
    load_registry,
    provider_dir,
    remove_provider as _registry_remove,
)
from datapipe.tools.validation import (
    ProviderValidationError,
    StaticValidationError,
    compute_digest,
    validate_dynamic,
    validate_static,
)

# Built-in names that may never be used as provider aliases.
_RESERVED_ALIASES = frozenset({"fromjson", "tojson"})


class InstallationError(Exception):
    """Raised when provider installation cannot proceed."""


def install_provider(
    path: "str | Path",
    *,
    editable: bool = False,
    force: bool = False,
    yes: bool = False,
    registry_dir: "Path | None" = None,  # noqa: ARG001 — reserved for tests
) -> "ProviderEntry | None":
    """Validate, prompt, and register a tool provider.

    Returns the new :class:`~datapipe.tools.registry.ProviderEntry` on
    success, or ``None`` when the user declines the confirmation prompt.
    Raises :class:`InstallationError` on any hard failure.
    """
    path = Path(path).resolve()

    # --- Static validation --------------------------------------------------
    try:
        source_bytes = validate_static(path)
    except StaticValidationError as exc:
        raise InstallationError(str(exc)) from exc

    digest = compute_digest(source_bytes)

    # --- Derive IDs ---------------------------------------------------------
    stem = path.stem
    # Sanitise non-identifier characters for the provider_id slug.
    slug = re.sub(r"[^a-zA-Z0-9_]", "-", stem)
    provider_id = f"local:{slug}"
    alias = stem

    if not alias.isidentifier():
        raise InstallationError(
            f"provider alias {alias!r} (derived from filename stem) is not a "
            "valid Python identifier; rename the file"
        )

    if alias in _RESERVED_ALIASES:
        raise InstallationError(
            f"alias {alias!r} is a built-in name and cannot be used for a "
            "provider even with --force"
        )

    # --- Conflict checks ----------------------------------------------------
    registry = load_registry()

    if provider_id in registry.providers and not force:
        raise InstallationError(
            f"provider {provider_id!r} is already installed; "
            "use --force to replace it"
        )

    for existing in registry.providers.values():
        if existing.alias == alias and existing.provider_id != provider_id and not force:
            raise InstallationError(
                f"ambiguous name: alias {alias!r} is already used by provider "
                f"{existing.provider_id!r}; use --force to replace it"
            )

    # --- Dynamic validation -------------------------------------------------
    try:
        metadata = validate_dynamic(path, source_bytes)
    except ProviderValidationError as exc:
        raise InstallationError(str(exc)) from exc

    tool_names = [t["name"] for t in metadata.tools]

    # --- Confirmation prompt ------------------------------------------------
    if not yes:
        mode_label = "editable" if editable else "copied"
        print(f"Provider: {provider_id}")
        print(f"Source:   {path}")
        print(f"Mode:     {mode_label}")
        print(f"Tools:    {', '.join(tool_names) if tool_names else '(none)'}")
        print()
        print(
            "This provider contains executable Python and will run inside "
            "datapipe workers\nwith your user permissions. Install? [y/N] ",
            end="",
            flush=True,
        )
        answer = sys.stdin.readline().strip()
        if answer not in ("y", "Y"):
            print("Installation cancelled.")
            return None

    # --- Copy or editable install -------------------------------------------
    import json as _json
    pdir = provider_dir(provider_id)
    # Record whether the provider directory already existed.  A rollback must
    # remove a directory this install created, otherwise a failed first-time
    # install leaves an empty directory behind that later reads mistake for a
    # real (but empty) installation.
    pdir_preexisted = pdir.exists()
    pdir.mkdir(parents=True, exist_ok=True)

    # For a transactional install: back up any pre-existing files so that if
    # the registry update fails we can restore them and leave the installation
    # in its previous (valid) state rather than a partially-overwritten one.
    backups: dict[Path, bytes | None] = {}  # path → original bytes (None = did not exist)
    files_written: list[Path] = []          # paths we created or overwrote

    def _backup(p: Path) -> None:
        """Record the original bytes of *p* before we write to it."""
        if p not in backups:
            backups[p] = p.read_bytes() if p.exists() else None

    def _rollback() -> None:
        """Restore every touched path to its pre-install state."""
        for p in files_written:
            original = backups.get(p)
            try:
                if original is None:
                    p.unlink(missing_ok=True)
                else:
                    p.write_bytes(original)
            except OSError:
                pass
        # Remove the provider directory only if this install created it and
        # nothing else has since been written into it.
        if not pdir_preexisted:
            try:
                pdir.rmdir()
            except OSError:
                pass  # non-empty or already gone — leave it alone

    if editable:
        source_path = str(path)
    else:
        dest = pdir / "source.py"
        _backup(dest)
        dest.write_bytes(source_bytes)
        files_written.append(dest)
        source_path = str(dest)

    # --- Build and register entry -------------------------------------------
    installed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    entry = ProviderEntry(
        provider_id=provider_id,
        alias=alias,
        mode="editable" if editable else "copied",
        source_path=source_path,
        digest=digest,
        installed_at=installed_at,
        datapipe_api=1,
        tools={t["name"]: t for t in metadata.tools},
    )

    # Write a provider.json alongside the source (even for editable).
    provider_json = pdir / "provider.json"
    _backup(provider_json)
    try:
        provider_json.write_text(
            _json.dumps(
                {
                    "provider_id": entry.provider_id,
                    "alias": entry.alias,
                    "mode": entry.mode,
                    "source_path": entry.source_path,
                    "digest": entry.digest,
                    "installed_at": entry.installed_at,
                    "datapipe_api": entry.datapipe_api,
                    "tools": entry.tools,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
    except Exception:
        _rollback()
        raise
    files_written.append(provider_json)

    try:
        add_provider(entry)
    except Exception:
        # Registry update failed — restore everything to its pre-install state.
        _rollback()
        raise

    return entry


def remove_provider(provider_id: str) -> None:
    """Remove a provider from the registry and (for copied mode) delete its files.

    Raises :class:`InstallationError` when the provider is not installed.
    """
    registry = load_registry()
    entry = registry.providers.get(provider_id)
    if entry is None:
        raise InstallationError(f"provider {provider_id!r} is not installed")

    try:
        _registry_remove(provider_id)
    except KeyError:
        pass  # already gone

    if entry.mode == "copied":
        pdir = provider_dir(provider_id)
        if pdir.exists():
            shutil.rmtree(pdir, ignore_errors=True)
