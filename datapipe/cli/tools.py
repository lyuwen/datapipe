"""CLI sub-commands for tool provider management.

Sub-commands
------------
datapipe tools install   Install a .py provider file
datapipe tools validate  Validate without installing
datapipe tools list      List installed providers
datapipe tools inspect   Inspect a provider or tool
datapipe tools remove    Remove an installed provider
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

def add_tools_parser(subparsers: argparse._SubParsersAction) -> None:
    """Attach the ``tools`` sub-parser tree to *subparsers*."""
    tools_p = subparsers.add_parser(
        "tools",
        help="manage installable tool providers (Phase 4)",
    )
    tools_sub = tools_p.add_subparsers(dest="tools_command")

    install_p = tools_sub.add_parser("install", help="install a tool provider")
    install_p.add_argument(
        "path", nargs="?", metavar="PATH",
        help="path to the .py provider file to install",
    )
    install_p.add_argument(
        "--editable", "-e", action="store_true",
        help="install in editable mode (live-reload on each run)",
    )
    install_p.add_argument(
        "--force", action="store_true",
        help="replace an existing provider with the same name",
    )
    install_p.add_argument(
        "--yes", "-y", action="store_true",
        help="skip the interactive confirmation prompt (for CI)",
    )

    validate_p = tools_sub.add_parser("validate", help="validate a tool provider file")
    validate_p.add_argument("path", nargs="?", metavar="PATH")

    tools_sub.add_parser("list", help="list installed tool providers")

    inspect_p = tools_sub.add_parser("inspect", help="inspect a tool or provider")
    inspect_p.add_argument("name", nargs="?", metavar="NAME")
    inspect_p.add_argument("--json", dest="as_json", action="store_true")

    remove_p = tools_sub.add_parser("remove", help="remove a tool provider")
    remove_p.add_argument("name", nargs="?", metavar="PROVIDER_OR_NAME")


# ---------------------------------------------------------------------------
# Top-level dispatcher
# ---------------------------------------------------------------------------

def tools_command(args: argparse.Namespace) -> int:
    """Dispatch to the appropriate tools sub-command."""
    cmd = getattr(args, "tools_command", None)
    if cmd is None:
        print(
            "datapipe tools: sub-command required (not yet implemented shorthand); "
            "use: datapipe tools {install,validate,list,inspect,remove}",
            file=sys.stderr,
        )
        return 2
    dispatch = {
        "install": _cmd_install,
        "validate": _cmd_validate,
        "list": _cmd_list,
        "inspect": _cmd_inspect,
        "remove": _cmd_remove,
    }
    fn = dispatch.get(cmd)
    if fn is None:
        print(f"Unknown tools sub-command: {cmd!r}", file=sys.stderr)
        return 2
    return fn(args)


# ---------------------------------------------------------------------------
# install
# ---------------------------------------------------------------------------

def _cmd_install(args: argparse.Namespace) -> int:
    if not args.path:
        print("error: PATH argument is required for 'datapipe tools install'", file=sys.stderr)
        return 1
    from datapipe.tools.installer import InstallationError, install_provider
    try:
        entry = install_provider(
            args.path,
            editable=args.editable,
            force=args.force,
            yes=args.yes,
        )
    except InstallationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    if entry is None:
        # User declined the prompt.
        return 1
    tool_count = len(entry.tools)
    noun = "tool" if tool_count == 1 else "tools"
    print(f"Installed provider {entry.provider_id!r} with {tool_count} {noun}.")
    return 0


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------

def _cmd_validate(args: argparse.Namespace) -> int:
    if not args.path:
        print("error: PATH argument is required for 'datapipe tools validate'", file=sys.stderr)
        return 1
    from datapipe.tools.validation import (
        ProviderValidationError,
        StaticValidationError,
        validate_dynamic,
        validate_static,
    )
    path = Path(args.path).resolve()
    print(f"Validating {path} ...")
    try:
        source_bytes = validate_static(path)
        print("  static validation passed")
    except StaticValidationError as exc:
        print(f"  static validation FAILED: {exc}", file=sys.stderr)
        return 1
    try:
        metadata = validate_dynamic(path, source_bytes)
        print("  dynamic validation passed")
    except ProviderValidationError as exc:
        print(f"  dynamic validation FAILED: {exc}", file=sys.stderr)
        return 1
    tool_names = [t["name"] for t in metadata.tools]
    if tool_names:
        print(f"  tools: {', '.join(tool_names)}")
    else:
        print("  tools: (none)")
    return 0


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------

def _cmd_list(args: argparse.Namespace) -> int:  # noqa: ARG001
    from datapipe.tools.registry import list_providers
    providers = list_providers()
    if not providers:
        print("No providers installed.")
        return 0
    col_id   = max(len(e.provider_id) for e in providers)
    col_alias = max(len(e.alias) for e in providers)
    col_mode  = max(len(e.mode) for e in providers)
    header = (
        f"{'PROVIDER':<{col_id}}  {'ALIAS':<{col_alias}}  "
        f"{'MODE':<{col_mode}}  {'TOOLS':>5}  INSTALLED"
    )
    print(header)
    print("-" * len(header))
    for entry in providers:
        print(
            f"{entry.provider_id:<{col_id}}  {entry.alias:<{col_alias}}  "
            f"{entry.mode:<{col_mode}}  {len(entry.tools):>5}  {entry.installed_at}"
        )
    return 0


# ---------------------------------------------------------------------------
# inspect
# ---------------------------------------------------------------------------

def _cmd_inspect(args: argparse.Namespace) -> int:
    from datapipe.tools.registry import list_providers, load_registry
    name = getattr(args, "name", None)
    as_json = getattr(args, "as_json", False)
    registry = load_registry()

    if name is None:
        # Show all providers.
        data = [
            {
                "provider_id": e.provider_id,
                "alias": e.alias,
                "mode": e.mode,
                "source_path": e.source_path,
                "digest": e.digest,
                "installed_at": e.installed_at,
                "datapipe_api": e.datapipe_api,
                "tools": list(e.tools.keys()),
            }
            for e in sorted(registry.providers.values(), key=lambda x: x.provider_id)
        ]
        if as_json:
            print(json.dumps(data, indent=2))
        else:
            for item in data:
                print(f"provider_id:   {item['provider_id']}")
                print(f"alias:         {item['alias']}")
                print(f"mode:          {item['mode']}")
                print(f"source_path:   {item['source_path']}")
                print(f"digest:        {item['digest']}")
                print(f"installed_at:  {item['installed_at']}")
                print(f"tools:         {', '.join(item['tools']) or '(none)'}")
                print()
        return 0

    # Check built-in tools first so they are always inspectable.
    _BUILTIN_TOOL_NAMES = {"fromjson", "tojson"}
    if name in _BUILTIN_TOOL_NAMES:
        from datapipe.tools.decorator import get_contract
        from datapipe.tools.builtins.json import fromjson, tojson
        _builtins = {"fromjson": fromjson, "tojson": tojson}
        fn = _builtins[name]
        contract = get_contract(fn)
        if contract is None:
            print(f"error: built-in {name!r} has no contract", file=sys.stderr)
            return 1
        from datapipe.tools.types import describe
        data_tool: dict = {
            "provider_id": "builtin:json",
            "tool": {
                "name": contract.name,
                "target": contract.target,
                "cardinality": contract.cardinality.value,
                "deterministic": contract.deterministic,
                "description": contract.description,
                "input": describe(contract.input_type),
                "output": describe(contract.output_type),
                "parameters": [
                    {"name": p.name, "default": p.default, "required": p.required}
                    for p in contract.parameters
                ],
            },
        }
        if as_json:
            print(json.dumps(data_tool, indent=2))
        else:
            t = data_tool["tool"]
            print(f"tool:          {t['name']}")
            print(f"provider:      builtin:json")
            print(f"target:        {t['target']}")
            print(f"cardinality:   {t['cardinality']}")
            print(f"deterministic: {t['deterministic']}")
            print(f"input:         {t['input']}")
            print(f"output:        {t['output']}")
            if t.get("description"):
                print(f"description:   {t['description']}")
            if t["parameters"]:
                print("parameters:")
                for p in t["parameters"]:
                    print(f"  {p['name']} (default={p['default']!r})")
        return 0

    # Try as provider_id first, then as alias, then as tool name.
    entry = registry.providers.get(name)
    if entry is None:
        for e in registry.providers.values():
            if e.alias == name:
                entry = e
                break
    if entry is not None:
        tool_contract = None
    else:
        # Search for a tool name across all providers.
        entry = None
        tool_contract = None
        for e in registry.providers.values():
            if name in e.tools:
                entry = e
                tool_contract = e.tools[name]
                break

    if entry is None:
        print(f"error: no provider or tool found with name {name!r}", file=sys.stderr)
        return 1

    if tool_contract is not None:
        data_tc: dict = {"provider_id": entry.provider_id, "tool": tool_contract}
        if as_json:
            print(json.dumps(data_tc, indent=2))
        else:
            print(f"tool:          {tool_contract.get('name', name)}")
            print(f"provider:      {entry.provider_id}")
            print(f"target:        {tool_contract.get('target', '?')}")
            print(f"cardinality:   {tool_contract.get('cardinality', '?')}")
            print(f"deterministic: {tool_contract.get('deterministic', '?')}")
            desc = tool_contract.get("description", "")
            if desc:
                print(f"description:   {desc}")
            params = tool_contract.get("parameters", [])
            if params:
                print("parameters:")
                for p in params:
                    print(f"  {p['name']} (default={p['default']!r})")
        return 0

    data_p: dict = {
        "provider_id": entry.provider_id,
        "alias": entry.alias,
        "mode": entry.mode,
        "source_path": entry.source_path,
        "digest": entry.digest,
        "installed_at": entry.installed_at,
        "datapipe_api": entry.datapipe_api,
        "tools": entry.tools,
    }
    if as_json:
        print(json.dumps(data_p, indent=2))
    else:
        print(f"provider_id:   {entry.provider_id}")
        print(f"alias:         {entry.alias}")
        print(f"mode:          {entry.mode}")
        print(f"source_path:   {entry.source_path}")
        print(f"digest:        {entry.digest}")
        print(f"installed_at:  {entry.installed_at}")
        print(f"datapipe_api:  {entry.datapipe_api}")
        print(f"tools:         {', '.join(entry.tools) or '(none)'}")
    return 0


# ---------------------------------------------------------------------------
# remove
# ---------------------------------------------------------------------------

def _cmd_remove(args: argparse.Namespace) -> int:
    name = getattr(args, "name", None)
    if not name:
        print("error: PROVIDER_OR_NAME argument is required for 'datapipe tools remove'",
              file=sys.stderr)
        return 1
    from datapipe.tools.installer import InstallationError, remove_provider
    from datapipe.tools.registry import load_registry
    # Resolve alias to provider_id if necessary.
    registry = load_registry()
    provider_id = name
    if name not in registry.providers:
        for entry in registry.providers.values():
            if entry.alias == name:
                provider_id = entry.provider_id
                break
    try:
        remove_provider(provider_id)
    except InstallationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"Removed provider {provider_id!r}.")
    return 0
