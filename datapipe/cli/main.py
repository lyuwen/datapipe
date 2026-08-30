"""CLI entrypoint for ``datapipe``.

Commands
--------
datapipe run       Execute a Python-defined Pipeline
datapipe inspect   Inspect a Pipeline's stage structure
datapipe transform [stub] jq-like transform expression  (Phase 3)
datapipe tools     [stub] tool management sub-commands   (Phase 4)
datapipe --version Print the installed version

The shorthand positional form (§3.1 of the CLI plan) dispatches to
``transform`` when the first argument is neither a recognized sub-command
nor a flag:

    datapipe 'expression' input.jsonl output.jsonl
"""

from __future__ import annotations

import argparse
import sys


# ---------------------------------------------------------------------------
# Parser construction
# ---------------------------------------------------------------------------

_RECOGNIZED_COMMANDS = {"run", "inspect", "transform", "tools"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="datapipe",
        description=(
            "Parallel record processing pipeline.\n\n"
            "Run a Python-defined pipeline:  datapipe run pipeline.py:obj ...\n"
            "Inspect a pipeline definition:  datapipe inspect pipeline.py:obj\n"
            "Transform records (jq-like):    datapipe 'EXPR' input.jsonl output.jsonl"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--version", action="store_true", help="show version and exit"
    )

    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    # -- run & inspect -------------------------------------------------------
    from datapipe.cli.run import add_run_parser
    from datapipe.cli.inspect import add_inspect_parser

    add_run_parser(sub)
    add_inspect_parser(sub)

    # -- transform (Phase 3 stub) --------------------------------------------
    transform_p = sub.add_parser(
        "transform",
        help="apply a jq-like expression to records (Phase 3)",
        description=(
            "Apply a jq-like transform expression to a JSONL source.\n\n"
            "This command is not yet implemented. See the CLI plan for the\n"
            "full expression language specification."
        ),
    )
    transform_p.add_argument("expression", nargs="?", help="transform expression")
    transform_p.add_argument("input", nargs="?", help="input path")
    transform_p.add_argument("output", nargs="?", help="output path")

    # -- tools (Phase 4 stub) ------------------------------------------------
    tools_p = sub.add_parser(
        "tools",
        help="manage installable tool providers (Phase 4)",
    )
    tools_sub = tools_p.add_subparsers(dest="tools_command")
    tools_sub.add_parser("install", help="install a tool provider")
    tools_sub.add_parser("validate", help="validate a tool provider file")
    tools_sub.add_parser("list", help="list installed tool providers")
    tools_sub.add_parser("inspect", help="inspect a tool or provider")
    tools_sub.add_parser("remove", help="remove a tool provider")

    return parser


# ---------------------------------------------------------------------------
# Shorthand dispatch: treat unknown first positional as an expression
# ---------------------------------------------------------------------------

def _is_expression_shorthand(argv: list[str]) -> bool:
    """Return True when argv looks like the shorthand transform form.

    The shorthand is active when the first non-flag token is not a recognized
    sub-command, implying the user typed an expression directly:

        datapipe 'expr' input.jsonl output.jsonl
    """
    for token in argv:
        if token.startswith("-"):
            continue
        return token not in _RECOGNIZED_COMMANDS
    return False


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    # Shorthand: datapipe EXPRESSION ... → datapipe transform EXPRESSION ...
    if argv and _is_expression_shorthand(argv):
        argv = ["transform"] + argv

    from datapipe import __version__

    parser = build_parser()
    args = parser.parse_args(argv)

    if args.version:
        print(f"datapipe {__version__}")
        return 0

    if args.command is None:
        parser.print_help()
        return 0

    if args.command == "run":
        from datapipe.cli.run import run_command
        return run_command(args)

    if args.command == "inspect":
        from datapipe.cli.inspect import inspect_command
        return inspect_command(args)

    if args.command == "transform":
        print(
            "datapipe transform is not yet implemented (Phase 3 of the CLI plan).\n"
            "Use 'datapipe run pipeline.py:pipeline ...' for a Python-defined pipeline.",
            file=sys.stderr,
        )
        return 2

    if args.command == "tools":
        print(
            "datapipe tools is not yet implemented (Phase 4 of the CLI plan).",
            file=sys.stderr,
        )
        return 2

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())


def install_main(argv: list[str] | None = None) -> int:
    """Entry point for the ``datapipe-install`` alias.

    Equivalent to ``datapipe tools install <args>``.  This is a thin shim so
    that::

        datapipe-install --editable ./my_tools.py

    works identically to::

        datapipe tools install --editable ./my_tools.py

    The actual implementation lives in Phase 4.
    """
    if argv is None:
        argv = sys.argv[1:]
    return main(["tools", "install"] + argv)
