"""CLI entrypoint for ``datapipe``.

Commands
--------
datapipe run                Execute a Python-defined Pipeline
datapipe inspect            Inspect a Pipeline's stage structure
datapipe transform          jq-like transform expression
datapipe inspect-expression Compile an expression and show how it resolves
datapipe tools              tool provider management sub-commands
datapipe --version          Print the installed version

Logging
-------
Set ``DATAPIPE_LOG_LEVEL`` (DEBUG/INFO/WARNING/ERROR) to control CLI log
verbosity.  At INFO the run start-up summary is logged before execution.

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

_RECOGNIZED_COMMANDS = {"run", "inspect", "inspect-expression", "transform", "tools"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="datapipe",
        description=(
            "Parallel record processing pipeline.\n\n"
            "Run a Python-defined pipeline:  datapipe run pipeline.py:obj ...\n"
            "Inspect a pipeline definition:  datapipe inspect pipeline.py:obj\n"
            "Transform records (jq-like):    datapipe 'EXPR' input.jsonl output.jsonl\n"
            "Inspect an expression:          datapipe inspect-expression 'EXPR'\n\n"
            "Set DATAPIPE_LOG_LEVEL=INFO for verbose run logging."
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

    from datapipe.cli.transform import (
        add_inspect_expression_parser,
        add_transform_parser,
    )
    add_transform_parser(sub)
    add_inspect_expression_parser(sub)

    from datapipe.cli.tools import add_tools_parser
    add_tools_parser(sub)

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

    import logging as _logging
    import os as _os
    _log_level = _os.environ.get("DATAPIPE_LOG_LEVEL", "WARNING").upper()
    _logging.basicConfig(
        level=getattr(_logging, _log_level, _logging.WARNING),
        format="%(levelname)s %(name)s: %(message)s",
    )

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
        from datapipe.cli.transform import transform_command
        return transform_command(args)

    if args.command == "inspect-expression":
        from datapipe.cli.transform import inspect_expression_command
        return inspect_expression_command(args)

    if args.command == "tools":
        from datapipe.cli.tools import tools_command
        return tools_command(args)

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

    """
    if argv is None:
        argv = sys.argv[1:]
    return main(["tools", "install"] + argv)
