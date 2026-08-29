"""CLI entrypoint (skeleton for Phase 4).

Provides ``datapipe run`` and ``datapipe inspect`` scaffolding. Full CLI
implementation is a later phase; this module keeps the package layout honest
and exposes a working ``--version``.
"""

from __future__ import annotations

import argparse
import sys


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="datapipe")
    parser.add_argument(
        "--version", action="store_true", help="show version and exit"
    )
    sub = parser.add_subparsers(dest="command")

    run_p = sub.add_parser("run", help="run a pipeline definition")
    run_p.add_argument("pipeline_ref", help="module:object or file.py:object")
    run_p.add_argument("--input", help="input path or jsonl:path")
    run_p.add_argument("--output", help="output path or jsonl:path")
    run_p.add_argument("--workers", type=int, default=None)
    run_p.add_argument("--max-in-flight", type=int, default=None)

    inspect_p = sub.add_parser("inspect", help="inspect a pipeline definition")
    inspect_p.add_argument("pipeline_ref", help="module:object or file.py:object")

    return parser


def main(argv: list[str] | None = None) -> int:
    from datapipe import __version__

    parser = build_parser()
    args = parser.parse_args(argv)
    if args.version:
        print(f"datapipe {__version__}")
        return 0
    if args.command is None:
        parser.print_help()
        return 0
    parser.error(
        f"command {args.command!r} is not implemented yet (Phase 4). "
        "Use the Python API: Pipeline([...]).run(...)"
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
