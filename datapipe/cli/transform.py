"""``datapipe transform``: compile a DSL expression and run it over records.

Implements the user-facing transform command from §3.1 of the CLI plan:

    datapipe transform [OPTIONS] EXPRESSION INPUT OUTPUT
    datapipe [OPTIONS] EXPRESSION INPUT OUTPUT          # shorthand

The expression is compiled to a ``CompiledToolProgramStage``, wrapped in a
``Pipeline``, and executed via the same ``Pipeline.run()`` path as
``datapipe run``.  No separate executor or IO adapters are added — the
transform command is purely a frontend over the existing runtime.

Implicit outer JSON load/dump (§6.3 of the CLI plan):
  The source is opened in raw mode so each worker receives an unparsed
  JSON line string.  A ``JsonLoadStage`` is prepended and a ``JsonDumpStage``
  is appended around the compiled tool program so that:

    raw JSONL line → JsonLoadStage → tool program → JsonDumpStage → raw line

  This means the tool program always operates on Python dicts/lists, and the
  output is always a valid JSONL line.  Users should NOT include
  fromjson(.) or tojson(.) for the outer row in their expression.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import argparse


# ---------------------------------------------------------------------------
# Argument-parser fragment (registered by main.py)
# ---------------------------------------------------------------------------


def add_transform_parser(subparsers) -> None:
    """Attach the ``transform`` sub-command to *subparsers*."""
    p = subparsers.add_parser(
        "transform",
        help="apply a jq-like expression to JSONL records",
        description=(
            "Compile a jq-like expression and apply it to every record in a\n"
            "JSONL file.  Outer JSON parsing and serialization happen inside\n"
            "workers automatically — do not include fromjson(.) or tojson(.)\n"
            "for the outer row.\n\n"
            "Examples:\n"
            "  datapipe transform 'fromjson(.tools)' in.jsonl out.jsonl\n"
            "  datapipe 'fromjson(.tools) | tojson(.tools[].name)' in.jsonl out.jsonl"
        ),
        formatter_class=__import__("argparse").RawDescriptionHelpFormatter,
    )

    # Positional arguments
    p.add_argument("expression", help="jq-like transform expression")
    p.add_argument("input", help="input JSONL file or directory")
    p.add_argument("output", help="output JSONL file or directory")

    # Error handling
    p.add_argument(
        "--error-output", dest="error_output",
        metavar="PATH",
        help="JSONL file to write per-record error payloads to",
    )
    p.add_argument(
        "--errors", choices=["raise", "skip", "return"], default="raise",
        help="per-record error policy (default: raise)",
    )

    # Executor
    p.add_argument(
        "--executor", choices=["process", "thread", "sequential"],
        default="process",
        help="execution backend (default: process)",
    )
    p.add_argument(
        "--workers", type=int, default=None, metavar="N",
        help="number of worker processes/threads (default: CPU count)",
    )
    p.add_argument(
        "--max-in-flight", dest="max_in_flight", type=int, default=None,
        metavar="N",
        help="maximum in-flight futures (default: workers × 4)",
    )

    # Ordering
    ordering = p.add_mutually_exclusive_group()
    ordering.add_argument(
        "--ordered", dest="ordered", action="store_true", default=True,
        help="preserve input order (default)",
    )
    ordering.add_argument(
        "--unordered", dest="ordered", action="store_false",
        help="emit results in completion order",
    )

    # Progress
    progress = p.add_mutually_exclusive_group()
    progress.add_argument(
        "--progress", dest="progress", action="store_true", default=True,
        help="show progress bar (default)",
    )
    progress.add_argument(
        "--no-progress", dest="progress", action="store_false",
        help="suppress progress bar",
    )

    # Distributed runtime
    p.add_argument("--rank", type=int, default=None)
    p.add_argument(
        "--world-size", dest="world_size", type=int, default=None, metavar="N"
    )
    p.add_argument("--local-rank", dest="local_rank", type=int, default=None)

    # Validation mode (§10.3 of the CLI plan)
    p.add_argument(
        "--validate-tools",
        choices=["always", "sample", "off"],
        default="always",
        dest="validate_tools",
        help="runtime type-contract validation mode (default: always)",
    )

    # Expression inspection without running data
    p.add_argument(
        "--dry-run", dest="dry_run", action="store_true",
        help=(
            "compile the expression and print the resulting pipeline stages "
            "without opening the input or running any records"
        ),
    )


# ---------------------------------------------------------------------------
# Command implementation
# ---------------------------------------------------------------------------


def transform_command(args: "argparse.Namespace") -> int:
    """Execute the ``transform`` sub-command.  Returns an exit code."""

    # 1. Compile the expression — do this before opening any files so
    #    invalid expressions fail immediately.
    try:
        from datapipe.dsl.compiler import compile_expression
        from datapipe.dsl.errors import (
            ExpressionSyntaxError,
            ToolConfigurationError,
            ToolResolutionError,
        )
        compiled = compile_expression(args.expression)
    except (ExpressionSyntaxError, ToolResolutionError, ToolConfigurationError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"error compiling expression: {exc}", file=sys.stderr)
        return 1

    # 2. Dry-run: print the pipeline stages and exit.
    if getattr(args, "dry_run", False):
        _print_compiled(compiled, args.expression)
        return 0

    # 3. Build the pipeline: JsonLoadStage + tool program + JsonDumpStage.
    try:
        pipeline = _build_pipeline(
            compiled, validate=getattr(args, "validate_tools", "always")
        )
    except Exception as exc:  # noqa: BLE001
        print(f"error building pipeline: {exc}", file=sys.stderr)
        return 1

    # 4. Open source and sink in raw mode (workers handle JSON parse/dump).
    try:
        from datapipe.io.jsonl import JsonlSink, JsonlSource
        source = JsonlSource(args.input, raw=True)
        sink = JsonlSink(args.output, raw=True)
    except Exception as exc:  # noqa: BLE001
        print(f"error: {exc}", file=sys.stderr)
        return 1

    error_sink = None
    if args.error_output:
        from datapipe.io.jsonl import JsonlSink as _JS
        error_sink = _JS(args.error_output)

    # 5. Build executor and runtime (inside error boundary).
    try:
        from datapipe.cli.run import _build_executor, _build_runtime
        executor = _build_executor(args.executor, args.workers, args.max_in_flight)
        runtime = _build_runtime(args)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    # 6. Run.
    try:
        stats = pipeline.run(
            source=source,
            sink=sink,
            executor=executor,
            runtime=runtime,
            ordered=args.ordered,
            progress=args.progress,
            errors=args.errors,
            error_sink=error_sink,
            max_in_flight=args.max_in_flight,
        )
    except KeyboardInterrupt:
        print("\ninterrupted", file=sys.stderr)
        return 130
    except Exception as exc:  # noqa: BLE001
        print(f"error: {exc}", file=sys.stderr)
        return 1

    # 7. Print summary.
    _print_stats(stats)
    return 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_pipeline(compiled, validate: str = "always"):
    """Wrap a compiled expression in a Pipeline with outer JSON stages."""
    from datapipe.pipeline import Pipeline
    from datapipe.stage import JsonDumpStage, JsonLoadStage
    from datapipe.stages.tool_program import CompiledToolProgramStage

    return Pipeline([
        JsonLoadStage(),
        CompiledToolProgramStage(compiled, validate=validate),
        JsonDumpStage(),
    ])


def _print_compiled(compiled, expression: str) -> None:
    """Print a human-readable description of the compiled expression."""
    print(f"Expression: {expression!r}")
    print(f"Invocations: {len(compiled.invocations)}")
    for i, inv in enumerate(compiled.invocations):
        sel_str = inv.selector.render()
        args_str = (
            ", ".join(f"{k}={v!r}" for k, v in inv.arguments.items())
            if inv.arguments
            else "(defaults)"
        )
        print(f"  [{i}] {inv.tool_name}({sel_str})  args={args_str}")


def _print_stats(stats) -> None:
    elapsed = getattr(stats, "elapsed_seconds", None)
    rate = getattr(stats, "records_per_second", None)
    completed = getattr(stats, "completed_records", "?")
    failed = getattr(stats, "failed_records", 0)
    dropped = getattr(stats, "dropped_records", 0)
    parts = [f"completed={completed}"]
    if failed:
        parts.append(f"failed={failed}")
    if dropped:
        parts.append(f"dropped={dropped}")
    if elapsed is not None:
        parts.append(f"elapsed={elapsed:.2f}s")
    if rate is not None:
        parts.append(f"rate={rate:.0f} rec/s")
    print("  ".join(parts))
