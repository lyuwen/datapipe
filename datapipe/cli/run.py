"""``datapipe run``: load a Pipeline definition and execute it.

Supports the full set of execution flags from the architecture plan (§26.1):

    datapipe run ./pipeline.py:pipeline \\
      --source jsonl:input.jsonl \\
      --sink parquet:output/ \\
      --workers 32 \\
      --max-in-flight 128 \\
      --executor process \\
      --ordered \\
      --errors skip \\
      --error-output errors.jsonl \\
      --rank 0 --world-size 1

Source and sink may also be given positionally via ``--input``/``--output`` or
the legacy bare ``--source``/``--sink`` flags with optional ``format:path``
prefixes.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from datapipe.cli.loaders import PipelineLoadError, load_pipeline_ref

if TYPE_CHECKING:
    import argparse


# ---------------------------------------------------------------------------
# Argument-parser fragment (registered by main.py)
# ---------------------------------------------------------------------------


def add_run_parser(subparsers) -> None:
    """Attach the ``run`` sub-command to *subparsers*."""
    p = subparsers.add_parser(
        "run",
        help="run a pipeline definition",
        description=(
            "Load a Python-defined Pipeline and execute it over an input "
            "source, writing results to an output sink."
        ),
    )
    p.add_argument(
        "pipeline_ref",
        help="'module.submodule:object' or './file.py:object'",
    )

    # -- IO -------------------------------------------------------------------
    p.add_argument(
        "--source", "--input", dest="source",
        metavar="[FORMAT:]PATH",
        help=(
            "input source; optionally prefix with 'jsonl:', 'parquet:', or "
            "'csv:' to force the format (default: inferred from extension)"
        ),
    )
    p.add_argument(
        "--sink", "--output", dest="sink",
        metavar="[FORMAT:]PATH",
        help=(
            "output sink; optionally prefix with 'jsonl:', 'parquet:', or "
            "'csv:' to force the format (default: inferred from extension)"
        ),
    )
    p.add_argument(
        "--error-output", dest="error_output",
        metavar="PATH",
        help="JSONL file to write per-record error payloads to",
    )

    # -- Executor -------------------------------------------------------------
    p.add_argument(
        "--executor", choices=["process", "thread", "sequential"],
        default="process",
        help="execution backend (default: process)",
    )
    p.add_argument(
        "--workers", type=int, default=None,
        metavar="N",
        help="number of worker processes/threads (default: CPU count)",
    )
    p.add_argument(
        "--max-in-flight", dest="max_in_flight", type=int, default=None,
        metavar="N",
        help="maximum number of in-flight futures (default: workers × 4)",
    )

    # -- Ordering & error policy ----------------------------------------------
    ordering = p.add_mutually_exclusive_group()
    ordering.add_argument(
        "--ordered", dest="ordered", action="store_true", default=True,
        help="preserve input order in the output (default)",
    )
    ordering.add_argument(
        "--unordered", dest="ordered", action="store_false",
        help="emit results in completion order (faster for skewed workloads)",
    )
    p.add_argument(
        "--errors", choices=["raise", "skip", "return"], default="raise",
        help="per-record error policy (default: raise)",
    )

    # -- Progress -------------------------------------------------------------
    progress = p.add_mutually_exclusive_group()
    progress.add_argument(
        "--progress", dest="progress", action="store_true", default=True,
        help="show progress bar (default)",
    )
    progress.add_argument(
        "--no-progress", dest="progress", action="store_false",
        help="suppress the progress bar",
    )

    # -- Distributed runtime --------------------------------------------------
    p.add_argument(
        "--rank", type=int, default=None,
        help="this process's rank (overrides environment detection)",
    )
    p.add_argument(
        "--world-size", dest="world_size", type=int, default=None,
        metavar="N",
        help="total number of ranks (overrides environment detection)",
    )
    p.add_argument(
        "--local-rank", dest="local_rank", type=int, default=None,
        help="local rank on this node (overrides environment detection)",
    )


# ---------------------------------------------------------------------------
# Command implementation
# ---------------------------------------------------------------------------


def run_command(args: "argparse.Namespace") -> int:
    """Execute the ``run`` sub-command.  Returns an exit code."""
    # --- 1. Load the pipeline -----------------------------------------------
    try:
        pipeline = load_pipeline_ref(args.pipeline_ref)
    except PipelineLoadError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    from datapipe.pipeline import Pipeline

    if not isinstance(pipeline, Pipeline):
        print(
            f"error: {args.pipeline_ref!r} resolved to "
            f"{type(pipeline).__name__}, expected a Pipeline instance",
            file=sys.stderr,
        )
        return 1

    # --- 2. Build source / sink ---------------------------------------------
    try:
        source = _resolve_source(args.source) if args.source else None
        sink = _resolve_sink(args.sink) if args.sink else None
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if source is None:
        print("error: --source / --input is required", file=sys.stderr)
        return 1
    if sink is None:
        print("error: --sink / --output is required", file=sys.stderr)
        return 1

    # --- 3. Build error sink ------------------------------------------------
    error_sink = None
    if args.error_output:
        from datapipe.io.jsonl import JsonlSink
        error_sink = JsonlSink(args.error_output)

    # --- 4. Build executor --------------------------------------------------
    executor = _build_executor(args.executor, args.workers, args.max_in_flight)

    # --- 5. Build runtime context -------------------------------------------
    runtime = _build_runtime(args)

    # --- 6. Run -------------------------------------------------------------
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

    # --- 7. Print summary ---------------------------------------------------
    _print_stats(stats)
    return 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_FORMAT_PREFIXES = ("jsonl:", "parquet:", "csv:")


def _parse_format_and_path(spec: str) -> tuple[str | None, str]:
    """Split an optional ``FORMAT:PATH`` spec into (format, path)."""
    for prefix in _FORMAT_PREFIXES:
        if spec.startswith(prefix):
            return prefix.rstrip(":"), spec[len(prefix):]
    return None, spec


def _resolve_source(spec: str):
    """Turn a ``[FORMAT:]PATH`` string into a Source object."""
    fmt, path = _parse_format_and_path(spec)
    if fmt is None:
        fmt = _infer_format(path)
    return _build_source(fmt, path, spec)


def _resolve_sink(spec: str):
    """Turn a ``[FORMAT:]PATH`` string into a Sink object."""
    fmt, path = _parse_format_and_path(spec)
    if fmt is None:
        fmt = _infer_format(path)
    return _build_sink(fmt, path, spec)


def _infer_format(path: str) -> str:
    """Infer the IO format from a file-system path's extension."""
    import os

    lower = path.lower().rstrip("/\\")
    for suffix in (".jsonl", ".jsonl.gz", ".jsonl.zst", ".ndjson"):
        if lower.endswith(suffix):
            return "jsonl"
    for suffix in (".parquet",):
        if lower.endswith(suffix):
            return "parquet"
    # directory paths default to jsonl
    if path.endswith("/") or os.path.isdir(path):
        return "jsonl"
    # fallback
    return "jsonl"


def _build_source(fmt: str, path: str, original: str):
    """Construct a Source object for *fmt*/*path*."""
    if fmt == "jsonl":
        from datapipe.io.jsonl import JsonlSource
        return JsonlSource(path)
    if fmt == "parquet":
        from datapipe.io.parquet import ParquetSource
        return ParquetSource(path)
    raise ValueError(
        f"unsupported source format {fmt!r} in {original!r}; "
        "use 'jsonl:PATH' or 'parquet:PATH'"
    )


def _build_sink(fmt: str, path: str, original: str):
    """Construct a Sink object for *fmt*/*path*."""
    if fmt == "jsonl":
        from datapipe.io.jsonl import JsonlSink
        return JsonlSink(path)
    if fmt == "parquet":
        from datapipe.io.parquet import ParquetSink
        return ParquetSink(path)
    raise ValueError(
        f"unsupported sink format {fmt!r} in {original!r}; "
        "use 'jsonl:PATH' or 'parquet:PATH'"
    )


def _build_executor(name: str, workers: int | None, max_in_flight: int | None):
    """Instantiate the requested executor."""
    if name == "sequential":
        from datapipe.execution.sequential import SequentialExecutor
        return SequentialExecutor()
    if name == "thread":
        from datapipe.execution.thread import ThreadExecutor
        kwargs: dict = {}
        if workers is not None:
            kwargs["workers"] = workers
        if max_in_flight is not None:
            kwargs["max_in_flight"] = max_in_flight
        return ThreadExecutor(**kwargs)
    # default: process
    from datapipe.execution.process import ProcessExecutor
    kwargs = {}
    if workers is not None:
        kwargs["workers"] = workers
    if max_in_flight is not None:
        kwargs["max_in_flight"] = max_in_flight
    return ProcessExecutor(**kwargs)


def _build_runtime(args: "argparse.Namespace"):
    """Build a RuntimeContext, respecting explicit CLI overrides."""
    from datapipe.runtime.context import RuntimeContext

    # Start from environment detection, then override with explicit flags.
    runtime = RuntimeContext.auto()
    if args.rank is not None:
        runtime = RuntimeContext(
            rank=args.rank,
            world_size=args.world_size if args.world_size is not None else runtime.world_size,
            local_rank=args.local_rank if args.local_rank is not None else runtime.local_rank,
        )
    elif args.world_size is not None:
        runtime = RuntimeContext(
            rank=runtime.rank,
            world_size=args.world_size,
            local_rank=args.local_rank if args.local_rank is not None else runtime.local_rank,
        )
    elif args.local_rank is not None:
        runtime = RuntimeContext(
            rank=runtime.rank,
            world_size=runtime.world_size,
            local_rank=args.local_rank,
        )
    return runtime


def _print_stats(stats) -> None:
    """Print a concise execution summary to stdout."""
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
