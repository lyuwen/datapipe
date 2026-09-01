"""``datapipe transform``: compile a DSL expression and run it over records.

Implements the user-facing transform command from §3.1 of the CLI plan:

    datapipe transform [OPTIONS] EXPRESSION INPUT OUTPUT
    datapipe [OPTIONS] EXPRESSION INPUT OUTPUT          # shorthand

The expression is compiled to a ``CompiledToolProgramStage`` (legacy
single-expression form) or a ``CompiledProgramStage`` (multi-statement
programs and focused pipes), wrapped in a ``Pipeline``, and executed via the
same ``Pipeline.run()`` path as ``datapipe run``.  No separate executor or IO adapters are added — the
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

import json
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

    # IO formats (§3.1).  JSONL only: the transform path wraps the tool program
    # in JsonLoadStage/JsonDumpStage, which require raw line strings.  Parquet
    # awaits column/schema semantics for selectors and dynamic tool output.
    p.add_argument(
        "--input-format", dest="input_format",
        choices=["jsonl"], default="jsonl",
        help="input record format (default: jsonl)",
    )
    p.add_argument(
        "--output-format", dest="output_format",
        choices=["jsonl"], default="jsonl",
        help="output record format (default: jsonl)",
    )

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
            "compile the expression and print the resolved tools, contracts "
            "and pipeline stages without opening the input or running records"
        ),
    )
    p.add_argument(
        "--json", dest="as_json", action="store_true",
        help="with --dry-run, emit the compilation result as JSON",
    )


def add_inspect_expression_parser(subparsers) -> None:
    """Attach the ``inspect-expression`` sub-command to *subparsers* (§3.3)."""
    p = subparsers.add_parser(
        "inspect-expression",
        help="compile a transform expression and show how it resolves",
        description=(
            "Compile a jq-like expression without opening any data and report\n"
            "the resolved tools, their providers and contracts, the bound\n"
            "arguments, and the pipeline stages the expression produces.\n\n"
            "Examples:\n"
            "  datapipe inspect-expression 'fromjson(.tools)'\n"
            "  datapipe inspect-expression --json 'fromjson(.a) | tojson(.a.b)'"
        ),
        formatter_class=__import__("argparse").RawDescriptionHelpFormatter,
    )
    p.add_argument("expression", help="jq-like transform expression")
    p.add_argument(
        "--json", dest="as_json", action="store_true",
        help="emit the compilation result as JSON instead of text",
    )
    p.add_argument(
        "--validate-tools",
        choices=["always", "sample", "off"],
        default="always",
        dest="validate_tools",
        help="validation mode to report for the generated stage (default: always)",
    )


def inspect_expression_command(args: "argparse.Namespace") -> int:
    """Execute the ``inspect-expression`` sub-command.  Returns an exit code."""
    compiled = _compile_or_report(args.expression)
    if compiled is None:
        return 1
    return _emit_compiled(compiled, args)


# ---------------------------------------------------------------------------
# Command implementation
# ---------------------------------------------------------------------------


def _needs_program_path(program) -> bool:
    """Return True when *program* uses syntax only ``compile_program`` supports.

    A single invocation-first statement with no bare pipes — ``fromjson(.a)`` —
    has identical semantics under either compiler, so it keeps the legacy
    ``CompiledExpression`` shape.  That holds the single-invocation
    ``--dry-run`` / ``inspect-expression`` output stable.  Multi-statement
    programs, selector-first focused statements, and bare pipes all require
    the program compiler.

    The positive cases would also reach ``compile_program`` via the legacy
    fallback in ``_compile_or_report`` (they fail the legacy parse), so this
    predicate is strictly load-bearing only for its negative case.  It stays
    explicit regardless: routing the canonical language on a *parse failure*
    of the deprecated grammar would be an accident waiting to break.
    """
    if len(program.statements) > 1:
        return True
    return any(
        stmt.focus_selector is not None or stmt.pipes
        for stmt in program.statements
    )


def _compile_or_report(expression: str):
    """Compile *expression*, printing a diagnostic and returning None on error.

    Routing tries ``parse_program`` first because the multi-statement form is
    the canonical language.  The legacy ``compile_expression`` path is the
    fallback: it is the only one that accepts ``invocation | invocation``
    (which it also deprecates).  When *neither* form parses, the
    ``parse_program`` diagnostic is the one reported.
    """
    from datapipe.dsl.compiler import compile_expression, compile_program
    from datapipe.dsl.errors import (
        ExpressionSyntaxError,
        ToolConfigurationError,
        ToolResolutionError,
    )
    from datapipe.dsl.parser import parse_program

    program_ast = None
    program_error = None
    try:
        try:
            program_ast = parse_program(expression)
        except ExpressionSyntaxError as exc:
            program_error = exc

        if program_ast is not None and _needs_program_path(program_ast):
            return compile_program(expression)

        # Either the program parse failed (legacy `a | b` form) or the program
        # is a single plain invocation whose legacy output shape we preserve.
        try:
            return compile_expression(expression)
        except ExpressionSyntaxError:
            if program_ast is not None:
                # parse_program accepted it; only the legacy grammar objects.
                return compile_program(expression)
            # Neither grammar parses — report the canonical diagnostic.
            raise program_error
    except (ExpressionSyntaxError, ToolResolutionError, ToolConfigurationError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return None
    except Exception as exc:  # noqa: BLE001
        print(f"error compiling expression: {exc}", file=sys.stderr)
        return None


def _emit_compiled(compiled, args: "argparse.Namespace") -> int:
    """Print a compiled expression as text or JSON.  Returns an exit code."""
    validate = getattr(args, "validate_tools", "always")
    if getattr(args, "as_json", False):
        print(json.dumps(
            describe_compiled(compiled, args.expression, validate=validate),
            indent=2,
        ))
    else:
        _print_compiled(compiled, args.expression, validate=validate)
    return 0


def transform_command(args: "argparse.Namespace") -> int:
    """Execute the ``transform`` sub-command.  Returns an exit code."""

    # 1. Compile the expression — do this before opening any files so
    #    invalid expressions fail immediately.
    compiled = _compile_or_report(args.expression)
    if compiled is None:
        return 1

    # 2. Dry-run: print the resolved program and exit.
    if getattr(args, "dry_run", False):
        return _emit_compiled(compiled, args)

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
    _log_io(source, sink, error_sink)
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


def describe_io(source, sink, error_sink=None) -> str:
    """Render source/sink identity for the run start-up summary."""
    parts = [
        f"source={type(source).__name__}({getattr(source, 'path', '?')})",
        f"sink={type(sink).__name__}({getattr(sink, 'path', '?')})",
    ]
    if error_sink is not None:
        parts.append(
            f"error_sink={type(error_sink).__name__}"
            f"({getattr(error_sink, 'path', '?')})"
        )
    return " | ".join(parts)


def _log_io(source, sink, error_sink=None) -> None:
    import logging

    logging.getLogger("datapipe").info("IO | %s", describe_io(source, sink, error_sink))


def _build_pipeline(compiled, validate: str = "always"):
    """Wrap a compiled expression in a Pipeline with outer JSON stages."""
    from datapipe.pipeline import Pipeline
    from datapipe.stage import JsonDumpStage, JsonLoadStage
    from datapipe.dsl.compiler import CompiledProgram

    if isinstance(compiled, CompiledProgram):
        from datapipe.stages.tool_program import CompiledProgramStage
        stage = CompiledProgramStage(compiled, validate=validate)
    else:
        from datapipe.stages.tool_program import CompiledToolProgramStage
        stage = CompiledToolProgramStage(compiled, validate=validate)

    return Pipeline([
        JsonLoadStage(),
        stage,
        JsonDumpStage(),
    ])


def _literal(value) -> str:
    """Render an argument value in DSL literal syntax (``true``, not ``True``)."""
    return json.dumps(value)


def _describe_provider(descriptor) -> dict:
    """Render the provider identity block for a tool descriptor (None = built-in)."""
    if descriptor is None:
        return {"provider_id": "builtin", "alias": None, "mode": "builtin"}
    pd = descriptor.provider
    return {
        "provider_id": pd.provider_id,
        "alias": pd.alias,
        "mode": pd.mode,
        "source_path": pd.source_path,
        "sha256": pd.sha256,
        "api_version": pd.api_version,
    }


def _describe_contract(contract) -> dict | None:
    """Render a ``ToolContract`` as a JSON-serializable dict (None passes through)."""
    if contract is None:
        return None
    from datapipe.tools.types import describe as describe_type

    return {
        "target": contract.target,
        "input": describe_type(contract.input_type),
        "output": describe_type(contract.output_type),
        "cardinality": contract.cardinality.value,
        "deterministic": contract.deterministic,
    }


def _describe_invocation(inv) -> dict:
    """Render one ``ToolInvocation`` (base operation or legacy pipe element)."""
    return {
        "index": inv.expression_index,
        "tool": inv.tool_name,
        "selector": inv.selector.render(),
        "provider": _describe_provider(inv.tool_descriptor),
        "contract": _describe_contract(inv.contract),
        "arguments": dict(inv.arguments),
    }


def _describe_bare_call(bare) -> dict:
    """Render one ``CompiledBareCall`` from a focused pipe.

    A ``CompiledBareCall`` carries no ``selector`` (its target is the enclosing
    statement's focus) and no contract — provider bare calls hold only a
    descriptor, so the contract is recovered best-effort by resolving the tool.
    Resolution failure degrades to a null contract rather than crashing a
    ``--dry-run``.
    """
    from datapipe.tools.decorator import get_contract

    fn = bare.callable
    if fn is None and bare.descriptor is not None:
        try:
            from datapipe.tools.loader import resolve_tool
            fn = resolve_tool(bare.descriptor.provider, bare.descriptor.tool_name)
        except Exception:  # noqa: BLE001 - inspection must not fail the command
            fn = None

    contract = get_contract(fn) if fn is not None else None
    if contract is not None:
        tool_name = contract.name
    elif bare.descriptor is not None:
        tool_name = bare.descriptor.tool_name
    else:
        tool_name = getattr(fn, "__name__", None) or "<unknown>"

    return {
        "index": bare.expression_index,
        "tool": tool_name,
        "provider": _describe_provider(bare.descriptor),
        "contract": _describe_contract(contract),
        "arguments": dict(bare.bound_args),
    }


def describe_compiled(compiled, expression: str, *, validate: str = "always") -> dict:
    """Build a JSON-serializable description of a compiled expression or program.

    Shared by ``transform --dry-run`` and ``inspect-expression`` so both
    surfaces report identical resolution results (§3.3 of the CLI plan).

    ``CompiledExpression`` yields the legacy ``invocations`` shape.
    ``CompiledProgram`` yields a ``statements`` list instead, since it has no
    ``invocations`` attribute: each entry reports its index, focus selector,
    base operation and pipes.
    """
    from datapipe.dsl.compiler import CompiledProgram

    stages = [
        {"index": i, "name": stage.name, "type": type(stage).__name__}
        for i, stage in enumerate(_build_pipeline(compiled, validate=validate))
    ]

    if isinstance(compiled, CompiledProgram):
        statements = [
            {
                "index": i,
                "focus": (
                    None if stmt.focus_selector is None
                    else stmt.focus_selector.render()
                ),
                "operation": _describe_invocation(stmt.operation),
                "pipes": [_describe_bare_call(b) for b in stmt.pipes],
            }
            for i, stmt in enumerate(compiled.statements)
        ]
        return {
            "expression": expression,
            "expression_language": 1,
            "statements": statements,
            "stages": stages,
            "validate": validate,
        }

    return {
        "expression": expression,
        "invocations": [_describe_invocation(inv) for inv in compiled.invocations],
        "stages": stages,
        "validate": validate,
    }


def _print_call(call: dict, *, indent: str, label: str) -> None:
    """Print one operation or pipe entry from a ``describe_compiled`` document."""
    args = call["arguments"]
    args_str = (
        ", ".join(f"{k}={_literal(v)}" for k, v in args.items())
        if args
        else "(none)"
    )
    provider = call["provider"]
    contract = call["contract"]
    print(f"{indent}[{call['index']}] {label}")
    provider_str = provider["provider_id"]
    alias = provider["alias"]
    if alias:
        provider_str += f" (alias {alias}, {provider['mode']})"
    detail = indent + "      "
    print(f"{detail}provider:    {provider_str}")
    if contract is not None:
        print(f"{detail}target:      {contract['target']}")
        print(f"{detail}input:       {contract['input']}")
        print(f"{detail}output:      {contract['output']}")
        print(f"{detail}cardinality: {contract['cardinality']}")
    print(f"{detail}arguments:   {args_str}")


def _print_compiled(compiled, expression: str, *, validate: str = "always") -> None:
    """Print a human-readable description of the compiled expression or program."""
    doc = describe_compiled(compiled, expression, validate=validate)
    print("expression-language: 1")
    print(f"Expression: {doc['expression']}")

    if "statements" in doc:
        print(f"Statements: {len(doc['statements'])}")
        for stmt in doc["statements"]:
            focus = stmt["focus"]
            header = f"  statement [{stmt['index']}]"
            if focus is not None:
                header += f"  focus: {focus}"
            print(header)
            op = stmt["operation"]
            _print_call(
                op, indent="    ", label=f"{op['tool']}({op['selector']})"
            )
            for pipe in stmt["pipes"]:
                _print_call(pipe, indent="    ", label=f"| {pipe['tool']}")
    else:
        print(f"Invocations: {len(doc['invocations'])}")
        for inv in doc["invocations"]:
            _print_call(
                inv, indent="  ", label=f"{inv['tool']}({inv['selector']})"
            )

    print("Stages:")
    for stage in doc["stages"]:
        print(f"  [{stage['index']}] {stage['name']}  ({stage['type']})")


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
