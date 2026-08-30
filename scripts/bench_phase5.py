#!/usr/bin/env python3
"""Phase 5 item 3: benchmark selector traversal, validation, provider lookup,
JSON parsing, and outer serialization.

Run from the repository root::

    python scripts/bench_phase5.py [--quick]

Requires the datapipe package to be installed (``pip install -e .``).  No
external benchmark libraries needed — uses only ``timeit`` from the standard
library.

``--quick`` reduces the iteration counts by 10× for a fast smoke test; omit
it for production numbers.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import timeit
from pathlib import Path
from typing import Any

# Allow running from the repo root without installing the package.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mean_us(total_sec: float, n: int) -> float:
    """Return mean microseconds per iteration."""
    return (total_sec / n) * 1_000_000


def _report(label: str, us: float) -> None:
    print(f"  {label:<55} {us:>8.1f} µs/iter")


# ---------------------------------------------------------------------------
# Benchmark functions
# ---------------------------------------------------------------------------


def bench_selector_simple(n: int) -> None:
    """Single-field selector: ``.tools``."""
    from datapipe.dsl.parser import parse
    from datapipe.dsl.selector import CompiledSelector

    expr = parse("fromjson(.tools)")
    sel = CompiledSelector(expr.invocations[0].selector)
    record = {"tools": [1, 2, 3], "meta": {"x": "y"}}

    def _run():
        sel.resolve(record)

    t = timeit.timeit(_run, number=n)
    _report(".tools  resolve()", _mean_us(t, n))


def bench_selector_nested(n: int) -> None:
    """Nested selector: ``.metadata.annotation``."""
    from datapipe.dsl.parser import parse
    from datapipe.dsl.selector import CompiledSelector

    expr = parse("fromjson(.metadata.annotation)")
    sel = CompiledSelector(expr.invocations[0].selector)
    record = {"metadata": {"annotation": "hello", "other": 42}}

    def _run():
        sel.resolve(record)

    t = timeit.timeit(_run, number=n)
    _report(".metadata.annotation  resolve()", _mean_us(t, n))


def bench_selector_wildcard_10(n: int) -> None:
    """Wildcard over a 10-element array: ``.tools[]``."""
    from datapipe.dsl.parser import parse
    from datapipe.dsl.selector import CompiledSelector

    expr = parse("tojson(.tools[])")
    sel = CompiledSelector(expr.invocations[0].selector)
    record = {"tools": [{"name": f"t{i}"} for i in range(10)]}

    def _run():
        sel.resolve(record)

    t = timeit.timeit(_run, number=n)
    _report(".tools[] (10 items)  resolve()", _mean_us(t, n))


def bench_selector_wildcard_deep(n: int) -> None:
    """Wildcard then nested path: ``.tools[].function.parameters``."""
    from datapipe.dsl.parser import parse
    from datapipe.dsl.selector import CompiledSelector

    expr = parse("tojson(.tools[].function.parameters)")
    sel = CompiledSelector(expr.invocations[0].selector)
    record = {
        "tools": [
            {"function": {"parameters": {"type": "object", "props": {}}}}
            for _ in range(10)
        ]
    }

    def _run():
        sel.resolve(record)

    t = timeit.timeit(_run, number=n)
    _report(".tools[].function.parameters (10×)  resolve()", _mean_us(t, n))


def bench_compile_expression(n: int) -> None:
    """Compile a three-operation expression from scratch."""
    from datapipe.dsl.compiler import _build_builtin_registry
    from datapipe.dsl.parser import parse
    from datapipe.dsl.selector import CompiledSelector
    from datapipe.dsl.compiler import compile_expression

    # Force registry build once before timing.
    _build_builtin_registry()

    src = (
        "fromjson(.tools) | "
        "fromjson(.metadata.annotation, recursive=true) | "
        "tojson(.tools[].function.parameters)"
    )

    def _run():
        compile_expression(src)

    t = timeit.timeit(_run, number=n)
    _report("compile_expression (3 ops)", _mean_us(t, n))


def bench_validation_always(n: int) -> None:
    """Per-record validation overhead: always mode, no type mismatch."""
    from datapipe.dsl.compiler import compile_expression
    from datapipe.stages.tool_program import CompiledToolProgramStage
    from datapipe.context import WorkerContext

    compiled = compile_expression("fromjson(.payload)")
    stage_always = CompiledToolProgramStage(compiled, validate="always")
    stage_off = CompiledToolProgramStage(compiled, validate="off")
    ctx = WorkerContext(rank=0, world_size=1, worker_id=0, record_index=0)
    record = {"payload": '{"x": 1}'}

    def _run_always():
        stage_always._validated_records = 0
        stage_always.process({"payload": '{"x": 1}'}, ctx)

    def _run_off():
        stage_off.process({"payload": '{"x": 1}'}, ctx)

    t_always = timeit.timeit(_run_always, number=n)
    t_off = timeit.timeit(_run_off, number=n)
    _report("tool invocation, validate=always", _mean_us(t_always, n))
    _report("tool invocation, validate=off", _mean_us(t_off, n))
    overhead = _mean_us(t_always - t_off, n)
    _report("  → validation overhead", overhead)


def bench_json_outer_parse_dump(n: int) -> None:
    """Outer JSON parse + dump as done implicitly for each JSONL record."""
    sample = json.dumps(
        {
            "id": 1234,
            "tools": '[{"name": "fn", "function": {"parameters": {"type": "object"}}}]',
            "metadata": {"annotation": '{"label": "test", "score": 0.9}'},
            "text": "some longer text field " * 10,
        }
    )

    def _run():
        json.loads(sample)

    def _dump():
        json.dumps({"id": 1234, "tools": [{"name": "fn"}], "text": "x" * 100})

    t_parse = timeit.timeit(_run, number=n)
    t_dump = timeit.timeit(_dump, number=n)
    _report("json.loads (realistic record)", _mean_us(t_parse, n))
    _report("json.dumps (realistic record)", _mean_us(t_dump, n))


def bench_provider_lookup_cached(n: int, tmp_dir: Path) -> None:
    """Provider load when already cached (hot path in workers)."""
    import os

    os.environ["DATAPIPE_USER_DATA"] = str(tmp_dir / "dp_data")

    provider = tmp_dir / "bench_tool.py"
    provider.write_text(
        "from datapipe.tools import tool, JsonType\n"
        "@tool(name='id_tool', target='value', "
        "input=JsonType.ANY, output=JsonType.ANY)\n"
        "def id_tool(v): return v\n"
    )

    from datapipe.tools.installer import install_provider
    from datapipe.tools import loader as _loader

    _loader._loaded_providers.clear()
    entry = install_provider(provider, yes=True)

    from datapipe.tools.descriptor import ProviderDescriptor

    desc = ProviderDescriptor(
        provider_id=entry.provider_id,
        alias=entry.alias,
        mode=entry.mode,
        source_path=entry.source_path,
        sha256=entry.digest,
        api_version=entry.datapipe_api,
    )

    # Warm the cache.
    from datapipe.tools.loader import load_provider

    load_provider(desc)

    def _run():
        load_provider(desc)

    t = timeit.timeit(_run, number=n)
    _report("load_provider (cached)", _mean_us(t, n))

    # Cold (clear cache).
    def _run_cold():
        _loader._loaded_providers.clear()
        load_provider(desc)

    t_cold = timeit.timeit(_run_cold, number=n // 10 or 1)
    _report("load_provider (cold, module re-import)", _mean_us(t_cold, n // 10 or 1))


def bench_end_to_end_sequential(n: int, tmp_dir: Path) -> None:
    """End-to-end throughput: fromjson + tojson via sequential executor."""
    import os

    os.environ["DATAPIPE_USER_DATA"] = str(tmp_dir / "dp_data2")

    from datapipe import (
        IterableSource,
        ListSink,
        Pipeline,
        SequentialExecutor,
    )
    from datapipe.dsl.compiler import compile_expression
    from datapipe.stages.tool_program import CompiledToolProgramStage
    from datapipe.stage import JsonLoadStage, JsonDumpStage

    compiled = compile_expression("fromjson(.payload)")
    stage = CompiledToolProgramStage(compiled, validate="off")
    pipeline = Pipeline([JsonLoadStage(), stage, JsonDumpStage()])

    records = [
        json.dumps({"id": i, "payload": json.dumps({"val": i})})
        for i in range(100)
    ]

    def _run():
        sink = ListSink()
        pipeline.run(
            source=IterableSource(list(records)),
            sink=sink,
            executor=SequentialExecutor(),
            progress=False,
        )

    t = timeit.timeit(_run, number=n)
    per_record_us = _mean_us(t, n * 100)
    _report("end-to-end sequential (100 records, validate=off)", per_record_us)
    print(
        f"    → throughput: {1_000_000 / per_record_us:,.0f} records/sec"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="datapipe Phase 5 benchmark suite"
    )
    parser.add_argument(
        "--quick", action="store_true",
        help="reduce iteration counts 10× for fast smoke testing"
    )
    args = parser.parse_args()

    divisor = 10 if args.quick else 1
    N_MICRO = 50_000 // divisor   # microbenchmarks
    N_COMPILE = 5_000 // divisor  # compilation
    N_E2E = 20 // divisor         # end-to-end (100 records each)

    print("datapipe Phase 5 benchmark")
    print(f"  iterations: micro={N_MICRO:,}  compile={N_COMPILE:,}  e2e={N_E2E:,}")
    print()

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)

        print("Selector traversal:")
        bench_selector_simple(N_MICRO)
        bench_selector_nested(N_MICRO)
        bench_selector_wildcard_10(N_MICRO)
        bench_selector_wildcard_deep(N_MICRO)

        print()
        print("Expression compilation:")
        bench_compile_expression(N_COMPILE)

        print()
        print("Runtime validation overhead:")
        bench_validation_always(N_COMPILE)

        print()
        print("JSON parsing and serialization:")
        bench_json_outer_parse_dump(N_MICRO)

        print()
        print("Provider lookup:")
        bench_provider_lookup_cached(N_COMPILE, tmp_dir)

        print()
        print("End-to-end throughput (validate=off, sequential):")
        bench_end_to_end_sequential(N_E2E, tmp_dir)


if __name__ == "__main__":
    main()
