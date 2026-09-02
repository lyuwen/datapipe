#!/usr/bin/env python3
"""Phase S7 benchmarks for the structural transform DSL.

Run from the repository root::

    python scripts/bench_structural.py [--quick]

Requires the datapipe package to be installed (``pip install -e .``).  No
external benchmark libraries — ``timeit`` and ``tracemalloc`` only, following
the conventions of ``scripts/bench_phase5.py``.

``--quick`` reduces iteration counts 10x for a fast smoke test.

Measured (plan §14.1-§14.4):

  1. explicit field selection vs complement selection
  2. structural mutation scaling with metadata object size
  3. the S3 container deep-copy cost on assignment
  4. symbolic ``<<`` vs the equivalent ``nest(...)`` call
  5. inner-program validation cost in ``nest``/``unnest``
  6. time to first output record, and peak memory
"""

from __future__ import annotations

import argparse
import copy
import json
import sys
import time
import timeit
import tracemalloc
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _report(label: str, us: float, suffix: str = "") -> None:
    print(f"  {label:<52} {us:>9.1f} µs/rec {suffix}")


def _note(text: str) -> None:
    print(f"    → {text}")


def _stage(expression: str, validate: str = "off"):
    from datapipe.dsl.compiler import compile_program
    from datapipe.stages.tool_program import CompiledProgramStage

    return CompiledProgramStage(compile_program(expression), validate=validate)


def _per_record_us(stage, record: Any, n: int) -> float:
    """Mean µs/record for ``stage.process``, with the deepcopy setup subtracted.

    Each iteration needs a fresh record (structural statements mutate in
    place), so the copy is timed separately and removed; what remains is the
    stage's own cost.
    """
    stage.process(copy.deepcopy(record), None)  # warm

    def _run():
        stage.process(copy.deepcopy(record), None)

    def _base():
        copy.deepcopy(record)

    total = timeit.timeit(_run, number=n)
    base = timeit.timeit(_base, number=n)
    return max(total - base, 0.0) / n * 1_000_000


def _gross_per_record_us(stage, record: Any, n: int) -> float:
    """Mean µs/record for ``stage.process`` *including* the fresh-record copy.

    Unlike :func:`_per_record_us` this subtracts no baseline.  The deep-copy
    probe compares two variants of the same statement, and subtracting a
    ``deepcopy`` baseline breaks exactly that comparison: with ``_detached``
    neutered, the stage's remaining work is smaller than one ``deepcopy`` of
    the record, so ``max(total - base, 0.0)`` clamps to zero and the probe
    reports the copy as 100% of the statement regardless of its real cost.
    Both variants carry an identical setup cost, so the *difference* between
    two gross timings is still the deep copy's true cost.
    """
    stage.process(copy.deepcopy(record), None)  # warm

    def _run():
        stage.process(copy.deepcopy(record), None)

    return timeit.timeit(_run, number=n) / n * 1_000_000


# ---------------------------------------------------------------------------
# Record builders
# ---------------------------------------------------------------------------


def _flat_record(n_fields: int) -> dict:
    """A record with stable root fields plus *n_fields* movable fields."""
    record: dict[str, Any] = {
        "instance_id": "inst-0001",
        "messages": [{"role": "user", "content": "hello"}],
        "tools": [{"name": "fn", "function": {"parameters": {"type": "object"}}}],
    }
    for i in range(n_fields):
        record[f"field_{i}"] = {"value": i, "label": f"label-{i}"}
    return record


def _nested_record(n_keys: int, depth: int = 3) -> dict:
    """A record whose ``.source`` holds a deep object of *n_keys* entries."""
    def _sub(d: int, i: int) -> Any:
        if d == 0:
            return {"leaf": i, "text": "x" * 24}
        return {"child": _sub(d - 1, i), "idx": i}

    return {
        "id": "rec-1",
        "source": {f"k{i}": _sub(depth, i) for i in range(n_keys)},
        "dest": {},
    }


# ---------------------------------------------------------------------------
# §14.1 — explicit versus complement field selection
# ---------------------------------------------------------------------------


def bench_explicit_vs_complement(n: int) -> None:
    """Compare a positive field set against the complement that selects the same set."""
    print("Explicit vs complement field selection (§14.1):")

    for n_fields in (4, 16, 64):
        record = _flat_record(n_fields)
        names = "|".join(f"field_{i}" for i in range(n_fields))
        explicit = _stage(f".metadata << .({names}) | tojson")
        complement = _stage(".metadata << .(^instance_id|messages|tools) | tojson")

        # Equivalence is the premise of the comparison; assert it, do not assume.
        a = explicit.process(copy.deepcopy(record), None)
        b = complement.process(copy.deepcopy(record), None)
        assert a == b, "explicit and complement forms diverged"

        iters = max(n // (1 + n_fields // 8), 200)
        t_exp = _per_record_us(explicit, record, iters)
        t_cmp = _per_record_us(complement, record, iters)
        _report(f"{n_fields:3d} fields  explicit  .(f0|f1|...)", t_exp)
        _report(f"{n_fields:3d} fields  complement .(^stable)", t_cmp)
        delta = (t_cmp - t_exp) / t_exp * 100 if t_exp else 0.0
        _note(f"complement is {delta:+.1f}% vs explicit")


# ---------------------------------------------------------------------------
# §14.2 — structural mutation with large metadata objects
# ---------------------------------------------------------------------------


def bench_metadata_scaling(n: int) -> None:
    """Vary metadata size so the scaling curve is visible, not one data point."""
    print("Structural mutation vs metadata size (§14.2):")

    prev_us = None
    prev_size = None
    for n_fields in (1, 4, 16, 64, 256):
        record = _flat_record(n_fields)
        stage = _stage(".metadata << .(^instance_id|messages|tools) | tojson")
        iters = max(n // (1 + n_fields // 4), 100)
        us = _per_record_us(stage, record, iters)
        _report(f"move {n_fields:3d} fields into .metadata | tojson", us)
        if prev_us is not None and prev_us > 0:
            growth = us / prev_us
            factor = n_fields / prev_size
            _note(f"{factor:.0f}x fields → {growth:.2f}x time")
        prev_us, prev_size = us, n_fields


# ---------------------------------------------------------------------------
# The S3 container deep-copy cost (deferred from S3 for measurement here)
# ---------------------------------------------------------------------------


def bench_deepcopy_cost(n: int) -> None:
    """Quantify ``_detached()`` — the deep copy S3 added to fix copy aliasing.

    The copy is what makes ``.a = .b`` a real copy rather than a live alias.
    Measuring it means running the same statement with ``_detached`` replaced
    by identity, which is *not* a correct implementation — it is the broken
    behavior S3 fixed — so this is a cost probe, not a proposed configuration.
    """
    print("S3 container deep-copy cost on assignment:")

    import datapipe.stages.tool_program as tp

    for n_keys in (1, 8, 64, 512):
        record = _nested_record(n_keys)
        stage = _stage(".dest = .source")
        iters = max(n // (1 + n_keys // 4), 100)

        with_copy = _gross_per_record_us(stage, record, iters)

        original = tp._detached
        tp._detached = lambda value: value  # noqa: E731 — probe only
        try:
            without = _gross_per_record_us(stage, record, iters)
        finally:
            tp._detached = original

        payload = len(json.dumps(record["source"]))
        _report(f".dest = .source  {n_keys:3d} keys ({payload:>7,} B)", with_copy)
        _report("  same, _detached() neutered (INCORRECT)", without)
        overhead = with_copy - without
        pct = overhead / with_copy * 100 if with_copy else 0.0
        _note(f"deep copy costs {overhead:.1f} µs = {pct:.0f}% of the statement")


# ---------------------------------------------------------------------------
# Symbolic `<<` versus the named `nest(...)` tool
# ---------------------------------------------------------------------------


def bench_symbolic_vs_named(n: int) -> None:
    """``<<`` and ``nest(...)`` compile to the same IR; report the gap either way."""
    print("Symbolic `<<` vs named nest()/unnest():")

    record = _flat_record(16)
    symbolic = _stage(".metadata << .(^instance_id|messages|tools) | tojson")
    named = _stage(
        'nest(., key="metadata", '
        'exclude=["instance_id","messages","tools"], jsonify=true)'
    )
    assert symbolic.process(copy.deepcopy(record), None) == named.process(
        copy.deepcopy(record), None
    ), "symbolic and named forms diverged"

    t_sym = _per_record_us(symbolic, record, n)
    t_named = _per_record_us(named, record, n)
    _report(".metadata << .(^stable) | tojson", t_sym)
    _report('nest(., key="metadata", exclude=[...], jsonify)', t_named)
    delta = (t_named - t_sym) / t_sym * 100 if t_sym else 0.0
    _note(f"nest() is {delta:+.1f}% vs the symbolic form")

    unnest_record = {
        "instance_id": "i",
        "metadata": json.dumps(
            {"temperature": 0.7, "score": 1, "note": "n", "extra": "e" * 64}
        ),
    }
    desugared = _stage(
        "fromjson(.metadata); . << .metadata.(temperature|score); tojson(.metadata)"
    )
    named_unnest = _stage(
        'unnest(., key="metadata", include=["temperature","score"], '
        "parse=true, jsonify=true)"
    )
    assert desugared.process(copy.deepcopy(unnest_record), None) == (
        named_unnest.process(copy.deepcopy(unnest_record), None)
    ), "unnest and its desugaring diverged"

    t_desugar = _per_record_us(desugared, unnest_record, n)
    t_unnest = _per_record_us(named_unnest, unnest_record, n)
    _report("fromjson; . << .metadata.(t|s); tojson  (desugared)", t_desugar)
    _report("unnest(., include=[...], parse, jsonify)", t_unnest)
    delta = (t_unnest - t_desugar) / t_desugar * 100 if t_desugar else 0.0
    _note(f"unnest() is {delta:+.1f}% vs its desugaring")


# ---------------------------------------------------------------------------
# Inner validation cost in nest/unnest (S6 measured ~13%; confirm or correct)
# ---------------------------------------------------------------------------


def bench_inner_validation(n: int) -> None:
    """``nest``/``unnest`` run an inner program; validate mode reaches it via ContextVar."""
    print("Inner-program validation cost in nest()/unnest():")

    cases = [
        (
            "nest(., exclude=[...], jsonify)",
            'nest(., key="metadata", exclude=["instance_id"], jsonify=true)',
            _flat_record(16),
        ),
        (
            "unnest(., include=[...], parse, jsonify)",
            'unnest(., key="metadata", include=["temperature"], '
            "parse=true, jsonify=true)",
            {
                "instance_id": "i",
                "metadata": json.dumps(
                    {"temperature": 0.7, "note": "n", "blob": list(range(64))}
                ),
            },
        ),
    ]

    for label, expression, record in cases:
        always = _stage(expression, validate="always")
        off = _stage(expression, validate="off")
        t_always = _per_record_us(always, record, n)
        t_off = _per_record_us(off, record, n)
        _report(f"{label}  validate=always", t_always)
        _report(f"{label}  validate=off", t_off)
        pct = (t_always - t_off) / t_always * 100 if t_always else 0.0
        _note(f"validation is {pct:.0f}% of runtime")


# ---------------------------------------------------------------------------
# §14.4 — time to first output record and peak memory
# ---------------------------------------------------------------------------


def bench_first_record_and_memory(n_records: int) -> None:
    """Time to first output and peak memory over a streaming structural run.

    Time to first output is what proves the pipeline streams: if the source
    were materialized it would scale with the whole input, not stay flat.
    """
    print("Time to first output and peak memory (§14.4):")

    from datapipe import IterableSource, Pipeline, SequentialExecutor
    from datapipe.io.base import Sink
    from datapipe.stage import JsonDumpStage, JsonLoadStage

    expression = ".metadata << .(^instance_id|messages|tools) | tojson"

    class _FirstRecordSink(Sink):
        """Records the wall time at which the first output arrives."""

        def __init__(self) -> None:
            self.first_at: float | None = None
            self.count = 0

        def write(self, record: Any) -> None:
            if self.first_at is None:
                self.first_at = time.perf_counter()
            self.count += 1

    def _lazy_source(count: int, pulled: list[int]):
        for i in range(count):
            pulled[0] += 1
            record = _flat_record(8)
            record["instance_id"] = f"inst-{i}"
            yield json.dumps(record)

    for total in (n_records, n_records * 10):
        pulled = [0]
        sink = _FirstRecordSink()
        pipeline = Pipeline([JsonLoadStage(), _stage(expression), JsonDumpStage()])

        tracemalloc.start()
        started = time.perf_counter()
        pipeline.run(
            source=IterableSource(_lazy_source(total, pulled)),
            sink=sink,
            executor=SequentialExecutor(),
            progress=False,
        )
        elapsed = time.perf_counter() - started
        _current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        assert sink.count == total, f"{sink.count} != {total}"
        ttf = (sink.first_at - started) * 1000 if sink.first_at else float("nan")
        print(
            f"  {total:>6,} records: first output {ttf:6.2f} ms, "
            f"total {elapsed * 1000:8.1f} ms, peak {peak / 1024:9.1f} KiB"
        )


# ---------------------------------------------------------------------------
# Worker-boundary serialization (§14.3)
# ---------------------------------------------------------------------------


def bench_pickle_payload() -> None:
    """Compare what crosses the process boundary for structural vs plain programs."""
    print("Worker-boundary pickle payload (§14.3):")

    import pickle

    from datapipe.dsl.compiler import compile_expression, compile_program
    from datapipe.stage import JsonDumpStage, JsonLoadStage
    from datapipe.stages.tool_program import (
        CompiledProgramStage,
        CompiledToolProgramStage,
    )

    baseline = CompiledToolProgramStage(compile_expression("fromjson(.payload)"))
    cases = [
        ("baseline  fromjson(.payload)  [legacy stage]", baseline),
        ("program   fromjson(.payload)", _stage("fromjson(.payload)")),
        ("program   fromjson(.a); tojson(.b)", _stage("fromjson(.a); tojson(.b)")),
        (
            "program   .metadata << .(^a|b) | tojson",
            _stage(".metadata << .(^a|b) | tojson"),
        ),
        (
            "program   4 statements, assignments + moves",
            _stage(".x = .a; .y <- .b; .m << .(^x|y); tojson(.m)"),
        ),
    ]

    for label, stage in cases:
        payload = pickle.dumps(
            [JsonLoadStage(), stage, JsonDumpStage()], protocol=pickle.HIGHEST_PROTOCOL
        )
        print(f"  {label:<52} {len(payload):>7,} B")
    _note("payload is per-worker (pool initializer), not per record")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="datapipe Phase S7 structural DSL benchmark suite"
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="reduce iteration counts 10x for fast smoke testing",
    )
    args = parser.parse_args()

    divisor = 10 if args.quick else 1
    n_micro = 5_000 // divisor
    n_records = 200 // divisor

    print("datapipe Phase S7 structural benchmark")
    print(f"  iterations: micro={n_micro:,}  stream={n_records:,} records")
    print()

    bench_explicit_vs_complement(n_micro)
    print()
    bench_metadata_scaling(n_micro)
    print()
    bench_deepcopy_cost(n_micro)
    print()
    bench_symbolic_vs_named(n_micro)
    print()
    bench_inner_validation(n_micro)
    print()
    bench_first_record_and_memory(n_records)
    print()
    bench_pickle_payload()


if __name__ == "__main__":
    main()
