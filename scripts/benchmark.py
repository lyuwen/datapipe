"""Lightweight benchmarks (plan §42).

Compares the bounded-future scheduler against ``ProcessPoolExecutor.map``
and a sequential baseline across several workload classes.

Usage:
    python scripts/benchmark.py [--records N] [--workers W] [--max-in-flight M]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datapipe import (  # noqa: E402
    GenericStage,
    IterableSource,
    ListSink,
    Pipeline,
    ProcessExecutor,
    SequentialExecutor,
)


# --- top-level workload functions (must be pickleable under spawn) ---------


def w_trivial(x):
    return x + 1


def w_cpu_1ms(x):
    total = 0
    for i in range(2000):
        total += (x * x + i) % 97
    return x + total


def w_cpu_10ms(x):
    total = 0
    for i in range(200_000):
        total += i % 7
    return x + total


def w_json(x):
    return json.loads(json.dumps({"x": x, "y": [x] * 3}))


def w_transform(x):
    return {"id": x, "double": x * 2, "text": f"row {x}" * 3}


_WORKLOADS = {
    "trivial": w_trivial,
    "cpu_1ms": w_cpu_1ms,
    "cpu_10ms": w_cpu_10ms,
    "json": w_json,
    "transform": w_transform,
}


def bench_bounded(records, workers, mif, fn):
    sink = ListSink()
    p = Pipeline([GenericStage(process=fn, name="w")])
    t0 = time.monotonic()
    stats = p.run(
        source=IterableSource(range(records)),
        sink=sink,
        executor=ProcessExecutor(workers=workers, max_in_flight=mif),
        ordered=True,
        progress=False,
    )
    dt = time.monotonic() - t0
    return dt, stats.output_records, stats.max_in_flight_observed


def bench_sequential(records, fn):
    p = Pipeline([GenericStage(process=fn, name="w")])
    sink = ListSink()
    t0 = time.monotonic()
    stats = p.run(
        source=IterableSource(range(records)),
        sink=sink,
        executor=SequentialExecutor(),
        progress=False,
    )
    dt = time.monotonic() - t0
    return dt, stats.output_records


def bench_pool_map(records, workers, fn):
    t0 = time.monotonic()
    with ProcessPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(fn, range(records)))
    dt = time.monotonic() - t0
    return dt, len(results)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--records", type=int, default=20000)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--max-in-flight", type=int, default=32)
    ap.add_argument(
        "--workloads",
        nargs="*",
        default=["trivial", "cpu_1ms", "json", "transform"],
    )
    args = ap.parse_args()

    print(
        f"records={args.records} workers={args.workers} "
        f"max_in_flight={args.max_in_flight}"
    )
    print(
        f"{'workload':<12} {'bounded':>12} {'pool.map':>12} "
        f"{'sequential':>12} {'mif':>4}"
    )
    for name in args.workloads:
        if name not in _WORKLOADS:
            print(f"unknown workload {name!r}; known: {sorted(_WORKLOADS)}")
            sys.exit(1)
        fn = _WORKLOADS[name]
        dt_b, _, mif = bench_bounded(args.records, args.workers, args.max_in_flight, fn)
        dt_m, _ = bench_pool_map(args.records, args.workers, fn)
        dt_s, _ = bench_sequential(args.records, fn)
        print(
            f"{name:<12} {dt_b:>10.3f}s {dt_m:>10.3f}s {dt_s:>10.3f}s {mif:>4}"
        )


if __name__ == "__main__":
    main()
