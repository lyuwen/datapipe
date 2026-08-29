# datapipe

Small, inspectable, high-throughput Python package for **embarrassingly
parallel record processing**.

> Define a per-record processing program, then execute that program
> concurrently over a stream.

Not a distributed dataflow engine. A record is dispatched to a worker once,
processed through the *entire* pipeline inside that worker, and gathered once
at the end. Pipeline stages are compositional definitions of a per-record
program, not independently scheduled operators.

## Quick start

```python
import json

from datapipe import (
    FilterStage,
    GenericStage,
    JsonlSink,
    JsonlSource,
    Pipeline,
    ProcessExecutor,
)

def normalize(x):
    x["text"] = x["text"].strip()
    return x

def is_valid(x):
    return bool(x["text"])

def score(x):
    x["length"] = len(x["text"])
    return x

pipeline = Pipeline([
    GenericStage(input=json.loads, process=normalize, name="normalize"),
    FilterStage(is_valid),
    GenericStage(process=score, name="score"),
])

if __name__ == "__main__":
    pipeline.run(
        source=JsonlSource("input.jsonl", raw=True),
        sink=JsonlSink("output.jsonl"),
        executor=ProcessExecutor(workers=32, max_in_flight=128),
        ordered=True,
        progress=True,
    )
```

This compiles to a single worker-local callable:

```python
def worker(record, ctx):
    x = json.loads(record)
    x = normalize(x)
    if not is_valid(x):
        return DROP
    x = score(x)
    return x
```

The runtime parallelizes only this final callable.

## Core ideas

### 1. Fused per-record stages

Stages run sequentially inside one worker. The runtime never inserts queues
between stages and never gives a stage its own process pool.

```python
pipeline = Pipeline([
    GenericStage(process=enrich, setup=load_model, name="enrich"),
    FilterStage(is_valid),
    GenericStage(process=score, name="score"),
])
```

`setup()` runs once per worker (perfect for loading models, tokenizers, or
clients); `process()` runs once per record; `teardown()` is best-effort.

### 2. Bounded dispatch

The executor never eagerly submits the whole dataset. At most `max_in_flight`
tasks exist as submitted work at any moment:

```python
ProcessExecutor(workers=32, max_in_flight=128)
```

This keeps memory bounded regardless of input size and makes progress visible
immediately — before the input has been fully consumed.

### 3. Ordered or unordered output

Every input record gets a monotonic `seq`. Workers may finish out of order.

- `ordered=True` (default): results are buffered and emitted in input order.
- `ordered=False`: results are emitted as they complete.

> Note: a single very slow early record can grow the reorder buffer when
> `ordered=True`. For distributed runs, ordering is local to each rank.

### 4. Error handling

```python
pipeline.run(
    ...,
    errors="raise",  # default: first error aborts the run
    # errors="skip",               # count and omit failed rows
    # errors="return",
    # error_sink=JsonlSink("errors.jsonl"),
)
```

Errors are attributed to the failing stage via `StageExecutionError`, which
carries `stage_name`, `record_seq`, and the original `cause`.

### 5. Sequential fallback

The exact same pipeline runs under any executor:

```python
pipeline.run(..., executor=SequentialExecutor())  # deterministic, debuggable
pipeline.run(..., executor=ThreadExecutor(workers=8, max_in_flight=32))
```

## IO

```python
# JSONL (raw or parsed, gzip/zstd auto by extension)
JsonlSource("in.jsonl")          # parsed dicts
JsonlSource("in.jsonl", raw=True)  # raw lines -> parse in workers
JsonlSink("out/")                # directory -> part-NNNNN.jsonl per rank

# Parquet (requires: pip install datapipe[parquet])
ParquetSource("dataset/", columns=["id", "text"], batch_size=4096)
ParquetSink("out/", schema=OUTPUT_SCHEMA, batch_size=4096)

# Python-native
IterableSource([...])
CallableSink(fn)
ListSink()  # great for tests
```

## Distributed (rank/world-size)

Distributed execution is orthogonal to local concurrency. Each rank reads
its own source shard, runs its own local `ProcessExecutor`, and writes its
own output shard. No inter-rank communication is required.

```python
from datapipe import RuntimeContext

# local
runtime = RuntimeContext(rank=0, world_size=1)

# distributed (Slurm, torchrun, K8s indexed job, or manual)
runtime = RuntimeContext(rank=37, world_size=64)
```

`RuntimeContext.auto()` detects `torchrun` (`RANK`/`WORLD_SIZE`), Slurm
(`SLURM_PROCID`/`SLURM_NTASKS`), and K8s indexed jobs
(`JOB_COMPLETION_INDEX`/`WORLD_SIZE`), falling back to local.

```python
pipeline.run(
    source=JsonlSource("dataset/"),   # physical sharding: files[rank::world_size]
    sink=JsonlSink("output/"),        # output/part-{rank:05d}.jsonl
    executor=ProcessExecutor(workers=56, max_in_flight=224),
    runtime=RuntimeContext.auto(),
)
```

Sharding strategies: `NoSharding`, `ModuloSharding`, `HashSharding`
(stable SHA-256 based), `RangeSharding`.

## Installing

```bash
pip install .              # core (Python >= 3.10, tqdm)
pip install .[parquet]     # + pyarrow
pip install .[zstd]        # + zstandard
```

## Running tests

```bash
python -m pytest tests/
```

## Documentation

- [Concepts](docs/concepts.md)
- [Pipeline](docs/pipeline.md)
- [Execution](docs/execution.md)
- [JSONL IO](docs/io-jsonl.md)
- [Parquet IO](docs/io-parquet.md)
- [Distributed](docs/distributed.md)

## Notes for contributors

- **Pickling**: stages must be pickleable. Prefer top-level functions; avoid
  lambdas and nested closures under the `spawn` start method. Heavy resources
  belong in `setup()`.
- **Worker teardown** under process executors is best-effort (`atexit`) and
  must not be relied on for correctness.

## Project layout

```
datapipe/
├── __init__.py
├── pipeline.py       # Pipeline + CompiledPipeline + run loop
├── stage.py          # Stage, GenericStage, Transform/Filter/Tap/Json stages
├── record.py         # Record, sentinels (DROP, ...)
├── context.py        # WorkerContext
├── result.py         # TaskResult, ExecutionStats
├── errors.py
├── execution/        # Executor, Sequential/Thread/Process, worker entrypoints
├── sharding/         # NoSharding, ModuloSharding, HashSharding, RangeSharding
├── runtime/          # RuntimeContext, env detection (torchrun/Slurm/K8s)
├── io/               # Source/Sink, JSONL, Parquet, iterable
├── progress/         # ProgressReporter, TqdmProgress
└── cli/              # CLI skeleton (Phase 4)
```
