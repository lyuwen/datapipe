# Parallel Record Pipeline: Package Architecture and Implementation Plan

## 1. Purpose

Build a small, inspectable, high-throughput Python package for embarrassingly parallel record processing.

The package should target the recurring data-processing pattern:

```text
source
  -> load
  -> process_1
  -> process_2
  -> ...
  -> serialize
  -> sink
```

The defining execution property is:

> A record is dispatched to a worker once, processed through the entire pipeline inside that worker, and gathered once at the end.

The package should not implement stage-by-stage distributed dataflow. Pipeline stages are compositional definitions of a per-record program, not independently scheduled operators.

Primary goals:

- make parallel batch processing concise;
- provide immediate progress reporting;
- avoid eager submission of an entire dataset;
- use bounded in-flight work and bounded memory;
- preserve input/output ordering when requested;
- make processing stages modular and reusable;
- support JSONL and Parquet as first-class data formats;
- separate pipeline semantics from execution, sharding, and IO;
- support local multiprocessing first;
- design cleanly for later `world_size` / `rank` distributed execution under Slurm, K8s, torchrun-style launchers, or similar environments;
- keep the runtime simple enough that the entire scheduling model can be understood and debugged locally.

Non-goals for the initial implementation:

- general DAG execution;
- inter-record dependencies;
- shuffle/reduce/group-by semantics;
- stage-specific worker pools;
- distributed RPC scheduling;
- dynamic cluster resource management;
- Ray/Beam/Spark replacement;
- transactional workflow orchestration.

---

# 2. Core Mental Model

The library should be described as:

> Define a per-record processing program, then execute that program concurrently over a stream.

Not:

> Build a parallel dataflow graph.

Example:

```python
pipeline = Pipeline([
    GenericStage(
        input=json.loads,
        process=normalize,
        name="normalize",
    ),
    GenericStage(
        process=enrich,
        name="enrich",
    ),
    FilterStage(is_valid),
    GenericStage(
        process=score,
        output=json.dumps,
        name="score",
    ),
])

pipeline.run(
    source=JsonlSource("input.jsonl", raw=True),
    sink=JsonlSink("output.jsonl", raw=True),
    executor=ProcessExecutor(
        workers=32,
        max_in_flight=128,
    ),
    ordered=True,
    progress=True,
)
```

Conceptually this compiles to:

```python
def worker(record, ctx):
    x = record

    x = json.loads(x)
    x = normalize(x)

    x = enrich(x)

    if not is_valid(x):
        return DROP

    x = score(x)
    x = json.dumps(x)

    return x
```

The runtime parallelizes only this final worker callable.

Execution:

```text
main process                         worker processes                    main process

source
  |
  +-- record 0 -------------------> full pipeline(record 0) ----------> result 0
  +-- record 1 -------------------> full pipeline(record 1) ----------> result 1
  +-- record 2 -------------------> full pipeline(record 2) ----------> result 2
  ...
                                    bounded concurrency
```

There must be no dispatch/gather boundary between stages.

---

# 3. High-Level Architecture

Recommended decomposition:

```text
Pipeline
|
|-- Stage
|-- Stage
|-- Stage
|
+-- defines what happens to one record


Executor
|
|-- SequentialExecutor
|-- ThreadExecutor
|-- ProcessExecutor
|
+-- defines local concurrency


Sharding
|
|-- NoSharding
|-- ModuloSharding
|-- RangeSharding
|-- HashSharding
|
+-- defines which records belong to this rank


RuntimeContext
|
|-- rank
|-- world_size
|-- local_rank
|-- node_rank
|-- job metadata
|
+-- defines where this process is running


IO
|
|-- Source
|   |-- JsonlSource
|   |-- ParquetSource
|   `-- IterableSource
|
`-- Sink
    |-- JsonlSink
    |-- ParquetSink
    `-- CallableSink
```

The composition should be approximately:

```python
pipeline.run(
    source=...,
    sink=...,
    executor=...,
    sharding=...,
    runtime=...,
    ordered=...,
    progress=...,
)
```

The `Pipeline` object should be inert until `run()` is called.

---

# 4. Package Layout

Suggested package name in this document: `datapipe`.

Rename freely later.

```text
datapipe/
├── __init__.py
├── pipeline.py
├── stage.py
├── record.py
├── context.py
│
├── execution/
│   ├── __init__.py
│   ├── base.py
│   ├── sequential.py
│   ├── thread.py
│   ├── process.py
│   ├── scheduler.py
│   └── worker.py
│
├── sharding/
│   ├── __init__.py
│   ├── base.py
│   ├── none.py
│   ├── modulo.py
│   ├── range.py
│   └── hash.py
│
├── runtime/
│   ├── __init__.py
│   ├── context.py
│   ├── detect.py
│   ├── slurm.py
│   ├── torchrun.py
│   └── k8s.py
│
├── io/
│   ├── __init__.py
│   ├── base.py
│   ├── iterable.py
│   ├── jsonl.py
│   ├── parquet.py
│   └── utils.py
│
├── progress/
│   ├── __init__.py
│   ├── base.py
│   ├── tqdm.py
│   └── stats.py
│
├── errors.py
├── result.py
├── sentinels.py
├── config.py
│
└── cli/
    ├── __init__.py
    ├── main.py
    ├── run.py
    ├── inspect.py
    └── loaders.py
```

Potential future modules:

```text
datapipe/io/sql.py
datapipe/io/arrow.py
datapipe/io/s3.py
datapipe/io/hf.py
datapipe/execution/distributed.py
datapipe/checkpoint.py
datapipe/retry.py
```

Do not implement these until needed.

---

# 5. Core Data Structures

## 5.1 Record envelope

Internally, each record should have a stable sequence identifier.

```python
from dataclasses import dataclass, field
from typing import Any

@dataclass
class Record:
    seq: int
    value: Any
    metadata: dict[str, Any] = field(default_factory=dict)
```

This sequence identifier enables:

- ordered output;
- deterministic error reporting;
- record-level logging;
- traceability;
- future checkpointing;
- debugging.

Avoid forcing users to manipulate `Record` objects directly.

Normal stage functions should receive the record value.

The runtime can optionally expose context.

---

## 5.2 Worker context

```python
@dataclass
class WorkerContext:
    rank: int
    world_size: int

    worker_id: int
    local_rank: int | None = None

    record_index: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
```

This is available to stages that request it.

Do not require every stage function to accept `ctx`.

The library should support both:

```python
def process(x):
    ...
```

and:

```python
def process(x, ctx):
    ...
```

Prefer explicit declaration on the stage rather than runtime signature guessing where possible.

Example:

```python
GenericStage(
    process=process,
    with_context=True,
)
```

Signature inspection may be supported as convenience, but explicit configuration is easier to reason about.

---

# 6. Stage Model

## 6.1 Base stage

A stage represents a local transformation within one worker.

```python
class Stage:
    name: str

    def setup(self, ctx: WorkerContext) -> None:
        pass

    def process(self, value, ctx: WorkerContext):
        raise NotImplementedError

    def teardown(self, ctx: WorkerContext) -> None:
        pass
```

Important semantics:

- `setup()` runs once per worker process/thread;
- `process()` runs once per record passing through that stage;
- `teardown()` runs once per worker;
- stages execute sequentially inside the worker;
- the runtime does not insert queues between stages.

---

## 6.2 GenericStage

Primary user-facing stage.

Proposed constructor:

```python
GenericStage(
    *,
    process,
    input=None,
    output=None,
    setup=None,
    teardown=None,
    name=None,
    with_context=False,
)
```

Semantics:

```python
def run_stage(x, ctx):
    if input is not None:
        x = input(x)

    x = process(x)

    if output is not None:
        x = output(x)

    return x
```

If `with_context=True`, context should be provided to the relevant callables according to a consistent documented convention.

Recommended convention:

```python
callable(value, ctx)
```

for `input`, `process`, and `output`.

For `setup` and `teardown`:

```python
setup(ctx)
teardown(ctx)
```

Do not mix multiple calling conventions in v1.

---

## 6.3 Predefined stages

Implement a small initial set:

```python
TransformStage(fn)
FilterStage(predicate)
FlatMapStage(fn)
TapStage(fn)
JsonLoadStage()
JsonDumpStage()
```

Possible semantics:

### TransformStage

```python
x -> fn(x)
```

### FilterStage

```python
if predicate(x):
    return x
else:
    return DROP
```

### FlatMapStage

```python
x -> iterable[y]
```

This introduces one-to-many output.

Because this complicates ordering and output accounting, implement only after the one-to-one path is stable.

It is acceptable to defer `FlatMapStage` to phase 2.

### TapStage

```python
fn(x)
return x
```

Useful for diagnostics, side metrics, validation, or logging.

### JsonLoadStage / JsonDumpStage

Convenience wrappers around JSON parsing/serialization.

Do not make them special runtime operations.

They remain normal worker-local stages.

---

# 7. Pipeline Model

## 7.1 Construction

```python
pipeline = Pipeline([
    StageA(...),
    StageB(...),
    StageC(...),
])
```

The constructor should validate:

- all entries are `Stage` instances or coercible callables;
- stage names are stable;
- invalid duplicate configuration is rejected;
- unsupported stage types fail early.

Optionally allow simple callables:

```python
Pipeline([
    json.loads,
    normalize,
    score,
    json.dumps,
])
```

which are coerced into:

```python
TransformStage(...)
```

However, `GenericStage` remains the preferred explicit form.

---

## 7.2 Compilation

`Pipeline.compile()` should produce a worker-local callable or worker program.

Conceptually:

```python
class CompiledPipeline:
    def setup(self, ctx):
        for stage in stages:
            stage.setup(ctx)

    def process(self, value, ctx):
        x = value

        for stage in stages:
            x = stage.process(x, ctx)

            if x is DROP:
                return DROP

        return x

    def teardown(self, ctx):
        for stage in reversed(stages):
            stage.teardown(ctx)
```

The compiled object must be pickleable for `ProcessPoolExecutor` usage.

Avoid nested closures where possible.

Use top-level classes/functions to make multiprocessing behavior portable.

---

## 7.3 Pipeline.run()

Primary public API:

```python
pipeline.run(
    source,
    sink,
    *,
    executor=None,
    sharding=None,
    runtime=None,
    ordered=True,
    progress=True,
    errors="raise",
)
```

Suggested defaults:

```python
executor = ProcessExecutor()
sharding = AutoSharding()
runtime = RuntimeContext.auto()
ordered = True
progress = True
errors = "raise"
```

Potential convenience:

```python
Pipeline.run(
    source="input.jsonl",
    sink="output.jsonl",
)
```

may infer source/sink types later, but explicit IO objects should be the canonical API.

---

# 8. Execution Abstraction

## 8.1 Executor responsibilities

The executor owns local parallelism only.

Interface:

```python
class Executor:
    def run(
        self,
        *,
        records,
        worker,
        on_result,
        on_error,
        progress,
    ) -> ExecutionStats:
        ...
```

Alternatively, expose an iterator:

```python
for result in executor.map_stream(worker, records):
    ...
```

The latter is likely cleaner.

Core requirement:

> The executor must never eagerly submit the full dataset.

It must maintain a bounded number of in-flight futures.

---

## 8.2 ProcessExecutor

Initial main backend:

```python
ProcessExecutor(
    workers=None,
    max_in_flight=None,
    mp_context=None,
)
```

Defaults:

```python
workers = os.cpu_count()
max_in_flight = workers * 4
```

The initial value should be configurable.

Algorithm:

```text
1. Create ProcessPoolExecutor.
2. Pull records lazily from the source.
3. Submit at most max_in_flight.
4. Wait for FIRST_COMPLETED.
5. Gather completed results immediately.
6. Submit replacements immediately.
7. Repeat until source exhausted and no futures remain.
```

Pseudo-code:

```python
pending = {}
source_iter = iter(records)

fill_window()

while pending:
    done, _ = wait(
        pending,
        return_when=FIRST_COMPLETED,
    )

    for future in done:
        meta = pending.pop(future)

        result = future.result()

        yield meta, result

        submit_next_if_available()
```

This solves the original progress issue caused by:

```python
executor.map(...)
```

performing substantial/eager submission work before visible consumption.

---

## 8.3 SequentialExecutor

Critical for:

- testing;
- debugging;
- deterministic reproduction;
- profiling stage logic;
- environments where multiprocessing is undesirable.

```python
SequentialExecutor()
```

The exact same pipeline must work.

---

## 8.4 ThreadExecutor

Useful for IO-heavy record processing.

```python
ThreadExecutor(
    workers=32,
    max_in_flight=128,
)
```

Its API should match `ProcessExecutor`.

Do not duplicate scheduling logic.

Extract bounded-future scheduling into a shared implementation where practical.

---

# 9. Worker Lifecycle

The process executor should use worker initialization so heavy stage state is initialized once per worker.

Desired lifecycle:

```text
worker starts
    |
    +-- Pipeline.setup()
    |     +-- stage 1 setup
    |     +-- stage 2 setup
    |     `-- ...
    |
    +-- process record
    +-- process record
    +-- process record
    |   ...
    |
    `-- Pipeline.teardown()
```

Implementation options:

1. `ProcessPoolExecutor(initializer=..., initargs=...)`
2. process-local global compiled pipeline
3. callable worker object serialized once

Recommended initial approach:

- use an executor initializer;
- store compiled pipeline and worker context in worker-process globals;
- submit only the smallest necessary record payload.

Example:

```python
_WORKER_PIPELINE = None
_WORKER_CONTEXT = None

def _init_worker(compiled_pipeline, runtime_info):
    global _WORKER_PIPELINE, _WORKER_CONTEXT

    _WORKER_PIPELINE = compiled_pipeline
    _WORKER_CONTEXT = make_worker_context(runtime_info)

    _WORKER_PIPELINE.setup(_WORKER_CONTEXT)

def _process_record(record):
    ctx = _WORKER_CONTEXT
    ctx.record_index = record.seq

    return _WORKER_PIPELINE.process(
        record.value,
        ctx,
    )
```

Teardown is harder with `ProcessPoolExecutor`, because there is no robust normal worker finalizer API equivalent to explicit process lifecycle control.

For v1:

- support `setup()` fully;
- make `teardown()` best effort;
- register process-local `atexit` cleanup;
- document that teardown must not be relied upon for correctness.

If strict lifecycle control becomes important, replace `ProcessPoolExecutor` with a small explicit multiprocessing worker-pool implementation later.

---

# 10. Bounded Submission and Backpressure

This is a central design requirement.

At most:

```python
max_in_flight
```

records should exist as submitted tasks.

Benefits:

- bounded memory;
- immediate progress;
- safe processing of arbitrarily large sources;
- natural backpressure;
- avoids millions of `Future` objects.

Example:

```python
ProcessExecutor(
    workers=32,
    max_in_flight=128,
)
```

No matter whether the source contains:

```text
10,000 records
100,000,000 records
```

the runtime stores roughly the same number of submitted tasks.

---

# 11. Ordered Result Gathering

Every input record receives:

```python
seq = monotonically increasing integer
```

Workers may finish out of order.

For `ordered=False`:

```text
emit result immediately
```

For `ordered=True`:

```python
next_to_emit = 0
buffer = {}
```

When a result completes:

```python
buffer[seq] = result

while next_to_emit in buffer:
    sink.write(buffer.pop(next_to_emit))
    next_to_emit += 1
```

Important caveat:

A single very slow early record may cause the reorder buffer to grow.

Example:

```text
record 0: 60 seconds
record 1..5000: 10 ms
```

With ordered output, completed results must wait.

Document this clearly.

Potential later controls:

```python
ordered=True
max_reorder_buffer=...
order_timeout=...
```

Do not implement unless needed.

For distributed execution, ordering is local to a rank by default. Do not attempt strict global ordering across ranks.

---

# 12. Result and Error Model

Define a structured result:

```python
@dataclass
class TaskResult:
    seq: int
    value: Any = None
    error: BaseException | None = None
    dropped: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)
```

Potential error policies:

```python
errors="raise"
errors="skip"
errors="return"
```

### raise

First processing error aborts the run.

### skip

Failed rows are counted and omitted.

### return

Errors are passed to an error sink or exposed as structured results.

Recommended API:

```python
pipeline.run(
    ...,
    errors="skip",
    error_sink=JsonlSink("errors.jsonl"),
)
```

Error record should include:

- input sequence number;
- exception type;
- exception message;
- traceback;
- optional input preview or metadata.

Be careful about dumping huge record bodies automatically.

Default to metadata plus a configurable truncated representation.

---

# 13. Progress and Statistics

## 13.1 Default progress meaning

Progress should represent records successfully handled at the sink boundary, not merely submitted jobs.

For one-to-one processing:

```text
completed == written + dropped + failed
```

A sensible default bar:

```text
Processing  1,482,310 / 2,000,000  31.5k rec/s  errors=23
```

If the total is unknown:

```text
Processing  1,482,310  31.5k rec/s  errors=23
```

---

## 13.2 Progress implementation

Create a small abstraction:

```python
class ProgressReporter:
    def start(self, total=None):
        ...

    def update(self, n=1, **stats):
        ...

    def close(self):
        ...
```

Initial implementation:

```python
TqdmProgress
```

Avoid tightly coupling the runtime to tqdm.

Future CLI progress may use richer terminal rendering.

---

## 13.3 Execution stats

Return a summary object:

```python
@dataclass
class ExecutionStats:
    input_records: int
    completed_records: int
    output_records: int
    dropped_records: int
    failed_records: int

    elapsed_seconds: float
    records_per_second: float

    rank: int
    world_size: int
```

Potentially include queue/future high-water marks.

---

# 14. Source Abstraction

## 14.1 Base source

A source should support logical iteration and optionally efficient physical sharding.

```python
class Source:
    def __iter__(self):
        raise NotImplementedError

    def iter_shard(
        self,
        rank: int,
        world_size: int,
    ):
        return None
```

Better interface:

```python
class Source:
    supports_physical_sharding: bool = False

    def iter_records(self):
        ...

    def iter_shard(self, rank, world_size):
        ...
```

If `iter_shard()` is unsupported, the runner applies logical sharding.

---

# 15. Logical vs Physical Sharding

This distinction should be designed from day one.

## Logical sharding

Every rank reads the whole source but only retains owned records.

Example:

```python
if seq % world_size == rank:
    yield record
```

Advantages:

- universally applicable;
- simple.

Disadvantages:

- terrible for large shared files;
- duplicates IO by `world_size`.

Use only as fallback.

---

## Physical sharding

The source itself reads only the rank's portion.

Examples:

- assign files to ranks;
- assign Parquet row groups;
- seek to byte ranges;
- range queries in SQL.

This should be preferred whenever supported.

---

# 16. JSONL Backend

JSONL should be the ergonomic default.

## 16.1 JsonlSource

Suggested API:

```python
JsonlSource(
    path,
    *,
    raw=False,
    encoding="utf-8",
    compression="auto",
)
```

Modes:

### `raw=False`

Yield parsed Python objects.

### `raw=True`

Yield raw lines.

The raw mode is important when parsing should happen inside workers:

```python
pipeline = Pipeline([
    JsonLoadStage(),
    ...,
])
```

This keeps coordinator work small.

---

## 16.2 JsonlSink

```python
JsonlSink(
    path,
    *,
    raw=False,
    encoding="utf-8",
    compression="auto",
    flush_every=None,
)
```

`raw=False`:

```python
json.dumps(record)
```

`raw=True`:

assume the pipeline already returns serialized JSON strings.

---

## 16.3 Directory datasets

Support paths containing multiple JSONL shards.

Example:

```text
dataset/
  part-00000.jsonl
  part-00001.jsonl
  part-00002.jsonl
```

Physical rank sharding:

```python
files_for_rank = files[rank::world_size]
```

This should be the preferred distributed JSONL format.

A single giant JSONL file can initially fall back to logical sharding.

Byte-range JSONL splitting can be added later.

---

## 16.4 Compression

Optional initial support:

- `.gz`
- `.zst`

If implementation cost is too high, support gzip first and defer zstd.

Compression must remain streaming.

---

# 17. Parquet Backend

Parquet should be the scalable default.

Use `pyarrow`.

## 17.1 ParquetSource

Suggested API:

```python
ParquetSource(
    path,
    *,
    columns=None,
    filters=None,
    batch_size=4096,
)
```

Logical output should remain row records, probably dictionaries by default.

Physical reading should remain batched.

Flow:

```text
read record batch
    |
    +-- row
    +-- row
    +-- row
    ...
```

Do not issue one Parquet IO operation per row.

---

## 17.2 Physical sharding

Prefer, in order:

1. files;
2. row groups;
3. record batches if necessary.

Example:

```text
rank 0 -> row groups 0, 8, 16...
rank 1 -> row groups 1, 9, 17...
```

For dataset directories, file-level assignment is simpler and often sufficient.

---

## 17.3 ParquetSink

Suggested API:

```python
ParquetSink(
    path,
    *,
    schema=None,
    batch_size=4096,
    compression="zstd",
)
```

The sink must buffer output rows into batches before writing.

Never write one row at a time.

---

## 17.4 Schema handling

Support:

```python
ParquetSink(
    ...,
    schema=explicit_schema,
)
```

Preferred for production.

Optional later convenience:

```python
schema="infer"
```

with schema inferred from an initial configurable sample/window.

For v1, explicit schema plus simple first-batch inference is sufficient.

Schema changes mid-run should fail clearly.

---

# 18. Python-native IO

Implement these early because they make testing and customization easy.

## IterableSource

```python
IterableSource(iterable)
```

## CallableSink

```python
CallableSink(fn)
```

## ListSink

Useful for tests:

```python
sink = ListSink()
pipeline.run(...)
assert sink.items == ...
```

Avoid coupling tests to filesystem IO unnecessarily.

---

# 19. SQL Support: Design Now, Implement Later

SQL should not be a v1 dependency.

The source/sink abstractions should make it straightforward to add later.

Possible API:

```python
SqlSource(
    url,
    query,
    *,
    shard_by=None,
    fetch_size=1000,
)
```

Physical sharding could use:

```sql
WHERE MOD(id, :world_size) = :rank
```

or preferably range partitioning:

```sql
WHERE id >= :start AND id < :end
```

Sink:

```python
SqlSink(
    url,
    table,
    *,
    batch_size=1000,
    conflict="error",
)
```

Avoid per-row inserts.

Do not initially use SQL as a dynamic work queue.

Avoid requiring:

```sql
SELECT ... FOR UPDATE SKIP LOCKED
```

unless a future workload genuinely requires work stealing.

The initial distributed model should remain deterministic rank-based sharding.

---

# 20. Sharding Abstraction

Base interface:

```python
class ShardingStrategy:
    def owns(
        self,
        *,
        seq: int,
        value,
        rank: int,
        world_size: int,
    ) -> bool:
        ...
```

Initial implementations:

## NoSharding

```python
world_size == 1
```

Everything belongs to rank 0.

## ModuloSharding

```python
seq % world_size == rank
```

Good generic fallback.

## HashSharding

```python
hash(key(record)) % world_size == rank
```

Useful when stable item keys are available.

Be careful with Python's randomized process hash.

Use a stable hash function.

## RangeSharding

Useful for sources with known total cardinality or key ranges.

---

# 21. Runtime Context and Environment Detection

`RuntimeContext.auto()` should detect common launch environments.

```python
@dataclass
class RuntimeContext:
    rank: int = 0
    world_size: int = 1
    local_rank: int = 0
    node_rank: int | None = None
    job_id: str | None = None
    environment: str = "local"
```

Detection priority should be deterministic.

Recommended:

1. explicit arguments;
2. torchrun-compatible env;
3. Slurm;
4. K8s indexed job;
5. local fallback.

---

## 21.1 torchrun-style

Recognize:

```text
RANK
WORLD_SIZE
LOCAL_RANK
LOCAL_WORLD_SIZE
```

---

## 21.2 Slurm

Recognize:

```text
SLURM_PROCID
SLURM_NTASKS
SLURM_LOCALID
SLURM_NODEID
SLURM_JOB_ID
```

Map:

```python
rank = SLURM_PROCID
world_size = SLURM_NTASKS
local_rank = SLURM_LOCALID
node_rank = SLURM_NODEID
```

---

## 21.3 Kubernetes

K8s has no universal rank environment.

Support explicit env variables first:

```text
RANK
WORLD_SIZE
LOCAL_RANK
```

Also support Indexed Job:

```text
JOB_COMPLETION_INDEX
```

with externally provided:

```text
WORLD_SIZE
```

Do not embed Kubernetes API logic into the runtime in v1.

Kubernetes is primarily a launcher.

---

# 22. Distributed Execution Model

Do not build a central dispatcher.

Bad:

```text
rank 0 reads all data
    |
    +--> rank 1
    +--> rank 2
    +--> rank 3
    ...
    |
rank 0 gathers all output
```

This creates network and coordinator bottlenecks.

Preferred:

```text
shared / partitionable dataset

rank 0:
  read shard 0
  -> local ProcessExecutor
  -> output shard 0

rank 1:
  read shard 1
  -> local ProcessExecutor
  -> output shard 1

rank 2:
  read shard 2
  -> local ProcessExecutor
  -> output shard 2
```

Each rank runs independently.

No rank-to-rank communication should be necessary for ordinary processing.

This allows scaling under:

- Slurm `srun`;
- torchrun;
- K8s Indexed Jobs;
- manually launched containers;
- other rank-aware launchers.

---

# 23. Distributed Output

Default distributed sink behavior should write one shard per rank.

Example:

```text
output/
  part-00000.jsonl
  part-00001.jsonl
  ...
  part-00031.jsonl
```

Likewise for Parquet.

Provide a utility naming convention:

```python
RankedPath(
    base="output/",
    rank=rank,
    world_size=world_size,
)
```

Avoid multiple ranks appending to the same file.

Do not guarantee global ordering.

If a merged file is needed, provide a separate utility later:

```bash
datapipe merge ...
```

This merge utility is not required for v1.

---

# 24. Source/Sink Rank Awareness

Rather than making the runner manually rewrite paths everywhere, define optional rank-aware IO.

Possible design:

```python
source.open(runtime)
sink.open(runtime)
```

Then:

```python
JsonlSink("output/")
```

can automatically choose:

```text
output/part-00037.jsonl
```

when:

```text
rank = 37
world_size > 1
```

For single-rank mode it may simply use the given file.

Be explicit and predictable in path semantics.

Do not silently reinterpret a file path as a directory unless clearly documented.

---

# 25. Pipeline Serialization and CLI Loading

CLI execution needs a way to load pipeline definitions.

Preferred Python-native mechanism:

```bash
datapipe run mypkg.pipeline:training_pipeline ...
```

where:

```python
# mypkg/pipeline.py

training_pipeline = Pipeline([
    ...
])
```

Loader syntax:

```text
module.submodule:object_name
```

Also support file path:

```bash
datapipe run ./pipeline.py:pipeline
```

by importing the module dynamically.

This is preferable to encoding complex stage definitions directly on the command line.

The CLI should execute a pipeline object, not construct a chain of shell pipes.

---

# 26. CLI Design

Initial commands:

```text
datapipe run
datapipe inspect
```

Possible later:

```text
datapipe merge
datapipe validate
```

---

## 26.1 datapipe run

Example:

```bash
datapipe run ./pipeline.py:pipeline \
  --input input.jsonl \
  --output output.parquet \
  --workers 32 \
  --max-in-flight 128
```

Or explicit IO formats:

```bash
datapipe run ./pipeline.py:pipeline \
  --source jsonl:input.jsonl \
  --sink parquet:output/ \
  --workers 32
```

Potential options:

```text
--workers
--max-in-flight
--executor process|thread|sequential

--ordered
--unordered

--errors raise|skip|return
--error-output errors.jsonl

--rank
--world-size
--local-rank

--progress
--no-progress
```

Explicit rank options override environment detection.

---

## 26.2 datapipe inspect

Example:

```bash
datapipe inspect ./pipeline.py:pipeline
```

Output:

```text
Pipeline
  0  JsonLoadStage
  1  normalize
       type: GenericStage
       process: mypkg.steps.normalize
  2  enrich
       type: GenericStage
       setup: mypkg.steps.load_resources
       process: mypkg.steps.enrich
  3  FilterStage
       predicate: mypkg.steps.is_valid
  4  JsonDumpStage
```

Also display whether objects appear pickleable if feasible.

---

# 27. Configuration Files

Do not make YAML configuration a core dependency initially.

Python pipeline definitions are sufficient and much more flexible.

A later config layer may represent execution settings:

```yaml
executor:
  type: process
  workers: 32
  max_in_flight: 128

ordered: true

errors:
  policy: skip
  output: errors.jsonl
```

But stage logic should remain Python-native.

Avoid building a large declarative DSL unless there is a concrete need.

---

# 28. Serialization and Multiprocessing Constraints

This package will rely heavily on pickling.

Document clearly:

- top-level functions are recommended;
- lambdas may fail under spawn;
- nested local functions may fail;
- stages must be pickleable;
- stage configuration should avoid open file handles or non-pickleable clients;
- heavyweight resources should be initialized in `setup()` inside workers.

Example good pattern:

```python
class ModelStage(Stage):
    def __init__(self, model_path):
        self.model_path = model_path
        self.model = None

    def setup(self, ctx):
        self.model = load_model(self.model_path)

    def process(self, x, ctx):
        return self.model(x)
```

Do not construct `self.model` in the parent process if it is large or non-pickleable.

---

# 29. Process Start Method

Be explicit about multiprocessing behavior.

Default should likely follow platform behavior, but allow:

```python
ProcessExecutor(
    mp_context="spawn",
)
```

or:

```python
mp.get_context("spawn")
```

For Linux-only high-performance workloads, `fork` may be desirable, but it is unsafe with some libraries.

Do not silently force one start method globally.

Make it executor-local.

---

# 30. Cancellation and Ctrl-C

The runtime must handle interruption cleanly.

Requirements:

- Ctrl-C stops new submission;
- pending futures are cancelled where possible;
- executor is shut down;
- sink is flushed/closed;
- progress bar is closed;
- original `KeyboardInterrupt` propagates.

Pseudo lifecycle:

```python
try:
    run_loop()
except KeyboardInterrupt:
    stop_submission = True
    cancel_pending()
    raise
finally:
    executor.shutdown(...)
    sink.close()
    progress.close()
```

This needs explicit integration tests.

---

# 31. Sink Safety

For file sinks, consider temporary output + atomic rename where possible.

Example:

```text
output.jsonl.tmp.<pid>
    ->
output.jsonl
```

Only rename after successful completion.

For distributed output:

```text
part-00003.jsonl.tmp
    ->
part-00003.jsonl
```

This helps distinguish completed shards from interrupted shards.

Could be optional:

```python
atomic=True
```

This feature can be phase 2 if it slows down initial implementation.

---

# 32. Checkpointing and Resume

Do not implement initially, but leave room for it.

Future potential:

```python
pipeline.run(
    ...,
    checkpoint=CheckpointConfig(...),
)
```

For deterministic sharded file datasets, simplest resume mechanism may be completed output shard discovery rather than record-level checkpoints.

Avoid introducing per-record persistent state until there is a real need.

---

# 33. Retry Model

Do not overbuild retries in v1.

Possible later stage or runtime retry policy:

```python
RetryPolicy(
    max_attempts=3,
    backoff=...,
    retry_if=...,
)
```

Because the whole record pipeline is executed in one worker invocation, the simplest retry semantics are:

> retry the entire record pipeline.

This preserves the one-dispatch/one-gather model.

Per-stage retries can be handled explicitly inside a stage if needed.

---

# 34. Logging

Use Python `logging`.

Recommended fields:

```text
rank
world_size
worker_id
record_seq
stage_name
```

Avoid noisy per-record logs by default.

Provide clear startup summary:

```text
Pipeline: mypkg.pipeline:pipeline
Executor: process
Workers: 32
Max in flight: 128
Rank: 3/16
Source: ...
Sink: ...
Ordered: true
```

On errors, report stage name when possible.

---

# 35. Stage-Level Error Attribution

The compiled pipeline should wrap errors so the user knows which stage failed.

Example:

```python
class StageExecutionError(Exception):
    stage_name: str
    record_seq: int
    cause: BaseException
```

Conceptually:

```python
for stage in stages:
    try:
        x = stage.process(x, ctx)
    except Exception as exc:
        raise StageExecutionError(
            stage_name=stage.name,
            record_seq=ctx.record_index,
            cause=exc,
        ) from exc
```

This is much better than a generic remote worker traceback.

---

# 36. Progress Total Discovery

Source interface can expose:

```python
@property
def total(self) -> int | None:
    ...
```

Examples:

- list/sequence source: exact total;
- Parquet metadata: often exact;
- directory of known shards: potentially exact;
- arbitrary generator: unknown;
- JSONL single file: unknown unless pre-counted.

Do not scan a huge JSONL file once merely to obtain a progress total.

An unknown-total progress bar is preferable.

Optional future flag:

```python
JsonlSource(..., count_lines=True)
```

but default should not double-read the file.

---

# 37. Performance Principles

The runtime should be designed around the following rules.

## Rule 1: one task boundary per record

Do not submit each stage separately.

## Rule 2: bounded futures

Never create futures proportional to total dataset size.

## Rule 3: minimal coordinator work

Prefer raw source reading and worker-local deserialization when parsing is expensive.

## Rule 4: batch physical IO where appropriate

Parquet source and sink should operate on batches internally.

## Rule 5: no inter-rank communication by default

Distributed scaling should be embarrassingly parallel.

## Rule 6: avoid global ordering

Only local rank ordering is supported by default.

## Rule 7: heavy state belongs in worker setup

Models, tokenizers, clients, parsers, etc.

---

# 38. Potential Optimization: Chunked Worker Submission

Per-record `Future` submission can become expensive for extremely cheap transformations.

Do not optimize prematurely, but leave room for:

```python
ProcessExecutor(
    workers=32,
    chunk_size=32,
)
```

Then each submitted task carries a small list of records:

```text
dispatch:
  [record 0..31]

worker:
  process each independently

gather:
  [result 0..31]
```

This can significantly reduce IPC/Future overhead.

However:

- progress granularity becomes chunk-based internally;
- errors need per-record attribution;
- ordered handling becomes slightly more complex.

Implement only after benchmarks demonstrate need.

---

# 39. Potential Optimization: Shared Memory / Arrow

Do not include in v1.

For large Parquet/Arrow workloads, process IPC can eventually become expensive.

Possible future paths:

- Arrow RecordBatch;
- shared memory;
- Plasma-like mechanisms;
- chunked submission.

The API should not assume worker inputs are always dictionaries.

Keep value type generic.

---

# 40. Testing Strategy

Testing should be extensive because concurrency bugs are subtle.

## 40.1 Unit tests

### Pipeline

- stage ordering;
- generic input/process/output semantics;
- filter/drop behavior;
- setup invocation;
- error wrapping.

### Sharding

- modulo ownership;
- stable hash sharding;
- range sharding;
- rank/world-size validation.

### Runtime detection

Mock environment variables for:

- local;
- torchrun;
- Slurm;
- K8s indexed job.

### JSONL

- read;
- write;
- raw mode;
- parsed mode;
- malformed lines;
- empty file;
- Unicode.

### Parquet

- read/write;
- schema;
- columns;
- directory datasets;
- row-group sharding.

---

## 40.2 Executor tests

### Bounded submission

Critical test.

Create a source that tracks how far it has been consumed.

Verify that before workers complete, source consumption does not exceed approximately:

```text
max_in_flight
```

This proves the runtime is lazy.

### Immediate progress

Use slow worker functions.

Verify progress updates occur before the input source has been fully consumed.

This specifically protects against regression to `executor.map()`-style eager behavior.

### Ordered mode

Workers sleep different durations.

Input:

```text
0 1 2 3 ...
```

Completion intentionally:

```text
3 1 4 0 2 ...
```

Verify output remains input ordered.

### Unordered mode

Verify results can be emitted as completed.

### Error policies

Test:

```text
raise
skip
return
```

### KeyboardInterrupt

Use subprocess-based integration tests where needed.

### Worker setup

Ensure setup happens once per worker, not per record.

---

# 41. Distributed Tests Without a Cluster

Simulate ranks locally.

Given a dataset of 1000 records:

```python
for rank in range(world_size):
    run(
        runtime=RuntimeContext(
            rank=rank,
            world_size=world_size,
        )
    )
```

Verify:

- union of outputs equals full dataset;
- no record appears in two ranks;
- all expected records are present;
- shard naming is correct.

For physical Parquet sharding, verify ranks open only assigned files/row groups if practical.

---

# 42. Benchmark Suite

Create lightweight benchmarks early.

Compare against:

```python
ProcessPoolExecutor.map
```

and a hand-written bounded-future implementation.

Workload classes:

1. trivial CPU operation;
2. 1 ms CPU operation;
3. 10 ms CPU operation;
4. JSON parse/serialize;
5. moderate Python transformation;
6. large-record serialization;
7. Parquet read -> transform -> Parquet write.

Measure:

```text
records/sec
peak RSS
time to first progress update
time to first result
CPU utilization
```

The package should not claim performance wins without these measurements.

---

# 43. Public API Proposal

Top-level exports:

```python
from datapipe import (
    Pipeline,
    Stage,
    GenericStage,
    TransformStage,
    FilterStage,
    JsonLoadStage,
    JsonDumpStage,

    ProcessExecutor,
    ThreadExecutor,
    SequentialExecutor,

    RuntimeContext,

    NoSharding,
    ModuloSharding,
    HashSharding,
    RangeSharding,

    JsonlSource,
    JsonlSink,
    ParquetSource,
    ParquetSink,
    IterableSource,
    CallableSink,
)
```

Example:

```python
pipeline = Pipeline([
    GenericStage(
        input=json.loads,
        process=normalize,
        name="normalize",
    ),
    GenericStage(
        setup=init_resources,
        process=enrich,
        name="enrich",
    ),
    FilterStage(is_valid),
    GenericStage(
        process=score,
        output=json.dumps,
        name="score",
    ),
])

pipeline.run(
    source=JsonlSource("input.jsonl", raw=True),
    sink=JsonlSink("output.jsonl", raw=True),
    executor=ProcessExecutor(
        workers=32,
        max_in_flight=128,
    ),
    runtime=RuntimeContext.auto(),
    sharding=ModuloSharding(),
    ordered=True,
)
```

---

# 44. Optional Convenience API

After the explicit API is stable, add convenience helpers.

Example:

```python
pipeline.run(
    input="input.jsonl",
    output="output.jsonl",
    workers=32,
)
```

Internally translated to:

```python
source=JsonlSource(...)
sink=JsonlSink(...)
executor=ProcessExecutor(...)
```

Do not make the convenience API the architectural foundation.

---

# 45. Implementation Phases

## Phase 0: Skeleton

Goal: establish package structure and interfaces.

Implement:

- `Stage`;
- `GenericStage`;
- `TransformStage`;
- `FilterStage`;
- `Pipeline`;
- `Record`;
- `TaskResult`;
- `RuntimeContext`;
- abstract Source/Sink;
- abstract Executor;
- basic tests.

No multiprocessing yet.

Deliverable:

```python
Pipeline([...]).run(
    ...,
    executor=SequentialExecutor(),
)
```

works.

---

## Phase 1: Local bounded multiprocessing

This is the first truly useful version.

Implement:

- `ProcessExecutor`;
- bounded future submission;
- `FIRST_COMPLETED` gather loop;
- `max_in_flight`;
- ordered/unordered result handling;
- tqdm progress;
- structured error handling;
- worker setup;
- graceful shutdown;
- `IterableSource`;
- `CallableSink`;
- `JsonlSource`;
- `JsonlSink`.

Acceptance criteria:

1. Processing begins before the entire input is consumed.
2. Progress updates while more input is still being submitted.
3. At most approximately `max_in_flight` futures exist.
4. Output can preserve input order.
5. 10M-record JSONL does not require memory proportional to dataset size.
6. Ctrl-C terminates cleanly.
7. One malformed row can be skipped when configured.
8. Sequential and Process executors produce equivalent outputs.

This phase directly solves the original problem.

---

## Phase 2: Parquet and richer stages

Implement:

- `ParquetSource`;
- `ParquetSink`;
- batched physical IO;
- Parquet file/dataset discovery;
- schema support;
- file-level and row-group physical sharding;
- `TapStage`;
- optionally `FlatMapStage`;
- execution statistics.

Acceptance criteria:

- Parquet dataset can be transformed without loading entire dataset;
- output is written in batches;
- multiple ranks can process non-overlapping Parquet partitions;
- memory remains bounded.

---

## Phase 3: Rank/world-size execution

Implement:

- `RuntimeContext.auto()`;
- torchrun env detection;
- Slurm env detection;
- K8s indexed-job env detection;
- `NoSharding`;
- `ModuloSharding`;
- `HashSharding`;
- rank-aware output paths;
- distributed simulation tests.

Acceptance criteria:

```bash
WORLD_SIZE=4 RANK=0 ...
WORLD_SIZE=4 RANK=1 ...
WORLD_SIZE=4 RANK=2 ...
WORLD_SIZE=4 RANK=3 ...
```

processes the dataset exactly once in aggregate.

No inter-rank communication is required.

---

## Phase 4: CLI

Implement:

```text
datapipe run
datapipe inspect
```

Pipeline loader:

```text
module:object
file.py:object
```

Example:

```bash
datapipe run pipeline.py:pipeline \
  --source jsonl:input.jsonl \
  --sink jsonl:output.jsonl \
  --workers 32 \
  --max-in-flight 128
```

Acceptance criteria:

- CLI and Python API use the exact same internal runtime;
- no alternate CLI-specific execution implementation;
- pipeline inspection works without running data.

---

## Phase 5: Production hardening

Potential additions based on real usage:

- retry policy;
- atomic sink outputs;
- failure sidecar;
- shard completion markers;
- richer CLI progress;
- chunked worker submissions;
- resume/checkpoint support;
- better worker teardown;
- zstd JSONL;
- configurable metrics hooks.

Only implement features justified by actual workloads.

---

## Phase 6: Optional SQL backend

Only after a concrete use case exists.

Implement:

- `SqlSource`;
- streaming/fetchmany reads;
- deterministic rank sharding;
- range partition support;
- `SqlSink`;
- batch inserts/upserts;
- connection initialization per worker or coordinator depending on semantics.

Do not turn SQL into a scheduler/work queue unless required.

---

# 46. Suggested Internal Run Flow

`Pipeline.run()` should roughly perform:

```text
1. Resolve RuntimeContext.
2. Resolve sharding strategy.
3. Open source.
4. Ask source for physical shard if supported.
5. Otherwise wrap source in logical sharding iterator.
6. Open sink for current rank.
7. Compile Pipeline.
8. Create Executor.
9. Initialize progress reporter.
10. Execute bounded parallel map.
11. Gather results.
12. Apply ordered reorder buffer if requested.
13. Write results to sink.
14. Update progress/stats.
15. Close executor.
16. Close sink.
17. Close source.
18. Return ExecutionStats.
```

Pseudo-code:

```python
def run(...):
    runtime = runtime or RuntimeContext.auto()
    sharding = sharding or default_sharding(runtime)
    executor = executor or ProcessExecutor()

    source_iter = source.iter_for_runtime(
        runtime=runtime,
        sharding=sharding,
    )

    compiled = self.compile()

    with source, sink, progress:
        results = executor.map_stream(
            compiled,
            enumerate(source_iter),
            runtime=runtime,
        )

        for result in gather(results, ordered=ordered):
            if result.error:
                handle_error(result)
                continue

            if result.dropped:
                stats.dropped += 1
                progress.update(1)
                continue

            sink.write(result.value)
            stats.output_records += 1
            progress.update(1)

    return stats
```

---

# 47. Important Architectural Invariants

The worker agent implementing this should preserve these invariants.

## Invariant 1

`Pipeline([...])` performs no data movement and starts no workers.

## Invariant 2

A normal record crosses the executor/process boundary once in and once out.

## Invariant 3

Stages do not get independent process pools.

## Invariant 4

The runtime never eagerly submits the entire source.

## Invariant 5

Memory usage is bounded primarily by:

```text
max_in_flight
+ reorder buffer
+ source/sink internal batch buffers
```

not dataset size.

## Invariant 6

Distribution is orthogonal to local concurrency.

```text
world_size/rank
```

defines global ownership.

```text
workers
```

defines local concurrency.

## Invariant 7

Distributed ranks do not communicate for normal processing.

## Invariant 8

Source physical sharding is preferred over logical filtering.

## Invariant 9

JSONL and Parquet are IO adapters, not special execution modes.

## Invariant 10

The same pipeline should run under sequential, threaded, process, and future distributed-local execution without stage changes.

---

# 48. Explicit Anti-Patterns to Avoid

Do not implement:

```text
source -> worker pool for stage 1
       -> queue
       -> worker pool for stage 2
       -> queue
       -> worker pool for stage 3
```

Do not use:

```python
ProcessPoolExecutor.map(...)
```

as the main scheduling primitive if it causes eager submission behavior.

Do not create:

```text
one Future per dataset record
```

up front.

Do not make:

```text
rank 0
```

a global dispatcher/gatherer.

Do not force users to write:

```python
pipeline.map(...).map(...).filter(...)
```

The preferred construction is:

```python
Pipeline([
    Stage(...),
    Stage(...),
])
```

Do not model the CLI as Unix pipes between multiple `datapipe` processes.

The CLI should load one complete pipeline definition and launch one runtime.

Do not make SQL a mandatory dependency.

Do not add DAG semantics until a concrete use case requires them.

---

# 49. Example: JSONL to JSONL

```python
import json

from datapipe import (
    Pipeline,
    GenericStage,
    FilterStage,
    JsonlSource,
    JsonlSink,
    ProcessExecutor,
)

def normalize(x):
    x["text"] = x["text"].strip()
    return x

def valid(x):
    return bool(x["text"])

def score(x):
    x["length"] = len(x["text"])
    return x

pipeline = Pipeline([
    GenericStage(
        input=json.loads,
        process=normalize,
        name="normalize",
    ),
    FilterStage(valid),
    GenericStage(
        process=score,
        output=json.dumps,
        name="score",
    ),
])

if __name__ == "__main__":
    pipeline.run(
        source=JsonlSource(
            "input.jsonl",
            raw=True,
        ),
        sink=JsonlSink(
            "output.jsonl",
            raw=True,
        ),
        executor=ProcessExecutor(
            workers=32,
            max_in_flight=128,
        ),
        ordered=True,
    )
```

---

# 50. Example: Parquet Processing

```python
pipeline = Pipeline([
    GenericStage(
        process=normalize,
        name="normalize",
    ),
    GenericStage(
        process=enrich,
        name="enrich",
    ),
    FilterStage(valid),
])

pipeline.run(
    source=ParquetSource(
        "input_dataset/",
        columns=[
            "id",
            "text",
            "metadata",
        ],
    ),
    sink=ParquetSink(
        "output_dataset/",
        schema=OUTPUT_SCHEMA,
        batch_size=4096,
    ),
    executor=ProcessExecutor(
        workers=32,
        max_in_flight=256,
    ),
)
```

---

# 51. Example: Slurm

Pipeline code remains unchanged.

Launcher:

```bash
srun \
  --nodes=8 \
  --ntasks-per-node=1 \
  python process_dataset.py
```

Runtime detection obtains:

```text
rank       = SLURM_PROCID
world_size = SLURM_NTASKS
```

Each rank:

```text
selects its source shard
runs local ProcessExecutor
writes its own output shard
```

If each node has 64 CPUs:

```python
ProcessExecutor(
    workers=56,
    max_in_flight=224,
)
```

can be selected by the application.

The package should not assume one specific workers-per-node value.

---

# 52. Example: K8s Indexed Job

Environment:

```text
JOB_COMPLETION_INDEX=3
WORLD_SIZE=16
```

Runtime:

```python
RuntimeContext(
    rank=3,
    world_size=16,
)
```

Each pod independently executes:

```python
pipeline.run(...)
```

No Kubernetes control-plane interaction is necessary from the library.

---

# 53. Documentation Required for Initial Release

Write these docs:

```text
README.md
docs/concepts.md
docs/pipeline.md
docs/execution.md
docs/io-jsonl.md
docs/io-parquet.md
docs/distributed.md
docs/cli.md
```

README should include:

1. problem statement;
2. 20-line quick start;
3. explanation of bounded dispatch;
4. explanation of fused per-record stages;
5. local multiprocessing example;
6. rank/world-size example;
7. JSONL and Parquet examples.

---

# 54. Initial Dependency Recommendation

Core:

```text
Python >= 3.10
tqdm
```

JSON can use standard library.

Parquet optional extra:

```text
pyarrow
```

Suggested packaging:

```toml
[project.optional-dependencies]
parquet = ["pyarrow>=..."]
```

CLI may use:

```text
argparse
```

initially.

Do not add Click/Typer unless their ergonomics are clearly useful.

Keep the dependency tree small.

---

# 55. Definition of Done for the First Worker-Agent Implementation

The first implementation should be considered successful when all of the following work:

```python
pipeline = Pipeline([
    GenericStage(...),
    GenericStage(...),
    FilterStage(...),
])

pipeline.run(
    source=JsonlSource("input.jsonl"),
    sink=JsonlSink("output.jsonl"),
    executor=ProcessExecutor(
        workers=32,
        max_in_flight=128,
    ),
    ordered=True,
)
```

and demonstrates:

- bounded submission;
- progress visible immediately;
- no full-input pre-submission delay;
- ordered output;
- worker-local pipeline composition;
- worker setup;
- robust errors;
- clean Ctrl-C;
- sequential fallback;
- JSONL IO;
- unit/integration tests.

The implementation should also contain the interfaces and basic structures for:

- `RuntimeContext(rank, world_size)`;
- `ShardingStrategy`;
- physical-shard-aware `Source`;
- rank-aware `Sink`;

even if full distributed launch testing is deferred to the next phase.

The architecture must make the following future transition require no changes to pipeline stage definitions:

```python
# local
runtime = RuntimeContext(
    rank=0,
    world_size=1,
)

# distributed
runtime = RuntimeContext(
    rank=37,
    world_size=64,
)
```

That is the central scalability requirement.

---

# 56. Summary

The package should remain intentionally narrow.

Its core abstraction is:

```text
Pipeline = modular per-record program
Executor = local concurrency
Sharding = global record ownership
RuntimeContext = rank/world metadata
Source/Sink = storage adapters
```

Execution is:

```text
source shard
    |
    v
bounded dispatch
    |
    v
worker:
    stage 1
    stage 2
    ...
    stage N
    |
    v
bounded gather
    |
    v
ordered/unordered sink
```

For distributed processing:

```text
dataset
  |
  +-- rank 0 shard -> local worker pool -> output shard 0
  +-- rank 1 shard -> local worker pool -> output shard 1
  +-- ...
```

No central dispatcher, no stage-wise scheduling, and no inter-rank communication are required for the target workload.

Start with:

```text
SequentialExecutor
ProcessExecutor
JSONL
bounded scheduling
progress
ordering
errors
```

then add:

```text
Parquet
physical sharding
rank/world-size runtime
CLI
```

and only later add SQL, retries, checkpoints, chunked dispatch, or other advanced behavior when real workloads justify them.
