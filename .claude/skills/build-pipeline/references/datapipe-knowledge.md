# datapipe Domain Knowledge

Generated from source at commit HEAD of the `skills` branch. Run
`/sync-knowledge` to regenerate after architectural changes.

---

## Core mental model

datapipe's defining property:

> A record is dispatched to a worker **once**, processed through the entire
> pipeline **inside that worker**, and gathered **once** at the end.

The system compiles a sequence of stages into a single fused callable
(`CompiledPipeline`), then runs that callable concurrently over a stream of
records. Stages are *composition units inside one worker*, not independently
scheduled operators with their own queues.

The four orthogonal axes:

| Axis | Role |
|------|------|
| `Pipeline` | Modular per-record program (sequence of stages) |
| `Executor` | Local concurrency — sequential, thread, or process |
| `Sharding` | Global record ownership — which rank owns which records |
| `RuntimeContext` | rank / world_size / local_rank / job metadata |

These are deliberately independent: the same `Pipeline` runs unchanged under
any executor; distribution (`world_size`/`rank`) is orthogonal to local
concurrency (`workers`); `Sharding` decides global ownership, `Executor`
decides local parallelism; JSONL and Parquet are IO adapters, not execution
modes.

Bounded dispatch: the executor never eagerly submits the full dataset. At most
`max_in_flight` tasks exist as submitted work, giving bounded memory, immediate
progress, natural backpressure, and safe processing of arbitrarily large sources.

---

## Stage composition API

### `Stage` base class

```python
from datapipe.stage import Stage
from datapipe.context import WorkerContext
from datapipe.sentinels import DROP

class MyStage(Stage):
    name = "my_stage"          # shown in inspect output; must be unique in a Pipeline

    def setup(self, ctx: WorkerContext) -> None:
        """Runs once per worker. Load models, open connections, build caches here.
        Failure here aborts the entire run — never swallow exceptions."""

    def process(self, value: Any, ctx: WorkerContext) -> Any:
        """Runs once per record. Return the transformed value, or DROP to discard."""
        raise NotImplementedError

    def teardown(self, ctx: WorkerContext) -> None:
        """Runs once per worker (best-effort under ProcessExecutor via atexit)."""
```

`WorkerContext` fields: `rank`, `world_size`, `worker_id`, `local_rank`,
`record_index` (set just before each `process()` call).

### `GenericStage`

Wraps up to three callables into one stage without subclassing:

```python
from datapipe.stage import GenericStage

stage = GenericStage(
    process=normalize,        # required
    input=json.loads,         # applied before process (optional)
    output=json.dumps,        # applied after process (optional)
    setup=load_resources,     # optional
    teardown=release,         # optional
    name="normalize",
    with_context=False,       # set True to receive WorkerContext as second arg
)
```

Per-record semantics: `output(process(input(value)))`. Any callable returning
`DROP` drops the record.

### `JsonLoadStage` / `JsonDumpStage`

```python
from datapipe.stage import JsonLoadStage, JsonDumpStage

JsonLoadStage()   # parses a JSON string → Python object
JsonDumpStage()   # serializes a Python object → JSON string
```

Used in pairs when the source/sink is opened in `raw=True` mode so workers
handle JSON parsing (keeping coordinator work small).

### `coerce_stage`

`Pipeline` calls `coerce_stage()` on each entry: `Stage` instances pass
through; plain callables become `TransformStage`. You can pass lambdas or
named functions as pipeline entries directly.

### Deep-copy and lock safety for `ThreadExecutor`

`Stage.__deepcopy__` replaces `threading.Lock` / `threading.RLock` instances
with fresh equivalents rather than sharing them across threads. This means
stages with locks in construction-time state work correctly under
`ThreadExecutor` without any extra care from the author.

---

## Executor options and trade-offs

### `SequentialExecutor`

```python
from datapipe.execution.sequential import SequentialExecutor
executor = SequentialExecutor()
```

Single-threaded, runs in the calling process. `workers = 1`. Best for
debugging, deterministic reproduction, profiling, and environments where
multiprocessing is undesirable. No pickling requirement.

### `ThreadExecutor`

```python
from datapipe.execution.thread import ThreadExecutor
executor = ThreadExecutor(workers=8, max_in_flight=32)
```

Bounded concurrency with threads. Each worker thread gets its own deep copy of
the compiled pipeline and its own `WorkerContext`. Useful for IO-heavy workloads
where GIL contention is acceptable. No pickling requirement (shared memory).
`max_in_flight` defaults to `workers * 4`.

### `ProcessExecutor` (default)

```python
from datapipe.execution.process import ProcessExecutor
executor = ProcessExecutor(workers=16, max_in_flight=64)
# or with explicit multiprocessing context:
executor = ProcessExecutor(workers=16, mp_context="spawn")
```

True parallelism via `ProcessPoolExecutor`. `workers` defaults to
`os.cpu_count()`. `max_in_flight` defaults to `workers * 4`. Uses `spawn`
start method on most platforms. **Everything that crosses the process boundary
must be pickleable** — compiled pipelines, stage state, tool descriptors,
errors. `setup()` runs once per worker process; `teardown()` is best-effort
via `atexit`.

`Pipeline.run()` uses `ProcessExecutor()` when no executor is specified.

---

## IO sources and sinks

### `JsonlSource` / `JsonlSink`

```python
from datapipe.io.jsonl import JsonlSource, JsonlSink

# Coordinator parses JSON (default)
source = JsonlSource("input.jsonl")
sink   = JsonlSink("output.jsonl")

# Raw mode: coordinator yields/accepts unparsed strings;
# workers handle JSON via JsonLoadStage / JsonDumpStage
source = JsonlSource("input.jsonl", raw=True)
sink   = JsonlSink("output.jsonl", raw=True)
```

Additional `JsonlSource` kwargs: `encoding` (default `"utf-8"`),
`compression` (default `"auto"` — detects `.gz` / `.zst`).

Directory paths are supported: a directory source reads all JSONL shards; a
directory sink writes `part-{rank:05d}.jsonl` per rank.

`raw=True` is required when the pipeline begins with `JsonLoadStage` (as the
`datapipe transform` command does). Using `raw=True` source with a non-raw
pipeline is a bug — the first stage receives a string, not a dict.

### `ParquetSource` / `ParquetSink`

```python
from datapipe.io.parquet import ParquetSource, ParquetSink
```

Requires the `pyarrow` extra (`pip install datapipe[parquet]`). For
Hive-partitioned directories, creates one dataset from the directory. Filtering
happens before column projection. Not yet usable with DSL expressions (awaiting
column/schema selector semantics).

### `Pipeline.run()` convenience coercions

- A `str` source/sink is auto-coerced to `JsonlSource`/`JsonlSink`.
- A non-`Source` iterable is auto-coerced to `IterableSource`.
- A callable sink is auto-coerced to `CallableSink`.

---

## Error policies

Set via the `errors` parameter on `Pipeline.run()` (default `"raise"`):

| Policy | Behaviour |
|--------|-----------|
| `"raise"` | First per-record error aborts the run immediately. |
| `"skip"` | Failed records are counted (`stats.failed_records`) and dropped from the output. |
| `"return"` | Error payloads are written to `error_sink` (or into the primary sink as structured dicts if no `error_sink` is given). |

`setup()` failure is **never** subject to the error policy. A `WorkerSetupError`
always aborts the run, regardless of the `errors` setting. This is by design:
a worker that failed initialization cannot process any records safely.

`Pipeline.run()` full signature for reference:

```python
stats = pipeline.run(
    source=source,
    sink=sink,
    executor=ProcessExecutor(),      # default
    sharding=None,                   # auto-detected from runtime
    runtime=None,                    # auto-detected from environment
    ordered=True,                    # preserve input order
    progress=True,                   # show tqdm progress bar
    errors="raise",                  # "raise" | "skip" | "return"
    error_sink=None,                 # Sink for error payloads (errors="return")
    max_in_flight=None,              # hard cap; default = workers * 4
    progress_reporter=None,
) -> ExecutionStats
```

`ExecutionStats` fields: `completed_records`, `failed_records`,
`dropped_records`, `elapsed_seconds`, `records_per_second`,
`max_in_flight_observed`.

---

## Key invariants

These must never be violated in generated pipelines:

- **One dispatch and one gather per record.** No per-stage process pools,
  no per-stage futures, no stage-level parallelism.
- **`max_in_flight` is a hard cap on submitted futures**, not a guideline.
  The scheduler enforces it; the reorder buffer may grow if a straggler
  blocks, but submission is always bounded.
- **`stage.setup()` failure aborts the run.** It is never converted into a
  per-record skip, regardless of the `errors` policy.
- **Only `one_to_one` cardinality is executable.** Returning a list from
  `process()` makes a list the output value — it is not implicit flat-map.
- **Everything crossing the process boundary must be pickleable.** Stage
  instances, their construction-time state, tool descriptors, and errors
  all travel between processes.
- **The data plane never imports from the CLI or mutable registry state.**
  `datapipe.cli`, `datapipe.dsl`, and `datapipe.tools.registry` must not
  be imported inside stage `setup()` or `process()`.

---

## Common anti-patterns

**Creating an executor inside a stage:**
```python
# WRONG — nested parallelism, process-pool-inside-worker
class BadStage(Stage):
    def process(self, value, ctx):
        with ProcessPoolExecutor() as p:
            return list(p.map(fn, value["items"]))
```
Workers already run inside an executor. Create a `ThreadExecutor` or
`ProcessExecutor` only at the `Pipeline.run()` call site.

**Expecting flat-map from a returned list:**
```python
# WRONG — process() returning a list makes list the output value,
# it does NOT fan out into multiple records
class BadStage(Stage):
    def process(self, value, ctx):
        return [transform(item) for item in value["items"]]  # list is ONE output
```

**Importing CLI/registry inside a stage:**
```python
# WRONG — data plane imports control plane
class BadStage(Stage):
    def process(self, value, ctx):
        from datapipe.cli.transform import transform_command  # forbidden
```

**Mutable class-level state:**
```python
# WRONG — shared across all instances in all workers
class BadStage(Stage):
    _cache = {}   # class variable mutated at runtime

    def process(self, value, ctx):
        BadStage._cache[value["id"]] = result  # races between workers
```
Use instance variables set in `setup()` instead.

**Swallowing setup errors:**
```python
# WRONG — masks initialization failures as per-record skips
class BadStage(Stage):
    def setup(self, ctx):
        try:
            self.model = load_model()
        except Exception:
            self.model = None   # silently broken worker continues

    def process(self, value, ctx):
        if self.model is None:
            return DROP         # should have aborted the run
```

---

## DSL expression syntax and `datapipe transform` flags

### Expression grammar

```
expression  = invocation ( '|' invocation )*
invocation  = tool_name '(' selector [ ',' arg ( ',' arg )* ] ')'
selector    = '.'                    # whole record
            | '.' field              # top-level field
            | '.' field '.' field    # nested field
arg         = name '=' json_literal
```

Example expressions:
```
fromjson(.tools)
fromjson(.tools) | tojson(.tools[].name)
uppercase(.title, locale="en")
```

The compiler resolves tool names against the registry, validates
selector/target scope, binds arguments, and fills defaults. The resulting
`CompiledExpression` carries resolved callables and is registry-independent
at execution time.

Workers automatically receive `JsonLoadStage → CompiledToolProgramStage →
JsonDumpStage` — do not include `fromjson(.)` / `tojson(.)` for the outer
row in your expression.

### `datapipe transform` key flags

```bash
datapipe transform 'EXPR' input.jsonl output.jsonl \
  --executor process|thread|sequential   # default: process
  --workers N                            # default: CPU count
  --max-in-flight N                      # default: workers * 4
  --errors raise|skip|return             # default: raise
  --error-output errors.jsonl            # sink for error payloads
  --ordered / --unordered                # default: --ordered
  --progress / --no-progress             # default: --progress
  --validate-tools always|sample|off     # default: always
  --dry-run                              # compile + inspect, no data
  --json                                 # with --dry-run: JSON output
```

### `datapipe run` key flags

```bash
datapipe run ./pipeline.py:pipeline \
  --source [FORMAT:]PATH               # required; FORMAT: jsonl|parquet
  --sink [FORMAT:]PATH                 # required
  --error-output errors.jsonl
  --executor process|thread|sequential
  --workers N
  --max-in-flight N
  --errors raise|skip|return
  --ordered / --unordered
  --progress / --no-progress
  --raw                                # open JSONL in raw mode
  --rank N --world-size N              # distributed override
```

Format is inferred from extension if not prefixed: `.jsonl`/`.ndjson`/`.jsonl.gz`/`.jsonl.zst` → jsonl; `.parquet` → parquet; directories → jsonl.
