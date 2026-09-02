# datapipe Domain Knowledge

Generated from `feat/structural-transform-dsl` (b5d3fba), which is `main` plus
the structural DSL work. Verified against that branch's test suite: 1467 passed,
5 skipped. Run `/sync-knowledge` to regenerate after architectural changes.

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

### Error types

| Type | Raised by | Notes |
|------|-----------|-------|
| `StageExecutionError` | coordinator | Outer wrapper; carries `stage_name` and `record_seq`. |
| `ToolExecutionError` | tool call / type-check failure | Full diagnostic context. |
| `StructuralExecutionError` | `=`, `<-`, `<<` statements | Carries `statement_index`, `operation` (`copy`/`move`/`move-into`), `selector`, `source_path`, `destination_path`, `expression_span`, and `reason`. |

All three define `__reduce__` plus a module-level rebuild factory, so they survive
the `spawn` process boundary intact. Under `errors="return"`, the error payload
carries a `structural` dict for structural failures alongside the existing `tool`
dict for tool failures.

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

## Structural transform DSL

The expression language is at **version 2**. Version 1 was the single-expression
form (one or more `invocation`s joined by `|`); it still compiles, but the
multi-invocation form is deprecated (see *Deprecated syntax* below).

### Program grammar

```
program        = statement ( ';' statement )* ';'?
statement      = assignment | move_into | focused | invocation ( '|' bare_call )*
invocation     = qualified_name '(' selector ( ',' arg )* ')'
qualified_name = IDENT ( '.' IDENT )?
arg            = name '=' literal
```

Tool names may be namespace-qualified — `my_tools.normalize(.body)` — for tools
installed from a provider that declares a namespace.

A program is a sequence of statements separated by `;`, applied in order to one
evolving root record. Each statement sees the mutations of the statements before
it. A trailing `;` is legal; an empty statement (`;;`) is an error. Two statements
juxtaposed without a `;` produce an error naming the missing `;` as the likely
cause.

`;` also **resets focus** — each statement establishes its own target. Focus never
leaks across a `;`.

### Operators

| Operator | Name | LHS | RHS | Semantics |
|----------|------|-----|-----|-----------|
| `;` | sequence | — | — | Order statements against one evolving record; resets focus |
| `\|` | pipe | current focus | bare call | Chain a tool onto the current target (see *Focus*) |
| `=` | copy assign | exact selector (root OK w/ literal) | selector, literal, or invocation | Copy; source retained |
| `<-` | exact move | exact selector | selector or invocation (**not** a literal) | Move; source removed only after destination write succeeds |
| `<<` | move-into | destination object selector | comma-separated selectors / field-sets | Move each source into the destination object, keyed by its final field name |
| `^` | complement | — | — | Only inside `.( … )`; complements the whole parenthesized name set |

Notes:
- `.a <- 5` is rejected at compile time with a diagnostic pointing at `=`.
- `. = {"x": 1}` (root literal) is legal — a constant has no path that can overlap
  with root. `. = .meta` is rejected for overlap.
- Destinations of `=`, `<-`, `<<` must be exact: no `[]` wildcards.
- `<<` creates a missing destination object as `{}`.
- `,` binds tighter than `|` in a `<<` source list.
- There is no bare `<` token; only `<-` and `<<`.

### Selectors

Strict by default — a missing field or out-of-range index raises
`SelectorResolutionError`, never a silent skip.

| Selector | Meaning |
|----------|---------|
| `.` | Root — one match, the whole record |
| `.field` | Dict key lookup |
| `["key.with.dots"]` | Dict key lookup via quoted string |
| `[0]` | List index (negative indices are errors) |
| `[]` | Every element of a list; empty list → zero matches, still success |

Applying `[]` to a non-list raises `SelectorResolutionError`.

**Field sets** appear only as `<<` sources:

```
.metadata.(temperature|score)     # named fields of .metadata
.(^instance_id|messages|tools)    # every root field EXCEPT these three
```

Here `|` joins field *names*, not operations; the closing `)` ends that scope, so
a `|` after `)` is a statement pipe again. `^` complements the entire set.

### Focus and pipes

A `bare_call` is a tool call with no selector — it takes the **current focus** as
its target. Focus is established by:

| Statement form | Focus |
|----------------|-------|
| `.sel \| tool` | the leading selector |
| `.dest = rhs \| tool` | the destination (pipes apply to the value just written) |
| `.dest << srcs \| tool` | the destination (pipes apply to the assembled object) |
| `tool(.sel) \| tool2` | inferred from the invocation's selector |

Wildcard focus is **elementwise**: `.items[] | fromjson | tojson` applies the whole
chain to each element independently.

Mixing an explicit selector into a pipe chain after a bare call is a hard parse
error, not a warning:

```
fromjson(.a) | tojson | tojson(.b)
# error: ambiguous `|`: 'tojson' is given an explicit selector but follows a
# bare tool call, which takes the current focus; `|` cannot mean both.
# Use `;` to sequence record mutations:  fromjson(.a) | tojson; tojson(.b)
```

### Literals

JSON spelling. Accepted: strings (`"a"` / `'a'`), integers, floats (incl. `1e10`),
`true`/`false`, `null`, arrays, objects. `True`/`False`/`None` are also accepted as
aliases. Max literal nesting depth is **64**.

Legal as: the RHS of `=`, and argument values. **Not** legal as the RHS of `<-`.
Literals may carry trailing pipes — `.a = 5 | tojson` writes the string `"5"`.
Container literals are copied per record.

### Built-in tools

Four tools ship built-in. Anything else requires a provider installed via
`datapipe tools install`.

**Value-targeted** (any selector):

```python
fromjson(value, *, recursive=False, containers_only=True)
    # input: string|array|object   output: any
tojson(value, *, ensure_ascii=False, compact=True, sort_keys=False)
    # input: any                   output: string
```

**Record-targeted** (selector must be `.`; the tool receives and returns the whole row):

```python
nest(record, *, key="metadata", include=None, exclude=None,
     jsonify=False, collision="error", missing="error")
    # input: object  output: object
unnest(record, *, key="metadata", include=None, exclude=None, parse=False,
       jsonify=False, collision="error", missing="error")
    # input: object  output: object
```

`nest`/`unnest` are convenience sugar over the structural operators:

```
nest(., key=K, include=[f1,f2])   ≡  .K << .(f1|f2)
nest(., key=K, exclude=[a,b])     ≡  .K << .(^a|b)
nest(., key=K, jsonify=true)      ≡  .K << .(<every other field>) | tojson
unnest(., key=K, include=[x])     ≡  . << .K.(x)
unnest(., key=K, exclude=[x])     ≡  . << .K.(^x)
unnest(., key=K, parse=true, jsonify=true, include=[x])
                                  ≡  fromjson(.K); . << .K.(x); tojson(.K)
```

Error modes: an `include` naming a missing field is an error; an `exclude` naming a
missing field is silent. A destination key collision is an error. Supplying both
`include` and `exclude` raises immediately. `collision`/`missing` accept only
`"error"` today. `include=[]` and `exclude=[]` are both treated as "not supplied".
In `nest`, the destination key is automatically excluded from its own complement.

### Deprecated syntax

`|` joining two **explicit-selector** invocations is deprecated but still works:

```
fromjson(.a) | tojson(.b)      # deprecated
fromjson(.a); tojson(.b)       # use this
```

The compiler emits a `DeprecationWarning`; the CLI prints it once to stderr as
`warning: …`. The suggested rewrite preserves all non-default arguments. It only
triggers when there is more than one invocation and no selector is root or
wildcard. Removal is deferred — the stated window is one minor release after the
release that introduces `;`, and `;` has not shipped in a release yet.

### Execution model for programs

The CLI wraps the compiled program exactly as before:

```
JsonLoadStage → <program stage> → JsonDumpStage
```

with `raw=True` on the JSONL source and sink, so workers do the JSON parsing.
Only the middle stage type varies:

| Compiled form | Stage |
|---------------|-------|
| `CompiledExpression` (single plain invocation) | `CompiledToolProgramStage` |
| `CompiledProgram` (multi-statement, focused, assignment, or piped) | `CompiledProgramStage` |

Both are reached from the same command; the CLI picks the program path when the
expression has more than one statement, any focus selector, or any bare pipe.
Do not include `fromjson(.)` / `tojson(.)` for the outer row — that is what the
implicit load/dump stages already do.

Per record, `CompiledProgramStage` runs every statement in order inside **one**
worker call, rebinding the record after each statement. Statement N+1 sees all
writes made by statements 0…N. A failure on any statement aborts that record;
assignments write only after all preconditions pass, so no partial writes persist.

### `datapipe transform` flags

```bash
datapipe transform 'PROGRAM' input.jsonl output.jsonl \
  --executor process|thread|sequential   # default: process
  --workers N                            # default: CPU count
  --max-in-flight N                      # default: workers * 4
  --errors raise|skip|return             # default: raise
  --error-output errors.jsonl            # sink for error payloads
  --ordered / --unordered                # default: --ordered
  --progress / --no-progress             # default: --progress
  --validate-tools always|sample|off     # default: always
  --input-format jsonl                   # default: jsonl
  --output-format jsonl                  # default: jsonl
  --rank N --world-size N --local-rank N # distributed override
  --dry-run                              # compile + inspect, no data processed
  --json                                 # with --dry-run: emit JSON
```

`--dry-run` and `--json` are new alongside the program syntax.

### Inspecting before running

`datapipe inspect-expression 'PROGRAM'` and `datapipe transform --dry-run …`
produce the same report. For a program it prints `expression-language: 2`, a
`Statements: N` block (each with its focus, operation kind — `call` / `copy` /
`move` / `move-into` — resolved provider, target, input/output types,
cardinality, bound arguments, and any `pipe:` sub-entries), then the `Stages:`
list. The legacy single-expression form prints `Invocations: N` instead.
With `--json`, a program yields a `statements` array and a legacy expression
yields `invocations`.

Use `--dry-run` to confirm argument defaults and selector scope before spending
a full pass over the data.

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

### Shell quoting

**Always wrap the program in single quotes.** Every structural operator is a
shell metacharacter: `<<` starts a heredoc, `<-` redirects input, `;` ends the
command, `|` pipes it, `(` and `)` open a subshell, `^` is history substitution
in some shells, and `[]` globs. Unquoted, the shell consumes them before
datapipe ever starts.

```bash
# Right
datapipe transform '.m << .(^id)' in.jsonl out.jsonl
# Wrong — the shell eats the operators
datapipe transform .m << .(^id) in.jsonl out.jsonl
```

Single quotes pass the text through verbatim; double quotes still expand `$` and
backticks, so prefer single. To embed a literal single quote, close and reopen:

```bash
datapipe transform 'normalize(.body, pad='"'"'-'"'"')' in.jsonl out.jsonl
```

This applies to any generated `pipeline.sh` — an unquoted program there is
silently corrupted rather than rejected.

### Worked examples

```bash
# Decode one JSON-encoded column
datapipe transform 'fromjson(.tools)' in.jsonl out.jsonl

# Two independent mutations, sequenced
datapipe transform 'tojson(.tools); tojson(.metadata)' in.jsonl out.jsonl

# Fold everything except three fields into .metadata, then serialize it
datapipe transform '.metadata << .(^instance_id|messages|tools) | tojson' in.jsonl out.jsonl

# Same thing via the convenience tool
datapipe transform 'nest(., key="metadata", exclude=["instance_id","messages","tools"], jsonify=true)' in.jsonl out.jsonl

# Lift two fields back out of a JSON-encoded .metadata and re-serialize it
datapipe transform 'fromjson(.metadata); . << .metadata.(temperature|score); tojson(.metadata)' in.jsonl out.jsonl

# Elementwise over a list
datapipe transform '.items[] | fromjson | tojson(compact=false)' in.jsonl out.jsonl

# Copy, move, and set a constant
datapipe transform '.backup = .payload; .id <- .legacy_id; .status = "processed"' in.jsonl out.jsonl
```

### Validation modes

`--validate-tools` controls runtime type-contract checking:

| Mode | Behaviour |
|------|-----------|
| `always` | Check every record (default). |
| `sample` | Check the first 100 records per worker, then stop. |
| `off` | No contract checks. |

The decision is made once per record, before any statement runs, and is inherited
by the inner program that `nest`/`unnest` desugar into — so `sample` genuinely
samples through those tools rather than degrading to `always`.

### Limits

Argument-literal nesting in a DSL expression is bounded at **64 levels**; deeper
literals raise `ExpressionSyntaxError` rather than crashing the parser.
