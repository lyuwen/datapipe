# AGENTS.md

## Mission

`datapipe` is a small, inspectable, high-throughput Python package for
embarrassingly parallel record processing.

The core mental model is:

> Define one per-record program, then execute that complete program
> concurrently over a stream.

The project is not a general distributed dataflow engine. Preserve its small,
composable execution model as features are added.

## Architectural sources of truth

Read the relevant documents before making architectural changes:

1. [`parallel_record_pipeline_architecture.md`](parallel_record_pipeline_architecture.md)
   defines the foundational runtime, Python API, IO, executor, ordering,
   sharding, progress, and error model.
2. [`configurable_transform_cli_plan.md`](configurable_transform_cli_plan.md)
   defines the higher-level jq-like transform CLI, typed tool contracts,
   provider registry, and installation system built on that runtime.
3. `README.md` and `docs/` describe the currently supported user-facing
   behavior.
4. The current code and tests determine what is implemented today.

The second plan extends the first; it does not replace it. When the documents
appear to conflict, preserve the foundational execution invariants and treat
the configurable CLI as an authoring and distribution layer above them.

Plans may describe work that has not been implemented. Do not present planned
features as existing behavior. Inspect the code and tests first.

If a requested change intentionally alters an architectural invariant, call it
out explicitly and update the relevant design document in the same change.
Do not allow implementation and architectural documentation to drift silently.

## Layered system model

Keep the dependency and responsibility direction clear:

```text
User interface
  Python API | transform expression | inspection | tool management
                              |
Authoring and distribution control plane
  DSL parser | contracts | registry | installer | provider validation
                              |
Compilation boundary
  Python stages or expression -> immutable descriptors -> Stage objects
                              |
Record-processing data plane
  Source -> Pipeline -> Executor -> ordering/progress -> Sink
                              |
Runtime environment
  local/thread/process | rank/world-size | physical/logical sharding
```

The control plane prepares and validates programs. It must not schedule data or
participate in per-record IO. The data plane executes already-compiled
programs. It must not depend on CLI parsing or mutable registry lookup.

Dependencies should point downward:

```text
CLI -> DSL compiler -> tool stage/descriptors -> pipeline/runtime
installer -> contracts/provider validation -> registry
worker loader -> immutable provider descriptors/contracts
```

The foundational runtime must remain usable as a library without a configured
tool registry.

## Non-negotiable runtime invariants

### One dispatch and one gather per record

A normal record crosses the executor boundary once in each direction. The
complete compiled pipeline runs inside one worker invocation.

Never introduce:

- a process pool per stage;
- a future per stage;
- coordinator round-trips between stages;
- Unix processes connected to represent DSL `|` operations;
- eager materialization between transformations.

Stages are semantic composition units, not independently scheduled operators.

### Bounded streaming

`max_in_flight` is a hard bound on submitted work. Do not eagerly submit or
materialize the complete source. The scheduler should gather completed work and
submit replacements incrementally.

Tests for executor or source changes must demonstrate that:

- progress/results can occur before the source is exhausted;
- submitted futures do not exceed the configured bound;
- memory use does not scale with the entire input solely because of dispatch.

Ordered result buffering is separate from the in-flight submission bound. A
straggler may grow the reorder buffer; document and measure that behavior
rather than confusing it with eager submission.

### Executor independence

The same pipeline semantics must hold under:

- `SequentialExecutor`;
- `ThreadExecutor`;
- `ProcessExecutor`.

Executors own local concurrency only. They do not own stage semantics,
sharding policy, source/sink behavior, or error policy.

Use sequential execution as the reference implementation for deterministic
tests. Any user-visible transform should have equivalence coverage across
executors where practical.

### Distribution is orthogonal to local parallelism

`RuntimeContext` describes rank and world size. `Sharding` decides global
record ownership. An executor decides concurrency within one rank.

Do not add a global rank-zero dispatcher or gatherer for ordinary workloads.
There should be no required inter-rank communication. Each rank reads its
assigned records and writes its own output shard.

### Worker lifecycle and serialization

Compiled pipelines, stages, and cross-process descriptors must work with the
standard multiprocessing `spawn` model.

- Prefer top-level, importable callables.
- Do not rely on lambdas or nested functions crossing process boundaries.
- Transport small record payloads and immutable descriptors.
- Initialize heavy or non-pickleable state in `setup()` once per worker.
- Treat process-worker teardown as best-effort; correctness must not depend on
  it.
- Do not pickle arbitrary registry state or dynamically imported module
  objects.

Thread workers require isolated worker context and lifecycle semantics. Never
share mutable record-specific context across concurrent threads.

### Ordering

Every input record receives a monotonic local sequence number.

- `ordered=True` emits the contiguous input-order sequence.
- `ordered=False` emits in completion order.
- A skipped or returned error must advance ordered sequence handling at its
  position according to policy.
- Abort and interruption must not flush records across an unresolved ordering
  gap.
- Distributed ordering is local to each rank, not global.

When changing ordering code, test success, drop, every error policy,
stragglers, cancellation, and keyboard interruption.

### Progress

Submission, completion, and sink emission are different events. Do not use
"submitted" as the primary meaning of processed progress.

For the product-layer CLI, ordered output must not make progress appear frozen.
Progress should be able to distinguish:

- submitted/in-flight;
- processed/completed;
- written;
- buffered;
- failed;
- dropped.

Keep progress reporting behind the `ProgressReporter` abstraction. The runtime
must not become tightly coupled to tqdm or a particular terminal UI.

### Errors and resource finalization

Worker failures must retain the record sequence and failing stage. Tool-system
failures should additionally retain provider, tool, invocation, selector, and
concrete path when available.

Error policies belong to the pipeline run:

- `raise`: abort on the first processing failure;
- `skip`: count and omit the failed record;
- `return`: route structured errors to the error sink or result stream.

Source opening, sink opening, compilation, provider validation, and material
sink-close failures are run-level errors. Do not silently turn them into
per-record skips. A sink close can contain the final buffered write and must not
be logged and discarded as if the run succeeded.

Open and close primary and error sinks through consistent lifecycle handling.
Preserve the original run error if both processing and cleanup fail, while
making the cleanup failure observable.

## Component boundaries

### Pipeline and stages

`Pipeline` describes an inert per-record program. Construction must not start
workers, open data, or mutate external state.

`CompiledPipeline` runs stages sequentially in one worker:

```text
setup in order
process in order, stopping at DROP
teardown in reverse order
```

Plain callables may be coerced into transform stages, but public behavior
should remain explicit and inspectable. Stage names must be stable enough for
diagnostics.

### Sources and sinks

Sources own reading and optional physical sharding. Sinks own persistence and
format-specific buffering. They do not own worker scheduling.

For JSONL work that should parse and serialize in workers, prefer:

```python
JsonlSource(path, raw=True)
JsonlSink(path, raw=True)
```

with JSON load/dump stages inside the compiled worker program. Do not scan a
large JSONL file merely to determine a progress total.

Keep compression and directory/shard path behavior in IO adapters rather than
duplicating it in the CLI.

### Sharding and runtime detection

Environment detection should remain deterministic and overrideable. Keep
torchrun, Slurm, Kubernetes, and local fallback logic isolated from pipeline
semantics.

Hash sharding must use a stable algorithm; never use Python's randomized
process-local `hash()` for persistent ownership decisions.

## Configurable transform CLI extension

The compact expression form is the primary intended workflow:

```bash
datapipe \
  'fromjson(.tools) | tojson(.tools[].function.parameters)' \
  input.jsonl output.jsonl
```

The explicit form may be offered as an equivalent alias:

```bash
datapipe transform EXPRESSION INPUT OUTPUT
```

`datapipe run module_or_file:pipeline` remains the Python-authored pipeline
path. Both authoring paths must compile to and execute through the same
`Pipeline.run` implementation.

The CLI is a compiler frontend, not a second runtime. DSL `|` means sequential
composition inside one record program; it does not mean shell piping or an
executor boundary.

### DSL scope

Implement a deliberately small jq-like language. Do not claim full jq
compatibility and never use `eval`.

The initial language consists of:

- named tool invocations;
- one selector argument;
- typed literal configuration arguments;
- sequential `|` composition;
- root, object field, quoted key, array index, and array wildcard selectors.

Keep syntax parsing separate from semantic compilation. AST nodes should retain
source spans so errors can point to the exact expression location.

Selectors choose values; tools transform values; the selector engine assigns
results back. Do not make every tool reimplement nested traversal.

### Tool contracts

Built-in and installed tools use the same public contract. A tool declaration
must describe:

- stable name and API version;
- value-level or record-level target scope;
- accepted JSON input type;
- produced JSON output type;
- typed keyword-only configuration and defaults;
- record cardinality;
- behavioral metadata such as determinism and description.

Only one-to-one execution should be enabled initially. Do not interpret a list
result as implicit flat-map output. Filtering and one-to-many behavior require
explicit runtime, ordering, and statistics semantics.

Configuration should be bound and validated before opening the input. Runtime
input/output validation remains necessary because JSONL usually has no static
schema.

### Built-in JSON tools

`fromjson` and `tojson` are ordinary tools registered by the built-in provider,
not compiler special cases.

- `fromjson(path)` replaces a selected JSON string with its decoded value.
- `fromjson(path, recursive=true)` recursively expands partially serialized
  containers according to the documented scalar/container policy.
- `tojson(path)` serializes every selected match independently.
- The transform command implicitly parses and serializes the outer JSONL row.

Nested `tojson` operations occur before final outer-row serialization.

### Installed providers

`datapipe-install` and `datapipe tools install` are control-plane operations.
Installation should validate source, declarations, signatures, configuration
defaults, isolated import behavior, and spawn-worker loading before updating
the registry.

Copied installs preserve a validated source snapshot. Editable installs point
to a canonical source path, revalidate on content changes, and include the
expected source digest in compiled descriptors.

Workers resolve immutable provider/tool descriptors during setup. They must not
query a mutable registry for every record or receive arbitrary dynamically
loaded function objects through pickle.

Installing Python code grants it the permissions of a datapipe worker.
Validation catches mistakes but is not a security sandbox. Keep this trust
boundary visible in CLI confirmation and documentation.

## Implementation status discipline

The configurable transform and installation documents are plans. Before using
or modifying a planned API:

1. Search the repository for its implementation and tests.
2. Distinguish skeleton modules from complete behavior.
3. Do not add documentation examples implying availability before end-to-end
   tests pass.
4. Implement vertical slices that produce a usable behavior rather than many
   disconnected placeholders.
5. Preserve backward compatibility for the Python API unless a breaking change
   is explicitly requested.

When implementing the extension, prefer this dependency order:

1. runtime correctness prerequisites;
2. JSON type and tool contracts;
3. built-in tools;
4. selectors and DSL parser;
5. semantic compiler and worker-local tool stage;
6. transform CLI;
7. registry and local provider installation;
8. hardening, inspection, and documentation.

## Coding conventions

- Support Python 3.10 and newer unless project metadata changes explicitly.
- Use type hints for public interfaces and non-trivial internal structures.
- Prefer small immutable dataclasses for AST, contract, and descriptor values.
- Keep imports and optional dependencies lazy where they protect the core
  installation from Parquet or compression extras.
- Avoid broad dependencies for small problems; the DSL grammar is small enough
  for a focused tokenizer and parser.
- Use structured exception classes with machine-readable attributes.
- Preserve original exceptions with `raise ... from ...` where attribution is
  added.
- Keep hot per-record paths simple. Move parsing, provider discovery, argument
  binding, and setup outside the record loop.
- Avoid module-level mutable state except deliberately process-local worker
  state with a clear lifecycle.
- Public objects and error messages should have stable, inspectable
  representations.
- Add new public exports deliberately through `datapipe/__init__.py`; do not
  expose internal registry implementation accidentally.
- Do not weaken validation merely to make malformed data silently pass. Make
  strict versus permissive behavior explicit and configurable.

## Repository workflow

The worktree may contain user changes. Inspect `git status` before editing,
preserve unrelated modifications, and do not rewrite or discard changes you do
not own.

When making a change:

1. Identify the owning layer and read its architecture/docs/tests.
2. State the invariant or user-visible behavior being changed.
3. Add or update focused tests before relying on broad integration tests.
4. Implement the smallest coherent change within the owning component.
5. Run targeted tests, then the complete core suite when feasible.
6. Update README/docs and architecture plans when public or architectural
   behavior changes.
7. Inspect the final diff for unrelated edits and generated artifacts.

Do not solve a local issue by crossing ownership boundaries casually. For
example, do not put sharding in an executor, DSL parsing in `Pipeline.run`, or
registry lookup in a per-record transform.

## Verification

Install core development dependencies as needed:

```bash
python -m pip install -e '.[test]'
```

Run the full core suite:

```bash
python -m pytest tests/
```

Prefer the repository virtual environment when it is present and usable:

```bash
.venv/bin/python -m pytest tests/
```

Optional adapters require their extras:

```bash
python -m pip install -e '.[test,parquet]'
python -m pip install -e '.[test,zstd]'
```

Minimum verification by change type:

- **Stage/pipeline:** sequential behavior, setup/process/teardown, `DROP`, and
  stage-attributed failures.
- **Executor:** sequential/thread/process equivalence, bounded submission,
  early progress, cancellation, and spawn compatibility.
- **Ordering/errors:** all error policies, drops, stragglers, buffer gaps, and
  interruption.
- **IO:** open/write/flush/close, empty input, malformed records, compression,
  directories/shards, and final buffered write failures.
- **Sharding/runtime:** deterministic ownership, no duplication/loss, and
  environment precedence.
- **DSL/selectors:** parser spans, malformed expressions, missing/type policy,
  wildcards, root replacement, and sequential mutation semantics.
- **Tools:** contract inference, configuration binding, JSON types, built-ins,
  input/output validation, and structured errors.
- **Installer/registry:** copied/editable behavior, atomic updates, locking,
  timeouts, digest changes, name collisions, and spawned-worker resolution.
- **CLI:** subprocess-level tests of documented commands and exit codes.

Use `git diff --check` for documentation and source edits. Do not claim tests
passed unless they were actually run in the current environment.

## Review priorities

Review changes in this order:

1. Correctness and data-loss risk.
2. Preservation of bounded streaming and one-dispatch execution.
3. Ordering, cancellation, and cleanup behavior.
4. Cross-executor and multiprocessing correctness.
5. Public API and CLI compatibility.
6. Tool/provider trust boundaries and validation.
7. Diagnostics and observability.
8. Performance and maintainability.

Pay special attention to failures hidden during cleanup, unbounded reorder
buffers caused by sequence gaps, source-side work that bypasses error policy,
shared mutable thread state, non-pickleable dynamic objects, and registry
updates that are not atomic.

## Non-goals unless explicitly authorized

Do not expand the project into any of the following without a concrete request
and corresponding architectural update:

- a general DAG engine;
- shuffle, reduce, join, or group-by execution;
- inter-record dependencies;
- stage-specific worker pools;
- a central distributed scheduler;
- distributed RPC/control-plane orchestration;
- complete jq compatibility;
- arbitrary Python evaluation in the DSL;
- a YAML representation of arbitrary Python stages;
- implicit flat-map semantics;
- a security claim for unsandboxed provider Python;
- mandatory SQL, Arrow, or heavyweight parser dependencies in the core.

Keep the package understandable locally. New abstraction is justified when it
preserves clear ownership, removes repeated user work, and does not obscure the
record-level execution model.
