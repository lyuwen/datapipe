# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install for development
python -m pip install -e '.[test]'

# Optional extras
python -m pip install -e '.[test,parquet]'
python -m pip install -e '.[test,zstd]'

# Run the full test suite
python -m pytest tests/

# Run a single test file
python -m pytest tests/test_executors.py

# Run a single test by name
python -m pytest tests/test_executors.py::TestBoundedSubmission::test_bounded_process -x

# Benchmark (smoke test with --quick, full run without)
python scripts/bench_phase5.py --quick
```

The project uses `setuptools` with `pyproject.toml`. No build step is needed — `pip install -e .` makes the package importable directly from the working tree. There is no linter configured.

## Architecture

The core mental model: define one per-record program, compile it once, dispatch each record to a worker that runs the entire program in isolation. A record crosses the executor boundary exactly once in each direction. Stages are composition units, not independently scheduled operators.

The system is layered. Dependencies point downward:

```
CLI / DSL expressions
    ↓
Authoring control plane (compiler, contracts, registry, installer, validation)
    ↓
Compilation boundary (Python stages or DSL expression → immutable Stage objects)
    ↓
Record-processing data plane (Source → Pipeline → Executor → Sink)
    ↓
Runtime environment (local / thread / process, rank / world-size, sharding)
```

The data plane must not depend on CLI parsing or mutable registry state. The foundational runtime is usable as a library without any tool registry.

### The pipeline data plane (`datapipe/pipeline.py`, `datapipe/execution/`, `datapipe/stage.py`)

`Pipeline` is a list of `Stage` objects. Calling `pipeline.compile()` produces a `CompiledPipeline` — a fused callable that runs all stages in sequence for one record. The executor receives `CompiledPipeline` as the worker function; it never touches individual stages directly.

`ProcessExecutor` defaults to the `spawn` start method. Everything that crosses the process boundary — compiled pipelines, stage state, tool descriptors, errors — must be pickleable. `Stage.__deepcopy__` handles `threading.Lock` instances so thread workers get isolated copies.

The scheduler in `datapipe/execution/base.py` enforces the `max_in_flight` bound. It never eagerly submits more than that many futures. Ordered output and bounded submission are separate concerns: the reorder buffer can grow if a straggler blocks, but the submission count is always capped.

Error policies (`raise`, `skip`, `return`) are handled at the pipeline level, not inside executors. Setup failures (`stage.setup()` raises) are never treated as per-record errors — they abort the worker.

### The transform CLI layer (`datapipe/cli/`, `datapipe/dsl/`, `datapipe/stages/tool_program.py`)

`datapipe transform 'expr' in.jsonl out.jsonl` compiles the expression, wraps the result in `JsonLoadStage → CompiledToolProgramStage → JsonDumpStage`, and calls `Pipeline.run()` with `raw=True` on the JSONL source and sink so workers handle JSON parsing.

The DSL compilation path: `datapipe/dsl/lexer.py` tokenizes → `parser.py` builds an AST (every node carries a source `Span`) → `compiler.py` resolves tool names against the registry, validates selector/target scope, binds arguments, fills defaults, and returns a `CompiledExpression` containing `ToolInvocation` objects. The compiler is the only place that touches the registry; the resulting invocations carry resolved callables and are registry-independent at execution time.

`CompiledToolProgramStage.process()` is the per-record hot path. It resolves selectors, optionally validates input/output against the tool's `TypeSpec`, calls the tool function, and replaces matched values in-place. The `validate` parameter controls the mode: `"always"` checks every record, `"sample"` checks the first 100 per worker, `"off"` skips checks.

`ToolExecutionError` in `datapipe/tools/errors.py` carries the full §11 diagnostic context and is pickleable via `__reduce__` + a module-level factory — the same pattern as `StageExecutionError` in `datapipe/errors.py`.

### The provider system (`datapipe/tools/`)

`datapipe/tools/types.py` — `JsonType` enum, `TypeSpec` hierarchy, `OneOf`, `matches()`, `infer_json_type()`, `describe()`.

`datapipe/tools/contract.py` — `ToolContract`, `ParameterSpec`, `Cardinality`. All frozen dataclasses; pickleable.

`datapipe/tools/decorator.py` — `@tool` decorator. Validates the function signature at import time and attaches a `ToolContract` as `fn.__tool_contract__`.

`datapipe/tools/registry.py` — JSON registry at `~/.local/share/datapipe/registry.json` (overridable via `DATAPIPE_USER_DATA`). Atomic writes via tmp-file + fsync + rename, guarded by `fcntl.flock`.

`datapipe/tools/validation.py` — two-stage validation: static (AST parse, size, UTF-8, duplicate tool names) then dynamic (subprocess with timeout, collects `__tool_contract__` metadata as JSON).

`datapipe/tools/installer.py` — copied (snapshot into the registry directory) and editable (pointer to the original file) installation modes. Nothing is registered until both validation passes succeed.

`datapipe/tools/loader.py` — worker-side import with a per-process `_loaded_providers` cache. Copied providers have their SHA-256 digest verified before import; editable providers skip digest enforcement by design. The loader compiles and executes the source bytes directly (not via `spec.loader.exec_module`) to avoid the `(mtime, size)` bytecode-cache keying problem that would make same-size edits invisible within the same second.

### IO (`datapipe/io/`)

`JsonlSource(path, raw=False)`: `raw=True` yields unparsed strings so workers handle JSON parsing — required for pipelines with `JsonLoadStage`. `raw=False` parses in the coordinator.

`ParquetSource` requires `pyarrow` (optional extra). For Hive-partitioned directories it creates one dataset from the directory (preserving partition metadata) rather than per-file datasets. Filtering happens before column projection.

### Key invariants to preserve

- One dispatch and one gather per record. Never a process pool per stage, a future per stage, or eager full-source materialization.
- `max_in_flight` is a hard bound on submitted futures, not a guideline.
- The same pipeline semantics hold under `SequentialExecutor`, `ThreadExecutor`, and `ProcessExecutor`.
- Worker setup failure (`setup()` raises) aborts the run — it is never converted into a per-record skip.
- Only `one_to_one` cardinality is executable. Do not interpret a returned list as implicit flat-map output.
- The foundational runtime works without any tool registry configured.

## Architectural sources of truth

Before making architectural changes, read:
1. `parallel_record_pipeline_architecture.md` — foundational runtime, API, IO, executors, ordering, sharding, errors.
2. `configurable_transform_cli_plan.md` — the DSL, tool contracts, provider registry, and CLI layer.

The plans may describe work not yet implemented. Check the code and tests first. If a change intentionally alters an architectural invariant, update the relevant design document in the same commit.

## Testing guidance

Run targeted tests first, then the full suite. Key test files by area:

- **Executor behavior**: `test_executors.py`, `test_thread_lifecycle.py`
- **Pipeline/stages**: `test_pipeline.py`, `test_stages.py`
- **IO**: `test_io_jsonl.py`, `test_io_parquet.py`
- **Sharding/runtime**: `test_sharding.py`, `test_runtime.py`, `test_distributed.py`
- **DSL**: `test_dsl_phase2.py`
- **Tool contracts/built-ins**: `test_tools_phase1.py`
- **Validation/validation modes**: `test_validation_phase5.py`
- **Provider install/load/CLI**: `test_tools_phase4.py`
- **Transform CLI**: `test_transform_phase3.py`
- **Spawn correctness**: `test_spawn_phase5.py`
- **CLI commands**: `test_cli.py`

Provider-system tests redirect `DATAPIPE_USER_DATA` to a `tmp_path` and clear `datapipe.tools.loader._loaded_providers` via `autouse` fixtures. Do the same in any new provider tests to avoid hitting the real user registry.
