---
name: sync-knowledge
description: >
  Regenerate the datapipe domain-knowledge reference file used by the
  build-pipeline skill. Use this skill whenever the datapipe codebase evolves —
  new stage types, executor changes, new IO formats, CLI flag changes, or error
  policy updates. Triggers on: "update datapipe knowledge", "sync knowledge",
  "regenerate knowledge doc", "pipeline skill knowledge is stale", or after any
  architectural change to datapipe.
---

# sync-knowledge: regenerate datapipe-knowledge.md

Read the current source and architecture documents, synthesize them into an
updated knowledge reference, and write it to
`.claude/skills/build-pipeline/references/datapipe-knowledge.md`.

## Step 1: Read architecture documents

Read all of these files before writing anything:

- `parallel_record_pipeline_architecture.md`
- `configurable_transform_cli_plan.md`
- `CLAUDE.md`

## Step 2: Read key implementation files

- `datapipe/pipeline.py` — `Pipeline`, `CompiledPipeline`, `RunConfig`, `Pipeline.run()` signature
- `datapipe/stage.py` — `Stage`, `GenericStage`, `JsonLoadStage`, `JsonDumpStage`, `coerce_stage`
- `datapipe/execution/base.py` — `Executor` ABC, scheduler, `max_in_flight` contract
- `datapipe/execution/sequential.py`, `datapipe/execution/thread.py`, `datapipe/execution/process.py` — executor constructors and defaults
- `datapipe/io/base.py` — `Source`, `Sink` ABC
- `datapipe/io/jsonl.py` — `JsonlSource`, `JsonlSink`, `raw` parameter
- `datapipe/errors.py` — `StageExecutionError`, error policies
- `datapipe/cli/run.py` — `run_command`, all CLI flags
- `datapipe/cli/transform.py` — `transform_command`, DSL expression flags
- `datapipe/tools/contract.py` — `ToolContract`, `Cardinality`
- `datapipe/tools/decorator.py` — `@tool` decorator usage

## Step 3: Write the synthesized knowledge file

Write `.claude/skills/build-pipeline/references/datapipe-knowledge.md` with
these sections in order:

1. **Core mental model** — the "define once, dispatch per record" philosophy;
   why stages are fused, not scheduled independently; the four orthogonal axes
   (Pipeline / Executor / Sharding / RuntimeContext).

2. **Stage composition API** — `Stage` base class (`setup`, `process`,
   `teardown` signatures with `WorkerContext`); `GenericStage` for
   function-based stages; `JsonLoadStage` / `JsonDumpStage`; `coerce_stage`;
   deep-copy / lock-replacement semantics for `ThreadExecutor`.

3. **Executor options and trade-offs** — `SequentialExecutor` (no
   parallelism, good for debugging); `ThreadExecutor(workers, max_in_flight)`
   (shared memory, GIL-limited CPU); `ProcessExecutor(workers, max_in_flight)`
   (default, true parallelism, spawn start method, pickling requirement).

4. **IO sources and sinks** — `JsonlSource(path, raw=False)` and
   `JsonlSink(path, raw=False)`; `raw=True` mode and when to use it
   (with `JsonLoadStage` / `JsonDumpStage`); `ParquetSource` / `ParquetSink`
   (requires `pyarrow` extra).

5. **Error policies** — `errors="raise"` (default, first error aborts),
   `errors="skip"` (failed records counted and dropped),
   `errors="return"` (errors delivered to `error_sink`); `setup()` failure
   always aborts regardless of policy.

6. **Key invariants** — exact list, verbatim:
   - One dispatch and one gather per record.
   - `max_in_flight` is a hard cap on submitted futures, not a guideline.
   - `stage.setup()` failure aborts the run; never a per-record skip.
   - Only `one_to_one` cardinality is executable.
   - Everything crossing the process boundary must be pickleable.
   - The data plane never imports from the CLI or mutable registry state.

7. **Common anti-patterns** — creating a process pool inside a stage;
   returning a list from `process()` and expecting flat-map semantics;
   importing from `datapipe.cli` inside a stage; treating `max_in_flight` as
   a hint; using mutable class-level state across workers.

8. **DSL expression syntax and `datapipe transform` flags** — expression
   grammar (tool invocations, selectors, pipes); key flags: `--executor`,
   `--workers`, `--max-in-flight`, `--errors`, `--error-output`,
   `--ordered`/`--unordered`, `--validate-tools`, `--dry-run`; the implicit
   `JsonLoadStage` / `JsonDumpStage` wrapping.

## Step 4: Diff and report

After writing the file, read the previous version (if it existed) and
report a concise summary of what changed: new sections, removed content,
updated API signatures, corrected invariants.
