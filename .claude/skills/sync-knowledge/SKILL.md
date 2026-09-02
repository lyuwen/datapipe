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
- `datapipe/dsl/lexer.py`, `parser.py`, `ast.py` — token set, program/statement
  grammar, operators (`;`, `|`, `=`, `<-`, `<<`, `^`), selectors, field sets,
  literal forms and the literal depth limit
- `datapipe/dsl/compiler.py` — `compile_expression` vs `compile_program`,
  scope validation, deprecation diagnostics
- `datapipe/tools/builtins/` — the full built-in tool inventory, including
  `structural.py` (`nest` / `unnest`) and their desugarings
- `datapipe/stages/tool_program.py` — `CompiledToolProgramStage` and
  `CompiledProgramStage`; per-record statement sequencing; validation modes
- `datapipe/tools/errors.py` — `ToolExecutionError`, `StructuralExecutionError`,
  and their `__reduce__` / factory pickling

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

8. **Structural transform DSL** — the expression-language version; the
   program grammar (`;`-separated statements over one evolving record); every
   operator (`;`, `|`, `=`, `<-`, `<<`, `^`) with what is legal on each side;
   selector syntax including field sets and complements; focus and bare-call
   pipe semantics, including the ambiguous-pipe hard error; literal forms and
   where literals are legal; the complete built-in tool inventory with
   value-targeted vs record-targeted distinction and the `nest`/`unnest`
   desugarings; namespace-qualified tool names; shell-quoting guidance for
   the operators (read the `EXPRESSION_EPILOG` in `datapipe/cli/transform.py`);
   and any deprecated syntax with its replacement.

9. **Program execution, CLI flags, and inspection** — which compiled form maps
   to which stage; per-record statement sequencing; error types
   (`StageExecutionError`, `ToolExecutionError`, `StructuralExecutionError`);
   validation modes; parser limits; key flags: `--executor`, `--workers`,
   `--max-in-flight`, `--errors`, `--error-output`, `--ordered`/`--unordered`,
   `--validate-tools`, `--dry-run`, `--json`; `datapipe inspect-expression`;
   the implicit `JsonLoadStage` / `JsonDumpStage` wrapping; worked examples.

Verify every tool name, flag name, and operator against the code before writing
it. This file must not contain a tool or flag that does not exist. Prefer
example expressions taken from the test suite over invented ones.

## Step 4: Diff and report

After writing the file, read the previous version (if it existed) and
report a concise summary of what changed: new sections, removed content,
updated API signatures, corrected invariants.
