# datapipe Pipeline-Builder Skill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create two Claude Code skills — `build-pipeline` and `sync-knowledge` — as a project-scoped plugin that lets users interactively scaffold rerunnable datapipe pipelines (shell or Python) and keep the domain-knowledge reference current as the codebase evolves.

**Architecture:** A `.claude-plugin/plugin.json` manifest enables user-scope installation; skills are auto-discovered from `.claude/skills/`. `build-pipeline` loads a hierarchical reference file (`references/datapipe-knowledge.md`) for deep domain context. `sync-knowledge` regenerates that reference by reading current source and architecture docs.

**Tech Stack:** Claude Code plugin system (SKILL.md + plugin.json), datapipe Python library (Pipeline, Stage, ProcessExecutor, JsonlSource/Sink), datapipe CLI (`datapipe transform`, `datapipe run`).

## Global Constraints

- Skill files must be named `SKILL.md` (not `index.md` or any other name)
- Skills live in `.claude/skills/<skill-name>/SKILL.md` for project-scope auto-discovery
- Plugin manifest lives in `.claude-plugin/plugin.json` at the repo root
- `datapipe-knowledge.md` lives in `.claude/skills/build-pipeline/references/datapipe-knowledge.md`
- The `sync-knowledge` skill's step 4 must write to that exact path
- All paths in SKILL.md instructions must be relative to the repo root
- SKILL.md body should stay under 500 lines; overflow goes into `references/`

---

### Task 1: Scaffold — directories and plugin manifest

**Files:**
- Create: `.claude-plugin/plugin.json`
- Create: `.claude/skills/build-pipeline/references/.gitkeep` (placeholder until Task 3 fills it)
- Create: `.claude/skills/sync-knowledge/` (directory only; SKILL.md added in Task 2)

**Interfaces:**
- Produces: directory tree and `plugin.json` that later tasks populate with skill files

- [ ] **Step 1: Create the directory tree**

```bash
mkdir -p .claude-plugin
mkdir -p .claude/skills/build-pipeline/references
mkdir -p .claude/skills/sync-knowledge
```

- [ ] **Step 2: Write the plugin manifest**

```bash
cat > .claude-plugin/plugin.json << 'EOF'
{
  "name": "datapipe",
  "version": "0.1.0",
  "description": "Skills for building and maintaining datapipe data pipelines.",
  "author": {
    "name": "Lyuwen Fu"
  },
  "keywords": ["datapipe", "pipeline", "data-processing"]
}
EOF
```

- [ ] **Step 3: Verify the manifest is valid JSON**

```bash
python -c "import json; json.load(open('.claude-plugin/plugin.json')); print('ok')"
```

Expected: `ok`

- [ ] **Step 4: Commit the scaffold**

```bash
git add .claude-plugin/plugin.json .claude/skills/
git commit -m "feat: scaffold datapipe plugin structure"
```

---

### Task 2: Write the `sync-knowledge` skill

**Files:**
- Create: `.claude/skills/sync-knowledge/SKILL.md`

**Interfaces:**
- Produces: `sync-knowledge/SKILL.md` — a skill that reads source + docs and writes `.claude/skills/build-pipeline/references/datapipe-knowledge.md`
- Consumed by: Task 3 (invoked to generate the knowledge file), and ongoing maintenance

- [ ] **Step 1: Write `sync-knowledge/SKILL.md`**

The file must have YAML frontmatter with `name` and `description`, then a body describing the exact synthesis procedure. The description must be specific enough that Claude triggers this skill when the user says anything like "update the datapipe knowledge", "regenerate knowledge doc", or "sync the pipeline skill knowledge".

```markdown
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
```

- [ ] **Step 2: Verify the SKILL.md exists and has valid frontmatter**

```bash
head -10 .claude/skills/sync-knowledge/SKILL.md
```

Expected: YAML frontmatter starting with `---` then `name: sync-knowledge`.

- [ ] **Step 3: Commit**

```bash
git add .claude/skills/sync-knowledge/SKILL.md
git commit -m "feat: add sync-knowledge skill for datapipe domain reference"
```

---

### Task 3: Generate the initial `datapipe-knowledge.md`

**Files:**
- Create: `.claude/skills/build-pipeline/references/datapipe-knowledge.md`

**Interfaces:**
- Consumed by: `build-pipeline/SKILL.md` (loaded as a hierarchical reference)
- Produced by: running the `sync-knowledge` skill procedure manually for the first time

This task does not write code — it invokes the `sync-knowledge` procedure to produce the initial knowledge file. Because `sync-knowledge` was just written in Task 2, follow its steps now rather than running it as an installed skill.

- [ ] **Step 1: Read all architecture and implementation files listed in `sync-knowledge` Step 1–2**

Read in order:
1. `parallel_record_pipeline_architecture.md`
2. `configurable_transform_cli_plan.md`
3. `CLAUDE.md`
4. `datapipe/pipeline.py`, `datapipe/stage.py`, `datapipe/execution/base.py`
5. `datapipe/execution/sequential.py`, `datapipe/execution/thread.py`, `datapipe/execution/process.py`
6. `datapipe/io/base.py`, `datapipe/io/jsonl.py`, `datapipe/errors.py`
7. `datapipe/cli/run.py`, `datapipe/cli/transform.py`
8. `datapipe/tools/contract.py`, `datapipe/tools/decorator.py`

- [ ] **Step 2: Write `.claude/skills/build-pipeline/references/datapipe-knowledge.md`**

Write a complete document with all eight sections from the `sync-knowledge` skill procedure. Include actual API signatures read from source, not paraphrases.

- [ ] **Step 3: Verify the file has all eight sections**

```bash
grep "^## " .claude/skills/build-pipeline/references/datapipe-knowledge.md
```

Expected output — eight headings:
```
## Core mental model
## Stage composition API
## Executor options and trade-offs
## IO sources and sinks
## Error policies
## Key invariants
## Common anti-patterns
## DSL expression syntax and datapipe transform flags
```

- [ ] **Step 4: Commit**

```bash
git add .claude/skills/build-pipeline/references/datapipe-knowledge.md
git commit -m "feat: generate initial datapipe-knowledge.md reference"
```

---

### Task 4: Write the `build-pipeline` skill

**Files:**
- Create: `.claude/skills/build-pipeline/SKILL.md`

**Interfaces:**
- Consumes: `.claude/skills/build-pipeline/references/datapipe-knowledge.md` (loaded at skill invocation time)
- Produces: interactive skill that scaffolds shell or Python pipeline artifacts for the user

- [ ] **Step 1: Write `build-pipeline/SKILL.md`**

```markdown
---
name: build-pipeline
description: >
  Interactively scaffold a rerunnable datapipe data pipeline and persist it
  as a shell script (for simple DSL expressions) or a Python file (for
  multi-stage, stateful, or custom pipelines). Use this skill whenever the
  user wants to process, transform, enrich, or filter a dataset with datapipe
  — even if they don't say "pipeline" explicitly. Triggers on: "process my
  data", "write a pipeline", "transform this dataset", "build a datapipe
  script", "I want to run datapipe on", "how do I pipeline X", "scaffold a
  pipeline", or when the user shares sample input/output data and asks how to
  automate the transformation.
---

# build-pipeline: scaffold a datapipe pipeline

## Before anything else: load domain knowledge

Read `references/datapipe-knowledge.md` in full before starting the
conversation. Every design decision you make depends on the invariants,
API signatures, and anti-patterns in that file. Do not start gathering
requirements until you have read it.

## Interaction flow

### 1. Gather intent

Ask the user what they want to process. Accept any of:
- A description of the transformation ("I want to extract the `.name` field
  from each record and uppercase it")
- Sample input JSONL records
- A sample output they want to produce
- Both input and output

If the user provides only input or only output, ask about the missing side
**only if** knowing it would meaningfully change the pipeline design (e.g.,
if the output format is ambiguous or the transformation could be done
multiple ways). Do not ask for the sake of completeness — ask only when it
affects your design.

If the description is too vague to make a design decision (e.g., "I want to
process my dataset"), ask one targeted question: what transformation should
happen to each record?

### 2. Classify complexity

**Shell path** — use when ALL of the following are true:
- A single transformation (one or a piped sequence of DSL tool invocations)
- No custom Python logic needed
- No per-worker state (no resources to load at setup time)
- Standard built-in tools (`fromjson`, `tojson`, etc.) cover the need

**Python path** — use when ANY of the following is true:
- Multiple distinct processing stages
- Custom logic that can't be expressed as a DSL tool invocation
- Per-worker state (model loading, database connection, cache initialization)
- Non-standard tools not in the built-in registry
- The user explicitly asks for a Python pipeline

**Ambiguous** — ask one question to disambiguate. Good question: "Does this
need any custom Python logic, or can it be expressed as a sequence of
field transformations?"

### 3. Shell path

Guide the user to a complete `datapipe transform` invocation:

1. Help them write the DSL expression (one or more tool invocations separated
   by `|`).
2. Show the full command with all relevant flags:

```bash
datapipe transform 'EXPR' input.jsonl output.jsonl \
  --executor process \
  --workers 8 \
  --errors skip \
  --error-output errors.jsonl
```

3. Explain each flag briefly.
4. Write the command to a `pipeline.sh` file at the user's working directory
   (or a path they specify).
5. Offer to execute it: "Want me to run this now?"

When running, use `bash pipeline.sh` and report the completion stats.

### 4. Python path

Produce a complete, runnable `pipeline.py`. Structure:

```python
"""Brief description of what this pipeline does."""

from __future__ import annotations

from datapipe.pipeline import Pipeline
from datapipe.stage import Stage
from datapipe.execution.process import ProcessExecutor
from datapipe.context import WorkerContext


class MyStage(Stage):
    """One-line description."""

    name = "my_stage"

    def __init__(self) -> None:
        # Initialize construction-time config here.
        # Do NOT open files, connections, or load models here.
        pass

    def setup(self, ctx: WorkerContext) -> None:
        # Load per-worker resources here (model, DB connection, lookup table).
        # This runs once per worker process/thread.
        pass

    def process(self, value: dict, ctx: WorkerContext) -> dict:
        # Transform one record. Must return a dict (or DROP to discard it).
        raise NotImplementedError

    def teardown(self, ctx: WorkerContext) -> None:
        # Release per-worker resources (best-effort under ProcessExecutor).
        pass


pipeline = Pipeline([
    MyStage(),
    # Add more stages here.
])


if __name__ == "__main__":
    import sys
    from datapipe.io.jsonl import JsonlSource, JsonlSink

    source_path = sys.argv[1] if len(sys.argv) > 1 else "input.jsonl"
    sink_path = sys.argv[2] if len(sys.argv) > 2 else "output.jsonl"

    stats = pipeline.run(
        source=JsonlSource(source_path),
        sink=JsonlSink(sink_path),
        executor=ProcessExecutor(workers=8),
        errors="skip",
        ordered=True,
        progress=True,
    )
    print(stats)
```

Fill in the actual stage logic for the user's task. Replace `MyStage` with a
descriptive class name. Add as many stage classes as the pipeline needs —
each should do one thing.

After writing `pipeline.py`, offer to run it:
"Want me to test this with a sample run? If so, share a small input file
(or I can generate one from your description)."

When testing:
```bash
datapipe run pipeline.py:pipeline \
  --source input.jsonl \
  --sink output.jsonl \
  --errors skip \
  --workers 4
```

Report the stats line from stdout and any errors from stderr.

## Invariants to enforce (from datapipe-knowledge.md)

Never produce output that violates these:

- Do not create a `ProcessExecutor` or `ThreadExecutor` inside a `Stage.process()` or `Stage.setup()`. Workers have their own executor — nesting pools is forbidden.
- Do not return a list from `process()` expecting flat-map semantics. Only `one_to_one` cardinality is supported; returning a list makes a list the output value.
- Do not import from `datapipe.cli` inside a stage. The data plane is CLI-independent.
- Do not store mutable class-level state that workers would share (class variables that get mutated at runtime).
- `setup()` failures abort the run — never catch exceptions in `setup()` and convert them to per-record behavior.
```

- [ ] **Step 2: Verify frontmatter and section headings**

```bash
head -5 .claude/skills/build-pipeline/SKILL.md
grep "^## " .claude/skills/build-pipeline/SKILL.md
```

Expected: YAML frontmatter with `name: build-pipeline`, and headings:
```
## Before anything else: load domain knowledge
## Interaction flow
## Invariants to enforce (from datapipe-knowledge.md)
```

- [ ] **Step 3: Commit**

```bash
git add .claude/skills/build-pipeline/SKILL.md
git commit -m "feat: add build-pipeline skill for interactive pipeline scaffolding"
```

---

### Task 5: Smoke-test both skills

**Files:** None new — verification only.

**Goal:** Confirm the plugin is wired up correctly and both skills can be found.

- [ ] **Step 1: Check that all skill files exist**

```bash
find .claude/skills .claude-plugin -type f | sort
```

Expected output (order may vary):
```
.claude-plugin/plugin.json
.claude/skills/build-pipeline/SKILL.md
.claude/skills/build-pipeline/references/datapipe-knowledge.md
.claude/skills/sync-knowledge/SKILL.md
```

- [ ] **Step 2: Check that `datapipe-knowledge.md` has all eight sections**

```bash
grep "^## " .claude/skills/build-pipeline/references/datapipe-knowledge.md | wc -l
```

Expected: `8`

- [ ] **Step 3: Check that `build-pipeline/SKILL.md` references the knowledge file**

```bash
grep "datapipe-knowledge" .claude/skills/build-pipeline/SKILL.md
```

Expected: at least one line referencing `references/datapipe-knowledge.md`.

- [ ] **Step 4: Check that `sync-knowledge/SKILL.md` writes to the correct path**

```bash
grep "datapipe-knowledge.md" .claude/skills/sync-knowledge/SKILL.md
```

Expected: the path `.claude/skills/build-pipeline/references/datapipe-knowledge.md`.

- [ ] **Step 5: Final commit**

```bash
git add -A
git commit -m "feat: complete datapipe pipeline-builder plugin

- .claude-plugin/plugin.json — plugin manifest for user-scope install
- .claude/skills/build-pipeline/SKILL.md — interactive scaffold skill
- .claude/skills/build-pipeline/references/datapipe-knowledge.md — generated domain knowledge
- .claude/skills/sync-knowledge/SKILL.md — knowledge-sync skill

Co-Authored-By: Claude <noreply@anthropic.com>"
```
