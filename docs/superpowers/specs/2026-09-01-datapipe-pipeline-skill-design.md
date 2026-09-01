# Design: datapipe pipeline-builder skill

**Date**: 2026-09-01  
**Status**: approved

---

## Overview

Two skills delivered as a project-scoped Claude Code plugin (`.claude-plugin/` at repo root):

1. **`build-pipeline`** — interactively scaffold a datapipe pipeline and write it to disk as either a shell script or a Python file, with optional run/test on demand.
2. **`sync-knowledge`** — regenerate the `datapipe-knowledge.md` reference file by reading current source and architecture docs. Run this whenever the codebase evolves.

---

## Plugin layout

```
.claude-plugin/
├── plugin.json
└── skills/
    ├── build-pipeline/
    │   ├── SKILL.md                      ← procedure + YAML frontmatter
    │   └── references/
    │       └── datapipe-knowledge.md     ← generated domain knowledge (read on demand)
    └── sync-knowledge/
        └── SKILL.md                      ← knowledge-sync procedure + YAML frontmatter
```

The `datapipe-knowledge.md` file lives in `references/` and is loaded by `build-pipeline/SKILL.md` at runtime — it does not need to be in context for every session, only when building a pipeline.

---

## Skill 1: `build-pipeline`

### Purpose

Guide the user through designing a rerunnable datapipe pipeline and produce a persisted, runnable artifact.

### Interaction flow

1. **Load domain knowledge** — read `references/datapipe-knowledge.md` before the conversation starts.
2. **Gather intent** — ask the user what they want to process. Accept sample input data, output examples, or both. If only one side is given, ask about the other only if it would change the design.
3. **Classify complexity** to choose output form:
   - Single transformation, no custom logic, standard tool → **shell path**
   - Multi-stage, stateful logic, custom stage classes, non-standard tools → **Python path**
   - Ambiguous → ask one targeted question before continuing
4. **Shell path**: guide the user to write a DSL expression; show the complete `datapipe transform` invocation with all relevant flags; offer to execute it.
5. **Python path**: produce a complete `pipeline.py` — stage classes, executor config, error policy, entry point — following the architectural invariants below. Offer to run or test on demand.
6. **Write the output file** and report the path.

### Output contracts

**Shell output** (`pipeline.sh` or inline invocation):
```bash
datapipe transform 'EXPR' input.jsonl output.jsonl \
  --executor process \
  --workers N \
  --errors skip|raise|return
```

**Python output** (`pipeline.py`):
```python
from datapipe.pipeline import Pipeline
from datapipe.stage import Stage
# ... stage definitions ...

pipeline = Pipeline([...])

if __name__ == "__main__":
    # runnable via: datapipe run pipeline.py:pipeline --source ... --sink ...
    pass
```

### Architectural invariants the skill enforces

These come from `datapipe-knowledge.md` and must not be violated:

- One dispatch and one gather per record — no per-stage process pools or futures.
- `max_in_flight` is a hard cap on submitted futures, not a soft guideline.
- `stage.setup()` failure aborts the entire run; it is never converted to a per-record skip.
- Only `one_to_one` cardinality is executable; a returned list is not implicit flat-map output.
- Everything that crosses the process boundary must be pickleable.
- The data plane never imports from the CLI or mutable registry state.

---

## Skill 2: `sync-knowledge`

### Purpose

Regenerate `datapipe-knowledge.md` from current source so `build-pipeline` always reflects the actual implementation.

### Procedure

1. Read the architectural source-of-truth documents:
   - `parallel_record_pipeline_architecture.md`
   - `configurable_transform_cli_plan.md`
   - `CLAUDE.md`
2. Read key implementation files:
   - `datapipe/pipeline.py`, `datapipe/stage.py`
   - `datapipe/execution/base.py`
   - `datapipe/io/base.py`
   - `datapipe/errors.py`
   - `datapipe/cli/run.py`, `datapipe/cli/transform.py`
   - `datapipe/tools/contract.py`, `datapipe/tools/decorator.py`
3. Synthesize into an updated `datapipe-knowledge.md` with these sections:
   - Core mental model and design philosophy
   - Stage composition API (`Stage`, `CompiledPipeline`, lifecycle)
   - Executor options and trade-offs (`SequentialExecutor`, `ThreadExecutor`, `ProcessExecutor`)
   - IO sources and sinks (JSONL, Parquet, raw mode)
   - Error policies (`raise`, `skip`, `return`) and setup-failure semantics
   - Key invariants (verbatim list, kept short)
   - Common anti-patterns to avoid
   - DSL expression syntax and `datapipe transform` flags
4. Write the file to `.claude-plugin/skills/build-pipeline/references/datapipe-knowledge.md`.
5. Report a brief summary of what changed since the last version.

### When to run

Run `sync-knowledge` after:
- Adding or removing stage types
- Changing executor semantics or constructor signatures
- Adding new IO formats or changing raw-mode behavior
- Updating error policy logic
- Any CLI flag changes to `datapipe run` or `datapipe transform`

---

## Testing the skills

**`build-pipeline` test cases**:
1. User provides a sample JSONL input and asks to extract a nested field → should produce a shell `datapipe transform` invocation.
2. User describes a multi-step enrichment pipeline (load → call tool A → call tool B → filter errors) → should produce a `pipeline.py` with three Stage subclasses and a `ProcessExecutor`.
3. User provides only "I want to process my dataset" with no detail → should ask one clarifying question before proceeding.

**`sync-knowledge` test cases**:
1. Run against the current codebase → should produce a complete `datapipe-knowledge.md` with all seven sections present.
2. Run after a hypothetical change to `execution/base.py` → summary should note the change.

---

## What is explicitly out of scope

- The skill does not add a new CLI command to datapipe itself.
- The skill does not validate that the user's input data matches the pipeline's expected schema; it trusts the user's description.
- Parquet output for the shell path is deferred (Parquet + DSL selectors need schema semantics not yet implemented).
