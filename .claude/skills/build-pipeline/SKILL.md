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
conversation. Every design decision you make depends on the invariants, API
signatures, and anti-patterns in that file. Do not start gathering
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
the output format is ambiguous or the transformation could be done multiple
ways). Do not ask for completeness — ask only when it affects your design.

If the description is too vague to make a design decision (e.g., "I want to
process my dataset"), ask one targeted question: what transformation should
happen to each record?

### 2. Classify complexity

**Shell path** — use when ALL of the following are true:
- No custom Python logic needed
- No per-worker state (no resources to load at setup time)
- The need is covered by the built-in tools (`fromjson`, `tojson`, `nest`,
  `unnest`), by the structural operators (`=`, `<-`, `<<`, `^`), or by a
  provider the user has already installed via `datapipe tools install`

The shell path is more capable than a single transformation. A program is a
`;`-separated sequence of statements applied to one evolving record, so
reshaping work — copying, moving, folding fields into a sub-object, lifting
them back out, setting constants — belongs on the shell path, not the Python
path. Reach for Python only when you need real logic or per-worker state.

**Python path** — use when ANY of the following is true:
- Multiple distinct processing stages
- Custom logic that can't be expressed as a DSL tool invocation
- Per-worker state (model loading, database connection, cache initialization)
- Non-standard tools not in the built-in registry
- The user explicitly asks for a Python pipeline

**Ambiguous** — ask one question to disambiguate before proceeding. Good
question: "Does this need any custom Python logic, or can it be expressed as
a sequence of field transformations?"

### 3. Shell path

Guide the user to a complete `datapipe transform` invocation:

1. Help them write the DSL program. Sequence independent record mutations with
   `;`. Use `|` only to chain a *bare* tool onto the current focus
   (`.metadata | fromjson | tojson`). Do **not** join two explicit-selector
   invocations with `|` — `fromjson(.a) | tojson(.b)` is deprecated; write
   `fromjson(.a); tojson(.b)`.
2. Verify the program compiles before running it over data:

```bash
datapipe transform --dry-run 'PROGRAM' input.jsonl output.jsonl
```

   Check the reported statements, resolved tools, and bound argument defaults.
3. **Single-quote the program.** Every structural operator (`;`, `|`, `<<`,
   `<-`, `^`, `(`, `)`, `[]`) is a shell metacharacter. Unquoted, the shell
   consumes it before datapipe starts — the generated `pipeline.sh` is
   silently corrupted rather than rejected. Prefer single quotes over double;
   double still expand `$` and backticks.
4. Show the full command with all relevant flags:

```bash
datapipe transform 'PROGRAM' input.jsonl output.jsonl \
  --executor process \
  --workers 8 \
  --errors skip \
  --error-output errors.jsonl
```

5. Explain each flag briefly.
6. Write the command to a `pipeline.sh` file at the user's working directory
   (or a path they specify).
7. Offer to execute it: "Want me to run this now?"

When running, use `bash pipeline.sh` and report the completion stats.

### 4. Python path

Produce a complete, runnable `pipeline.py`. Use this structure as the
starting point — fill in actual stage logic for the user's task and replace
`MyStage` with a descriptive class name. Add as many stage classes as needed;
each should do one thing.

```python
"""<One-line description of what this pipeline does.>"""

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
        # This runs once per worker process/thread — failure aborts the run.
        pass

    def process(self, value: dict, ctx: WorkerContext) -> dict:
        # Transform one record. Return the transformed value, or DROP to discard.
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
    sink_path   = sys.argv[2] if len(sys.argv) > 2 else "output.jsonl"

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

After writing `pipeline.py`, offer to run it:
"Want me to test this? Share a small input file or describe a few sample
records and I'll generate one."

When the user agrees, run:

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

- Do not create a `ProcessExecutor` or `ThreadExecutor` inside `Stage.process()`
  or `Stage.setup()`. Workers run inside an executor already — nesting pools
  is forbidden.
- Do not return a list from `process()` expecting flat-map semantics. Only
  `one_to_one` cardinality is supported; returning a list makes a list the
  output value.
- Do not import from `datapipe.cli` inside a stage. The data plane is
  independent of the control plane.
- Do not store mutable class-level state that workers would share (class
  variables mutated at runtime).
- Never catch exceptions in `setup()` and convert them to per-record behavior.
  Setup failures must propagate so the run aborts cleanly.
