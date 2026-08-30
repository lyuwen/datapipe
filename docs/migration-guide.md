# Migration Guide: from `use_cases/process.py` to datapipe transform

This guide shows how to migrate the hand-written `process.py` script in
`use_cases/` to the `datapipe transform` CLI.

## The original script

`use_cases/process.py` does the following:

1. Read a JSONL file line by line.
2. Parse each line as JSON.
3. Recursively decode any nested JSON-encoded strings in the `.tools` field.
4. Serialize the result back to a JSON string.
5. Write it to a JSONL output file.
6. Use `ProcessPoolExecutor.map()` for parallelism.

Its limitations:

- Eager submission via `executor.map()` — the entire input is submitted before
  any output appears. On a 10 million-line file, millions of futures accumulate
  before you see the first result.
- No progress bar during processing.
- No structured error handling — one bad record crashes the whole run.
- Boilerplate: argument parsing, file handling, executor setup, and the
  transformation logic are all mixed together.
- Fixed chunksize: tuning requires code changes.

## Direct replacement: one expression

The motivating transformation is:

> For each record, decode `.tools` from a JSON string to a Python object.
> Leave the rest of the record unchanged.

```bash
datapipe transform 'fromjson(.tools)' input.jsonl output.jsonl
```

That's it. `fromjson` with no `recursive` flag decodes the selected value once.
Outer JSON parse and dump happen inside workers automatically.

If you also need to recursively decode nested strings inside `.metadata.annotation`:

```bash
datapipe transform \
  'fromjson(.tools) | fromjson(.metadata.annotation, recursive=true)' \
  input.jsonl output.jsonl
```

## Feature comparison

| Feature | `process.py` | `datapipe transform` |
|---|---|---|
| Parallel execution | `ProcessPoolExecutor.map()` (eager) | bounded `max_in_flight` (streaming) |
| Memory on large files | proportional to input size | bounded by `max_in_flight` |
| Progress bar | tqdm on results (blocked until all submitted) | immediate, per completion |
| Error handling | crashes on first error | `--errors raise\|skip\|return` |
| Structured error output | none | `--error-output errors.jsonl` |
| Ordered output | yes (map preserves order) | `--ordered` (default) |
| Worker count | `-j N` | `--workers N` |
| Custom chunksize | `--chunksize N` | not needed (per-record futures) |
| JSON decode depth | manual recursive function | `fromjson(.field, recursive=true)` |
| stdin/stdout | supported | not in Phase 1 |

## Running with the same parallelism

`process.py` defaults to `os.cpu_count()` workers. `datapipe transform`
defaults to the same:

```bash
# Equivalent to: python process.py input.jsonl -o output.jsonl
datapipe transform 'fromjson(.tools)' input.jsonl output.jsonl
```

To match a specific worker count:

```bash
datapipe transform --workers 8 'fromjson(.tools)' input.jsonl output.jsonl
```

## Handling errors without crashing

`process.py` crashes on any malformed record. To skip bad records and continue:

```bash
datapipe transform \
  --errors skip \
  'fromjson(.tools)' input.jsonl output.jsonl
```

To keep the bad records for inspection:

```bash
datapipe transform \
  --errors return \
  --error-output errors.jsonl \
  'fromjson(.tools)' input.jsonl output.jsonl
```

Each error record includes `seq`, `error_type`, `error_message`, `traceback`,
and (for contract violations) a `tool` object with the selector and expected
type.

## When to keep using a Python pipeline

The `datapipe transform` CLI is designed for reusable, configurable map
operations. Use a Python-defined `Pipeline` when:

- your transformation requires state, branching, or logic outside the expression
  language;
- you need `FilterStage`, `FlatMapStage`, or multiple independent stage pools;
- your stage needs `setup()` / `teardown()` for resource lifecycle (model
  loading, database connections);
- you want to compose datapipe stages programmatically.

```python
from datapipe import Pipeline, GenericStage, FilterStage, ProcessExecutor

def process(record):
    record["tools"] = recursive_deserialize(record["tools"])
    return record

def is_valid(record):
    return bool(record.get("tools"))

pipeline = Pipeline([
    GenericStage(process=process, name="decode"),
    FilterStage(is_valid),
])

pipeline.run(
    source="input.jsonl",
    sink="output.jsonl",
    executor=ProcessExecutor(workers=8),
    errors="skip",
)
```

Run it through the CLI launcher:

```bash
datapipe run ./my_pipeline.py:pipeline \
  --source input.jsonl \
  --sink output.jsonl \
  --workers 8
```

## Writing a custom tool for reuse

If you have a transformation you use often across projects, package it as a
tool and install it:

```python
# decode_tools.py

from datapipe.tools import tool, JsonType
import json


def _recursive_decode(obj):
    if isinstance(obj, dict):
        return {k: _recursive_decode(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_recursive_decode(v) for v in obj]
    if isinstance(obj, str):
        try:
            decoded = json.loads(obj)
            if isinstance(decoded, (dict, list)):
                return _recursive_decode(decoded)
        except (json.JSONDecodeError, ValueError):
            pass
    return obj


@tool(
    name="recursive_decode",
    api_version=1,
    target="value",
    input=JsonType.ANY,
    output=JsonType.ANY,
    description="Recursively decode all nested JSON strings in a value.",
    deterministic=True,
)
def recursive_decode(value) -> object:
    return _recursive_decode(value)
```

Install it:

```bash
datapipe tools install decode_tools.py
```

Use it in any expression:

```bash
datapipe transform 'recursive_decode(.tools)' input.jsonl output.jsonl
```
