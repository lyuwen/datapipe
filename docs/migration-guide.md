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
  'fromjson(.tools); fromjson(.metadata.annotation, recursive=true)' \
  input.jsonl output.jsonl
```

## Metadata nesting and extraction

The workflow the structural language exists for: collapsing a wide record into
a compact `metadata` object, and pulling fields back out of one.

### Collapsing root fields into a metadata object

Suppose records arrive flat, and everything that is not an identifier or a
payload should be tucked into a serialized `metadata` string:

```json
{"instance_id": "i1", "messages": [{"role": "user"}], "temperature": 0.7, "score": 9}
```

**Before** — this needed either two passes or a custom stage, because no single
`tool(path)` invocation can move fields between paths:

```python
from datapipe import Pipeline, GenericStage, ProcessExecutor
import json

KEEP = {"instance_id", "messages"}

def nest_metadata(record):
    metadata = {k: v for k, v in record.items() if k not in KEEP}
    result = {k: record[k] for k in record if k in KEEP}
    result["metadata"] = json.dumps(metadata, separators=(",", ":"))
    return result

pipeline = Pipeline([GenericStage(process=nest_metadata, name="nest")])
pipeline.run(source="in.jsonl", sink="out.jsonl", executor=ProcessExecutor())
```

**After** — one expression, one pass:

```bash
datapipe transform '.metadata << .(^instance_id|messages) | tojson' in.jsonl out.jsonl
```

```json
{"instance_id": "i1", "messages": [{"role": "user"}], "metadata": "{\"temperature\":0.7,\"score\":9}"}
```

`^` makes the field set a *complement*: everything except the named fields
moves. The destination excludes itself automatically, so `.metadata` never ends
up nested inside itself, and the trailing `| tojson` serializes the assembled
object rather than the last source.

When the field list comes from config rather than being typed literally, use
the `nest` tool — same operation, keyword arguments:

```bash
datapipe transform \
  'nest(., key="metadata", exclude=["instance_id","messages"], jsonify=true)' \
  in.jsonl out.jsonl
```

```json
{"instance_id": "i1", "messages": [{"role": "user"}], "metadata": "{\"temperature\":0.7,\"score\":9}"}
```

To nest an explicit list instead of a complement, name the fields positively:

```bash
datapipe transform '.metadata << .(temperature|score) | tojson' in.jsonl out.jsonl
```

```json
{"instance_id": "i1", "temperature": 0.7, "score": 9}
{"instance_id": "i1", "metadata": "{\"temperature\":0.7,\"score\":9}"}
```

A positive set is strict — a named field the record lacks is an error, rather
than being silently skipped.

### Extracting fields back out

Now the other direction: `metadata` is a JSON string and two of its fields need
to be first-class columns again, with the rest left serialized.

```json
{"instance_id": "i1", "metadata": "{\"temperature\":0.7,\"score\":9,\"note\":\"keep\"}"}
```

**Before** — decode, move, re-encode, by hand:

```python
import json

def extract(record):
    metadata = json.loads(record["metadata"])
    for field in ("temperature", "score"):
        record[field] = metadata.pop(field)
    record["metadata"] = json.dumps(metadata, separators=(",", ":"))
    return record
```

**After** — three statements, one worker pass:

```bash
datapipe transform \
  'fromjson(.metadata); . << .metadata.(temperature|score); tojson(.metadata)' \
  in.jsonl out.jsonl
```

```json
{"instance_id": "i1", "metadata": "{\"note\":\"keep\"}", "temperature": 0.7, "score": 9}
```

Read it as: decode `.metadata` in place; move two of its fields up to the root;
re-serialize what is left. The `;` separators sequence mutations of the same
record — nothing is dispatched twice.

The complement works here too, when it is easier to name what should *stay*:

```bash
datapipe transform \
  'fromjson(.metadata); . << .metadata.(^note); tojson(.metadata)' \
  in.jsonl out.jsonl
```

```json
{"instance_id": "i1", "metadata": "{\"note\":\"keep\"}", "temperature": 0.7, "score": 9}
```

And the `unnest` tool packages the whole three-statement shape behind
`parse`/`jsonify` flags:

```bash
datapipe transform \
  'unnest(., key="metadata", include=["temperature"], parse=true, jsonify=true)' \
  in.jsonl out.jsonl
```

```json
{"instance_id": "i1", "metadata": "{\"temperature\":0.7,\"note\":\"keep\"}"}
{"instance_id": "i1", "metadata": "{\"note\":\"keep\"}", "temperature": 0.7}
```

To lift a single field, `<-` is more direct than a one-element field set, and
it can decode on the way:

```bash
datapipe transform '.temperature <- fromjson(.metadata.temperature)' in.jsonl out.jsonl
```

```json
{"id": "i1", "metadata": {"temperature": "0.7"}}
{"id": "i1", "metadata": {}, "temperature": 0.7}
```

### Why this is one pass

Every expression above compiles to a single per-record program. A record is
dispatched to a worker once, every statement runs inside that worker, and the
result is gathered once — regardless of how many statements, moves, or pipes
the expression contains. There is no per-statement future and no intermediate
materialization of the dataset.

Use `datapipe inspect-expression` to confirm what a structural expression will
do before running it against data:

```bash
datapipe inspect-expression '.metadata << .(^instance_id|messages) | tojson'
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
