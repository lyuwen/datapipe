# CLI Reference

## Commands

### `datapipe transform`

Apply a jq-like transform expression to every record in a JSONL file.

```
datapipe transform [OPTIONS] EXPRESSION INPUT OUTPUT
datapipe [OPTIONS] EXPRESSION INPUT OUTPUT   # shorthand form
```

Outer JSON parsing and serialization happen inside workers automatically.
Do not include `fromjson(.)` or `tojson(.)` for the outer row in your
expression.

**Examples:**

```bash
# Decode a nested JSON string in the .tools field
datapipe transform 'fromjson(.tools)' input.jsonl output.jsonl

# Decode .tools, then serialize each .tools[].function.parameters back to a string
datapipe transform \
  'fromjson(.tools) | tojson(.tools[].function.parameters)' \
  input.jsonl output.jsonl

# Use an installed tool
datapipe transform 'normalize_text(.body, lowercase=true)' input.jsonl output.jsonl

# Process in parallel, skip bad records, ordered output
datapipe transform \
  --executor process --workers 8 \
  --errors skip \
  --ordered \
  'fromjson(.payload)' input.jsonl output.jsonl
```

**Options:**

| Flag | Default | Description |
|---|---|---|
| `--executor process\|thread\|sequential` | `process` | Execution backend |
| `--workers N` | CPU count | Number of worker processes/threads |
| `--max-in-flight N` | workers × 4 | Max concurrently submitted tasks |
| `--ordered` / `--unordered` | ordered | Whether output preserves input order |
| `--errors raise\|skip\|return` | `raise` | Per-record error policy |
| `--error-output PATH` | none | JSONL file for structured error records |
| `--validate-tools always\|sample\|off` | `always` | Runtime contract validation |
| `--progress` / `--no-progress` | progress | Show/hide the progress bar |
| `--rank N` | auto-detected | This process's rank |
| `--world-size N` | auto-detected | Total number of ranks |
| `--local-rank N` | auto-detected | Local rank on this node |
| `--dry-run` | off | Compile expression and print stages; do not read data |

---

### `datapipe run`

Execute a Python-defined Pipeline from a module or file reference.

```
datapipe run PIPELINE_REF [OPTIONS]
```

**Pipeline reference syntax:**

```
module.submodule:object_name   # importable Python module
./relative/file.py:object_name # file-system path
/absolute/file.py:object_name  # absolute path
```

**Example:**

```bash
datapipe run ./my_pipeline.py:pipeline \
  --source input.jsonl \
  --sink output.jsonl \
  --executor process \
  --workers 16
```

**Options:** See `datapipe run --help` for the full list. Accepts all the same
execution, ordering, error policy, progress, and runtime flags as `transform`.

The `--source` and `--sink` flags accept optional format prefixes:
`jsonl:path`, `parquet:path`. Format is inferred from the file extension when
the prefix is absent.

Use `--raw` when the pipeline contains `JsonLoadStage` and `JsonDumpStage` for
worker-side parsing — this passes raw strings to workers instead of
coordinator-parsed dicts.

---

### `datapipe inspect`

Display the stage structure of a Python-defined Pipeline without running data.

```
datapipe inspect PIPELINE_REF [--json]
```

**Example output:**

```
Pipeline 'my_pipeline'
  0  json_load    JsonLoadStage
  1  normalize    GenericStage   process=my_module.normalize
  2  is_valid     FilterStage    predicate=my_module.is_valid
  3  json_dump    JsonDumpStage
```

---

### `datapipe tools`

Manage installable tool providers.

```
datapipe tools install [--editable] [--force] [--yes] PATH
datapipe tools validate PATH
datapipe tools list
datapipe tools inspect NAME [--json]
datapipe tools remove PROVIDER_OR_NAME
```

The `datapipe-install` command is an alias for `datapipe tools install`:

```bash
datapipe-install ./my_tools.py
datapipe-install --editable ./my_tools.py
```

---

## Expression language

### Syntax

```
expression  := invocation ("|" invocation)*
invocation  := name "(" selector ("," argument)* ")"
argument    := identifier "=" literal
selector    := "." selector_part*
```

### Selectors

| Syntax | Meaning |
|---|---|
| `.` | Root record |
| `.field` | Object field access |
| `.a.b.c` | Nested field access |
| `.[0]` | Array element by index |
| `.["key.with.dots"]` | Field access with quoted key |
| `.field[]` | Every element of an array (wildcard) |
| `.field[].name` | Wildcard then nested field |

Missing fields and out-of-range indexes are errors. An empty wildcard (`[]`
on an empty array) produces zero matches and succeeds silently.

### Literals in arguments

```
true, false, null        Boolean and null (also True, False, None)
42, -1, 3.14             Numbers
"hello", 'world'         Strings
[1, "a", true]           Arrays
```

### Pipeline (`|`) semantics

Each `|` passes the full record to the next operation. Selectors in later
operations are evaluated against the record *after* all prior operations have
applied:

```
fromjson(.tools) | tojson(.tools[].function.parameters)
```

Here `tojson` sees `.tools` as a decoded array because `fromjson` already ran.

### Built-in tools

**`fromjson(selector, *, recursive=false, containers_only=true)`**

Decode a JSON-encoded string. Without `recursive`, the selected value must be
a string and is decoded once. With `recursive=true`, the value is decoded and
then traversed depth-first; nested strings are decoded when they are valid JSON
arrays or objects (with `containers_only=true`, the default) or any valid JSON
value (with `containers_only=false`).

**`tojson(selector, *, ensure_ascii=false, compact=true, sort_keys=false)`**

Serialize a value to a JSON string. An already-string value is re-serialized
as a JSON string literal — it is not treated as already serialized.
Non-finite floats raise `ValueError`.

### Installed tool name resolution

Unqualified names resolve when unique. Qualified names (`alias.tool_name`)
always resolve. Built-in names (`fromjson`, `tojson`) are reserved and cannot
be shadowed unqualified.

---

## Error policies

| Policy | Behavior |
|---|---|
| `raise` (default) | First per-record error aborts the run |
| `skip` | Failed records are counted and omitted |
| `return` | Errors are sent to `--error-output` |

Errors during expression compilation, provider validation, or sink operations
are always run-level failures regardless of the policy.

The error output file contains one JSON record per failed input with fields:
`seq`, `error_type`, `error_message`, `traceback`, `stage_name`, and when the
failure is a tool contract violation, a `tool` object with `invocation_index`,
`tool_name`, `provider_id`, `selector`, `matched_path`, `expected_type`,
`actual_type`, and `stage` (`"input"`, `"output"`, or `"call"`).

---

## Runtime validation modes

| Mode | Behavior |
|---|---|
| `always` (default) | Validate input and output contracts for every record |
| `sample` | Validate the first 100 records per worker, then trust the provider |
| `off` | No contract validation; tool-body exceptions still surface |

---

## Distributed execution

datapipe runs independently per rank. Each rank reads its own shard, processes
it locally, and writes its own output shard. No inter-rank communication is
needed.

```bash
# torchrun-style: set RANK and WORLD_SIZE in the environment
RANK=0 WORLD_SIZE=4 datapipe transform 'fromjson(.tools)' in/ out/
RANK=1 WORLD_SIZE=4 datapipe transform 'fromjson(.tools)' in/ out/

# or override explicitly
datapipe transform --rank 0 --world-size 4 'fromjson(.tools)' in/ out/
```

`datapipe transform` also respects `SLURM_PROCID`/`SLURM_NTASKS` and
`JOB_COMPLETION_INDEX`/`WORLD_SIZE` from Kubernetes Indexed Jobs.
