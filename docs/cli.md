# CLI Reference

## Commands

| Command | Purpose |
|---|---|
| [`datapipe transform`](#datapipe-transform) | Apply a jq-like expression to JSONL records |
| [`datapipe inspect-expression`](#datapipe-inspect-expression) | Compile an expression and show how it resolves |
| [`datapipe run`](#datapipe-run) | Execute a Python-defined `Pipeline` |
| [`datapipe inspect`](#datapipe-inspect) | Show a `Pipeline`'s stage structure |
| [`datapipe tools`](#datapipe-tools) | Manage tool providers |

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
| `--input-format jsonl` | `jsonl` | Input record format |
| `--output-format jsonl` | `jsonl` | Output record format |
| `--rank N` | auto-detected | This process's rank |
| `--world-size N` | auto-detected | Total number of ranks |
| `--local-rank N` | auto-detected | Local rank on this node |
| `--dry-run` | off | Compile the expression and print resolved tools, contracts and stages; do not read data |
| `--json` | off | With `--dry-run`, emit the compilation result as JSON |

Only `jsonl` is currently accepted for `--input-format`/`--output-format`.
Passing anything else is a usage error. Parquet is deliberately deferred until
column and schema semantics are defined for selectors and dynamically typed
tool output.

#### Shell quoting

**Always wrap the expression in single quotes.** Expressions contain `.`, `|`,
`[]`, `(` and `)` — every one of which the shell would otherwise interpret
before `datapipe` ever sees it. Unquoted, `|` becomes a shell pipe and `[]`
becomes a glob.

In `bash`:

```bash
# Correct — the shell passes the expression through untouched
datapipe transform 'fromjson(.tools) | tojson(.tools[].name)' in.jsonl out.jsonl

# Wrong — the shell splits on | and runs `tojson(...)` as a command
datapipe transform fromjson(.tools) | tojson(.tools[].name) in.jsonl out.jsonl
```

In `zsh` the same single-quoting rule applies, and it matters more: `zsh`
errors on an unmatched glob rather than passing it through, so an unquoted
`.tools[].name` fails with `no matches found` instead of reaching the CLI.

```zsh
# Correct in zsh
datapipe transform 'fromjson(.tools) | tojson(.tools[].name)' in.jsonl out.jsonl

# zsh: no matches found: .tools[].name
datapipe transform fromjson(.tools) | tojson(.tools[].name) in.jsonl out.jsonl
```

To embed a literal single quote in a string argument, end the quoted run,
escape the quote, and reopen it — `'normalize_text(.body, pad='\''-'\'')'`.
Double quotes work in both shells but expose the expression to `$` expansion
and history expansion, so prefer single quotes.

---

### `datapipe inspect-expression`

Compile a transform expression and report how it resolves, without opening any
data. Use it to confirm which provider a tool name binds to, what the declared
contract is, and which arguments the compiler filled in from defaults.

```
datapipe inspect-expression [--json] [--validate-tools MODE] EXPRESSION
```

```bash
datapipe inspect-expression 'fromjson(.tools)'
datapipe inspect-expression --json 'fromjson(.a) | tojson(.a.b)'
```

Human-readable output reports, per invocation, the tool name and selector, the
resolved provider (id, alias, mode), the contract (target, input, output,
cardinality), and the bound arguments — followed by the pipeline stage chain
the expression generates:

```text
Expression: fromjson(.tools)
Invocations: 1
  [0] fromjson(.tools)
        provider:    builtin
        target:      value
        input:       string | array | object
        output:      any
        cardinality: one_to_one
        arguments:   recursive=false, containers_only=true
Stages:
  [0] json_load  (JsonLoadStage)
  [1] fromjson(.tools)  (CompiledToolProgramStage)
  [2] json_dump  (JsonDumpStage)
```

`--json` emits the same information as a JSON document (with real JSON
`true`/`false`), suitable for scripting. `transform --dry-run` reports exactly
the same content, so either surface can be used interchangeably.

Exit code is `1` on a syntax error, an unknown tool, or an invalid argument —
inspection is a compilation, so it fails on anything a real run would reject.

**Options:**

| Flag | Default | Description |
|---|---|---|
| `--json` | off | Emit JSON instead of human-readable text |
| `--validate-tools always\|sample\|off` | `always` | Validation mode to report for the generated stage |

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

Under physical sharding each rank's progress bar is scaled to that rank's own
share, so a four-rank Parquet run shows four bars that each reach 100% rather
than four that stall at 25%.

---

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `DATAPIPE_LOG_LEVEL` | `WARNING` | CLI log verbosity: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |
| `DATAPIPE_USER_DATA` | `~/.local/share` | Base directory for the tool provider registry |

### `DATAPIPE_LOG_LEVEL`

Logs go to stderr, so they never contaminate piped output. At `INFO` the CLI
emits a start-up summary before any record is processed — the source and sink
identities, then the executor, worker count, in-flight bound, rank, ordering
and error policy:

```bash
$ DATAPIPE_LOG_LEVEL=INFO datapipe transform 'fromjson(.a)' in.jsonl out.jsonl
INFO datapipe: IO | source=JsonlSource(in.jsonl) | sink=JsonlSink(out.jsonl)
INFO datapipe: Pipeline 'json_load' | executor=ProcessExecutor | workers=8 |
  max_in_flight=32 | rank=0/1 | ordered=True | errors=raise
completed=1000  elapsed=0.42s  rate=2381 rec/s
```

This is the fastest way to confirm which files a run actually opened and how
many workers it really started. An unrecognized value falls back to `WARNING`
rather than failing the run.
