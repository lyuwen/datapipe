# CLI Reference

## Commands

| Command | Purpose |
|---|---|
| [`datapipe transform`](#datapipe-transform) | Apply a structural transform expression to JSONL records |
| [`datapipe inspect-expression`](#datapipe-inspect-expression) | Compile an expression and show how it resolves |
| [`datapipe run`](#datapipe-run) | Execute a Python-defined `Pipeline` |
| [`datapipe inspect`](#datapipe-inspect) | Show a `Pipeline`'s stage structure |
| [`datapipe tools`](#datapipe-tools) | Manage tool providers |

### `datapipe transform`

Apply a structural transform expression to every record in a JSONL file. See
[Expression language](#expression-language) for the full language.

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
  'fromjson(.tools); tojson(.tools[].function.parameters)' \
  input.jsonl output.jsonl

# Two independent mutations in one pass
datapipe transform 'tojson(.tools); tojson(.metadata)' input.jsonl output.jsonl

# Nest everything except the named fields, then serialize the result
datapipe transform \
  '.metadata << .(^instance_id|messages) | tojson' \
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

**Always wrap the expression in single quotes.** Every structural operator is
also a shell metacharacter, and the shell consumes them before `datapipe` is
ever started:

| In an expression | What an unquoted shell does with it |
|---|---|
| `<<` | starts a heredoc — the shell waits for input that never comes |
| `<-` | `<` redirects stdin from a file named `-` |
| `;` | ends the command; the rest runs as a separate command |
| `\|` | pipes `datapipe` into whatever follows |
| `(` `)` | opens a subshell — usually a syntax error |
| `^` | history substitution in some interactive shells |
| `[]` | filename globbing |

In `bash`:

```bash
# Correct — the shell passes the expression through untouched
datapipe transform 'fromjson(.tools); tojson(.tools[].name)' in.jsonl out.jsonl

# Wrong — the shell splits on ; and runs `tojson(...)` as a command
datapipe transform fromjson(.tools); tojson(.tools[].name) in.jsonl out.jsonl

# Wrong — bash reads `<< .(^id)` as a heredoc and hangs waiting for input
datapipe transform .metadata << .(^id) in.jsonl out.jsonl
```

In `zsh` the same single-quoting rule applies, and it matters more: `zsh`
errors on an unmatched glob rather than passing it through, so an unquoted
`.tools[].name` fails with `no matches found` instead of reaching the CLI.

```zsh
# Correct in zsh
datapipe transform 'fromjson(.tools); tojson(.tools[].name)' in.jsonl out.jsonl

# zsh: no matches found: .tools[].name
datapipe transform fromjson(.tools); tojson(.tools[].name) in.jsonl out.jsonl
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
datapipe inspect-expression --json '.metadata << .(^id) | tojson'
```

Human-readable output reports, per invocation, the tool name and selector, the
resolved provider (id, alias, mode), the contract (target, input, output,
cardinality), and the bound arguments — followed by the pipeline stage chain
the expression generates:

```text
expression-language: 2
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

For a structural program the report is organized by **statement**, showing each
statement's focus, its operation kind, its sources, and its pipes — which is
how you confirm what a `<<` will actually move before running it:

```text
expression-language: 2
Expression: .metadata << .(^instance_id|messages) | tojson; nest(., key="m")
Statements: 2
  Statement 0  (focus: .metadata)
    move-into .metadata
      sources: complement(instance_id, messages)
    [0] pipe: tojson
          provider:    builtin
          target:      value
          input:       any
          output:      string
          cardinality: one_to_one
          arguments:   ensure_ascii=false, compact=true, sort_keys=false
  Statement 1
    [1] call nest at .
          provider:    builtin
          target:      record
          input:       object
          output:      object
          cardinality: one_to_one
          arguments:   key="m", include=null, exclude=null, jsonify=false, collision="error", missing="error"
Stages:
  [0] json_load  (JsonLoadStage)
  [1] .metadata << .(^instance_id|messages) |…  (CompiledProgramStage)
  [2] json_dump  (JsonDumpStage)
```

`expression-language` is the version of the expression language this build
implements: `1` was invocations and `|`; `2` adds statements, focused pipes,
`=`/`<-`, `<<` with field sets, and `nest`/`unnest`.

`--json` emits the same information as a JSON document (with real JSON
`true`/`false`), suitable for scripting: a structural program yields a
`statements` array whose entries carry `focus`, `operation` (with its `kind`,
`destination` and `sources`) and `pipes`, alongside `expression_language` and
`stages`. `transform --dry-run` reports exactly the same content, so either
surface can be used interchangeably.

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

An expression is a **program that runs once per record**. It is a sequence of
statements separated by `;`, each of which mutates the one evolving record.
Whatever the record looks like after the last statement is what gets written.

```
program     := statement (";" statement)* ";"?
statement   := invocation | focused | assignment | move_into
invocation  := name "(" selector ("," argument)* ")"
focused     := selector "|" bare_call ("|" bare_call)*
assignment  := selector ("=" | "<-") (selector | invocation) ("|" bare_call)*
move_into   := selector "<<" source ("," source)* ("|" bare_call)*
source      := selector | selector "." "(" "^"? name ("|" name)* ")"
bare_call   := name ("(" argument ("," argument)* ")")?
argument    := identifier "=" literal
selector    := "." selector_part*
```

The two kinds of composition are the thing to internalize:

| | Meaning |
|---|---|
| `;` | sequence independent mutations of the record; resets the focus |
| `\|` | keep transforming the **current focused value** |

Everything below is executable exactly as written. Wrap it in single quotes —
see [Shell quoting](#shell-quoting).

### Statement sequencing (`;`)

Each statement sees the record as the previous statement left it. All of them
run inside a single worker invocation — a record crosses the process boundary
once no matter how many statements there are.

```bash
datapipe transform 'tojson(.tools); tojson(.metadata)' in.jsonl out.jsonl
```

```json
{"id": "i1", "tools": [{"name": "search"}], "metadata": {"temperature": 0.7}}
{"id": "i1", "tools": "[{\"name\":\"search\"}]", "metadata": "{\"temperature\":0.7}"}
```

A trailing `;` is allowed. Statements are never reordered or parallelized
against each other.

### Focused pipes (`|`)

A statement can name a value and then pipe it through tools that take no
selector of their own. Each tool receives the value the previous one returned,
and the final result is written back to where the focus points.

```bash
datapipe transform '.metadata | fromjson | tojson' in.jsonl out.jsonl
```

```json
{"id": "i1", "metadata": "{\"temperature\":0.7,\"score\":9}"}
{"id": "i1", "metadata": "{\"temperature\":0.7,\"score\":9}"}
```

(That round-trips, which is the point — `fromjson` decodes it and `tojson`
re-encodes it.) A wildcard focus applies the whole chain elementwise:

```bash
datapipe transform '.tools[] | tojson' in.jsonl out.jsonl
```

```json
{"tools": [{"a": 1}, {"b": 2}]}
{"tools": ["{\"a\":1}", "{\"b\":2}"]}
```

`;` resets the focus, so the next statement starts from the root again:

```bash
datapipe transform '.metadata | fromjson; tojson(.tools)' in.jsonl out.jsonl
```

```json
{"metadata": "{\"a\":1}", "tools": [1]}
{"metadata": {"a": 1}, "tools": "[1]"}
```

### Copy (`=`) and move (`<-`)

`=` copies a value, leaving the source in place. `<-` moves it, removing the
source only after the write has succeeded.

```bash
datapipe transform '.temperature = .metadata.temperature' in.jsonl out.jsonl
```

```json
{"id": "i1", "metadata": {"temperature": 0.7}}
{"id": "i1", "metadata": {"temperature": 0.7}, "temperature": 0.7}
```

```bash
datapipe transform '.temperature <- .metadata.temperature' in.jsonl out.jsonl
```

```json
{"id": "i1", "metadata": {"temperature": 0.7, "score": 9}}
{"id": "i1", "metadata": {"score": 9}, "temperature": 0.7}
```

The right-hand side may be a tool call, which transforms the value on the way:

```bash
datapipe transform '.temperature <- fromjson(.metadata.temperature)' in.jsonl out.jsonl
```

```json
{"id": "i1", "metadata": {"temperature": "0.7"}}
{"id": "i1", "metadata": {}, "temperature": 0.7}
```

Overlapping source and destination paths are rejected — at compile time when
provable, otherwise per record — so an assignment can never silently destroy
the value it is reading.

### Move-into (`<<`) and field sets

`<<` moves several fields into one destination object at once, deriving each
destination key from the source's final field name. The destination is created
if it does not exist.

```bash
datapipe transform '.metadata << .temperature, .score' in.jsonl out.jsonl
```

```json
{"id": "i1", "temperature": 0.7, "score": 9}
{"id": "i1", "metadata": {"temperature": 0.7, "score": 9}}
```

A **field set** names several fields sharing a base — `.(a|b|c)`. Inside the
parentheses `|` is a name union, not a pipe:

```bash
datapipe transform '.metadata << .(temperature|score) | tojson' in.jsonl out.jsonl
```

```json
{"instance_id": "i1", "temperature": 0.7, "score": 9}
{"instance_id": "i1", "metadata": "{\"temperature\":0.7,\"score\":9}"}
```

A positive field set is strict: naming a field the record does not have is an
error.

A **complement** field set — `^` before the names — selects every field
*except* the ones named. This is the blanket-nesting form, and the destination
excludes itself automatically so `.metadata` is never nested inside itself:

```bash
datapipe transform '.metadata << .(^instance_id|messages) | tojson' in.jsonl out.jsonl
```

```json
{"instance_id": "i1", "messages": [{"role": "user"}], "temperature": 0.7, "score": 9}
{"instance_id": "i1", "messages": [{"role": "user"}], "metadata": "{\"temperature\":0.7,\"score\":9}"}
```

Unlike a positive set, naming a field the record lacks is harmless here — there
is simply nothing to exclude.

A trailing `| tojson` binds to the **whole** move-into, not to the last source,
so it serializes the assembled destination object.

Field sets work in the other direction too. The base can be nested and the
destination the root, which is how you lift fields out of a metadata object:

```bash
datapipe transform '. << .metadata.(temperature|score)' in.jsonl out.jsonl
```

```json
{"instance_id": "i1", "metadata": {"temperature": 0.7, "score": 9, "note": "keep"}}
{"instance_id": "i1", "metadata": {"note": "keep"}, "temperature": 0.7, "score": 9}
```

`<<` is atomic: nothing is written until every source has resolved and every
collision check has passed, so a failure on the third of three sources leaves
the record untouched.

### `nest` and `unnest`

These are named, argument-configurable equivalents of the symbolic forms.
They desugar into exactly the same compiled operations, so they behave
identically — use them when the field names come from a config file or a
script rather than being typed literally.

`nest` moves root fields into a nested object:

```bash
datapipe transform \
  'nest(., key="metadata", exclude=["instance_id","messages"], jsonify=true)' \
  in.jsonl out.jsonl
```

```json
{"instance_id": "i1", "messages": [{"role": "user"}], "temperature": 0.7, "score": 9}
{"instance_id": "i1", "messages": [{"role": "user"}], "metadata": "{\"temperature\":0.7,\"score\":9}"}
```

`unnest` moves them back out. `parse=true` decodes a JSON-encoded source
first; `jsonify=true` re-encodes whatever is left in it:

```bash
datapipe transform \
  'unnest(., key="metadata", include=["temperature"], parse=true, jsonify=true)' \
  in.jsonl out.jsonl
```

```json
{"instance_id": "i1", "metadata": "{\"temperature\":0.7,\"note\":\"keep\"}"}
{"instance_id": "i1", "metadata": "{\"note\":\"keep\"}", "temperature": 0.7}
```

| Parameter | Meaning |
|---|---|
| `key` | the nested object (default `"metadata"`) |
| `include` | field names to move; strict — a missing name is an error |
| `exclude` | move everything *but* these; missing names are harmless |
| `parse` | (`unnest`) decode the source with `fromjson` first |
| `jsonify` | serialize the destination (`nest`) or remainder (`unnest`) |

`include` and `exclude` are mutually exclusive; supplying neither means "every
field". Both tools are all-or-nothing: a failure anywhere leaves the input
record unmodified.

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
| `.a.(x\|y)` | Field set: `x` and `y` under `.a` |
| `.a.(^x\|y)` | Complement: everything under `.a` except `x` and `y` |

Missing fields and out-of-range indexes are errors. An empty wildcard (`[]`
on an empty array) produces zero matches and succeeds silently.

### Literals in arguments

```
true, false, null        Boolean and null (also True, False, None)
42, -1, 3.14             Numbers
"hello", 'world'         Strings
[1, "a", true]           Arrays
```

### Legacy `|` between explicit targets

Before `;` existed, two full invocations could be joined with `|`:

```bash
datapipe transform 'fromjson(.a) | tojson(.b)' in.jsonl out.jsonl
```

This still compiles and emits a `DeprecationWarning` naming the replacement:

```text
`|` between explicit record mutations is deprecated; use semicolons:
  fromjson(.a); tojson(.b)
```

Mixing the two readings of `|` in one expression is **not** guessed at. An
explicit selector after a bare call is rejected with the rewrite spelled out,
because `|` cannot mean "sequence records" and "transform the focus" at once:

```text
$ datapipe transform 'fromjson(.a) | tojson | tojson(.b)' in.jsonl out.jsonl
error: ambiguous `|`: 'tojson' is given an explicit selector but follows a bare
tool call, which takes the current focus; `|` cannot mean both. Use `;` to
sequence record mutations:
  fromjson(.a) | tojson; tojson(.b)
```

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
