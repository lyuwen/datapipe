# Writing Tools for datapipe

This guide explains how to create, test, and install your own transformation
tools for use in `datapipe transform` expressions.

## What a tool is

A tool is a Python function decorated with `@tool` that declares its
input/output types, target scope, and configuration parameters. Once installed,
it becomes available in transform expressions:

```bash
datapipe transform 'normalize_text(.description, lowercase=true)' in.jsonl out.jsonl
```

Tools are responsible only for transforming a single value. Path traversal,
error attribution, progress tracking, and executor choice all belong to
datapipe; your function handles only the transformation logic.

## Creating a provider file

A provider is a single `.py` file containing one or more `@tool`-decorated
functions. Create a file, import the decorator and type vocabulary, and
annotate each function:

```python
# my_tools.py

from datapipe.tools import tool, JsonType, ToolExample


@tool(
    name="normalize_text",
    api_version=1,
    target="value",
    input=JsonType.STRING,
    output=JsonType.STRING,
    description="Normalize whitespace in a string.",
    examples=[
        ToolExample(
            input="  Hello  world ",
            arguments={"lowercase": True},
            output="hello world",
        ),
    ],
)
def normalize_text(value: str, *, lowercase: bool = False, strip: bool = True) -> str:
    if strip:
        value = value.strip()
    value = " ".join(value.split())
    return value.lower() if lowercase else value
```

## The `@tool` decorator parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `name` | `str` | yes | Name used in DSL expressions |
| `api_version` | `int` | no | Defaults to `1`; must be `1` if given |
| `target` | `str` | yes | `"value"` or `"record"` (see below) |
| `input` | `JsonType \| TypeSpec` | yes | Acceptable input type |
| `output` | `JsonType \| TypeSpec` | yes | Expected output type |
| `cardinality` | `str` | no | Only `"one_to_one"` is supported; default |
| `deterministic` | `bool` | no | True when output depends only on input and config |
| `description` | `str` | no | Human-readable description |
| `examples` | `list[ToolExample]` | no | Input/output pairs run as smoke tests at install |

### Target scope: `"value"` vs. `"record"`

**`target="value"`** — the tool is called once for every value matched by
the selector. Use this for most transformations:

```python
# Signature: first positional argument is the selected value
def normalize_text(value: str, *, lowercase: bool = False) -> str: ...
```

**`target="record"`** — the tool is called once on the whole record. The
selector must be `.` (the root). Use this when you need to restructure the
entire row:

```python
# Signature: first positional argument is the complete record dict
def add_metadata(record: dict, *, source: str = "") -> dict:
    record["_source"] = source
    return record
```

### Record-level structural tools

A `target="record"` tool that *restructures* the record — moving fields between
paths rather than transforming one value in place — has some extra
considerations, because the structural language can already express most of
what such a tool would do.

**Reach for the symbolic form first.** These are equivalent:

```bash
datapipe transform '.metadata << .(^instance_id|messages) | tojson' in.jsonl out.jsonl
datapipe transform \
  'nest(., key="metadata", exclude=["instance_id","messages"], jsonify=true)' \
  in.jsonl out.jsonl
```

The symbolic form needs no installation, is checked at compile time, and
reports its structure to `inspect-expression`. Write a record-level tool when:

- the field names are **not known when the expression is written** — they come
  from a config file, a manifest, or a caller's arguments;
- the restructuring is **conditional on the record's contents**, which the
  language has no branching for;
- you are packaging a multi-step shape your team repeats, and want one name and
  one contract for it.

If you can type the field names literally, the expression is the better answer.

**Contract requirements.** A record-level tool must declare:

```python
@tool(
    name="nest_config",
    api_version=1,
    target="record",          # called once per record, not per matched value
    input=JsonType.OBJECT,    # a record is an object
    output=JsonType.OBJECT,
    cardinality="one_to_one", # the only executable cardinality
    deterministic=True,
)
def nest_config(record: dict, *, key: str = "metadata") -> dict: ...
```

- `target="record"` means the selector in the expression **must** be `.`; the
  compiler rejects `nest_config(.metadata)`.
- `cardinality` must be `one_to_one`. A returned list is one record whose value
  is a list, never an implicit flat-map.
- The record and any argument defaults cross the process boundary, so
  everything must be pickleable and every default JSON-serializable.

**Be all-or-nothing.** A record-level tool that mutates its argument and then
raises leaves a half-restructured record behind — and under `--errors skip`
that record is dropped with no indication of how far it got. Either validate
every precondition before the first write, or work on a copy and return it only
once everything has succeeded.

**Worked examples.** The built-in `nest` and `unnest`
(`datapipe/tools/builtins/structural.py`) are the reference implementations.
They are worth reading because of how they avoid re-implementing anything: each
one desugars its arguments into the *same* compiled operations the symbolic
`<<` form produces, and executes those. Collision rules, source ordering,
destination auto-creation, and atomicity are inherited rather than restated, so
the named and symbolic forms cannot drift apart.

If you write a structural tool of your own, that is the pattern to copy —
build the operation the language already implements rather than hand-rolling
dictionary surgery that has to keep its own semantics in sync.

### JSON type vocabulary

```python
JsonType.NULL        # Python None
JsonType.BOOLEAN     # Python bool
JsonType.INTEGER     # Python int (not bool)
JsonType.NUMBER      # Python int or non-bool, finite float
JsonType.STRING      # Python str
JsonType.ARRAY       # Python list
JsonType.OBJECT      # Python dict
JsonType.SCALAR      # any of NULL, BOOLEAN, NUMBER, STRING
JsonType.CONTAINER   # ARRAY or OBJECT
JsonType.ANY         # any JSON-representable value
```

For a tool that accepts multiple types, use `OneOf`:

```python
from datapipe.tools import OneOf

@tool(
    name="stringify",
    target="value",
    input=OneOf(JsonType.STRING, JsonType.INTEGER, JsonType.NULL),
    output=JsonType.STRING,
)
def stringify(value, *, prefix: str = "") -> str:
    return prefix + ("" if value is None else str(value))
```

## Signature rules

The decorator validates the signature at import time and raises
`ToolDecoratorError` if any of these rules are violated:

- exactly one positional parameter (the value or record);
- all remaining parameters must be keyword-only (placed after `*`);
- no `*args` or `**kwargs`;
- all keyword-only parameters must have JSON-serializable defaults;
- defaults must be JSON-serializable (`str`, `int`, `float`, `bool`, `None`,
  `list`, `dict` — not lambdas, class instances, or sets).

```python
# GOOD — keyword-only config with a default
def my_tool(value: str, *, max_len: int = 100, strip: bool = True) -> str: ...

# BAD — second parameter is not keyword-only (missing * before it)
def my_tool(value: str, max_len: int = 100) -> str: ...

# BAD — default is not JSON-serializable
def my_tool(value, *, fn=lambda x: x) -> str: ...
```

## Expensive state and worker setup

Avoid loading models, opening files, or creating HTTP clients at module level.
Resources created there are in the coordinator process and may not be needed
in workers.

For resources needed during processing, use a module-level lazy cache:

```python
_model = None

def _get_model():
    global _model
    if _model is None:
        _model = load_my_model("weights.bin")
    return _model


@tool(name="classify", target="value", input=JsonType.STRING, output=JsonType.STRING)
def classify(value: str) -> str:
    return _get_model().predict(value)
```

The cache is per-worker-process, so each worker initializes once. Teardown
is unavailable for function tools in Phase 1; do not rely on it for
correctness.

## Adding examples

`ToolExample` pairs are executed during installation and their output is
validated against the tool's declared output contract.  A provider with a
failing example is rejected and nothing is registered, so examples act as a
functional smoke test in addition to documenting intent.  Examples run in an
isolated subprocess, like the rest of dynamic validation:

```python
@tool(
    name="upper",
    target="value",
    input=JsonType.STRING,
    output=JsonType.STRING,
    examples=[
        ToolExample(input="hello", output="HELLO"),
        ToolExample(input="abc", arguments={"times": 2}, output="ABCABC"),
    ],
)
def upper(value: str, *, times: int = 1) -> str:
    return value.upper() * times
```

## Testing your provider

Write tests that call the function directly before installing it:

```python
from my_tools import normalize_text

def test_strips_whitespace():
    assert normalize_text("  hello  ") == "hello"

def test_lowercase():
    assert normalize_text("Hello World", lowercase=True) == "hello world"
```

Also test through `compile_expression` to verify the DSL integration:

```python
import os, tempfile
from datapipe.tools.installer import install_provider
from datapipe.tools import loader as _loader
from datapipe.dsl.compiler import compile_expression
from datapipe.stages.tool_program import CompiledToolProgramStage
from datapipe.context import WorkerContext

def test_via_dsl(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAPIPE_USER_DATA", str(tmp_path / "data"))
    _loader._loaded_providers.clear()
    install_provider("my_tools.py", yes=True)

    # Drive the real stage rather than calling the tool directly: the stage
    # resolves the selector, applies the tool to the selected value, and
    # writes the result back into the record.  Provider tools are resolved
    # per-worker from a descriptor, so a compiled ToolInvocation does not
    # carry a callable you can invoke here.
    stage = CompiledToolProgramStage(
        compile_expression("normalize_text(.text, lowercase=true)")
    )
    ctx = WorkerContext(rank=0, world_size=1, worker_id=0, local_rank=None)
    assert stage.process({"text": "  Hello  World "}, ctx) == {"text": "hello world"}
```

## Installing

```bash
# Copied installation (snapshot; later edits do not affect the install)
datapipe tools install my_tools.py

# Editable installation (changes to the file take effect on the next run)
datapipe tools install --editable my_tools.py

# The datapipe-install alias is equivalent to the above
datapipe-install my_tools.py
datapipe-install --editable my_tools.py
```

After installation, verify the tool appears:

```bash
datapipe tools list
datapipe tools inspect normalize_text
```

## Using your tool in an expression

Installed tools resolve by their unqualified name if it does not clash with a
built-in, or by the qualified `alias.name` form:

```bash
# Unqualified (works when the name is unique)
datapipe transform 'normalize_text(.body, lowercase=true)' in.jsonl out.jsonl

# Qualified (always works, required for ambiguous names)
datapipe transform 'my_tools.normalize_text(.body, lowercase=true)' in.jsonl out.jsonl
```

The alias is derived from the file stem (the part before `.py`). `my_tools.py`
becomes alias `my_tools`.

## Updating a provider

For a copied installation, reinstall with `--force`:

```bash
datapipe tools install --force my_tools.py
```

For an editable installation, just edit the file — the next run picks up the
changes automatically.

## Removing a provider

```bash
datapipe tools remove local:my_tools
```

Removing a copied provider deletes the snapshot from the registry directory.
Removing an editable provider does not delete the original file.
