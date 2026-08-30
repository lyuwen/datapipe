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
| `api_version` | `int` | yes | Must be `1` |
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

`ToolExample` pairs are declared for documentation and future smoke-test support. They are not yet executed during installation — a planned later release will run them as part of the installation validation pipeline. Declaring them now means they will be picked up automatically when that support ships:

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

def test_via_dsl(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAPIPE_USER_DATA", str(tmp_path / "data"))
    _loader._loaded_providers.clear()
    install_provider("my_tools.py", yes=True)

    ce = compile_expression("normalize_text(.text, lowercase=true)")
    inv = ce.invocations[0]
    result = inv.tool_fn({"text": "  Hello  "}, **inv.arguments)
    assert result == "hello"
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
