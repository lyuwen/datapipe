# Product-Layer Extension: Configurable Transform CLI and Installable Tools

> **Status:** implementation plan for an architectural extension  
> **Foundation:** [`parallel_record_pipeline_architecture.md`](parallel_record_pipeline_architecture.md)

## 1. Purpose

This document specifies a higher-level product and authoring architecture built
on top of the record-processing foundation in
[`parallel_record_pipeline_architecture.md`](parallel_record_pipeline_architecture.md).
It adds a jq-like command-line interface, typed transformation contracts, tool
discovery, and installable Python tool providers. It does not replace or fork
the initial pipeline architecture.

The intended user experience is:

```bash
datapipe --ordered --progress \
  'fromjson(.tools) |
   fromjson(.metadata.annotation, recursive=true) |
   tojson(.tools[].function.parameters)' \
  input.jsonl output.jsonl
```

Users must also be able to expose their own Python transformations:

```bash
datapipe-install --editable ./my_tools.py

datapipe --workers 32 \
  'fromjson(.tools) |
   normalize_text(.metadata.annotation, lowercase=true) |
   redact(.metadata.secret)' \
  input.jsonl output.jsonl
```

The product can be understood as:

> A streaming map engine with a jq-like CLI and installable, typed Python
> transformations.

It provides the convenience of a customizable Hugging Face Datasets `.map()`
operation without requiring the user to construct a Dataset object or write a
new Python driver for every transformation. It retains `datapipe`'s bounded
streaming, multiple executors, ordered or completion-order output, JSONL and
Parquet adapters, sharding, and error policies.

This is not an attempt to implement the complete jq language, provide a secure
Python sandbox, or turn each DSL operation into an independently scheduled
dataflow operator.

### 1.1 Relationship to the foundational architecture

The foundational architecture answers the execution question:

> How does a Python-defined per-record program run efficiently and correctly
> over a large streaming dataset?

It establishes the core data plane:

- `Source` and `Sink` adapters;
- composable, worker-local `Stage` objects;
- `Pipeline` and `CompiledPipeline`;
- sequential, thread, and process executors;
- bounded `max_in_flight` scheduling;
- ordered and unordered result handling;
- progress, error policies, runtime context, and sharding.

This extension answers the authoring and distribution question above it:

> How can users construct a useful per-record program from the shell, configure
> reusable transformations, and install new transformations without writing a
> dedicated pipeline driver for each use case?

The relationship is intentionally layered:

```text
User experience
  datapipe expression, flags, inspection, tool-management commands
                              |
Authoring and distribution control plane
  DSL parser, selectors, contracts, registry, installer, provider validation
                              |
Compilation boundary
  expression -> immutable invocation descriptors -> ordinary Stage objects
                              |
Existing record-processing data plane
  Pipeline -> CompiledPipeline -> Executor -> ordering/progress -> Sink
                              |
Existing environment and storage layer
  JSONL/Parquet, sharding, rank/world-size, local/thread/process execution
```

The compilation boundary is the architectural seam. Everything above it is
new in this plan. Everything below it remains the initial architecture and is
reused rather than reimplemented.

The installer and registry are a **control plane**. They validate and describe
available transformations before a run. They never schedule records, own
worker pools, or sit in the per-record data path. The existing pipeline runtime
is the **data plane**. It receives a compiled per-record program and remains
unaware of whether that program originated from handwritten Python or a CLI
expression.

### 1.2 Inherited invariants

All foundational runtime invariants continue to apply:

1. A record is dispatched to a worker once and gathered once.
2. The complete stage sequence for a record executes inside one worker.
3. Stages are semantic composition units, not independently scheduled jobs.
4. Submission remains bounded by `max_in_flight`.
5. Sources and sinks remain streaming abstractions.
6. Pipeline definitions remain independent of executor choice.
7. Local parallelism remains independent of distributed sharding.
8. Heavy state belongs to worker setup rather than per-record construction.
9. Errors retain record and stage attribution.
10. The Python API remains fully usable without the CLI or registry.

The extension may enrich an existing interface—for example, progress needs to
separate completed work from ordered emission—but it must not weaken these
invariants.

### 1.3 What this extension adds

The larger picture adds five capabilities above the original execution core:

1. **A transform expression language.** Users compose a per-record program
   with jq-like selectors and named operations.
2. **A typed tool model.** Built-in and custom transformations declare input,
   output, configuration, scope, and cardinality contracts.
3. **A compilation layer.** Expressions become validated, pickleable
   invocation descriptors and then normal worker-local stages.
4. **A provider ecosystem.** Users can validate, install, inspect, namespace,
   update, and remove transformation providers.
5. **A product CLI.** The existing execution settings become a reusable shell
   interface rather than boilerplate repeated across one-off scripts.

The result is not a second processing engine. It is a way to manufacture
programs for the existing processing engine.

### 1.4 Responsibilities that remain unchanged

This plan does not move the following responsibilities into tools or the DSL:

- Sources still decide how records are read and physically sharded.
- Sinks still decide how results are persisted.
- Executors still own concurrency and bounded submission.
- `Pipeline` still owns stage sequencing and error attribution.
- Runtime context still supplies rank, world size, and worker identity.
- Ordering remains a coordinator concern after worker completion.
- Error policy remains a run-level choice, not something each tool invents.

Likewise, a provider must not create its own process pool for ordinary mapping,
write directly to the primary output sink, consume the input iterator, or
control global ordering. Its responsibility is transformation of the value or
record described by its contract.

### 1.5 Two equivalent authoring paths

After this extension, users have two paths into the same runtime:

```text
Python authoring
  Pipeline([Stage(...), Stage(...)])
                   |
                   +--------------------+
                                        v
CLI authoring                     existing Pipeline.run
  expression -> compiler -> Stage(...)  |
                                        v
                              existing bounded executors
```

The Python path is the unrestricted escape hatch. The CLI path is the
configurable, inspectable, reusable subset. Executor behavior and output
semantics must be equivalent once both paths have produced the same stages.

## 2. Design principles

### 2.1 The CLI is a compiler frontend over the existing runtime

The expression language describes a per-record program. It is parsed,
validated, and compiled into ordinary `Stage` objects. The existing `Pipeline`
and executor remain responsible for execution.

The following expression:

```text
fromjson(.tools) | normalize(.metadata.annotation) | tojson(.tools[])
```

must not create three queues or three process pools. It compiles into one fused
worker-local program:

```text
raw input line
  -> parse outer JSON
  -> transform selected values
  -> serialize outer JSON
  -> one result crosses back to the coordinator
```

A normal record crosses the worker boundary once in each direction, regardless
of the number of expression operations.

### 2.2 Keep selection separate from transformation

The runtime owns:

- parsing selectors such as `.metadata.annotation` and
  `.tools[].function.parameters`;
- finding all matching locations;
- applying missing-path and type-mismatch policy;
- replacing selected values with results;
- attributing failures to the correct path and invocation.

A value tool normally owns only:

```python
new_value = function(old_value, **configuration)
```

This separation makes wildcard behavior, validation, error reporting, and
assignment consistent across built-in and third-party tools. A tool author
does not need to implement path traversal.

### 2.3 Built-ins and installed tools use one contract

`fromjson` and `tojson` are built-in tools, but they must use the same public
metadata and invocation model as installed tools. The compiler should not have
special execution paths for their names. Built-ins may be registered by a
built-in provider, while installed files are registered by local providers.

This keeps inspection, configuration validation, documentation, and error
attribution uniform.

### 2.4 Tool configuration is declarative and validated before data is read

Keyword arguments in the expression are configuration, not arbitrary Python.
The expression parser accepts literals such as strings, numbers, booleans,
null, arrays, and objects. It never uses `eval`.

The tool's callable signature and declaration define required arguments,
defaults, and allowed types. Unknown options and invalid configuration must be
reported during expression compilation, before the input source is opened.

### 2.5 Tool input and output contracts are explicit

A tool declares whether it operates on each selected value or the complete
record, which JSON-compatible input types it accepts, which output types it
returns, and its cardinality.

The first release should execute only one-to-one tools. The metadata model may
reserve one-to-zero and one-to-many cardinalities, but those modes should not be
accepted until their ordering, error, and statistics semantics are implemented.

### 2.6 Multiprocessing transports descriptors, not dynamic callables

Dynamically imported functions from arbitrary paths are fragile under the
`spawn` multiprocessing start method. The compiled pipeline should carry
pickleable tool invocation descriptors, not raw dynamic function objects.

Each worker resolves provider and tool descriptors once during setup, validates
the provider digest, and retains the resolved callable for all records handled
by that worker.

### 2.7 Streaming and bounded dispatch are invariants

The new frontend must preserve `max_in_flight` behavior. Expression parsing,
tool resolution, and validation happen before execution, but data must never be
materialized merely to validate a tool or calculate progress totals.

Input parsing and final row serialization should normally happen inside the
worker by using `JsonlSource(raw=True)` and `JsonlSink(raw=True)`.

### 2.8 Completion progress and ordered emission are distinct

`--ordered` controls output ordering; it must not make the UI appear frozen
when later records have completed behind an early slow record.

Progress reporting should distinguish at least:

- `processed`: worker results received by the coordinator;
- `written`: records emitted to the primary or error sink;
- `failed`: records that failed;
- `dropped`: records intentionally omitted;
- `buffered`: completed results waiting for an ordering gap;
- `in_flight`: submitted tasks that have not completed.

The primary progress counter should advance on completion. Ordered writing may
lag behind it.

### 2.9 Validation improves reliability but is not a security sandbox

An installed Python file is executable code. Static checks, isolated imports,
signature validation, and worker smoke tests can catch mistakes; they cannot
make hostile Python safe. Installation must clearly state that installed code
runs with the permissions of a datapipe worker.

OS-level isolation is a separate future feature.

### 2.10 Preserve the Python API as the escape hatch

The DSL is intended for reusable map-like transformations. Arbitrary business
logic, specialized IO, complex state, and operations outside the language can
continue to use Python-defined `Pipeline` objects. The DSL should compile to
the same underlying abstractions rather than replace them.

## 3. User-facing model

### 3.1 Canonical transform command

Support the compact form requested by the motivating use case:

```bash
datapipe [OPTIONS] EXPRESSION INPUT OUTPUT
```

For discoverability, also support an explicit equivalent:

```bash
datapipe transform [OPTIONS] EXPRESSION INPUT OUTPUT
```

Existing or future execution of Python pipeline definitions remains under:

```bash
datapipe run module_or_file:pipeline [OPTIONS]
```

The CLI parser should treat a recognized subcommand as a subcommand and any
other first positional argument as the transform expression. Tests must cover
ambiguous filenames and expressions so shorthand behavior remains predictable.

Initial transform options should include:

```text
--ordered / --unordered
--progress / --no-progress
--workers N
--max-in-flight N
--executor process|thread|sequential
--errors raise|skip|return
--error-output PATH
--validate-tools always|sample|off
--input-format jsonl
--output-format jsonl
```

JSONL should be the initial default. Parquet can be added after column/schema
semantics for selectors and dynamically typed tool output are defined.

### 3.2 Tool management commands

Provide the requested installer entry point:

```bash
datapipe-install [--editable] [--force] PATH
```

Also expose the same functionality under the main command for a coherent CLI:

```bash
datapipe tools install [--editable] [--force] PATH
datapipe tools validate PATH
datapipe tools list
datapipe tools inspect NAME
datapipe tools remove PROVIDER_OR_NAME
```

`datapipe-install` should be a thin entry-point alias over the same installer
implementation, not a second implementation.

### 3.3 Inspection

Tool inspection should produce both human-readable and machine-readable forms:

```bash
datapipe tools inspect normalize_text
datapipe tools inspect normalize_text --json
```

Example human-readable output:

```text
normalize_text(path, lowercase=false, strip=true, max_length=null)

Provider:    local:my-tools
Target:      selected value
Input:       string
Output:      string
Cardinality: one-to-one
Deterministic: yes
```

Add an expression inspection mode that performs compilation without opening
data:

```bash
datapipe inspect-expression \
  'fromjson(.tools) | tojson(.tools[].function.parameters)'
```

It should show parsed operations, resolved providers, normalized arguments,
input/output contracts, and the generated pipeline stages.

## 4. Expression language

### 4.1 Scope

Implement a deliberately small jq-like language. Do not claim complete jq
compatibility. Version the language so future syntax changes are manageable.

Initial grammar, expressed informally:

```text
expression  := invocation ("|" invocation)*
invocation  := qualified_name "(" selector ("," argument)* ")"
argument    := identifier "=" literal
selector    := "." selector_part*
selector_part :=
    "." identifier
  | "[" quoted_string "]"
  | "[" integer "]"
  | "[]"
literal := string | number | boolean | null | array | object
qualified_name := identifier | identifier "." identifier
```

The parser may accept `True`, `False`, and `None` as compatibility aliases,
but inspection should normalize them to jq/JSON-style `true`, `false`, and
`null`.

Do not support arbitrary arithmetic, variable references, function calls
inside arguments, shell expansion, Python expressions, or `eval`.

### 4.2 Selector semantics

The initial selector implementation supports:

```text
.                               root record
.tools                          object field
.metadata.annotation            nested object fields
.items[0]                       array index
.tools[]                        every array element
.tools[].function.parameters    wildcard followed by fields
.["key.with.dots"]             quoted object key
```

Selectors are evaluated against the record produced by all preceding
invocations. Therefore, this is valid:

```text
fromjson(.tools) | tojson(.tools[].function.parameters)
```

because the first invocation converts `.tools` from a string to an array
before the second selector is evaluated.

Path resolution should return references containing the parent container, key
or index, selected value, and a rendered concrete path for diagnostics. The
root selector uses a root reference abstraction so it can be replaced too.

Wildcard matches are transformed in stable array order. Every match is
replaced independently. A tool invocation remains one record-level operation
for failure and retry purposes: if any match fails, the entire record fails.

Initial default policies:

- a missing object field or out-of-range explicit index is an error;
- `[]` over an empty array produces zero matches and succeeds as a no-op;
- `[]` applied to a non-array is a type error;
- a selector that otherwise resolves to zero matches is a no-op only when the
  zero matches are caused by an empty wildcard.

Later, optional selection syntax or an invocation option such as
`missing="skip"` can relax missing-path behavior. Strict defaults catch data
shape mistakes early.

### 4.3 Parsing and source positions

Build a tokenizer and recursive-descent parser in the repository rather than
adding a large parser dependency for this small grammar. Every AST node must
retain start and end offsets. Diagnostics should render the expression and a
caret range:

```text
unknown argument `recusive` for fromjson
  fromjson(.metadata.annotation, recusive=true)
                                      ^^^^^^^^^
```

Core AST types:

```python
Expression(invocations: tuple[Invocation, ...])
Invocation(name, selector, arguments, span)
Selector(parts, span)
Field(name, span)
Index(index, span)
Each(span)
Literal(value, span)
```

The AST is syntax-only. Provider lookup, argument binding, and type contracts
belong to a separate semantic compilation pass.

## 5. Tool contract

### 5.1 Decorator API

Expose a small public authoring API:

```python
from datapipe.tools import JsonType, tool


@tool(
    name="normalize_text",
    api_version=1,
    target="value",
    input=JsonType.STRING,
    output=JsonType.STRING,
    cardinality="one_to_one",
    deterministic=True,
    description="Normalize whitespace in a string.",
)
def normalize_text(
    value: str,
    *,
    lowercase: bool = False,
    strip: bool = True,
) -> str:
    if strip:
        value = value.strip()
    value = " ".join(value.split())
    return value.lower() if lowercase else value
```

The decorator attaches immutable metadata to the function. Importing a
provider must not modify the persistent registry. The loader discovers
decorated functions after importing the module in its validation subprocess.

### 5.2 Target scope

Initial values:

- `target="value"`: invoke once for every value selected by the path;
- `target="record"`: invoke once on the complete row and require selector `.`.

Requiring `.` for record tools prevents misleading expressions in which a
record-level tool appears to target a nested field.

The callable signatures are:

```python
def value_tool(value, *, configuration...) -> value:
    ...

def record_tool(record, *, configuration...) -> record:
    ...
```

Worker context should not be exposed in the first public tool API. Add it later
through an explicit declaration such as `with_context=True`; do not infer it
from positional parameters.

### 5.3 JSON type system

Use JSON-oriented runtime types rather than general Python classes:

```text
null
boolean
integer
number
string
array
object
scalar
container       # array or object
any
```

Python `bool` must be checked before `int`, because `bool` is a subclass of
`int`. `number` accepts integer and non-boolean float values. Non-finite floats
should be rejected by strict JSON serialization unless explicitly enabled.

Represent contracts using immutable, pickleable `TypeSpec` values. Leave room
for later composite types:

```python
ArrayOf(JsonType.STRING)
OneOf(JsonType.STRING, JsonType.OBJECT)
ObjectSchema({...})
```

The first release need only implement the base JSON types and `OneOf` if a
built-in requires it.

### 5.4 Configuration schema

The first callable parameter is the selected value or record. All remaining
configuration parameters must be keyword-only. Allow configuration annotations
composed from:

- `str`, `int`, `float`, and `bool`;
- `None` and optional unions;
- lists and dictionaries of supported values;
- enums;
- JSON-compatible literal values.

Reject variadic positional arguments and `**kwargs` in version 1 because they
prevent complete preflight validation. Defaults must be JSON-serializable.

The installer derives a normalized `ParameterSpec` for each keyword-only
parameter. The expression compiler binds literals to it and creates a complete
configuration dictionary including defaults. Workers receive already-bound
configuration and do not repeat argument parsing.

Python annotations are useful documentation, but the explicit tool input and
output metadata is authoritative. The installer should reject clear conflicts,
such as `input=JsonType.STRING` with a first-parameter annotation of `dict`.

### 5.5 Cardinality

Define the metadata enum now:

```text
one_to_one
one_to_zero
one_to_many
```

Only `one_to_one` is executable in the first release. This guarantees that a
value tool replaces each match and a record tool returns one record.

Future cardinality behavior must be implemented as explicit pipeline semantics:

- `one_to_zero` maps a record tool's result to `DROP`;
- `one_to_many` requires a flat-map capable executor/result protocol and clear
  sequence-number semantics;
- nested value tools should remain one-to-one even after record flat-map exists.

Do not treat a returned list as multiple records implicitly.

### 5.6 Lifecycle and expensive state

Simple functions are sufficient for the first release. A subsequent contract
version may support tool factories or classes with `setup`, `transform`, and
`teardown`, allowing models and clients to initialize once per worker.

Reserve lifecycle metadata now, but do not invent implicit global state. A
function tool that needs expensive resources in version 1 can use a documented
process-local lazy cache, with the limitation that teardown is unavailable.

## 6. Built-in tools

### 6.1 `fromjson`

Declaration concept:

```python
@tool(
    name="fromjson",
    target="value",
    input=OneOf(JsonType.STRING, JsonType.ARRAY, JsonType.OBJECT),
    output=JsonType.ANY,
    cardinality="one_to_one",
    deterministic=True,
)
def fromjson(
    value,
    *,
    recursive: bool = False,
    containers_only: bool = True,
):
    ...
```

Default behavior:

- without `recursive`, the selected value must be a string and is decoded once;
- with `recursive=true`, a selected string is decoded first, then arrays and
  objects are traversed recursively;
- during recursive traversal, strings are decoded only when decoding produces
  an array or object if `containers_only=true`;
- strings that are not valid JSON remain unchanged during recursive traversal;
- failure to decode the selected root string is an error;
- `containers_only=false` permits decoded scalar JSON values such as `true`,
  `null`, and `123` to replace strings.

The distinction between root decoding and nested best-effort traversal prevents
a misspelled path or entirely malformed selected value from silently succeeding
while preserving the motivating behavior for partially serialized subtrees.

### 6.2 `tojson`

Declaration concept:

```python
@tool(
    name="tojson",
    target="value",
    input=JsonType.ANY,
    output=JsonType.STRING,
    cardinality="one_to_one",
    deterministic=True,
)
def tojson(
    value,
    *,
    ensure_ascii: bool = False,
    compact: bool = True,
    sort_keys: bool = False,
):
    ...
```

`tojson` serializes every selected match independently. If a selected value is
already a string, it is encoded as a JSON string literal; it is not treated as
already serialized. This explicit behavior avoids unreliable string heuristics.

When `compact=true`, use `separators=(",", ":")`. Final outer-row JSON
serialization should have independently configurable defaults but initially
match `ensure_ascii=false` and compact output for predictable JSONL.

### 6.3 Outer row operations

The transform command implicitly adds outer parsing and serialization:

```text
JsonLoadStage
CompiledToolProgramStage
JsonDumpStage
```

Users should not need to write `fromjson(.)` merely to parse each JSONL row.
Likewise, nested `tojson` must occur before the implicit final dump.

Use a raw JSONL source and sink so the implicit load and dump execute within
the worker:

```python
source = JsonlSource(input_path, raw=True)
sink = JsonlSink(output_path, raw=True)
```

## 7. Provider and registry model

### 7.1 Terminology

- **Tool**: a named transformation callable plus its contract.
- **Provider**: an installed source containing one or more tools.
- **Invocation**: a tool, selector, and bound configuration in an expression.
- **Registry**: persistent metadata describing installed providers and tools.
- **Descriptor**: immutable, pickleable identity used to resolve a provider or
  invocation in workers.

### 7.2 Provider identity and naming

Each provider gets a normalized ID such as `local:my-tools`. Every tool has a
fully qualified name:

```text
local:my-tools/normalize_text
```

The DSL may use an unqualified name when it resolves uniquely. Built-in names
such as `fromjson` and `tojson` are reserved and cannot be shadowed
unqualified. Provide a compact namespace syntax for conflicts, for example:

```text
my_tools.normalize_text(.text)
```

The exact display namespace must be stored separately from the internal
provider ID so paths and package names do not leak into expressions.

Installation fails on ambiguous unqualified names unless the new provider is
installed under an explicit alias. `--force` may replace the same provider but
must not silently replace a different provider or a built-in.

### 7.3 Registry location and format

Use a platform-appropriate user data directory, resolved by a small internal
helper. Avoid making the current working directory part of global registry
identity.

Conceptual layout:

```text
DATAPIPE_USER_DATA/
  registry.json
  providers/
    <provider-id>/
      provider.json
      source.py              # copied installations only
```

Registry writes must be atomic: write a complete temporary file in the same
directory, flush it, and replace the old registry. Use a lock to prevent two
installer processes from losing updates.

Persist normalized metadata rather than pickled Python objects. Include:

```json
{
  "schema_version": 1,
  "providers": {
    "local:my-tools": {
      "alias": "my_tools",
      "mode": "editable",
      "source": "/absolute/path/my_tools.py",
      "digest": "sha256:...",
      "installed_at": "...",
      "datapipe_api": 1,
      "tools": {}
    }
  }
}
```

Do not store secrets, environment snapshots, imported module objects, or
arbitrary pickle data in the registry.

### 7.4 Copied installation

For:

```bash
datapipe-install ./my_tools.py
```

validate the source and then copy the exact validated bytes into the provider
directory. Compute the digest from those bytes. Runs resolve the copied
snapshot, so later edits to the original file do not affect it.

### 7.5 Editable installation

For:

```bash
datapipe-install --editable ./my_tools.py
```

store a canonical absolute path rather than copying the source. On every
expression compilation:

1. read and hash the current file;
2. if its hash differs from the registry, re-run full provider validation;
3. update registry metadata only if validation succeeds;
4. include the expected digest in each compiled provider descriptor.

Every worker verifies the digest before importing. If the file changes between
compilation and worker setup, fail the run rather than allowing workers to load
different code versions. The next run can validate the new content.

Editable mode should be documented as a development convenience, not a
reproducible deployment mechanism.

### 7.6 Packaged providers

The initial installer accepts one `.py` file. Design provider descriptors so a
later version can support Python packages and standard entry points without
changing the DSL or tool contract:

```bash
datapipe tools install my-datapipe-tools
datapipe tools install git+https://example/repository.git
```

Do not implement remote installation in the first release.

## 8. Installation and validation pipeline

### 8.1 Static validation

Before executing provider code:

1. require a regular `.py` file and resolve its canonical path;
2. read bytes once and compute SHA-256;
3. enforce a configurable maximum source size;
4. decode as UTF-8 and parse with `ast.parse`;
5. locate potential `@tool` declarations for useful early diagnostics;
6. reject syntax errors and unsupported decorator forms;
7. reject duplicate declared tool names in the same provider.

Static validation is for diagnostics and policy enforcement, not a security
guarantee. Imports and top-level statements may still execute later.

### 8.2 Isolated provider inspection

Import the provider in a fresh subprocess with a timeout. The subprocess emits
a strictly JSON metadata document over stdout or a dedicated pipe. Provider
stdout/stderr should be captured separately so accidental prints cannot corrupt
the protocol.

Validate:

- each discovered object is decorated using a supported tool API version;
- tool metadata is complete and internally consistent;
- names and descriptions meet size and character rules;
- callable signatures match target scope;
- configuration parameters are supported and defaults serialize to JSON;
- input/output type declarations are valid;
- only supported cardinality is requested;
- explicit Python annotations do not conflict with JSON type declarations;
- metadata produced by import agrees with statically visible declarations where
  comparison is possible.

The installer must time out and terminate a provider that hangs during import.

### 8.3 Functional smoke tests

Allow optional examples in tool metadata:

```python
@tool(
    ...,
    examples=[
        ToolExample(
            input="  Hello  world ",
            arguments={"lowercase": True},
            output="hello world",
        )
    ],
)
```

Run every declared example and validate its output contract. Examples improve
installation confidence and generated documentation but should not initially
be mandatory.

At minimum, perform a metadata/load smoke test under the multiprocessing
`spawn` context. This catches providers that import in the installer process
but cannot be resolved in actual workers.

### 8.4 Installation confirmation

Unless a non-interactive confirmation flag is explicitly supplied, print:

```text
Provider: local:my-tools
Source:   /absolute/path/my_tools.py
Mode:     editable
Tools:    normalize_text, redact

This provider contains executable Python and will run inside datapipe workers
with your user permissions. Install? [y/N]
```

For CI, support `--yes`. A rejected or failed installation must leave the
previous registry and provider snapshot unchanged.

## 9. Expression compilation

Compilation is a sequence of explicit passes:

1. Tokenize and parse the expression into a source-positioned AST.
2. Resolve each unqualified or qualified tool name through the registry.
3. Refresh and revalidate changed editable providers.
4. Validate selector compatibility with tool target scope.
5. Bind configuration literals to `ParameterSpec` definitions.
6. Apply defaults and produce normalized, immutable argument values.
7. Perform any statically provable input/output compatibility checks between
   operations.
8. Produce `ToolInvocation` descriptors.
9. Group invocations into a `CompiledToolProgramStage`.
10. Wrap that stage with implicit outer JSON load and dump stages.

Conceptual descriptors:

```python
@dataclass(frozen=True)
class ProviderDescriptor:
    provider_id: str
    mode: str
    source_path: str
    sha256: str
    api_version: int


@dataclass(frozen=True)
class ToolDescriptor:
    provider: ProviderDescriptor
    tool_name: str
    contract: ToolContract


@dataclass(frozen=True)
class ToolInvocation:
    tool: ToolDescriptor
    selector: CompiledSelector
    arguments: tuple[tuple[str, JsonValue], ...]
    expression_index: int
```

Descriptors must contain only primitive values, tuples, and frozen dataclasses
that work with standard `pickle` under `spawn`.

Static type propagation is necessarily conservative because input JSONL has no
schema. It can still detect contradictions such as a tool declared to output a
string followed immediately at the same exact path by a tool accepting only an
object. Runtime validation remains authoritative.

## 10. Worker execution

### 10.1 Stage setup

`CompiledToolProgramStage.setup(ctx)` should:

1. deduplicate provider descriptors used by the expression;
2. verify each source digest;
3. load each provider under a stable internal module name derived from provider
   identity and digest;
4. discover decorated tools;
5. confirm loaded metadata matches compiled descriptors;
6. bind each invocation to its callable and normalized configuration;
7. retain the resolved program in worker-local state.

The same provider must be imported only once per worker even when several tools
or invocations use it.

### 10.2 Per-record processing

For every invocation:

1. resolve selector references against the current record;
2. validate each selected input against the tool input contract when runtime
   validation is enabled;
3. call the tool with its bound configuration;
4. validate the returned value against the output contract;
5. replace the selected location;
6. continue to the next invocation using the updated record.

For wildcard selectors, collect concrete references before replacing values so
mutations do not alter traversal midway. Apply replacements in deterministic
path order.

The implementation should not deep-copy the full row for every operation.
Records are already isolated per task. Tools may return a new value or mutate
and return the selected object; the returned value is always assigned back.

### 10.3 Runtime validation modes

Support:

- `always`: validate every selected input and output;
- `sample`: validate the first configurable number of records per worker, then
  trust the provider;
- `off`: skip runtime contract checks.

Default to `always` for the initial release. Correctness is more important than
optimizing validation before benchmarks show material overhead. Built-ins may
use efficient direct checks while preserving the same errors.

### 10.4 Teardown

Release provider lifecycle state in reverse setup order when lifecycle-capable
tools are introduced. Function providers have no teardown in version 1.
Existing process teardown limitations remain best-effort and must be documented.

## 11. Errors and diagnostics

Introduce errors with structured attributes rather than constructing all
context only in strings:

```text
ExpressionSyntaxError
ToolResolutionError
ToolConfigurationError
ToolInstallationError
ProviderValidationError
ProviderChangedError
SelectorResolutionError
ToolInputError
ToolOutputError
ToolExecutionError
```

A runtime tool error should include:

- record sequence number;
- invocation index;
- provider and tool identity;
- source expression span;
- configured selector;
- concrete matched path and wildcard match ordinal;
- expected and actual JSON types;
- original exception type, message, and traceback where applicable.

Example:

```text
record 1842 failed in fromjson at .metadata.annotation
provider: builtin:json
invocation: 2
expected input: string
actual input: null
```

Integrate these errors with existing `raise`, `skip`, and `return` policies.
Structured error output should preserve the additional tool fields.

Errors during expression compilation, provider validation, source opening, or
sink closing are run-level failures and must not be converted into per-record
skip behavior.

## 12. Progress and ordered output changes

The current sink-boundary progress behavior should be extended before the new
CLI promises useful `--ordered --progress` operation.

Add a structured progress snapshot or event interface rather than continually
expanding keyword arguments:

```python
@dataclass(frozen=True)
class ProgressSnapshot:
    submitted: int
    processed: int
    written: int
    failed: int
    dropped: int
    buffered: int
    in_flight: int
```

Update progress when a future completes, before ordered buffering. Update the
written and buffered fields when the reorder buffer drains. A slow early record
then produces visible progress such as:

```text
processed 82,193  written 82,011  buffered 182  in-flight 128  failed 3
```

Preserve bounded task submission independently of ordered result buffering.
Document that ordered buffering can grow behind a straggler even though the
number of submitted futures remains bounded.

Before relying on ordered mode, fix gap handling for skipped errors so a failed
sequence advances the ordering cursor rather than permanently retaining every
later result. On abort or interrupt, emit only the intended contiguous prefix;
do not flush records across unresolved ordering gaps.

## 13. Integration with the existing architecture

### 13.1 Component ownership

Implementation should follow this ownership map. It prevents the extension
from duplicating mechanisms that already exist in the foundation.

| Concern | Existing component | Extension work |
| --- | --- | --- |
| Record IO | `JsonlSource`, `JsonlSink`, Parquet adapters | Select raw JSONL mode and add CLI format resolution; do not create DSL-specific IO |
| Per-record composition | `Stage`, `CompiledPipeline` | Add one tool-program stage produced by the compiler |
| Parallel execution | `SequentialExecutor`, `ThreadExecutor`, `ProcessExecutor` | Resolve CLI flags to an existing executor; do not add a DSL executor |
| Backpressure | bounded scheduler and `max_in_flight` | Preserve and expose the existing setting |
| Ordering | pipeline gather/reorder logic | Repair gap semantics and expose ordered/unordered flags |
| Progress | progress reporter abstraction | Enrich events to distinguish completion and emission |
| Errors | stage attribution and run error policies | Add tool/selector context while preserving policy ownership |
| Distribution | runtime context and sharding strategies | Reuse without DSL-specific distributed coordination |
| Python authoring | `Pipeline` and existing stages | Preserve unchanged as the unrestricted authoring path |
| Expression authoring | none | Add parser, selectors, semantic compiler, and inspection |
| Tool distribution | none | Add contracts, providers, validation, registry, and installer |

### 13.2 Dependency direction

Dependencies must point downward through the layers:

```text
CLI -> DSL compiler -> tool descriptors/stage -> pipeline/runtime
installer -> tool contracts/provider validation -> registry
worker tool loader -> descriptors/contracts
```

The foundational pipeline and executor packages must not import the CLI,
installer, or persistent registry. At most, the pipeline executes a stage whose
implementation belongs to the tool package. This preserves the ability to use
the runtime as a small library with no global tool installation state.

The compiled stage must be self-describing: after CLI compilation, worker
execution should depend on embedded provider descriptors and provider source,
not on repeating user-facing name resolution against a mutable registry. The
registry is consulted in the control plane before the run; it is not queried
per record.

### 13.3 Existing abstractions to extend carefully

Only a small number of foundational interfaces need amendments:

- progress reporting needs structured completion and emission state;
- structured stage errors need optional tool invocation and concrete-path
  context;
- CLI loading needs a second compilation source in addition to Python pipeline
  references;
- the stage lifecycle must support resolving provider descriptors once per
  worker.

These changes should be useful to ordinary Python pipelines too. Avoid adding
DSL-specific flags to `Pipeline.run`; translate CLI settings into existing run
configuration and implement tool behavior inside its stage.

### 13.4 Proposed package layout

Add focused modules rather than placing parsing, installation, registry, and
execution logic in the CLI entry point:

```text
datapipe/
  dsl/
    __init__.py
    ast.py
    lexer.py
    parser.py
    selector.py
    compiler.py
    errors.py
  tools/
    __init__.py             # public authoring API
    contract.py
    decorator.py
    types.py
    descriptor.py
    loader.py
    registry.py
    installer.py
    validation.py
    builtins/
      __init__.py
      json.py               # fromjson and tojson
  stages/
    tool_program.py         # or keep beside existing stage module
  cli/
    main.py
    transform.py
    tools.py
    install.py
    inspect.py
```

If `datapipe.stage` remains a single module, `CompiledToolProgramStage` may
initially live in `datapipe.tools.stage` to avoid a disruptive package rename.

Add a second console entry point:

```toml
[project.scripts]
datapipe = "datapipe.cli.main:main"
datapipe-install = "datapipe.cli.install:main"
```

The public package root may export `tool`, `JsonType`, and selected contract
helpers, but internal registry and loader classes should remain under
`datapipe.tools`.

## 14. Delivery phases

### Phase 0: Runtime reliability prerequisites

Before exposing the transform CLI as a stable feature:

1. Fix ordered-buffer advancement when records fail under `errors="skip"`.
2. Ensure abort and keyboard interrupt never flush output across an ordering
   gap.
3. Propagate material sink-close failures instead of logging and returning
   success.
4. Open and close `error_sink` through the normal resource lifecycle.
5. Verify thread workers have isolated contexts and correct setup semantics.
6. Add completion-based progress events separate from sink emission.

These are not DSL features, but the CLI would make these runtime guarantees
part of a much more visible user contract.

### Phase 1: Tool contracts and built-ins

1. Implement `JsonType`, `TypeSpec`, and runtime type matching.
2. Implement immutable `ToolContract` and `ParameterSpec` models.
3. Implement the `@tool` decorator and signature introspection.
4. Implement built-in provider registration.
5. Implement `fromjson` and `tojson` with unit tests for recursive and scalar
   behavior.
6. Implement human-readable and JSON tool inspection for built-ins.

This phase can be tested without the DSL by invoking descriptors directly.

### Phase 2: Selectors and DSL

1. Implement tokenizer, AST, parser, and source-positioned diagnostics.
2. Implement compiled selectors and concrete references.
3. Test fields, quoted keys, indexes, wildcards, root replacement, empty arrays,
   missing fields, and type mismatches.
4. Implement semantic compilation against the built-in registry.
5. Implement `CompiledToolProgramStage` with direct built-in resolution.
6. Add expression inspection.

At the end of this phase, the motivating expression works with built-ins via
the Python API.

### Phase 3: Transform CLI

1. Implement explicit `datapipe transform` command.
2. Add shorthand expression dispatch.
3. Wire JSONL raw source/sink, process/thread/sequential executors, ordering,
   progress, bounded dispatch, and error policies.
4. Add shell-level integration tests using temporary JSONL files.
5. Document quoting for Bash, zsh, and common shells.

At the end of this phase, users can perform configurable built-in rewrites
without writing Python.

### Phase 4: Local provider installation

1. Implement registry paths, schemas, atomic updates, and locking.
2. Implement static AST validation.
3. Implement isolated subprocess inspection with timeout and JSON protocol.
4. Implement copied provider installation.
5. Implement editable provider installation and digest refresh.
6. Implement descriptor-based worker loading and digest verification.
7. Add install/list/inspect/remove commands and `datapipe-install` alias.
8. Add name collision, namespace, confirmation, rollback, and concurrent
   installer tests.

### Phase 5: Hardening and documentation

1. Add structured tool error payloads and expression spans.
2. Add `always`, `sample`, and `off` runtime validation modes.
3. Benchmark selector traversal, validation, dynamic provider lookup, JSON
   parsing, and outer serialization.
4. Cache only safe immutable compilation artifacts; do not cache transformed
   records in the first release.
5. Write a tool authoring guide, CLI reference, security/trust guide, and
   migration guide from scripts such as `use_cases/process.py`.
6. Test copied-provider reproducibility and editable-provider change detection
   under multiprocessing `spawn`.

### Phase 6: Optional extensions

Evaluate separately after the initial contract is stable:

- lifecycle-aware tool classes with per-worker setup and teardown;
- record filtering and flat-map cardinalities;
- batch tools analogous to `datasets.map(..., batched=True)`;
- Parquet schema-aware static validation;
- packaged providers through Python entry points;
- remote provider installation;
- optional selectors and richer path operations;
- deterministic result caching keyed by provider digest, configuration, and
  input digest;
- OS-level provider sandboxing.

## 15. Testing strategy

### 15.1 Contract tests

Cover:

- valid and invalid decorator metadata;
- signature/default inference;
- unsupported positional, variadic, and configuration types;
- Python annotation conflicts;
- all base JSON type matches, especially boolean versus integer;
- input and output validation failures;
- deterministic metadata serialization.

### 15.2 DSL tests

Use table-driven lexer and parser tests for valid expressions and exact source
spans. Include malformed quoting, unexpected pipes, duplicate arguments,
unknown literals, nested literal structures, qualified names, and whitespace.

Selector tests should exercise mutation and concrete error paths, including
multiple wildcards where supported by the grammar.

### 15.3 Built-in tests

For `fromjson`, cover:

- object and array roots;
- malformed root JSON;
- recursive dictionaries and arrays;
- partially serialized nested containers;
- valid JSON scalar strings with both `containers_only` settings;
- ordinary non-JSON strings;
- already parsed containers under recursive mode;
- Unicode.

For `tojson`, cover all JSON types, compact output, key sorting, Unicode,
already-string values, and non-finite numeric rejection.

### 15.4 Installer tests

Use isolated temporary registry roots. Cover:

- copied versus editable behavior;
- atomic rollback after every validation failure point;
- source changes after editable installation;
- source changes between compilation and worker setup;
- duplicate providers and tool names;
- built-in collision attempts;
- import timeout and crash;
- provider stdout/stderr noise;
- invalid JSON metadata protocol;
- spawn-process loading;
- registry locking under concurrent installers;
- removal without deleting editable source files.

### 15.5 End-to-end tests

Run the exact motivating workflow against representative records:

```bash
datapipe --ordered --progress \
  'fromjson(.tools) |
   fromjson(.metadata.annotation, recursive=true) |
   tojson(.tools[].function.parameters)' \
  input.jsonl output.jsonl
```

Assert:

- nested values have the intended types and serialization boundaries;
- output contains exactly one valid JSON value per line;
- process, thread, and sequential executors produce equivalent record content;
- ordered mode preserves input order;
- unordered mode emits promptly in completion order;
- progress advances before the source is exhausted;
- ordered progress advances while completed output is buffered;
- maximum submitted work never exceeds `max_in_flight`;
- installed tools work under multiprocessing `spawn`;
- error policies produce correct output and statistics.

Include a slow first record to verify that `processed` advances while `written`
waits in ordered mode.

### 15.6 Performance tests

Measure:

- time to first output and first progress event;
- records per second for no-op, one selector, and multiple wildcard selectors;
- overhead of validation modes;
- copied versus editable provider loading time;
- worker startup with one and many providers;
- memory use with large JSONL sources;
- ordered reorder-buffer behavior under skew.

The primary regression invariant is bounded memory and prompt completion
reporting, not parity with jq's native-code throughput.

## 16. Documentation requirements

Create documentation pages for:

1. **Transform CLI quick start**: motivating JSONL examples and common flags.
2. **Expression language reference**: exact grammar and selector semantics.
3. **Built-in tools**: contracts and defaults for `fromjson` and `tojson`.
4. **Writing tools**: decorator API, configuration types, examples, and tests.
5. **Installing tools**: copied/editable behavior, namespaces, updates, removal.
6. **Trust and security**: explicit explanation that provider Python executes
   with worker permissions.
7. **Execution semantics**: fused stages, bounded dispatch, ordering, progress,
   errors, and retries.
8. **Migration guide**: convert `use_cases/process.py` into a one-line CLI
   expression and show when a custom tool is still appropriate.

Avoid describing the expression language simply as jq. Consistently call it
"jq-like" and document the supported subset.

## 17. Definition of done

The initial feature is complete when all of the following are true:

1. The motivating expression runs unchanged except for documented boolean
   normalization accepted by the parser.
2. Outer JSON parsing and serialization execute in workers for JSONL transforms.
3. `fromjson` and `tojson` are ordinary registered tools using the public tool
   contract.
4. A user can install a `.py` provider in copied or editable mode and call its
   tools from the same expression language.
5. Installer validation covers syntax, declarations, signatures, defaults,
   isolated import, metadata, and spawn-worker loading.
6. Provider descriptors, rather than dynamic callables, cross process
   boundaries.
7. Tool inputs, outputs, target scope, configuration, and cardinality are
   inspectable before a run.
8. Invalid expressions and configurations fail before the input file is read.
9. Runtime errors identify record, tool, selector, and concrete path.
10. `max_in_flight` remains a hard upper bound on submitted tasks.
11. `--ordered` preserves output order while progress still reflects completed
    work.
12. Sequential, thread, and process execution produce equivalent transformed
    records.
13. Failed installs are atomic and never corrupt an existing registry.
14. Editable source changes are revalidated and cannot produce mixed provider
    versions within one run.
15. Documentation clearly describes the executable-code trust boundary.

## 18. Final architectural summary

The complete flow should be:

```text
CLI expression
  -> safe parser
  -> source-positioned AST
  -> registry and built-in tool resolution
  -> configuration and contract validation
  -> immutable ToolInvocation descriptors
  -> one fused CompiledToolProgramStage
  -> existing bounded Pipeline executor
  -> completion events and optional ordered buffer
  -> streaming sink
```

The central abstraction remains a per-record map program. The DSL makes that
program configurable from the shell; selectors let tools operate on precise
nested values; contracts make installed functions inspectable and fail early;
provider descriptors make dynamic tools reliable under multiprocessing; and
the existing fused, bounded executor keeps the system streaming and efficient.

That separation is the core of the design:

```text
selectors choose values
tools transform values
contracts validate values and configuration
the compiler constructs a per-record program
the executor parallelizes whole records
the coordinator streams results and manages ordering
```

Each layer has one responsibility, and custom tools gain the convenience of a
CLI-exposed `.map()` function without weakening the runtime's execution model.
