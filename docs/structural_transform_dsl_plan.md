# Structural Transform DSL Extension Plan

> **Status:** implemented on `feat/structural-transform-dsl` — §14 phases S0-S7,
> the §7 catalogue, and the §16 definition of done are all delivered and tested.
> The one deliberate deferral is the §13.5 legacy-pipe decision recorded below.
>
> **Runtime foundation:** [`parallel_record_pipeline_architecture.md`](parallel_record_pipeline_architecture.md)
>
> **CLI and tool foundation:** [`configurable_transform_cli_plan.md`](configurable_transform_cli_plan.md)

## 1. Purpose

The existing transform DSL is centered on in-place value operations:

```text
fromjson(.tools); tojson(.tools[].function.parameters)
```

Each invocation selects one or more values, transforms them, and writes the
results back to the same locations. This covers normalization of partially
serialized JSON well, but it does not concisely express record restructuring:

- moving arbitrary root fields into a metadata object;
- keeping a small set of identity/data fields at the root;
- serializing the resulting metadata object;
- decoding metadata and moving selected values back to the root;
- copying versus moving values between exact paths;
- applying several value-level transformations and then a whole-record tool.

This plan adds a small structural language without changing the runtime model.
The result remains one fused per-record program executed inside one worker.

The intended headline workflow is:

```text
.metadata << .(^instance_id|messages|tools) | tojson;
finalize(.)
```

Given:

```json
{
  "instance_id": "abc",
  "messages": [],
  "tools": [],
  "annotation_key": "good",
  "temperature": 0.7,
  "score": 0.9
}
```

it produces, before `finalize(.)`:

```json
{
  "instance_id": "abc",
  "messages": [],
  "tools": [],
  "metadata": "{\"annotation_key\":\"good\",\"temperature\":0.7,\"score\":0.9}"
}
```

## 2. Architectural relationship

This is an authoring-language extension only:

```text
structural DSL source
  -> parser and semantic compiler
  -> immutable compiled operations
  -> one CompiledToolProgramStage
  -> existing Pipeline and bounded Executor
```

It must not introduce:

- an executor per statement;
- a future per operation;
- a queue between `;` statements;
- a queue between `|` operations;
- an intermediate file or materialized dataset;
- another pass over the source;
- a second dispatch/gather boundary for one record.

For every normal record:

```text
raw line
  -> worker once
       -> outer JSON load
       -> statement 1
       -> statement 2
       -> ...
       -> outer JSON dump
  -> coordinator once
  -> sink
```

The semicolon is a language-level sequencing symbol. It is not a runtime or
stream boundary.

## 3. Design goals

1. Keep the symbol system small enough to remember without documentation.
2. Distinguish whole-record sequencing from focused value transformation.
3. Make common metadata restructuring concise.
4. Preserve explicit behavior for copy, move, collision, and deletion.
5. Retain `nest(...)` as a readable configurable alternative to symbolic syntax.
6. Preserve existing transform expressions during migration.
7. Compile all forms into the same worker-local operation model.
8. Fail before reading data whenever a structural error is statically knowable.
9. Attribute runtime failures to the statement, operation, selector, and
   concrete path.

## 4. Minimal symbol system

The language should use only the following structural symbols:

| Syntax | Meaning |
| --- | --- |
| `;` | Finish one record mutation statement and begin the next |
| `|` | Continue transforming the current target inside a statement |
| `|` inside `.()` | Union exact field names in a field-set selector |
| `=` | Copy/assign an expression to an exact destination |
| `<-` | Move an expression from an exact source to an exact destination |
| `<<` | Move one or more fields into an object, deriving destination keys |
| `^` inside `.()` | Complement: select every field except the named fields |

Named functions remain appropriate when behavior needs configuration:

```text
nest(., key="metadata", exclude=[...], jsonify=true)
move(.a, to=".b", collision="replace")
delete(.temporary)
```

Symbols cover the safe, obvious defaults. Named tools cover policy-rich cases.

## 5. Core semantic model

### 5.1 The evolving root record

Every statement operates on one evolving root record. A successful mutation is
immediately visible to later statements for that same record.

```text
fromjson(.metadata);
normalize_record(.)
```

`normalize_record(.)` receives the record after metadata has been decoded.

### 5.2 Current target

Within a statement, an operation may leave a current target: one reference or a
stable ordered collection of references into the root record.

A bare tool following `|` applies to that current target:

```text
.metadata << .annotation_key, .score | tojson
```

The `<<` operation leaves `.metadata` as the target, so bare `tojson` means:

```text
.metadata = tojson(.metadata)
```

The root record remains the final output. Focus never replaces the record sent
to the sink unless an explicit root operation replaces `.`.

### 5.3 Semicolon behavior

`;` commits the current mutation and resets implicit focus to the root context.
The following statement must establish its own target or explicitly operate on
`.`.

```text
tojson(.tools);
tojson(.metadata);
finalize(.)
```

This avoids ambiguity about whether `finalize` receives `.metadata` or the whole
record. It receives the complete updated record.

### 5.4 Existing in-place invocation

The existing form remains valid:

```text
fromjson(.metadata)
```

Its formal meaning is:

```text
.metadata = fromjson(.metadata)
```

For wildcard selectors, the same operation is applied independently to every
selected reference and assigned back to its original location.

```text
tojson(.tools[].function.parameters)
```

means conceptually:

```text
for each match p in .tools[].function.parameters:
    p = tojson(p)
```

### 5.5 Focused tool chaining

Inside a statement, `|` feeds the selected value through successive tools while
retaining its reference into the root record:

```text
.metadata | fromjson | normalize_metadata | tojson
```

This is equivalent to:

```text
tojson(normalize_metadata(fromjson(.metadata)))
```

with the final value written to `.metadata`.

Initial implementation may support focused bare tools only after an operation
that produces an unambiguous target. A bare tool at the beginning of a statement
must either be a declared record-level tool or be rejected with a diagnostic
requiring an explicit selector.

## 6. Structural operations

### 6.1 Copy assignment: `=`

```text
.temperature = .metadata.temperature
```

copies the value. The source remains present.

The right side may contain a transform:

```text
.temperature = fromjson(.metadata.temperature)
```

This decodes the source value and assigns the result to root `.temperature`,
without deleting `.metadata.temperature`.

Ordinary assignment must never imply deletion. This matches jq, Python, and
common user expectations.

The right side may also be a literal (`value_expression := path | literal |
invocation`, §9), which assigns a constant:

```text
.status = "processed"
.retries = 0
.tags = ["a", "b"]
```

Every JSON literal form is accepted, including nested arrays and objects, and
a literal may be followed by trailing pipes (`.a = 5 | tojson` writes `"5"`),
which operate on the constant at the published focus like any other value. A
container literal is copied per record, so no two records — and no record and
the compiled program — ever share one object.

The destination may be the root: `. = {"x": 1}` replaces the whole record, and
the replacement is what later statements see and what reaches the sink (§5.2's
"explicit root operation replaces `.`"). This is the one assignment form that
can target the root, because the reason `. = .meta` is rejected is overlap —
the root is an ancestor of every path — and a constant has no path to overlap
with.

### 6.2 Exact move assignment: `<-`

```text
.temperature <- .metadata.temperature
```

assigns the value to `.temperature` and removes `.metadata.temperature` only
after destination validation and assignment succeed.

A transform may be applied during a move:

```text
.temperature <- fromjson(.metadata.temperature)
```

The source expression must have one statically identifiable primary source
reference. Arbitrary computed expressions without a source path cannot be
destructively moved; use `=` instead.

For the same reason a literal is not a `movable_expression` (§9): a constant
names no location, so there is nothing for the move to remove. `.a <- 5` is
rejected at compile time and the diagnostic directs the user to `=`.

### 6.3 Move into object: `<<`

```text
.metadata << .temperature
```

is shorthand for:

```text
.metadata.temperature <- .temperature
```

The destination key is derived from the source path's final object field.

Multiple explicit sources can be grouped:

```text
.metadata << .annotation_key, .temperature, .score
```

Equivalent:

```text
.metadata.annotation_key <- .annotation_key;
.metadata.temperature <- .temperature;
.metadata.score <- .score
```

The grouped operation produces one target, `.metadata`, allowing:

```text
.metadata << .annotation_key, .temperature, .score | tojson
```

### 6.4 Positive field-set selector

```text
.(annotation_key|temperature|score)
```

selects those exact fields from the current object in source object order.

It is designed primarily for grouped structural moves:

```text
.metadata << .(annotation_key|temperature|score) | tojson
```

Nested field sets are allowed:

```text
.archive << .metadata.(temperature|score)
```

Equivalent:

```text
.archive.temperature <- .metadata.temperature;
.archive.score <- .metadata.score
```

### 6.5 Complement field-set selector

```text
.(^instance_id|messages|tools)
```

selects every field in the current object except the named fields.

The primary blanket-nesting workflow becomes:

```text
.metadata << .(^instance_id|messages|tools) | tojson
```

Complement names are exact keys, not regular expressions. `^` complements the
entire parenthesized set, not only the first name.

Nested complements are allowed:

```text
.archive << .metadata.(^annotation|source)
```

### 6.6 Root as a destination

The root object may receive fields moved out of a nested object:

```text
fromjson(.metadata);
. << .metadata.(temperature|score);
tojson(.metadata)
```

This:

1. decodes metadata;
2. moves `temperature` and `score` to the root;
3. removes those fields from metadata;
4. serializes the remaining metadata.

### 6.7 `nest(...)` convenience tool

The symbolic form is best for a concise explicit structural statement. The
named `nest` tool remains useful when options or programmatic configuration are
preferred:

```text
nest(
  .,
  key="metadata",
  exclude=["instance_id", "messages", "tools"],
  jsonify=true
)
```

This is semantically equivalent to:

```text
.metadata << .(^instance_id|messages|tools) | tojson
```

Suggested contract:

```python
@tool(
    name="nest",
    target="record",
    input=JsonType.OBJECT,
    output=JsonType.OBJECT,
)
def nest(
    record: dict,
    *,
    key: str = "metadata",
    include: list = [],
    exclude: list = [],
    jsonify: bool = False,
    collision: str = "error",
    missing: str = "error",
) -> dict:
    ...
```

`include` and `exclude` are mutually exclusive. `exclude` supports the blanket
case. `jsonify=true` applies the built-in `tojson` behavior to the destination
after nesting.

### 6.8 `unnest(...)` convenience tool

For the inverse operation:

```text
unnest(
  .,
  key="metadata",
  include=["temperature", "score"],
  parse=true,
  jsonify=true
)
```

Given:

```json
{
  "instance_id": "abc",
  "metadata": "{\"temperature\":0.7,\"score\":0.9,\"annotation\":\"good\"}"
}
```

it produces:

```json
{
  "instance_id": "abc",
  "metadata": "{\"annotation\":\"good\"}",
  "temperature": 0.7,
  "score": 0.9
}
```

Extracted keys are appended after the retained source object, because the
desugaring below moves them out of `.metadata` and into the root in source
object order. §15.4 makes byte-identity with that desugaring normative, so the
key order shown here follows it rather than the reading order of the `include`
list.

It is convenience sugar for:

```text
fromjson(.metadata);
. << .metadata.(temperature|score);
tojson(.metadata)
```

## 7. Complete use-case catalogue

This section is normative: implementation and acceptance tests must cover every
case discussed during design.

### 7.1 Deserialize selected fields in place

```text
fromjson(.tools);
fromjson(.metadata.annotation, recursive=true)
```

### 7.2 Serialize nested tool parameters in place

```text
tojson(.tools[].function.parameters)
```

### 7.3 Perform several independent serializations

```text
tojson(.keya);
tojson(.keyb)
```

### 7.4 Perform value operations, then a whole-record operation

```text
tojson(.tools);
tojson(.metadata);
finalize_record(.)
```

All three operations execute continuously inside one worker invocation.

### 7.5 Explicitly move selected root fields into metadata

```text
.metadata << .annotation_key, .temperature, .score | tojson
```

### 7.6 Move a positive field set into metadata

```text
.metadata << .(annotation_key|temperature|score) | tojson
```

### 7.7 Blanket-move every field except stable root fields

```text
.metadata << .(^instance_id|messages|tools) | tojson
```

### 7.8 Equivalent configurable `nest` form

```text
nest(
  .,
  key="metadata",
  exclude=["instance_id", "messages", "tools"],
  jsonify=true
)
```

### 7.9 Decode metadata, move selected fields out, and reserialize it

```text
fromjson(.metadata);
. << .metadata.(temperature|score);
tojson(.metadata)
```

### 7.10 Decode a nested serialized value while moving it out

```text
fromjson(.metadata);
.temperature <- fromjson(.metadata.temperature);
tojson(.metadata)
```

### 7.11 Copy a nested value rather than moving it

```text
fromjson(.metadata);
.temperature = .metadata.temperature;
tojson(.metadata)
```

The value remains both at the root and inside metadata.

### 7.12 Focused structural operation followed by a bare tool

```text
.metadata << .(^instance_id|messages|tools) | normalize_metadata | tojson
```

Both bare tools apply to `.metadata`; the complete root record is still emitted.

### 7.13 Existing syntax remains expressible

Legacy:

```text
fromjson(.tools) | fromjson(.metadata.annotation, recursive=true)
```

Canonical record-sequencing form:

```text
fromjson(.tools);
fromjson(.metadata.annotation, recursive=true)
```

## 8. Collision, missing-path, and mutation rules

### 8.1 Resolve before mutation

Every structural statement must:

1. resolve all sources;
2. resolve or prepare the destination;
3. validate source cardinality and destination types;
4. detect collisions and overlapping paths;
5. calculate transformed values;
6. apply destination writes;
7. remove move sources;
8. publish the resulting focus.

No source is removed until every precondition succeeds.

### 8.2 Destination creation

All three assignment operators create a missing **final** destination field when
its parent exists. Missing **intermediate** parents remain errors.

For `<<` the created field is an empty object `{}`, which the move then fills:

```text
.metadata << .temperature
```

creates `.metadata` when the root exists and no metadata field is present.

For `=` and `<-` the created field simply receives the assigned value, which is
ordinary assignment semantics — the same as jq and Python — and is what makes
`.status = "processed"` work against a record that has no `status` yet.

A missing intermediate parent is an error for every operator, so
`.absent.deep = .b` fails rather than conjuring `.absent`:

| Expression | `{"b": 1}` becomes |
| --- | --- |
| `.new = .b` | `{"b": 1, "new": 1}` |
| `.new <- .b` | `{"new": 1}` |
| `.new << .b` | `{"new": {"b": 1}}` |
| `.absent.deep = .b` | error: destination cannot be resolved |

### 8.3 Destination type

An existing `<<` destination must be an object. A serialized metadata string
must be explicitly decoded first:

```text
fromjson(.metadata);
.metadata << .temperature
```

### 8.4 Destination-key collisions

Symbolic move operations use `collision="error"` semantics. If the destination
already contains the derived key, the statement fails.

Replacement or merge behavior must be explicit through a named tool:

```text
move(.temperature, to=".metadata.temperature", collision="replace")
```

### 8.5 Positive missing fields

Positive field sets are strict. A requested field that is missing is an error:

```text
.(temperature|score)
```

requires both fields.

### 8.6 Complement missing exclusions

Missing names in a complement list are harmless. This supports evolving input
schemas:

```text
.(^instance_id|messages|tools|optional_future_field)
```

### 8.7 Destination self-exclusion

For blanket moves, the destination key is automatically excluded from the
source set. This prevents `.metadata` from being moved inside itself.

### 8.8 Overlapping paths

Reject moves where:

- destination is inside the source subtree;
- source is an ancestor of the destination;
- source and destination are both indices into the same array — deleting the
  source element renumbers the destination, so the write cannot be made stable
  in either order (moving *out* of an array into a different container stays
  valid, see §8.9);
- the same source is selected more than once;
- two sources derive the same destination key;
- wildcard expansion produces duplicate references.

**When these are checked (S6).** A positive field set is fully static, so all
of the above are compile errors. A complement's membership is only known per
record, so only two things can be decided statically for it: whether the base
resolves, and whether the base *is* the destination — `.m << .m.(^a)`, where
every derived key would land back on the field it came from, is rejected at
compile time.

Everything else about a complement is checked per record against the actual
membership, by the same effective-destination rule the static path uses. In
particular `. << .metadata.(^note)` compiles: a root destination is an ancestor
of every base, but that is §6.6's intended extraction, and only a *specific*
member (a `.metadata.metadata`) can genuinely conflict — that member is
rejected at runtime, leaving the record untouched. S4 rejected the whole form
on the possibility, which made complements strictly less powerful than positive
sets for no safety gain; S6 narrowed the check.

### 8.9 Array sources

Initial `<<` field inference applies only to object fields. Array indexes and
wildcards require an explicit destination:

```text
.metadata.first <- .values[0]
```

Do not infer a destination key from `[0]` or `[]`.

## 9. Grammar

Conceptual grammar additions:

```text
program          := statement (SEMICOLON statement)* SEMICOLON?

statement        := base_operation (PIPE focused_operation)*

base_operation   := invocation
                  | path
                  | assignment
                  | exact_move
                  | move_into

focused_operation := bare_tool
                   | invocation

assignment       := path EQUALS value_expression
exact_move       := path MOVE_FROM movable_expression
move_into        := path MOVE_IN move_source_list

move_source_list := move_source (COMMA move_source)*
move_source      := path | field_set

field_set        := path DOT LPAREN COMPLEMENT? field_union RPAREN
field_union      := field_name (PIPE field_name)*

value_expression := path
                  | literal
                  | invocation

movable_expression := path
                    | invocation_with_primary_path

bare_tool        := qualified_name
                  | qualified_name LPAREN argument_list? RPAREN

SEMICOLON        := ";"
EQUALS           := "="
MOVE_FROM        := "<-"
MOVE_IN          := "<<"
COMPLEMENT       := "^"
```

### 9.1 Precedence

Highest to lowest:

1. literals, paths, calls, and field-set parentheses;
2. `=`, `<-`, and `<<`;
3. `,` within a `<<` source list;
4. `|` focused chaining;
5. `;` record-statement sequencing.

Thus:

```text
.metadata << .(^instance_id|messages|tools) | tojson;
finalize(.)
```

parses as two statements. The first contains one field union inside selector
parentheses and one focused tool pipe.

### 9.2 Contextual `|`

Inside `.()` parentheses, `|` joins exact field names. Outside selector
parentheses, it chains focused operations. The parser knows the context without
guessing.

## 10. AST and compiled IR

### 10.1 Syntax AST

Add immutable, source-positioned nodes:

```python
@dataclass(frozen=True)
class Program:
    statements: tuple[Statement, ...]
    span: Span


@dataclass(frozen=True)
class Statement:
    operation: Operation
    pipes: tuple[FocusedCall, ...]
    span: Span


@dataclass(frozen=True)
class Assignment:
    destination: Selector
    value: ValueExpression
    span: Span


@dataclass(frozen=True)
class ExactMove:
    destination: Selector
    source: MovableExpression
    span: Span


@dataclass(frozen=True)
class MoveInto:
    destination: Selector
    sources: tuple[MoveSource, ...]
    span: Span


@dataclass(frozen=True)
class FieldSet:
    base: Selector
    names: tuple[str, ...]
    complement: bool
    span: Span
```

Every node must retain source spans for caret diagnostics.

### 10.2 Compiled IR

Keep the compiled representation free of live provider callables:

```python
@dataclass(frozen=True)
class CompiledProgram:
    statements: tuple[CompiledStatement, ...]
    source: str


@dataclass(frozen=True)
class CompiledStatement:
    operation: CompiledOperation
    pipes: tuple[CompiledToolCall, ...]
    span: tuple[int, int]


@dataclass(frozen=True)
class CompiledMoveInto:
    destination: CompiledSelector
    sources: tuple[CompiledMoveSource, ...]


@dataclass(frozen=True)
class CompiledFieldSet:
    base: CompiledSelector
    names: tuple[str, ...]
    complement: bool
```

Installed tools remain `ToolDescriptor` values and resolve once per worker in
stage setup.

### 10.3 Focus references

The runtime should reuse and extend the existing selector `Reference` model:

```python
@dataclass
class Focus:
    root: Any
    references: tuple[Reference, ...]
```

Focused tools apply elementwise to `references` in stable order. A structural
operation like `<<` returns one destination reference as its focus.

The root is always retained separately, ensuring the final emitted value is the
record rather than merely the focused nested value.

## 11. Worker execution

The existing `CompiledToolProgramStage` should evolve into a general compiled
program stage rather than creating one datapipe `Stage` per syntax statement.

Conceptual execution:

```python
class CompiledProgramStage(Stage):
    def setup(self, ctx):
        self.resolve_tool_descriptors_once_per_worker()

    def process(self, record, ctx):
        root = record

        for statement in self.program.statements:
            focus = statement.operation.apply(root, ctx)

            for tool in statement.pipes:
                focus.apply_and_replace(tool, ctx)

        return root
```

The transform pipeline remains:

```python
Pipeline([
    JsonLoadStage(),
    CompiledProgramStage(program),
    JsonDumpStage(),
])
```

The existing executor sees only the complete compiled pipeline.

## 12. Error model

Structural failures should extend `ToolExecutionError` or introduce a sibling
`StructuralExecutionError` with:

- record sequence;
- statement index;
- operation type;
- source span;
- configured selector;
- concrete source path;
- concrete destination path;
- collision or missing-path policy;
- original cause and traceback.

Examples:

```text
record 1842 failed in move-into
statement: 2
source: .temperature
destination: .metadata.temperature
cause: destination key already exists
```

Compilation errors should reject:

- an empty statement;
- a bare value tool without a current target;
- complement selection on a non-object form;
- array/wildcard source under inferred-key `<<`;
- a move RHS without an identifiable source;
- statically overlapping source/destination paths;
- duplicate exact field names inside a field set.

## 13. Backward compatibility and migration

### 13.1 Preserve existing calls

Existing in-place invocations remain valid:

```text
fromjson(.tools)
tojson(.metadata)
```

### 13.2 Canonical statement separator

`;` becomes the canonical separator between independent record mutations:

```text
fromjson(.tools);
tojson(.metadata)
```

### 13.3 Legacy explicit-target pipe

For one compatibility window, accept:

```text
fromjson(.tools) | tojson(.metadata)
```

when both sides are explicit-target invocations. Compile it as two statements
and emit a deprecation warning:

```text
`|` between explicit record mutations is deprecated;
use `fromjson(.tools); tojson(.metadata)`
```

Do not guess when either side uses bare focus semantics. Ambiguous expressions
must fail with a suggested rewrite.

### 13.4 Inspection

`datapipe inspect-expression` and `transform --dry-run` should show statement
and focus boundaries:

```text
Statement 0
  move-into .metadata
    sources: complement(instance_id, messages, tools)
  pipe: tojson

Statement 1
  call finalize at .
```

Implemented in S6. The real output carries the same structure plus an
`expression-language:` version line, a `(focus: ...)` annotation on statements
that publish one, and the provider/contract/argument block for each call. The
JSON path exposes the same tree — `statements[]` with `focus`, `operation`
(`kind`, `destination`, `sources`) and `pipes` — so both surfaces describe the
same thing.

An assignment operation carries both a `source` and a `literal` key, exactly
one of which is non-null: a path right-hand side renders into `source`, and a
literal one into `literal` (as `{"value": ...}`), since a constant names no
location. The text surface renders *from* the JSON document, so the two cannot
drift apart.

## 14. Implementation phases

### Phase S0: Freeze semantics and examples

1. Add this plan to architecture documentation.
2. Turn every example in Section 7 into a parser fixture and expected desugaring.
3. Decide and document the compatibility window for legacy `|` sequencing.
4. Add expression-language version metadata to inspection output.

No runtime behavior changes in this phase.

### Phase S1: Program and statement sequencing

1. Tokenize `;`.
2. Add `Program` and `Statement` AST nodes.
3. Compile semicolon-separated existing invocations.
4. Execute all statements in one compiled program stage.
5. Prove with executor instrumentation that one record is dispatched once.
6. Add legacy `|` migration diagnostics.

Acceptance expression:

```text
tojson(.keya); tojson(.keyb); finalize(.)
```

### Phase S2: Focused tool pipelines

1. Add current-target IR.
2. Support bare tools following `|`.
3. Support selector-first focused form:

   ```text
   .metadata | fromjson | normalize | tojson
   ```

4. Preserve wildcard elementwise semantics.
5. Reset focus at `;`.

Acceptance expression:

```text
.metadata | fromjson | normalize_metadata | tojson;
finalize(.)
```

### Phase S3: Exact assignment and move

1. Tokenize and parse `=` and `<-` as structural operations.
2. Implement copy assignment.
3. Implement exact move with resolve-before-mutate behavior.
4. Support transformed RHS values with one primary source.
5. Detect overlaps and destination collisions before mutation.

Acceptance expressions:

```text
.temperature = .metadata.temperature
.temperature <- .metadata.temperature
.temperature <- fromjson(.metadata.temperature)
```

### Phase S4: Grouped move-in and field sets

1. Tokenize and parse `<<`, `^`, and selector field unions.
2. Implement explicit grouped sources.
3. Implement positive field sets.
4. Implement complement field sets.
5. Create missing final destination objects.
6. Exclude destination from blanket source sets.
7. Preserve source object order.

Acceptance expressions:

```text
.metadata << .annotation_key, .temperature, .score | tojson
.metadata << .(annotation_key|temperature|score) | tojson
.metadata << .(^instance_id|messages|tools) | tojson
```

### Phase S5: `nest` and `unnest` conveniences

1. Implement record-level `nest` using the structural IR.
2. Implement record-level `unnest` using the structural IR.
3. Validate mutually exclusive include/exclude settings.
4. Support `jsonify`/`parse` conveniences through built-in JSON tools.
5. Ensure symbolic and named forms produce equivalent results.

### Phase S6: CLI, inspection, and documentation

1. Update expression help and shell quoting documentation.
2. Extend `inspect-expression` text and JSON output.
3. Add deprecation diagnostics for legacy sequencing.
4. Update the migration guide with metadata nesting/extraction.
5. Update tool-authoring documentation for record-level structural tools.

### Phase S7: Hardening and benchmarks

1. Benchmark explicit versus complement field selection.
2. Benchmark structural mutation with large metadata objects.
3. Confirm no additional worker-boundary serialization occurs.
4. Measure time to first progress and peak memory on structural workloads.
5. Fuzz parser precedence and structural path overlap detection.

## 15. Testing strategy

### 15.1 Parser and precedence

Cover:

- semicolons with optional trailing semicolon;
- field-union `|` versus focused-pipe `|`;
- comma grouping under `<<`;
- complement placement;
- nested field sets;
- malformed empty statements;
- source spans for every new node;
- legacy pipe diagnostics;
- shell-quoted expressions.

### 15.2 Structural semantics

Cover:

- copy retains source;
- exact move removes source;
- transformed move decodes before assignment;
- grouped move derives keys correctly;
- complement moves all and only non-excluded keys;
- destination field is self-excluded;
- source order is preserved;
- missing destination object is created;
- wrong destination type fails;
- collisions fail without applying partial moves;
- overlapping paths fail;
- arrays require explicit destinations.

### 15.3 Focus semantics

Cover:

- `<<` leaves its destination focused;
- bare tools transform the focus;
- wildcard focus applies elementwise;
- semicolon resets focus;
- final output is the root record;
- whole-record tools see all prior mutations.

### 15.4 Named/symbolic equivalence

For representative records, assert identical results from:

```text
.metadata << .(^instance_id|messages|tools) | tojson
```

and:

```text
nest(., key="metadata", exclude=["instance_id","messages","tools"], jsonify=true)
```

Also compare `unnest` with its lower-level desugaring.

### 15.5 Executor and streaming invariants

Run every principal expression under sequential, thread, and process executors.

Assert:

- equivalent record content;
- one submitted job per input record;
- no per-statement futures;
- bounded `max_in_flight` behavior;
- progress before source exhaustion;
- ordered and unordered output correctness;
- structured errors under raise/skip/return;
- provider descriptors still resolve once per worker.

### 15.6 End-to-end catalogue

Every use case in Section 7 must have a CLI-level JSONL test using the compact
shorthand command and an equivalent Python-level compiled-program test.

## 16. Definition of done

This extension is complete when:

1. `;` sequences independent record mutations inside one worker invocation.
2. `|` chains transformations on a well-defined current target.
3. Existing `tool(path)` syntax remains in-place shorthand.
4. `=` copies without deleting the source.
5. `<-` moves only after destination validation succeeds.
6. `<<` supports explicit lists, positive field sets, and complement field sets.
7. Blanket nesting automatically excludes the destination key.
8. `nest` and `unnest` match their symbolic desugarings.
9. Moving values out of serialized metadata and reserializing it works.
10. Several value transformations can precede a whole-record operation.
11. Every structural failure includes statement and concrete path attribution.
12. Legacy `|` sequencing has an actionable migration path.
13. Sequential, thread, and process execution produce equivalent records.
14. A record is submitted once and gathered once regardless of statement count.
15. No structural operation materializes the dataset or adds a runtime queue.
16. Inspection shows statement, focus, tool, and provider resolution.
17. Documentation includes every normative example from Section 7.

## 17. Final design summary

The language has two kinds of composition:

```text
;  sequence mutations of one evolving root record
|  continue transforming the current focused value
```

Structural operators remain explicit:

```text
=   copy
<-  exact move
<<  move fields into an object
^   complement a field set
```

The headline expressions are:

```text
# Blanket nesting and serialization
.metadata << .(^instance_id|messages|tools) | tojson

# Explicit grouped nesting
.metadata << .(annotation_key|temperature|score) | tojson

# Decode metadata, move values out, and reserialize
fromjson(.metadata);
. << .metadata.(temperature|score);
tojson(.metadata)

# Value work followed by whole-record work
tojson(.tools);
tojson(.metadata);
finalize_record(.)
```

All of them compile to one continuous per-record worker program:

```text
one input record
  -> one worker dispatch
  -> every statement and focused operation
  -> one gathered output record
```

### 13.5 Compatibility window decision

Legacy `|` sequencing (two explicit-target invocations joined by `|`) is
supported for **one minor release** from the introduction of `;` (Phase S1).
The deprecation warning is emitted at compile time, not at runtime, so it
appears once when the expression is compiled rather than once per record.

Operationally: Phase S1 emits a deprecation warning; Phase S6 updates the
migration guide and the CLI documentation. Removal lands in the first minor
release after the one that ships `;`.

Amended during S6 (decision recorded): the original text said S6 *removes* the
legacy form. That would have given a zero-length compatibility window, because
S1 through S6 all land in a single unreleased branch — the window is defined
above as one minor *release*, which requires `;` to have shipped first. §14's
S6 deliverable list asks only for "deprecation diagnostics", so removal is
deferred and the window is measured from the release that introduces `;`.

What S6 did add is the §13.3 *ambiguity* rejection: an explicit selector
following a bare focus call (`fromjson(.a) | tojson | tojson(.b)`) is no longer
a bare parse error but a diagnostic naming the `;` rewrite.

Delivery (added after review 3, amended after review 4): the compiler raises the
diagnostic as a `DeprecationWarning`, which Python's default filter hides outside
`__main__` — so on the real CLI it reached no one. The CLI now prints it to
stderr once per compile, in the same `warning: ...` form it already uses for
provider and registry problems, and does **not** re-issue that category: an
earlier version did, which printed the same notice twice under
`PYTHONWARNINGS=default` or `-Wd`, the second copy pointing at the CLI module
rather than at the user's expression. Warning categories the CLI does not render
itself are re-issued untouched. Library callers are unaffected —
`compile_expression()` still raises the filterable `DeprecationWarning`, which is
where a caller filtering on the category receives it.

One consequence is deliberate: because the CLI captures warnings before Python's
filter chain runs, `-W error` does not turn the legacy-pipe diagnostic into a
fatal error. The form stays non-fatal until its removal (see the window above).
