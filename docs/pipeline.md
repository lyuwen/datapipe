# Pipeline

## Construction

```python
pipeline = Pipeline([
    StageA(...),
    StageB(...),
    StageC(...),
])
```

- Entries may be `Stage` instances or plain callables (coerced to
  `TransformStage`).
- `Pipeline([...])` performs no data movement and starts no workers — it is
  inert until `run()` is called.
- Stage names must be stable and unique when explicitly provided;
  auto-generated names are deduplicated.

## Stages

| Stage | Semantics |
|-------|-----------|
| `GenericStage(process=..., input=..., output=..., setup=..., teardown=..., with_context=...)` | the primary user-facing stage |
| `TransformStage(fn)` | `x -> fn(x)` |
| `FilterStage(predicate)` | keep if `predicate(x)` else `DROP` |
| `TapStage(fn)` | call `fn(x)`, return `x` |
| `JsonLoadStage()` / `JsonDumpStage()` | JSON parse / serialize wrappers |

`GenericStage` composes three optional callables:

```
x -> output(process(input(x)))
```

### Context

Stages may receive the worker context:

```python
GenericStage(process=process, with_context=True)
```

With `with_context=True`, `input(value, ctx)`, `process(value, ctx)`,
`output(value, ctx)` receive the `WorkerContext`; `setup(ctx)` and
`teardown(ctx)` always receive it. Without it, callables are called with the
value only. One calling convention in v1 — no signature guessing.

### Dropping

Any stage returning `DROP` removes the record from the output stream:

```python
from datapipe import DROP

def keep_positive(x):
    return x if x > 0 else DROP
```

### Heavy state in setup

```python
class ModelStage(Stage):
    def __init__(self, model_path):
        self.model_path = model_path
        self.model = None

    def setup(self, ctx):
        self.model = load_model(self.model_path)  # once per worker

    def process(self, x, ctx):
        return self.model(x)
```

Never construct large or non-pickleable state in the parent process.

## Compilation

`Pipeline.compile()` produces a `CompiledPipeline` — a worker-local fused
program. `setup` runs stages in order, `process` chains them (stopping at
`DROP`), `teardown` runs stages in reverse. The compiled object is
pickleable.

## run()

```python
pipeline.run(
    source,
    sink,
    *,
    executor=None,      # default ProcessExecutor()
    sharding=None,      # default based on runtime
    runtime=None,       # default RuntimeContext.auto()
    ordered=True,
    progress=True,
    errors="raise",
    error_sink=None,
    max_in_flight=None,
)
```

Convenience coercions:

- string `source` -> `JsonlSource`
- string `sink` -> `JsonlSink`
- plain iterable `source` -> `IterableSource`
- callable `sink` -> `CallableSink`

Returns `ExecutionStats`.
