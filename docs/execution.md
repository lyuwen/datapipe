# Execution

## Executors

The executor owns local parallelism only. Its interface:

```
run(records, worker, runtime, on_result, max_in_flight) -> ExecutionStats
```

Three executors are provided:

| Executor | Concurrency | Use when |
|----------|-------------|----------|
| `SequentialExecutor` | none | testing, debugging, deterministic reproduction, profiling |
| `ThreadExecutor` | threads | IO-heavy record processing |
| `ProcessExecutor` | processes | CPU-heavy work; the default backend |

The exact same pipeline runs under all three — no stage changes needed.

## Process executor lifecycle

Worker processes are started with `ProcessPoolExecutor(initializer=...)`:

```
worker starts
  |
  +-- Pipeline.setup()      # once per worker (load models, clients, ...)
  |     +-- stage 1 setup
  |     +-- stage 2 setup
  |
  +-- process record        # payload is only (seq, value)
  +-- process record
  |   ...
  |
  +-- Pipeline.teardown()   # best-effort, via process-local atexit
```

Only the smallest necessary payload crosses the process boundary: the
sequence number and the record value. The compiled pipeline is installed once
per worker via the pool initializer.

**Teardown caveat**: with `ProcessPoolExecutor` there is no robust normal
worker finalizer, so `teardown()` is best-effort and must not be relied on
for correctness (plan §9).

## Bounded scheduler

The shared scheduler loop (used by both `ProcessExecutor` and
`ThreadExecutor`):

```
1. fill the window (submit up to max_in_flight tasks)
2. wait for FIRST_COMPLETED
3. gather each completed result immediately
4. submit a replacement for each
5. repeat until the source is exhausted and no tasks remain
```

This is what makes progress visible before the input is fully consumed, and
what keeps the number of live `Future` objects bounded.

## Cancellation / Ctrl-C

On `KeyboardInterrupt`:

- new submission stops;
- pending futures are cancelled;
- the pool is shut down;
- the sink is flushed/closed;
- the progress bar is closed;
- the original `KeyboardInterrupt` propagates.

## Error model

A worker failure is wrapped in `StageExecutionError` carrying `stage_name`,
`record_seq`, and the original `cause`. Error policies:

- `errors="raise"` (default): first error aborts the run.
- `errors="skip"`: failed rows are counted and omitted.
- `errors="return"`: errors go to `error_sink` (or are exposed as structured
  `TaskResult`s when no error sink is given).

## Stats

`ExecutionStats` reports input/output/dropped/failed counts, elapsed time,
records/sec, rank/world_size, and high-water marks for the in-flight window
and the reorder buffer.
