# Concepts

## The mental model

> Define a per-record processing program, then execute that program
> concurrently over a stream.

Not "build a parallel dataflow graph". `datapipe` fuses a sequence of stages
into a single worker-local program and runs that program concurrently over
records.

```
source
  -> load        \
  -> process_1    |   fused per-record program
  -> process_2    |
  -> serialize   /
  -> sink
```

The defining execution property:

> A record is dispatched to a worker **once**, processed through the entire
> pipeline **inside that worker**, and gathered **once** at the end.

There is no dispatch/gather boundary between stages, and no stage has its own
worker pool.

## Per-record programs from an expression

The same mental model applies to `datapipe transform`. A transform expression
*is* a per-record program: `;` sequences mutations of one evolving record, `|`
keeps transforming the current focused value, and `<<` / `=` / `<-` move values
between paths. However many statements it has, it compiles to one fused
per-record program — one dispatch, one gather.

```
'fromjson(.metadata); . << .metadata.(temperature|score); tojson(.metadata)'
   -> one worker dispatch
   -> all three statements, in order, on one record
   -> one gathered output record
```

See [cli.md](cli.md#expression-language) for the language, and
`datapipe inspect-expression EXPR` to see the statements a given expression
compiles to.

## The four orthogonal axes

```
Pipeline        = modular per-record program
Executor        = local concurrency (sequential / thread / process)
Sharding        = global record ownership (which rank owns which records)
RuntimeContext  = rank / world_size / environment metadata
Source / Sink   = storage adapters (JSONL, Parquet, iterables, ...)
```

These are orthogonal by design:

- The same `Pipeline` runs unchanged under any executor.
- Distribution (`world_size`/`rank`) is orthogonal to local concurrency
  (`workers`).
- `Sharding` decides global ownership; `Executor` decides local parallelism.
- JSONL and Parquet are IO adapters, not special execution modes.

## Bounded dispatch

The executor never eagerly submits the full dataset. At most `max_in_flight`
tasks exist as submitted work. This gives:

- bounded memory regardless of dataset size;
- immediate progress (results flow before the input is exhausted);
- natural backpressure;
- safe processing of arbitrarily large sources.

## Ordering

Every input record receives a monotonic `seq`. Workers may finish out of
order. With `ordered=True`, the runtime buffers completed results and emits
them in input order. A single very slow early record can grow the reorder
buffer; this is documented and expected.

For distributed runs, ordering is local to each rank. There is no global
ordering across ranks.
