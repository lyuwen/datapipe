# Distributed Execution

`datapipe` is designed for embarrassingly parallel scaling across ranks. The
model is deliberately simple:

```
shared / partitionable dataset

rank 0:  read shard 0 -> local ProcessExecutor -> output shard 0
rank 1:  read shard 1 -> local ProcessExecutor -> output shard 1
rank 2:  read shard 2 -> local ProcessExecutor -> output shard 2
...
```

Each rank runs **independently**. There is no central dispatcher, no
rank-to-rank communication, and no global gather for ordinary processing.

## Runtime context

```python
from datapipe import RuntimeContext

# local
runtime = RuntimeContext(rank=0, world_size=1)

# distributed
runtime = RuntimeContext(rank=37, world_size=64)
```

`RuntimeContext.auto()` detects the launch environment with deterministic
priority:

1. torchrun-style env (`RANK`, `WORLD_SIZE`, `LOCAL_RANK`)
2. Slurm (`SLURM_PROCID`, `SLURM_NTASKS`, `SLURM_LOCALID`, `SLURM_NODEID`)
3. K8s indexed job (`JOB_COMPLETION_INDEX`, `WORLD_SIZE`)
4. local fallback (`rank=0, world_size=1`)

Explicit arguments always override detection:
`RuntimeContext.auto(rank=2)`.

## Sharding

`ShardingStrategy.owns(seq, value, rank, world_size) -> bool` decides which
records belong to this rank.

| Strategy | Rule | Use when |
|----------|------|----------|
| `NoSharding` | rank 0 owns everything | single-rank runs |
| `ModuloSharding` | `seq % world_size == rank` | generic fallback |
| `HashSharding` | `stable_hash(key(value)) % world_size == rank` | stable item keys |
| `RangeSharding` | contiguous `seq` ranges (needs known total) | known cardinality |

**Physical sharding is preferred over logical filtering.** A `Source` with
`supports_physical_sharding=True` reads only its own portion (JSONL
`files[rank::world_size]`; Parquet files then row groups). Logical sharding
(`seq % world_size == rank`) is the universal fallback but duplicates IO by
`world_size`.

## Rank-aware output

A directory sink writes one shard per rank:

```
output/
  part-00000.jsonl
  part-00001.jsonl
  ...
```

```python
pipeline.run(
    source=JsonlSource("dataset/"),
    sink=JsonlSink("output/"),
    executor=ProcessExecutor(workers=56, max_in_flight=224),
    runtime=RuntimeContext.auto(),
)
```

There is no global ordering across ranks; ordering is local to each rank. If
a merged file is needed, use a separate utility (not yet implemented).

## Simulating ranks locally

Distributed behavior is testable without a cluster:

```python
for rank in range(world_size):
    pipeline.run(
        ...,
        runtime=RuntimeContext(rank=rank, world_size=world_size),
    )
```

The union of per-rank outputs equals the full dataset with no duplicates and
no loss (see `tests/test_distributed.py`).
