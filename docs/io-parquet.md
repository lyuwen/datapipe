# Parquet IO

Parquet is the scalable default. Requires `pyarrow`:

```bash
pip install datapipe[parquet]
```

## Source

```python
ParquetSource(path, *, columns=None, filters=None, batch_size=4096)
```

- Yields rows as Python dicts (by default).
- Physical reading is batched internally — never one Parquet IO operation per
  row.
- `columns` projects only the needed columns; `filters` applies row-group
  predicates (a `pyarrow` expression).
- Directory datasets are discovered automatically.

### Physical sharding

Preference order:

1. files (dataset directories): `files[rank::world_size]`;
2. row groups (single file): row groups `groups[rank::world_size]`.

## Sink

```python
ParquetSink(path, *, schema=None, batch_size=4096, compression="zstd")
```

- Buffers rows into batches and writes with a `ParquetWriter` — never one
  row at a time.
- `schema` may be an explicit `pyarrow.Schema`; otherwise it is inferred from
  the first batch.
- A directory path (trailing slash or existing directory) writes
  `path/part-{rank:05d}.parquet`.

## Example

```python
import pyarrow as pa

OUTPUT_SCHEMA = pa.schema([
    ("id", pa.int64()),
    ("text", pa.string()),
    ("length", pa.int64()),
])

pipeline = Pipeline([
    GenericStage(process=normalize, name="normalize"),
    GenericStage(process=enrich, name="enrich"),
    FilterStage(valid),
])

pipeline.run(
    source=ParquetSource("input_dataset/", columns=["id", "text", "metadata"]),
    sink=ParquetSink("output_dataset/", schema=OUTPUT_SCHEMA, batch_size=4096),
    executor=ProcessExecutor(workers=32, max_in_flight=256),
)
```

## Memory

The source reads record batches (default 4096 rows) and the sink buffers
output batches, so memory stays bounded regardless of dataset size.
