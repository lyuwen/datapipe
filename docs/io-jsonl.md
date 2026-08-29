# JSONL IO

JSONL is the ergonomic default format.

## Source

```python
JsonlSource(path, *, raw=False, encoding="utf-8", compression="auto")
```

- `raw=False` (default): yields parsed Python objects.
- `raw=True`: yields raw line strings — parse inside workers with
  `JsonLoadStage()` to keep coordinator work small.
- `compression="auto"` inspects the extension: `.gz` -> gzip, `.zst` -> zstd.

### Directory datasets

A directory of JSONL shards is supported:

```
dataset/
  part-00000.jsonl
  part-00001.jsonl
  part-00002.jsonl
```

Physical rank sharding assigns whole files to ranks:
`files[rank::world_size]`. A single giant file falls back to logical sharding.

## Sink

```python
JsonlSink(path, *, raw=False, encoding="utf-8", compression="auto", flush_every=None)
```

- `raw=False` (default): serializes objects with `json.dumps`.
- `raw=True`: assumes the pipeline already returns JSON strings (raises a
  clear `TypeError` otherwise).

### Rank-aware paths

A directory path (trailing slash or existing directory) writes
`path/part-{rank:05d}.jsonl`:

- multi-rank: `output/part-00037.jsonl`
- single-rank: `output/part-00000.jsonl`

A plain file path is used as-is for single-rank runs; multi-rank runs insert
a `.part-{rank}` suffix so ranks never clobber each other.

## Example

```python
from datapipe import (
    GenericStage, JsonDumpStage, JsonLoadStage,
    JsonlSink, JsonlSource, Pipeline, ProcessExecutor,
)

pipeline = Pipeline([
    JsonLoadStage(),
    GenericStage(process=enrich, name="enrich"),
    JsonDumpStage(),
])

pipeline.run(
    source=JsonlSource("input.jsonl", raw=True),
    sink=JsonlSink("output.jsonl", raw=True),
    executor=ProcessExecutor(workers=32, max_in_flight=128),
)
```
