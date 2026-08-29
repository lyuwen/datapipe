"""Parquet source and sink (requires pyarrow, an optional dependency)."""

from __future__ import annotations

import os
from typing import Any, Iterator

from datapipe.errors import ParquetError
from datapipe.io.base import Source, Sink
from datapipe.runtime.context import RuntimeContext


def _require_pyarrow():
    try:
        import pyarrow as pa  # noqa: PLC0415
        import pyarrow.parquet as pq  # noqa: PLC0415

        return pa, pq
    except ImportError as exc:  # pragma: no cover - env dependent
        raise ParquetError(
            "Parquet support requires pyarrow; install `datapipe[parquet]`"
        ) from exc


def _is_dataset_path(path: str) -> bool:
    """True if ``path`` denotes a dataset directory (not a single file).

    Explicit: a trailing separator, or an already-existing directory.
    """
    return path.endswith(("/", os.sep)) or os.path.isdir(path)


def _ranked_dataset_path(path: str, runtime: RuntimeContext | None) -> str:
    """Choose a rank-aware output directory for a dataset path.

    A directory path becomes ``path/part-{rank:05d}.parquet``. Multi-rank
    runs always use a ranked shard name; single-rank runs on a dataset path
    use ``part-00000.parquet`` for predictability. Plain file paths are
    returned unchanged.
    """
    if not _is_dataset_path(path):
        return path
    if runtime is not None and runtime.world_size > 1:
        return os.path.join(path, f"part-{runtime.rank:05d}.parquet")
    return os.path.join(path, "part-00000.parquet")


class ParquetSource(Source):
    """Reads rows from a Parquet file or a directory of Parquet files.

    Yields rows as Python dicts (by default). Reading is batched internally —
    never one Parquet IO operation per row.
    """

    supports_physical_sharding = True

    def __init__(
        self,
        path: str,
        *,
        columns: list[str] | None = None,
        filters=None,
        batch_size: int = 4096,
    ) -> None:
        self.path = path
        self.columns = columns
        self.filters = filters
        self.batch_size = batch_size

    def _files(self) -> list[str]:
        pa, pq = _require_pyarrow()
        if os.path.isdir(self.path):
            dataset = pq.ParquetDataset(self.path, filters=self.filters)
            return [str(p) for p in dataset.files]
        return [self.path]

    def __iter__(self) -> Iterator[Any]:
        _require_pyarrow()
        yield from self._iter_paths(self._files())

    def iter_shard(self, rank: int, world_size: int) -> Iterator[Any] | None:
        """Physically shard a dataset by assigning files (preferred), falling
        back to row-group assignment for a single file.
        """
        files = self._files()
        if world_size <= 1:
            return self.__iter__()
        if len(files) > 1:
            return self._iter_paths(files[rank::world_size])
        return self._iter_row_groups(rank, world_size)

    def _iter_paths(self, paths: list[str]) -> Iterator[Any]:
        pa, pq = _require_pyarrow()
        for path in paths:
            pf = pq.ParquetFile(path)
            for batch in pf.iter_batches(batch_size=self.batch_size, columns=self.columns):
                yield from _batches_to_rows(batch)

    def _iter_row_groups(self, rank: int, world_size: int) -> Iterator[Any]:
        pa, pq = _require_pyarrow()
        pf = pq.ParquetFile(self.path)
        groups = list(range(pf.metadata.num_row_groups))
        mine = groups[rank::world_size]
        if not mine:
            return
        for group in mine:
            for batch in pf.iter_batches(
                batch_size=self.batch_size,
                row_groups=[group],
                columns=self.columns,
            ):
                yield from _batches_to_rows(batch)

    @property
    def total(self) -> int | None:
        try:
            pa, pq = _require_pyarrow()
            files = self._files()
            return int(sum(pq.ParquetFile(f).metadata.num_rows for f in files))
        except Exception:
            return None


class ParquetSink(Sink):
    """Buffers rows into batches and writes a Parquet file (or dataset dir).

    Never writes one row at a time. ``schema`` may be an explicit pyarrow
    schema; if omitted, the schema is inferred from the first batch.
    """

    def __init__(
        self,
        path: str,
        *,
        schema=None,
        batch_size: int = 4096,
        compression: str = "zstd",
        **writer_kwargs: Any,
    ) -> None:
        self.path = path
        self.schema = schema
        self.batch_size = batch_size
        self.compression = compression
        self.writer_kwargs = writer_kwargs
        self._writer = None
        self._write_path: str | None = None
        self._buffer: list[dict] = []
        self._row_count = 0

    def open(self, runtime: RuntimeContext | None = None) -> None:
        if self._writer is not None:
            return
        _require_pyarrow()
        self._write_path = _ranked_dataset_path(self.path, runtime)
        parent = os.path.dirname(self._write_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._buffer = []
        self._row_count = 0

    def write(self, record: Any) -> None:
        # Buffer rows; flush a batch (creating the writer from its schema)
        # once enough rows accumulate. Never write one row at a time.
        self._buffer.append(record)
        if len(self._buffer) >= self.batch_size:
            self._flush_buffer()

    def _flush_buffer(self) -> None:
        if not self._buffer:
            return
        pa, pq = _require_pyarrow()
        table = pa.Table.from_pylist(self._buffer)
        if self._writer is None:
            schema = self.schema or table.schema
            self._writer = pq.ParquetWriter(
                self._write_path,
                schema=schema,
                compression=self.compression,
                **self.writer_kwargs,
            )
        self._writer.write_table(table)
        self._row_count += len(self._buffer)
        self._buffer = []

    def flush(self) -> None:
        self._flush_buffer()
        if self._writer is not None:
            self._writer.flush()

    def close(self) -> None:
        self._flush_buffer()
        if self._writer is not None:
            self._writer.close()
            self._writer = None

    @property
    def write_path(self) -> str | None:
        return self._write_path

    @property
    def row_count(self) -> int:
        return self._row_count


def _batches_to_rows(batch) -> Iterator[dict]:
    names = batch.column_names
    cols = batch.columns
    for i in range(batch.num_rows):
        yield {name: col[i].as_py() for name, col in zip(names, cols)}
