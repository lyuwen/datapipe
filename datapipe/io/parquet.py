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
        import pyarrow.dataset as ds  # noqa: PLC0415
        import pyarrow.parquet as pq  # noqa: PLC0415

        return pa, pq, ds
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
    """Choose a rank-aware output path.

    A dataset-directory path becomes ``path/part-{rank:05d}.parquet`` (both
    single-rank and multi-rank, for predictability).

    A plain file path is used as-is for single-rank runs. For multi-rank runs
    it gets a ``.part-{rank}`` suffix inserted before the extension, so ranks
    never write the same file (finding 7).
    """
    if _is_dataset_path(path):
        if runtime is not None and runtime.world_size > 1:
            return os.path.join(path, f"part-{runtime.rank:05d}.parquet")
        return os.path.join(path, "part-00000.parquet")
    if runtime is not None and runtime.world_size > 1:
        base, ext = os.path.splitext(path)
        return f"{base}.part-{runtime.rank:05d}{ext}"
    return path


def _normalize_filter(filters):
    """Normalize a pyarrow filter to a ``dataset.Expression``.

    Accepts either a modern ``dataset.Expression`` or the legacy
    ``(col, op, value)`` / list-of-those (ANDed) format.
    """
    if filters is None:
        return None
    _, _, ds = _require_pyarrow()
    if isinstance(filters, ds.Expression):
        return filters

    if isinstance(filters, tuple):
        filters = [filters]
    expr = None
    for item in filters:
        try:
            col, op, value = item
        except (TypeError, ValueError) as exc:
            raise ParquetError(
                "Parquet filters must be a pyarrow dataset Expression or "
                "(col, op, value) tuples"
            ) from exc
        field = ds.field(col)
        if op == "==":
            e = field == value
        elif op == "!=":
            e = field != value
        elif op == "<":
            e = field < value
        elif op == "<=":
            e = field <= value
        elif op == ">":
            e = field > value
        elif op == ">=":
            e = field >= value
        elif op.lower() == "in":
            e = field.isin(value)
        else:
            raise ParquetError(f"unsupported Parquet filter op {op!r}")
        expr = e if expr is None else (expr & e)
    return expr


class ParquetSource(Source):
    """Reads rows from a Parquet file or a directory of Parquet files.

    Yields rows as Python dicts (by default). Reading is batched internally —
    never one Parquet IO operation per row. ``filters`` (a pyarrow
    ``dataset.Expression`` or legacy ``(col, op, value)`` tuples) is applied
    to every row group actually read.
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
        self._filter_expr = None
        self.batch_size = batch_size

    def _files(self) -> list[str]:
        pa, pq, ds = _require_pyarrow()
        if os.path.isdir(self.path):
            dataset = pq.ParquetDataset(self.path, filters=self._filter_expr)
            return [str(p) for p in dataset.files]
        return [self.path]

    def _expr(self):
        if self._filter_expr is None:
            self._filter_expr = _normalize_filter(self.filters)
        return self._filter_expr

    def __iter__(self) -> Iterator[Any]:
        _require_pyarrow()
        self._expr()
        yield from self._iter_paths(self._files())

    def iter_shard(self, rank: int, world_size: int) -> Iterator[Any] | None:
        """Physically shard a dataset by assigning files (preferred), falling
        back to row-group assignment for a single file.
        """
        self._expr()
        files = self._files()
        if world_size <= 1:
            return self.__iter__()
        if len(files) > 1:
            return self._iter_paths(files[rank::world_size])
        return self._iter_row_groups(rank, world_size)

    def _iter_paths(self, paths: list[str]) -> Iterator[Any]:
        pa, pq, ds = _require_pyarrow()
        expr = self._expr()
        for path in paths:
            pf = pq.ParquetFile(path)
            for batch in pf.iter_batches(batch_size=self.batch_size, columns=self.columns):
                if expr is not None:
                    batch = batch.filter(expr)
                yield from _batches_to_rows(batch)

    def _iter_row_groups(self, rank: int, world_size: int) -> Iterator[Any]:
        pa, pq, ds = _require_pyarrow()
        pf = pq.ParquetFile(self.path)
        expr = self._expr()
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
                if expr is not None:
                    batch = batch.filter(expr)
                yield from _batches_to_rows(batch)

    @property
    def total(self) -> int | None:
        try:
            pa, pq, ds = _require_pyarrow()
            files = self._files()
            return int(sum(pq.ParquetFile(f).metadata.num_rows for f in files))
        except Exception:
            return None


class ParquetSink(Sink):
    """Buffers rows into batches and writes a Parquet file (or dataset dir).

    Never writes one row at a time. ``schema`` may be an explicit pyarrow
    schema; if omitted, the schema is inferred from the first batch. When an
    explicit schema is given, each batch is constructed with it directly
    (pyarrow converts compatible Python values), so schema mismatches surface
    at write time rather than silently.
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
        pa, pq, ds = _require_pyarrow()
        if self.schema is not None:
            table = pa.Table.from_pylist(self._buffer, schema=self.schema)
        else:
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
        # ParquetWriter has no flush(); flushing means draining the buffer so
        # the rows reach the writer. The OS-level flush happens on close.
        self._flush_buffer()

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
