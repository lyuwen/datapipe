"""JSONL source and sink with optional gzip/zstd compression."""

from __future__ import annotations

import json
import os
from typing import Any, Callable, Iterator

from datapipe.errors import SourceError
from datapipe.io.base import Source, Sink, SourceRecordError
from datapipe.io.utils import (
    detect_compression,
    iter_lines,
    list_jsonl_files,
    open_reader,
    open_writer,
)
from datapipe.runtime.context import RuntimeContext


def _is_dataset_path(path: str) -> bool:
    """True if ``path`` denotes a dataset directory (not a single file)."""
    return path.endswith(("/", os.sep)) or os.path.isdir(path)


def _ranked_path(path: str, runtime: RuntimeContext | None) -> str:
    """Choose a rank-aware output path when appropriate.

    A directory path becomes ``path/part-{rank:05d}.jsonl``. Multi-rank runs
    on a directory always use a ranked shard name; single-rank runs on a
    directory use ``part-00000.jsonl`` for predictability. A plain file path
    is used as-is (single-rank) or gets a ``.part-{rank}`` suffix inserted
    before the extension for multi-rank runs, so ranks never clobber each
    other's files.
    """
    if _is_dataset_path(path):
        if runtime is not None and runtime.world_size > 1:
            return os.path.join(path, f"part-{runtime.rank:05d}.jsonl")
        return os.path.join(path, "part-00000.jsonl")
    if runtime is not None and runtime.world_size > 1:
        base, ext = os.path.splitext(path)
        return f"{base}.part-{runtime.rank:05d}{ext}"
    return path


class JsonlSource(Source):
    """Reads a JSONL file (or a directory of JSONL shards).

    Modes:

    - ``raw=False`` (default): yield parsed Python objects.
    - ``raw=True``: yield raw line strings; parsing happens in workers
      (e.g. via ``JsonLoadStage``), keeping coordinator work small.
    """

    supports_physical_sharding = True

    def __init__(
        self,
        path: str,
        *,
        raw: bool = False,
        encoding: str = "utf-8",
        compression: str = "auto",
        loads: Callable[[str], Any] = json.loads,
    ) -> None:
        self.path = path
        self.raw = raw
        self.encoding = encoding
        self.compression = compression
        self.loads = loads
        self._files: list[str] | None = None

    def _resolve_files(self) -> list[str]:
        if self._files is None:
            self._files = list_jsonl_files(self.path)
            if not self._files:
                raise SourceError(f"no JSONL files found at {self.path!r}")
        return self._files

    def _iter_file(self, path: str) -> Iterator[Any]:
        comp = detect_compression(path, self.compression)
        with open_reader(path, comp) as raw:
            for line in iter_lines(raw, self.encoding):
                if self.raw:
                    yield line
                    continue
                try:
                    yield self.loads(line)
                except Exception as exc:  # noqa: BLE001
                    # Report a per-record decode failure as a resumable marker
                    # so the runner can apply the error policy and continue
                    # with subsequent lines (finding 9).
                    yield SourceRecordError(exc=exc, line=line)

    def __iter__(self) -> Iterator[Any]:
        for path in self._resolve_files():
            yield from self._iter_file(path)

    def iter_shard(self, rank: int, world_size: int) -> Iterator[Any] | None:
        """Physically shard by assigning whole files to ranks.

        ``files[rank::world_size]`` — the preferred distributed JSONL format
        (multiple shards on disk). A single giant file falls back to logical
        sharding (returns ``None``).
        """
        files = self._resolve_files()
        if world_size <= 1:
            return self.__iter__()
        if len(files) > 1:
            mine = files[rank::world_size]
            if not mine:
                return iter(())
            return self._iter_many(mine)
        return None  # single file -> logical sharding

    def _iter_many(self, files: list[str]) -> Iterator[Any]:
        for path in files:
            yield from self._iter_file(path)

    @property
    def total(self) -> int | None:
        # Counting lines would require reading the whole file; per plan §36
        # we do NOT scan a huge JSONL just to get a progress total.
        return None


class JsonlSink(Sink):
    """Writes records to a JSONL file (or a ranked shard directory).

    - ``raw=False`` (default): ``json.dumps(record)`` each value.
    - ``raw=True``: assume the pipeline already returns JSON strings.
    """

    def __init__(
        self,
        path: str,
        *,
        raw: bool = False,
        encoding: str = "utf-8",
        compression: str = "auto",
        flush_every: int | None = None,
        dumps: Callable[[Any], str] = json.dumps,
    ) -> None:
        self.path = path
        self.raw = raw
        self.encoding = encoding
        self.compression = compression
        self.flush_every = flush_every
        self.dumps = dumps
        self._handle = None
        self._write_path: str | None = None
        self._count = 0

    def open(self, runtime: RuntimeContext | None = None) -> None:
        if self._handle is not None:
            return
        self._write_path = _ranked_path(self.path, runtime)
        parent = os.path.dirname(self._write_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        comp = detect_compression(self._write_path, self.compression)
        self._handle = open_writer(self._write_path, comp)
        self._count = 0

    def write(self, record: Any) -> None:
        if self._handle is None:
            raise SourceError("JsonlSink.write() before open()")
        if self.raw:
            if isinstance(record, bytes):
                line = record.decode(self.encoding)
            elif isinstance(record, str):
                line = record
            else:
                raise TypeError(
                    "JsonlSink(raw=True) expects the pipeline to return "
                    "serialized JSON strings, got "
                    f"{type(record).__name__}. Use raw=False (default) to "
                    "have the sink serialize objects with json.dumps."
                )
            if not line.endswith("\n"):
                line = line + "\n"
            self._handle.write(line.encode(self.encoding))
        else:
            self._handle.write(
                (self.dumps(record) + "\n").encode(self.encoding)
            )
        self._count += 1
        if self.flush_every is not None and self._count % self.flush_every == 0:
            self._handle.flush()

    def flush(self) -> None:
        if self._handle is not None:
            self._handle.flush()

    def close(self) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None

    @property
    def write_path(self) -> str | None:
        return self._write_path
