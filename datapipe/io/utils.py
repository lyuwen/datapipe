"""Shared IO utilities: compression handling and path helpers."""

from __future__ import annotations

import gzip
import io
import os
from typing import BinaryIO, Iterator

_COMPRESSION_BY_EXT = {
    ".gz": "gzip",
    ".zst": "zstd",
    ".zstd": "zstd",
}


def detect_compression(path: str, compression: str = "auto") -> str | None:
    """Resolve a compression option ('auto' inspects the file extension)."""
    if compression in (None, "none"):
        return None
    if compression != "auto":
        return compression
    lower = path.lower()
    for ext, name in _COMPRESSION_BY_EXT.items():
        if lower.endswith(ext):
            return name
    return None


def open_reader(path: str, compression: str | None) -> BinaryIO:
    """Open a binary stream for reading, wrapping in a decompressor if needed.

    Closing the returned stream always closes the underlying file, so callers
    that use it as a context manager never leak a descriptor.
    """
    if compression == "gzip":
        # Passing ``filename`` (not ``fileobj``) makes GzipFile own the file it
        # opened, so GzipFile.close() closes it. With ``fileobj`` the caller
        # keeps ownership and the descriptor survives until GC.
        return gzip.GzipFile(filename=path, mode="rb")
    raw = open(path, "rb")
    if compression == "zstd":
        try:
            import zstandard  # noqa: PLC0415
        except ImportError as exc:  # pragma: no cover - env dependent
            raw.close()
            raise ImportError(
                "zstandard is required for .zst files; install `datapipe[zstd]`"
            ) from exc
        return zstandard.ZstdDecompressor().stream_reader(raw, closefd=True)
    return raw


def open_writer(
    path: str, compression: str | None, *, level: int | None = None
) -> BinaryIO:
    """Open a binary stream for writing, wrapping in a compressor if needed."""
    raw = open(path, "wb")
    if compression == "gzip":
        return gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=level or 6)
    if compression == "zstd":
        try:
            import zstandard  # noqa: PLC0415
        except ImportError as exc:  # pragma: no cover - env dependent
            raw.close()
            raise ImportError(
                "zstandard is required for .zst files; install `datapipe[zstd]`"
            ) from exc
        cctx = zstandard.ZstdCompressor(level=level or 3)
        return cctx.stream_writer(raw)
    return raw


def iter_lines(
    fileobj: BinaryIO,
    encoding: str = "utf-8",
) -> Iterator[str]:
    """Yield decoded, newline-stripped lines from a binary stream."""
    buffer = io.TextIOWrapper(fileobj, encoding=encoding)
    try:
        for line in buffer:
            if line.endswith("\n"):
                line = line[:-1]
            if line.endswith("\r"):
                line = line[:-1]
            yield line
    finally:
        buffer.detach()


def list_jsonl_files(path: str) -> list[str]:
    """Return the sorted list of JSONL files for a path or directory.

    Directories are scanned for ``*.jsonl``, ``*.jsonl.gz`` and ``*.jsonl.zst``.
    """
    if os.path.isdir(path):
        names = []
        for entry in sorted(os.listdir(path)):
            full = os.path.join(path, entry)
            if os.path.isfile(full) and (
                entry.endswith(".jsonl")
                or entry.endswith(".jsonl.gz")
                or entry.endswith(".jsonl.zst")
            ):
                names.append(full)
        return names
    return [path]
