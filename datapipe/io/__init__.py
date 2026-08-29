"""IO adapters: sources and sinks."""

from datapipe.io.base import Source, Sink
from datapipe.io.iterable import IterableSource, ListSink, CallableSink
from datapipe.io.jsonl import JsonlSource, JsonlSink
from datapipe.io.parquet import ParquetSource, ParquetSink

__all__ = [
    "Source",
    "Sink",
    "IterableSource",
    "ListSink",
    "CallableSink",
    "JsonlSource",
    "JsonlSink",
    "ParquetSource",
    "ParquetSink",
]
