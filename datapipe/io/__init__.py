"""IO adapters: sources and sinks."""

from datapipe.io.base import Source, Sink, SourceRecordError
from datapipe.io.iterable import IterableSource, ListSink, CallableSink
from datapipe.io.jsonl import JsonlSource, JsonlSink
from datapipe.io.parquet import ParquetSource, ParquetSink

__all__ = [
    "Source",
    "Sink",
    "SourceRecordError",
    "IterableSource",
    "ListSink",
    "CallableSink",
    "JsonlSource",
    "JsonlSink",
    "ParquetSource",
    "ParquetSink",
]
