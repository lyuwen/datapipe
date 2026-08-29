"""datapipe: parallel record processing pipeline.

Define a per-record processing program, then execute that program
concurrently over a stream.
"""

from __future__ import annotations

from datapipe.context import WorkerContext
from datapipe.execution import (
    ProcessExecutor,
    SequentialExecutor,
    ThreadExecutor,
)
from datapipe.io import (
    CallableSink,
    IterableSource,
    JsonlSink,
    JsonlSource,
    ListSink,
    ParquetSink,
    ParquetSource,
    SourceRecordError,
)
from datapipe.pipeline import Pipeline
from datapipe.result import ExecutionStats, TaskResult
from datapipe.runtime import RuntimeContext, default_sharding_for
from datapipe.sentinels import DROP
from datapipe.sharding import (
    HashSharding,
    ModuloSharding,
    NoSharding,
    RangeSharding,
)
from datapipe.stage import (
    FilterStage,
    GenericStage,
    JsonDumpStage,
    JsonLoadStage,
    Stage,
    TapStage,
    TransformStage,
)

__version__ = "0.1.0"

__all__ = [
    # Pipeline + stages
    "Pipeline",
    "Stage",
    "GenericStage",
    "TransformStage",
    "FilterStage",
    "TapStage",
    "JsonLoadStage",
    "JsonDumpStage",
    # Execution
    "ProcessExecutor",
    "ThreadExecutor",
    "SequentialExecutor",
    # Runtime / sharding
    "RuntimeContext",
    "default_sharding_for",
    "NoSharding",
    "ModuloSharding",
    "HashSharding",
    "RangeSharding",
    # IO
    "JsonlSource",
    "JsonlSink",
    "ParquetSource",
    "ParquetSink",
    "IterableSource",
    "CallableSink",
    "ListSink",
    "SourceRecordError",
    # Results / misc
    "WorkerContext",
    "TaskResult",
    "ExecutionStats",
    "DROP",
    "__version__",
]
