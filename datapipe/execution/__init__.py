"""Executors: local concurrency backends."""

from datapipe.execution.base import Executor, BoundedMapExecutor, ExecutionStats
from datapipe.execution.sequential import SequentialExecutor
from datapipe.execution.thread import ThreadExecutor
from datapipe.execution.process import ProcessExecutor

__all__ = [
    "Executor",
    "BoundedMapExecutor",
    "ExecutionStats",
    "SequentialExecutor",
    "ThreadExecutor",
    "ProcessExecutor",
]
