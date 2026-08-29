"""Pipeline construction/validation tests (no execution)."""

from __future__ import annotations

import json

import pytest

from datapipe import (
    FilterStage,
    GenericStage,
    IterableSource,
    JsonDumpStage,
    JsonLoadStage,
    ListSink,
    Pipeline,
    SequentialExecutor,
)
from datapipe.errors import PipelineValidationError


def test_empty_pipeline_rejected():
    with pytest.raises(PipelineValidationError):
        Pipeline([])


def test_plain_callables_coerced():
    p = Pipeline([lambda x: x + 1, lambda x: x * 2])
    assert len(p) == 2
    from datapipe.stage import TransformStage

    assert all(isinstance(s, TransformStage) for s in p.stages)


def test_duplicate_explicit_names_rejected():
    with pytest.raises(PipelineValidationError):
        Pipeline(
            [
                GenericStage(process=lambda x: x, name="same"),
                GenericStage(process=lambda x: x, name="same"),
            ]
        )


def test_auto_names_deduplicated():
    p = Pipeline([json.loads, json.loads])
    names = p.stage_names()
    assert len(set(names)) == 2


def test_non_stage_entry_rejected():
    with pytest.raises(PipelineValidationError):
        Pipeline([42])  # type: ignore[list-item]


def test_pipeline_inert_until_run():
    """Invariant 1: constructing a Pipeline does no data movement."""
    calls = []

    def proc(x):
        calls.append(x)
        return x

    p = Pipeline([GenericStage(process=proc, name="p")])
    assert calls == []  # nothing ran yet


def test_run_sequential_roundtrip():
    p = Pipeline([GenericStage(process=lambda x: x + 1, name="inc")])
    sink = ListSink()
    stats = p.run(
        source=IterableSource([1, 2, 3]),
        sink=sink,
        executor=SequentialExecutor(),
        progress=False,
    )
    assert sink.items == [2, 3, 4]
    assert stats.completed_records == 3
    assert stats.output_records == 3
    assert stats.input_records == 3


def test_drop_accounting():
    p = Pipeline([FilterStage(lambda x: x % 2 == 0, name="evens")])
    sink = ListSink()
    stats = p.run(
        source=IterableSource(range(10)),
        sink=sink,
        executor=SequentialExecutor(),
        progress=False,
    )
    assert sink.items == [0, 2, 4, 6, 8]
    assert stats.dropped_records == 5
    assert stats.output_records == 5
    assert stats.completed_records == 10


def test_ordered_default_is_true():
    p = Pipeline([GenericStage(process=lambda x: x, name="id")])
    sink = ListSink()
    p.run(
        source=IterableSource([3, 1, 2]),
        sink=sink,
        executor=SequentialExecutor(),
        progress=False,
    )
    assert sink.items == [3, 1, 2]  # sequential is trivially ordered


def test_jsonl_stages_compose_in_pipeline():
    p = Pipeline([JsonLoadStage(), GenericStage(process=lambda d: {"v": d["v"] * 2}, name="d"), JsonDumpStage()])
    sink = ListSink()
    p.run(
        source=IterableSource(['{"v": 1}', '{"v": 2}']),
        sink=sink,
        executor=SequentialExecutor(),
        progress=False,
    )
    assert sink.items == ['{"v": 2}', '{"v": 4}']


def test_error_policy_validation():
    p = Pipeline([GenericStage(process=lambda x: x, name="x")])
    with pytest.raises(PipelineValidationError):
        p.run(
            source=IterableSource([1]),
            sink=ListSink(),
            executor=SequentialExecutor(),
            errors="bogus",
            progress=False,
        )
