"""Unit tests for the stage model."""

from __future__ import annotations

import json

import pytest

from datapipe.context import WorkerContext
from datapipe.errors import PipelineValidationError
from datapipe.pipeline import CompiledPipeline
from datapipe.sentinels import DROP
from datapipe.stage import (
    FilterStage,
    GenericStage,
    JsonDumpStage,
    JsonLoadStage,
    Stage,
    TapStage,
    TransformStage,
    coerce_stage,
)


def ctx(**kw) -> WorkerContext:
    defaults = dict(rank=0, world_size=1, worker_id=0, record_index=None)
    defaults.update(kw)
    return WorkerContext(**defaults)


def test_transform_stage():
    s = TransformStage(lambda x: x * 2)
    assert s.process(4, ctx()) == 8


def test_filter_stage_keeps_and_drops():
    s = FilterStage(lambda x: x > 2)
    assert s.process(5, ctx()) == 5
    assert s.process(1, ctx()) is DROP


def test_generic_stage_input_process_output():
    s = GenericStage(
        input=json.loads,
        process=lambda d: {"v": d["v"] + 1},
        output=json.dumps,
        name="inc",
    )
    out = s.process('{"v": 1}', ctx())
    assert json.loads(out) == {"v": 2}


def test_generic_stage_drop_from_process():
    s = GenericStage(process=lambda x: DROP, name="drop_all")
    assert s.process(1, ctx()) is DROP


def test_generic_stage_drop_from_input():
    def inp(x):
        return DROP

    s = GenericStage(input=inp, process=lambda x: x, name="drop_in")
    assert s.process(1, ctx()) is DROP


def test_generic_stage_drop_from_output():
    def out(x):
        return DROP

    s = GenericStage(process=lambda x: x, output=out, name="drop_out")
    assert s.process(1, ctx()) is DROP


def test_generic_stage_with_context():
    seen = {}

    def proc(x, c):
        seen["rank"] = c.rank
        seen["world"] = c.world_size
        return x

    s = GenericStage(process=proc, name="ctx", with_context=True)
    s.process(10, ctx(rank=2, world_size=4))
    assert seen == {"rank": 2, "world": 4}


def test_generic_stage_setup_with_context():
    captured = {}

    def setup(c):
        captured["rank"] = c.rank

    s = GenericStage(process=lambda x: x, setup=setup, name="s", with_context=True)
    s.setup(ctx(rank=7))
    assert captured["rank"] == 7


def test_tap_stage_side_effect():
    calls = []
    s = TapStage(lambda x: calls.append(x), name="tap")
    assert s.process(42, ctx()) == 42
    assert calls == [42]


def test_json_stages():
    load = JsonLoadStage()
    dump = JsonDumpStage()
    assert load.process('{"a": 1}', ctx()) == {"a": 1}
    assert dump.process({"a": 1}, ctx()) == '{"a": 1}'


def test_compiled_pipeline_orders_stages():
    stages = [
        TransformStage(lambda x: x + 1, name="a"),
        TransformStage(lambda x: x * 10, name="b"),
        TransformStage(lambda x: x - 3, name="c"),
    ]
    cp = CompiledPipeline(stages)
    assert cp.process(1, ctx()) == ((1 + 1) * 10) - 3


def test_compiled_pipeline_stops_at_drop():
    cp = CompiledPipeline(
        [TransformStage(lambda x: x, name="a"), FilterStage(lambda x: False, name="f"), TransformStage(lambda x: 999, name="z")]
    )
    assert cp.process(5, ctx()) is DROP


def test_compiled_pipeline_wraps_error_with_stage_name():
    def boom(x):
        raise ValueError("nope")

    cp = CompiledPipeline([TransformStage(boom, name="explode")])
    c = ctx(record_index=12)
    with pytest.raises(Exception) as ei:
        cp.process(1, c)
    from datapipe.errors import StageExecutionError

    assert isinstance(ei.value, StageExecutionError)
    assert ei.value.stage_name == "explode"
    assert ei.value.record_seq == 12
    assert isinstance(ei.value.cause, ValueError)


def test_coerce_stage_callable():
    s = coerce_stage(lambda x: x)
    assert isinstance(s, TransformStage)


def test_coerce_stage_rejects_noncallable():
    with pytest.raises(PipelineValidationError):
        coerce_stage(42)


def test_stage_requires_process():
    with pytest.raises(PipelineValidationError):
        GenericStage(process=None)  # type: ignore[arg-type]


def test_custom_stage_lifecycle():
    calls = []

    class MyStage(Stage):
        name = "my"

        def setup(self, c):
            calls.append("setup")

        def process(self, v, c):
            calls.append("process")
            return v

        def teardown(self, c):
            calls.append("teardown")

    s = MyStage()
    c = ctx()
    s.setup(c)
    s.process(1, c)
    s.teardown(c)
    assert calls == ["setup", "process", "teardown"]
