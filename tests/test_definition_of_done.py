"""The plan §55 "Definition of Done" integration test.

Reproduces the canonical JSONL -> JSONL example end-to-end with a
ProcessExecutor, ordered output, and real worker setup.
"""

from __future__ import annotations

import json
import os
import time

from datapipe import (
    FilterStage,
    GenericStage,
    JsonlSink,
    JsonlSource,
    Pipeline,
    ProcessExecutor,
)
from datapipe.errors import StageExecutionError
from datapipe.stage import Stage


def _normalize(x):
    x["text"] = x["text"].strip()
    return x


def _is_valid(x):
    return bool(x["text"])


def _score(x):
    x["length"] = len(x["text"])
    return x


def _make_input(path, n=1000):
    with open(path, "w") as f:
        for i in range(n):
            f.write(json.dumps({"id": i, "text": f"  row {i}  "}) + "\n")


def test_definition_of_done(tmp_path):
    inp = str(tmp_path / "input.jsonl")
    out = str(tmp_path / "output.jsonl")
    _make_input(inp)

    pipeline = Pipeline(
        [
            GenericStage(input=json.loads, process=_normalize, name="normalize"),
            FilterStage(_is_valid),
            GenericStage(process=_score, name="score"),
        ]
    )

    stats = pipeline.run(
        source=JsonlSource(inp, raw=True),
        sink=JsonlSink(out),
        executor=ProcessExecutor(workers=4, max_in_flight=32),
        ordered=True,
        progress=False,
    )

    with open(out) as f:
        lines = [json.loads(l) for l in f if l.strip()]
    assert len(lines) == 1000
    assert all(l["length"] == len(l["text"]) for l in lines)
    # Ordered: ids ascending.
    assert [l["id"] for l in lines] == list(range(1000))
    assert stats.completed_records == 1000
    assert stats.output_records == 1000


def test_definition_of_done_with_errors(tmp_path):
    """A malformed row can be skipped when configured (acceptance #7)."""
    inp = str(tmp_path / "input.jsonl")
    out = str(tmp_path / "output.jsonl")
    with open(inp, "w") as f:
        f.write('{"id": 0, "text": "ok"}\n')
        f.write("NOT JSON\n")
        f.write('{"id": 2, "text": "ok2"}\n')

    pipeline = Pipeline([GenericStage(input=json.loads, process=_normalize, name="n")])
    stats = pipeline.run(
        source=JsonlSource(inp, raw=True),
        sink=JsonlSink(out),
        executor=ProcessExecutor(workers=2, max_in_flight=8),
        ordered=True,
        errors="skip",
        progress=False,
    )
    with open(out) as f:
        lines = [json.loads(l) for l in f if l.strip()]
    assert len(lines) == 2
    assert [l["id"] for l in lines] == [0, 2]
    assert stats.failed_records == 1


def _identity(x):
    return x


class _HeavySetupStage(Stage):
    """Simulates a heavyweight resource loaded once per worker in setup."""

    name = "heavy"

    def __init__(self, setup_time=0.01):
        self.setup_time = setup_time
        self.setup_count = 0

    def setup(self, ctx):
        time.sleep(self.setup_time)
        self.setup_count += 1

    def process(self, value, ctx):
        if self.setup_count != 1:
            raise RuntimeError(f"expected 1 setup, got {self.setup_count}")
        return value


def test_worker_setup_runs_once(tmp_path):
    """Heavy state is initialized once per worker (plan §28, invariant 7)."""
    inp = str(tmp_path / "in.jsonl")
    out = str(tmp_path / "out.jsonl")
    _make_input(inp, n=50)

    stage = _HeavySetupStage()
    pipeline = Pipeline(
        [GenericStage(process=_identity, name="n"), stage]
    )
    stats = pipeline.run(
        source=JsonlSource(inp),
        sink=JsonlSink(out),
        executor=ProcessExecutor(workers=3, max_in_flight=12),
        progress=False,
    )
    with open(out) as f:
        n = sum(1 for _ in f)
    assert n == 50
    assert stats.completed_records == 50
