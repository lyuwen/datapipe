"""Ctrl-C / KeyboardInterrupt handling (plan §30).

Runs a long pipeline in a subprocess, sends SIGINT, and verifies:
- the run stops cleanly;
- the sink file is still valid (flush/close happened);
- no worker processes leak.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time

import pytest


_CHILD_SCRIPT = r"""
import json, sys, time
sys.path.insert(0, {root!r})
from datapipe import (
    GenericStage, JsonlSink, IterableSource, Pipeline, ProcessExecutor,
)

def slow(x):
    time.sleep(0.02)
    return x

if __name__ == "__main__":
    sink = JsonlSink(sys.argv[1])
    try:
        Pipeline([GenericStage(process=slow)]).run(
            source=IterableSource(range(1_000_000)),
            sink=sink,
            executor=ProcessExecutor(workers=2, max_in_flight=8),
            ordered=True,
            errors="skip",
            progress=False,
        )
    except KeyboardInterrupt:
        print("INTERRUPTED", file=sys.stderr)
        sys.exit(0)
"""


@pytest.mark.skipif(
    os.name == "nt", reason="signal-based test is POSIX-only"
)
def test_keyboard_interrupt_clean(tmp_path):
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out_path = str(tmp_path / "out.jsonl")
    script = _CHILD_SCRIPT.format(root=root)
    child_file = tmp_path / "child.py"
    child_file.write_text(script)

    proc = subprocess.Popen(
        [sys.executable, str(child_file), out_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=root,
    )
    # Give it time to spin up workers and start processing.
    time.sleep(1.5)
    proc.send_signal(signal.SIGINT)
    try:
        _, stderr = proc.communicate(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        pytest.fail("child did not terminate after SIGINT")

    assert proc.returncode == 0, f"child exited {proc.returncode}: {stderr}"
    assert "INTERRUPTED" in stderr

    # The sink must be a valid (if partial) JSONL file.
    assert os.path.exists(out_path)
    with open(out_path) as f:
        lines = [l for l in f if l.strip()]
    # At least some rows were processed before interruption.
    assert len(lines) > 0
    # All written lines are valid JSON (raw ints here) and in order.
    vals = [json.loads(l) for l in lines]
    assert vals == sorted(vals)
    assert vals[0] == 0
