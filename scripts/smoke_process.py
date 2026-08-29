"""End-to-end ProcessExecutor smoke test (run as a script)."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datapipe import (  # noqa: E402
    FilterStage,
    GenericStage,
    IterableSource,
    ListSink,
    Pipeline,
    ProcessExecutor,
)


def normalize(x):
    return {"text": x.strip().upper(), "length": 0}


def is_valid(x):
    return len(x["text"]) > 0


def score(x):
    x["length"] = len(x["text"])
    return x


def main():
    pipeline = Pipeline(
        [
            GenericStage(process=normalize, name="normalize"),
            FilterStage(is_valid),
            GenericStage(process=score, name="score"),
        ]
    )
    sink = ListSink()
    stats = pipeline.run(
        source=IterableSource(["hello", "", "  world  ", "foo bar"]),
        sink=sink,
        executor=ProcessExecutor(workers=2, max_in_flight=4),
        ordered=True,
        progress=False,
    )
    print("items:", sink.items)
    print("stats:", stats)
    assert sink.items == [
        {"text": "HELLO", "length": 5},
        {"text": "WORLD", "length": 5},
        {"text": "FOO BAR", "length": 7},
    ], sink.items
    assert stats.completed_records == 4
    assert stats.output_records == 3
    assert stats.dropped_records == 1
    print("OK")


if __name__ == "__main__":
    main()
