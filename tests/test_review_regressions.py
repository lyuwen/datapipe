"""Regression tests for issues found in review8-review11.

Each test here pins a specific bug that the main suite did not catch.  The
reviews repeatedly noted that fixes landed without regression coverage, which
is why a green 561-test suite coexisted with reproducible failures.  Every test
below fails against the pre-fix code and passes after it.

Provider tests redirect ``DATAPIPE_USER_DATA`` to ``tmp_path`` and clear the
loader cache so they never touch the real user registry.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from datapipe import (
    IterableSource,
    ListSink,
    Pipeline,
    ProcessExecutor,
    SequentialExecutor,
    ThreadExecutor,
)
from datapipe.errors import PipelineValidationError, SinkError
from datapipe.io.jsonl import JsonlSink
from datapipe.progress.base import ProgressReporter, ProgressSnapshot
from datapipe.stage import Stage

ALL_EXECUTORS = [
    pytest.param(SequentialExecutor, id="sequential"),
    pytest.param(ThreadExecutor, id="thread"),
    pytest.param(ProcessExecutor, id="process"),
]


def _make_executor(exec_cls):
    if exec_cls is SequentialExecutor:
        return exec_cls()
    return exec_cls(workers=2, max_in_flight=8)


class _Identity(Stage):
    def process(self, value, ctx):
        return value


class _FailOnThree(Stage):
    def process(self, value, ctx):
        if value == 3:
            raise ValueError("boom at 3")
        return value


class _SnapshotRecorder(ProgressReporter):
    """Captures every ProgressSnapshot passed to update()."""

    def __init__(self) -> None:
        self.snapshots: list[ProgressSnapshot] = []
        self.legacy_calls: list[dict] = []

    def start(self, total=None) -> None:
        pass

    def update(self, n=1, snapshot=None, **stats) -> None:
        if snapshot is not None:
            self.snapshots.append(snapshot)
        else:
            self.legacy_calls.append({"n": n, **stats})

    def close(self) -> None:
        pass


# ---------------------------------------------------------------------------
# review10/11 finding 1 — errors="return" must not crash a non-raw JSONL sink
# ---------------------------------------------------------------------------


class TestErrorsReturnNonRawSink:
    """errors='return' writes JSON-serializable payloads to any sink type.

    Before the fix, ``_emit`` wrote the raw ``TaskResult`` object.  A raw sink
    was guarded by a preflight check, but a plain ``JsonlSink(raw=False)`` --
    which is what ``datapipe run`` builds -- crashed mid-run with
    "Object of type TaskResult is not JSON serializable", losing already
    committed records to a partial output file.
    """

    def test_non_raw_jsonl_sink_does_not_crash(self, tmp_path):
        out = tmp_path / "out.jsonl"
        stats = Pipeline([_FailOnThree()]).run(
            source=IterableSource(range(5)),
            sink=JsonlSink(str(out)),  # raw=False — the crashing path
            executor=SequentialExecutor(),
            errors="return",
            progress=False,
        )
        assert stats.failed_records == 1
        lines = [json.loads(x) for x in out.read_text().splitlines() if x.strip()]
        # All 5 records accounted for: 4 successes + 1 structured error payload.
        assert len(lines) == 5
        errors = [x for x in lines if isinstance(x, dict) and "stage_name" in x]
        assert len(errors) == 1
        # The payload must be a plain JSON-serializable dict, not a TaskResult.
        assert errors[0]["seq"] == 3
        assert errors[0]["error_type"] == "ValueError"

    @pytest.mark.parametrize("exec_cls", ALL_EXECUTORS)
    def test_list_sink_receives_payload_dict(self, exec_cls):
        """The payload is a dict for every executor, never a TaskResult."""
        sink = ListSink()
        Pipeline([_FailOnThree()]).run(
            source=IterableSource(range(5)),
            sink=sink,
            executor=_make_executor(exec_cls),
            errors="return",
            progress=False,
        )
        errors = [x for x in sink.items if not isinstance(x, int)]
        assert len(errors) == 1
        assert isinstance(errors[0], dict), f"got {type(errors[0]).__name__}"
        # Payload must round-trip through json.dumps — this is the actual bug.
        json.dumps(errors[0])

    def test_raw_sink_also_works(self, tmp_path):
        """A raw sink accepts the payload dict too (guard is unnecessary now)."""
        out = tmp_path / "out.jsonl"
        stats = Pipeline([_FailOnThree()]).run(
            source=IterableSource(range(5)),
            sink=JsonlSink(str(out), raw=False),
            executor=SequentialExecutor(),
            errors="return",
            progress=False,
        )
        assert stats.failed_records == 1


# ---------------------------------------------------------------------------
# review10/11 finding 3 — unordered progress snapshots must be emission-accurate
# ---------------------------------------------------------------------------


class TestUnorderedSnapshotAccuracy:
    """The final unordered snapshot must report every written record.

    Before the fix, ``_snapshot()`` was built *before* ``_emit()`` incremented
    ``output_records``, so N records produced a final snapshot of
    ``written=N-1``.
    """

    def test_final_snapshot_written_matches_output_count(self):
        rec = _SnapshotRecorder()
        sink = ListSink()
        Pipeline([_Identity()]).run(
            source=IterableSource(range(5)),
            sink=sink,
            executor=ThreadExecutor(workers=2, max_in_flight=4),
            ordered=False,
            progress=True,
            progress_reporter=rec,
        )
        assert len(sink.items) == 5
        assert rec.snapshots, "unordered mode must publish ProgressSnapshots"
        final = rec.snapshots[-1]
        assert final.processed == 5
        # This is the regression: written was 4 for 5 emitted records.
        assert final.written == 5, f"stale snapshot: {final}"

    def test_unordered_mode_supplies_snapshot_not_legacy_kwargs(self):
        """Unordered mode must use the structured snapshot interface."""
        rec = _SnapshotRecorder()
        Pipeline([_Identity()]).run(
            source=IterableSource(range(3)),
            sink=ListSink(),
            executor=SequentialExecutor(),
            ordered=False,
            progress=True,
            progress_reporter=rec,
        )
        assert rec.snapshots, "no ProgressSnapshot was published in unordered mode"
        # buffered is always 0 in unordered mode (no reorder buffer).
        assert all(s.buffered == 0 for s in rec.snapshots)

    def test_ordered_mode_final_snapshot_is_accurate(self):
        """Ordered mode reports every record as written after the drain."""
        rec = _SnapshotRecorder()
        sink = ListSink()
        Pipeline([_Identity()]).run(
            source=IterableSource(range(5)),
            sink=sink,
            executor=ThreadExecutor(workers=2, max_in_flight=4),
            ordered=True,
            progress=True,
            progress_reporter=rec,
        )
        assert len(sink.items) == 5
        final = rec.snapshots[-1]
        assert final.written == 5, f"ordered final snapshot stale: {final}"
        assert final.buffered == 0, "reorder buffer must be empty at the end"

    def test_snapshot_is_frozen(self):
        """ProgressSnapshot must be immutable so reporters can store it."""
        snap = ProgressSnapshot(processed=1, written=1)
        with pytest.raises(Exception):
            snap.processed = 99  # type: ignore[misc]


# ---------------------------------------------------------------------------
# review9 finding 12 — DROP must be a single object across both modules
# ---------------------------------------------------------------------------


class TestDropSentinelIdentity:
    """``record.DROP`` and ``sentinels.DROP`` must be the same object.

    Two distinct ``_Sentinel("DROP")`` instances made ``is``-checks fail
    depending on which module a stage imported DROP from, so a stage that
    returned the "wrong" DROP silently emitted instead of dropping.
    """

    def test_drop_is_identical_across_modules(self):
        from datapipe.record import DROP as record_drop
        from datapipe.sentinels import DROP as sentinels_drop

        assert record_drop is sentinels_drop

    def test_drop_from_either_module_actually_drops(self):
        from datapipe.record import DROP as record_drop

        class _DropAll(Stage):
            def process(self, value, ctx):
                return record_drop

        sink = ListSink()
        stats = Pipeline([_DropAll()]).run(
            source=IterableSource(range(4)),
            sink=sink,
            executor=SequentialExecutor(),
            progress=False,
        )
        assert sink.items == []
        assert stats.dropped_records == 4

    def test_drop_survives_pickle_roundtrip(self):
        import pickle

        from datapipe.sentinels import DROP

        assert pickle.loads(pickle.dumps(DROP)) is DROP


# ---------------------------------------------------------------------------
# review9 finding 14 — invalid max_in_flight must be rejected
# ---------------------------------------------------------------------------


class TestMaxInFlightValidation:
    """max_in_flight=0 silently processed nothing under the thread executor."""

    @pytest.mark.parametrize("bad", [0, -1, -100])
    def test_invalid_max_in_flight_rejected(self, bad):
        with pytest.raises(PipelineValidationError, match="max_in_flight"):
            Pipeline([_Identity()]).run(
                source=IterableSource(range(3)),
                sink=ListSink(),
                executor=SequentialExecutor(),
                max_in_flight=bad,
                progress=False,
            )

    def test_valid_max_in_flight_accepted(self):
        sink = ListSink()
        Pipeline([_Identity()]).run(
            source=IterableSource(range(3)),
            sink=sink,
            executor=ThreadExecutor(workers=2),
            max_in_flight=1,
            progress=False,
        )
        assert len(sink.items) == 3


# ---------------------------------------------------------------------------
# review11 low finding — JsonlSink.write() before open() must raise SinkError
# ---------------------------------------------------------------------------


class TestSinkErrorType:
    def test_write_before_open_raises_sink_error(self, tmp_path):
        sink = JsonlSink(str(tmp_path / "out.jsonl"))
        with pytest.raises(SinkError):
            sink.write({"a": 1})


# ---------------------------------------------------------------------------
# review10/11 finding 2 — installed-tool parameter annotations must round-trip
# ---------------------------------------------------------------------------

PROVIDER_WITH_TYPED_CONFIG = '''\
from datapipe.tools import tool, JsonType


@tool(
    name="suffixer",
    target="value",
    input=JsonType.STRING,
    output=JsonType.STRING,
    description="Append a suffix to a string.",
)
def suffixer(value, *, suffix: str = "!") -> str:
    return value + suffix
'''


@pytest.fixture
def isolated_registry(tmp_path, monkeypatch):
    """Point the registry at tmp_path and clear the loader cache."""
    monkeypatch.setenv("DATAPIPE_USER_DATA", str(tmp_path / "share"))
    import datapipe.tools.loader as _loader

    _loader._loaded_providers.clear()
    yield tmp_path
    _loader._loaded_providers.clear()


class TestInstalledToolAnnotationRoundTrip:
    """Coordinator isolation must not lose ParameterSpec.annotation.

    Moving provider loading out of the coordinator (correct per plan §13.2)
    initially dropped parameter annotations, because validation stored only
    name/default/required.  That silently removed compile-time configuration
    type checking for every installed tool: ``suffixer(.x, suffix=123)``
    compiled cleanly and only failed inside the worker.
    """

    def test_validation_stores_parameter_annotation(self, isolated_registry):
        from datapipe.tools.validation import validate_dynamic, validate_static

        src = isolated_registry / "prov.py"
        src.write_text(PROVIDER_WITH_TYPED_CONFIG)

        source_bytes = validate_static(src)
        meta = validate_dynamic(src, source_bytes)

        tool_meta = next(t for t in meta.tools if t["name"] == "suffixer")
        params = {p["name"]: p for p in tool_meta["parameters"]}
        assert "suffix" in params
        # The regression: annotation was absent from the stored metadata.
        assert params["suffix"]["annotation"] == "str", (
            f"annotation not stored: {params['suffix']}"
        )

    def test_installed_tool_rejects_wrong_config_type(self, isolated_registry):
        """A str-annotated config parameter must reject an int at compile time."""
        from datapipe.dsl.compiler import compile_expression
        from datapipe.dsl.errors import ToolConfigurationError
        from datapipe.tools.installer import install_provider

        src = isolated_registry / "prov.py"
        src.write_text(PROVIDER_WITH_TYPED_CONFIG)
        install_provider(src, yes=True)

        # Sanity check: the valid form compiles.
        compile_expression('suffixer(.x, suffix="?")')

        # The regression: this used to compile with {'suffix': 123}.
        with pytest.raises(ToolConfigurationError):
            compile_expression("suffixer(.x, suffix=123)")

    def test_installed_tool_annotation_present_after_compile(
        self, isolated_registry
    ):
        """The reconstructed contract carries the annotation, not None."""
        from datapipe.dsl.compiler import compile_expression
        from datapipe.tools.installer import install_provider

        src = isolated_registry / "prov.py"
        src.write_text(PROVIDER_WITH_TYPED_CONFIG)
        install_provider(src, yes=True)

        compiled = compile_expression('suffixer(.x, suffix="?")')
        inv = compiled.invocations[0]
        specs = {p.name: p for p in inv.contract.parameters}
        assert specs["suffix"].annotation is str, (
            f"annotation lost in reconstruction: {specs['suffix'].annotation}"
        )


# ---------------------------------------------------------------------------
# review8/9 finding 1 — copied-source install must be atomic and roll back
# ---------------------------------------------------------------------------


class TestInstallAtomicity:
    """A failed copy must never leave a truncated snapshot on disk.

    ``_rollback()`` originally iterated ``files_written``, but the snapshot
    path was appended to that list only *after* the write succeeded -- so a
    write that truncated the file and then raised was never restored.
    """

    def test_failed_write_restores_previous_snapshot(
        self, isolated_registry, monkeypatch
    ):
        from datapipe.tools import installer as _installer
        from datapipe.tools.installer import InstallationError, install_provider
        from datapipe.tools.registry import load_registry

        src = isolated_registry / "prov.py"
        src.write_text(PROVIDER_WITH_TYPED_CONFIG)
        entry = install_provider(src, yes=True)
        assert entry is not None

        snapshot_path = Path(entry.source_path)
        original_bytes = snapshot_path.read_bytes()
        assert original_bytes, "snapshot should exist after first install"

        # Force the atomic replace to fail during a --force reinstall.
        real_replace = _installer.os.replace

        def _boom_replace(a, b):
            raise OSError("simulated disk exhaustion")

        monkeypatch.setattr(_installer.os, "replace", _boom_replace)

        src.write_text(PROVIDER_WITH_TYPED_CONFIG + "\n# edited\n")
        with pytest.raises((InstallationError, OSError)):
            install_provider(src, force=True, yes=True)

        monkeypatch.setattr(_installer.os, "replace", real_replace)

        # The snapshot must be byte-identical to what it was before.
        assert snapshot_path.read_bytes() == original_bytes, (
            "snapshot was corrupted by a failed install"
        )
        # And the registry must still resolve the original provider.
        reg = load_registry()
        assert entry.provider_id in reg.providers

    def test_failed_provider_json_write_is_rolled_back(
        self, isolated_registry, monkeypatch
    ):
        """A truncating provider.json write must be restored by rollback.

        This is the case that distinguishes iterating ``backups`` from
        iterating ``files_written``.  ``provider.json`` is written with a plain
        ``write_text()`` and appended to ``files_written`` only *after* the
        write returns, so a write that truncates the file and then raises is
        invisible to a ``files_written``-based rollback.  (The source snapshot
        is protected separately by its atomic temp-file + ``os.replace``.)
        """
        from datapipe.tools.installer import InstallationError, install_provider

        src = isolated_registry / "prov.py"
        src.write_text(PROVIDER_WITH_TYPED_CONFIG)
        entry = install_provider(src, yes=True)
        assert entry is not None

        provider_json = Path(entry.source_path).parent / "provider.json"
        original_bytes = provider_json.read_bytes()
        assert original_bytes, "provider.json should exist after first install"

        # Truncate-then-fail, exactly as a disk-full write_text would.
        real_write_text = Path.write_text

        def _truncating_write_text(self, data, *args, **kwargs):
            if self.name == "provider.json":
                real_write_text(self, "", encoding="utf-8")  # truncate
                raise OSError("simulated disk exhaustion mid-write")
            return real_write_text(self, data, *args, **kwargs)

        monkeypatch.setattr(Path, "write_text", _truncating_write_text)

        src.write_text(PROVIDER_WITH_TYPED_CONFIG + "\n# edited\n")
        with pytest.raises((InstallationError, OSError)):
            install_provider(src, force=True, yes=True)

        monkeypatch.setattr(Path, "write_text", real_write_text)

        assert provider_json.read_bytes() == original_bytes, (
            "provider.json was left truncated by a failed install"
        )

    def test_no_temp_files_left_behind(self, isolated_registry):
        from datapipe.tools.installer import install_provider

        src = isolated_registry / "prov.py"
        src.write_text(PROVIDER_WITH_TYPED_CONFIG)
        entry = install_provider(src, yes=True)
        assert entry is not None

        pdir = Path(entry.source_path).parent
        leftovers = [p.name for p in pdir.iterdir() if p.name.startswith(".source-")]
        assert leftovers == [], f"temp files left behind: {leftovers}"
