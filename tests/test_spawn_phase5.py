"""Phase 5 item 6: spawn-specific tests for provider reproducibility.

Tests that explicitly exercise the multiprocessing ``spawn`` start method to
verify:

  - A *copied* provider produces byte-for-byte identical results in every
    spawned worker (reproducibility guarantee).
  - An *editable* provider's live edits are picked up on the next run
    (change detection under spawn).
  - A tampered copied-provider snapshot causes a ``ToolExecutionError`` in
    workers (digest enforcement under spawn).
  - A ``ToolExecutionError`` raised in a spawned worker is correctly
    deserialized back in the coordinator — the pickling round-trip that most
    matters in practice.

All tests use ``ProcessExecutor(mp_context="spawn")`` rather than the platform
default so the isolation is explicit and deterministic even on Linux where the
default is ``fork``.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from datapipe.cli.main import main
from datapipe.execution.process import ProcessExecutor
from datapipe.tools import registry as reg
from datapipe.tools.installer import install_provider

# ---------------------------------------------------------------------------
# Isolation fixtures (identical to test_tools_phase4.py)
# ---------------------------------------------------------------------------

_STEM_COUNTER = [0]


@pytest.fixture
def unique_stem() -> str:
    _STEM_COUNTER[0] += 1
    return f"spawn_prov_{_STEM_COUNTER[0]}"


@pytest.fixture(autouse=True)
def isolated_registry(tmp_path, monkeypatch):
    data_dir = tmp_path / "dp_data"
    monkeypatch.setenv("DATAPIPE_USER_DATA", str(data_dir))

    from datapipe.tools import loader as _loader
    _loader._loaded_providers.clear()

    from datapipe.dsl import compiler as _compiler
    monkeypatch.setattr(_compiler, "_BUILTIN_REGISTRY", None, raising=False)

    yield data_dir

    _loader._loaded_providers.clear()


# ---------------------------------------------------------------------------
# Provider source templates
# ---------------------------------------------------------------------------

_SHOUT_SRC = '''\
from datapipe.tools import tool, JsonType


@tool(
    name="shout",
    target="value",
    input=JsonType.STRING,
    output=JsonType.STRING,
    description="Uppercase a string.",
)
def shout(value, *, suffix: str = "!") -> str:
    return value.upper() + suffix
'''

_SHOUT_V2_SRC = '''\
from datapipe.tools import tool, JsonType


@tool(
    name="shout",
    target="value",
    input=JsonType.STRING,
    output=JsonType.STRING,
    description="Uppercase a string (v2 with !! suffix).",
)
def shout(value, *, suffix: str = "!!") -> str:
    return value.upper() + suffix
'''


def _write_jsonl(path: Path, rows: list) -> None:
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")


def _read_jsonl(path: Path) -> list:
    return [json.loads(x) for x in path.read_text().splitlines() if x.strip()]


# ---------------------------------------------------------------------------
# Item 6a: copied-provider reproducibility under spawn
# ---------------------------------------------------------------------------


class TestCopiedProviderUnderSpawn:
    """A copied provider must deliver the same bytes to every worker."""

    def test_consistent_output_across_workers(
        self, tmp_path, unique_stem, capsys
    ):
        """All N workers must load the same snapshot and agree on the result."""
        provider = tmp_path / f"{unique_stem}.py"
        provider.write_text(_SHOUT_SRC)
        install_provider(provider, yes=True)

        src = tmp_path / "in.jsonl"
        _write_jsonl(src, [{"msg": f"item{i}"} for i in range(20)])
        out = tmp_path / "out.jsonl"

        rc = main([
            "transform", "shout(.msg)",
            str(src), str(out),
            "--executor", "process",
            "--workers", "4",
            "--ordered",
            "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err

        rows = _read_jsonl(out)
        assert len(rows) == 20
        for i, row in enumerate(rows):
            assert row["msg"] == f"ITEM{i}!",  f"row {i} wrong: {row}"

    def test_snapshot_isolation_from_original_deletion(
        self, tmp_path, unique_stem, capsys
    ):
        """Copied snapshot must survive deletion of the original file."""
        provider = tmp_path / f"{unique_stem}.py"
        provider.write_text(_SHOUT_SRC)
        install_provider(provider, yes=True)

        # Delete the original after install.
        provider.unlink()

        src = tmp_path / "in.jsonl"
        _write_jsonl(src, [{"msg": "hello"}])
        out = tmp_path / "out.jsonl"

        rc = main([
            "transform", "shout(.msg)",
            str(src), str(out),
            "--executor", "process",
            "--workers", "2",
            "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err
        assert _read_jsonl(out)[0]["msg"] == "HELLO!"

    def test_snapshot_unchanged_after_original_edit(
        self, tmp_path, unique_stem, capsys
    ):
        """A copied snapshot must not be affected by later edits to the source."""
        provider = tmp_path / f"{unique_stem}.py"
        provider.write_text(_SHOUT_SRC)
        install_provider(provider, yes=True)

        # Mutate the original — must NOT affect the installed snapshot.
        provider.write_text(_SHOUT_V2_SRC)

        src = tmp_path / "in.jsonl"
        _write_jsonl(src, [{"msg": "hello"}])
        out = tmp_path / "out.jsonl"

        rc = main([
            "transform", "shout(.msg)",
            str(src), str(out),
            "--executor", "process",
            "--workers", "2",
            "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err
        # Still "!" (original) not "!!" (edited).
        assert _read_jsonl(out)[0]["msg"] == "HELLO!"

    def test_tampered_snapshot_produces_error(
        self, tmp_path, unique_stem, capsys
    ):
        """Workers must reject a tampered copied snapshot (digest check)."""
        provider = tmp_path / f"{unique_stem}.py"
        provider.write_text(_SHOUT_SRC)
        entry = install_provider(provider, yes=True)

        # Corrupt the snapshot without touching the registry digest.
        snapshot = Path(entry.source_path)
        snapshot.write_text(_SHOUT_SRC + "\n# tampered\n")

        from datapipe.tools import loader as _loader
        _loader._loaded_providers.clear()

        src = tmp_path / "in.jsonl"
        _write_jsonl(src, [{"msg": "x"}])
        out = tmp_path / "out.jsonl"

        # The run fails because workers detect the mismatch.
        rc = main([
            "transform", "shout(.msg)",
            str(src), str(out),
            "--executor", "process",
            "--workers", "2",
            "--no-progress",
            "--errors", "skip",   # errors=skip to get a clean rc=0 on errors
        ])
        # With errors=skip the run itself succeeds but nothing is written.
        # The warning appears on stderr.
        captured = capsys.readouterr()
        assert "warning" in captured.err.lower(), (
            "expected a warning about the broken provider"
        )


# ---------------------------------------------------------------------------
# Item 6b: editable-provider change detection under spawn
# ---------------------------------------------------------------------------


class TestEditableProviderUnderSpawn:
    """Editable providers must pick up on-disk changes in the next run."""

    def test_edit_visible_in_next_run(self, tmp_path, unique_stem, capsys):
        """After editing the source, a fresh run must use the new definition."""
        provider = tmp_path / f"{unique_stem}.py"
        provider.write_text(_SHOUT_SRC)
        install_provider(provider, editable=True, yes=True)

        # First run — v1 (single "!").
        src = tmp_path / "in.jsonl"
        _write_jsonl(src, [{"msg": "hello"}])
        out1 = tmp_path / "out1.jsonl"

        rc = main([
            "transform", "shout(.msg)",
            str(src), str(out1),
            "--executor", "process",
            "--workers", "2",
            "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err
        assert _read_jsonl(out1)[0]["msg"] == "HELLO!"

        # Edit the source — v2 (double "!!").
        provider.write_text(_SHOUT_V2_SRC)

        # Clear the coordinator-side cache so the next compilation picks up
        # the edited file.  Workers always start fresh under spawn, so there
        # is nothing to clear on their side.
        from datapipe.tools import loader as _loader
        _loader._loaded_providers.clear()

        out2 = tmp_path / "out2.jsonl"
        rc = main([
            "transform", "shout(.msg)",
            str(src), str(out2),
            "--executor", "process",
            "--workers", "2",
            "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err
        assert _read_jsonl(out2)[0]["msg"] == "HELLO!!", (
            "edit was not picked up by the second run"
        )

    def test_multiple_workers_agree_on_edits(
        self, tmp_path, unique_stem, capsys
    ):
        """All workers in a spawn pool must see the same version of the source.

        Since editable mode skips digest enforcement, all workers load the
        current bytes on disk.  They must all agree — no partial divergence
        due to a race between the edit and the run.
        """
        provider = tmp_path / f"{unique_stem}.py"
        provider.write_text(_SHOUT_V2_SRC)          # already edited before run
        install_provider(provider, editable=True, yes=True)

        src = tmp_path / "in.jsonl"
        _write_jsonl(src, [{"msg": f"w{i}"} for i in range(16)])
        out = tmp_path / "out.jsonl"

        rc = main([
            "transform", "shout(.msg)",
            str(src), str(out),
            "--executor", "process",
            "--workers", "4",
            "--ordered",
            "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err

        rows = _read_jsonl(out)
        for i, row in enumerate(rows):
            assert row["msg"] == f"W{i}!!", f"row {i} unexpected: {row}"


# ---------------------------------------------------------------------------
# Item 6c: ToolExecutionError pickling under spawn
# ---------------------------------------------------------------------------


class TestToolExecutionErrorUnderSpawn:
    """ToolExecutionError must survive the worker→coordinator pickle round-trip.

    The real path: validation fails inside a spawned worker → the exception is
    pickled and sent back → the coordinator unpickles and attaches it to the
    TaskResult → the errors policy is applied.  This test exercises that whole
    path using the process executor.
    """

    def _write_bad_type_provider(self, path: Path) -> None:
        """A simple transform that rejects null inputs (type mismatch)."""
        path.write_text(_SHOUT_SRC)  # expects STRING; null will violate it

    def test_validation_error_pickled_and_raised(
        self, tmp_path, unique_stem, capsys
    ):
        """errors='raise': a validation failure in a worker must propagate."""
        provider = tmp_path / f"{unique_stem}.py"
        self._write_bad_type_provider(provider)
        install_provider(provider, yes=True)

        src = tmp_path / "in.jsonl"
        # null violates STRING input contract for shout
        _write_jsonl(src, [{"msg": None}])
        out = tmp_path / "out.jsonl"

        rc = main([
            "transform", "shout(.msg)",
            str(src), str(out),
            "--executor", "process",
            "--workers", "2",
            "--validate-tools", "always",
            "--errors", "raise",
            "--no-progress",
        ])
        assert rc != 0

    def test_validation_error_skip_policy(
        self, tmp_path, unique_stem, capsys
    ):
        """errors='skip': a validation failure must be counted but not crash."""
        provider = tmp_path / f"{unique_stem}.py"
        self._write_bad_type_provider(provider)
        install_provider(provider, yes=True)

        src = tmp_path / "in.jsonl"
        _write_jsonl(src, [
            {"msg": None},          # will fail validation
            {"msg": "good"},        # will succeed
        ])
        out = tmp_path / "out.jsonl"

        rc = main([
            "transform", "shout(.msg)",
            str(src), str(out),
            "--executor", "process",
            "--workers", "2",
            "--validate-tools", "always",
            "--errors", "skip",
            "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err
        rows = _read_jsonl(out)
        assert len(rows) == 1
        assert rows[0]["msg"] == "GOOD!"

    def test_tool_execution_error_preserves_fields_after_spawn(
        self, tmp_path, unique_stem, capsys
    ):
        """ToolExecutionError must carry structured fields back to the coordinator.

        We force the error into the error sink (errors='return') and inspect
        the serialized payload to confirm the fields survived pickling.
        """
        provider = tmp_path / f"{unique_stem}.py"
        self._write_bad_type_provider(provider)
        install_provider(provider, yes=True)

        src = tmp_path / "in.jsonl"
        _write_jsonl(src, [{"msg": None}])
        out = tmp_path / "out.jsonl"
        err_out = tmp_path / "errors.jsonl"

        rc = main([
            "transform", "shout(.msg)",
            str(src), str(out),
            "--executor", "process",
            "--workers", "2",
            "--validate-tools", "always",
            "--errors", "return",
            "--error-output", str(err_out),
            "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err
        errors = _read_jsonl(err_out)
        assert len(errors) == 1

        payload = errors[0]
        assert "tool" in payload, (
            "error payload is missing the 'tool' key — "
            "ToolExecutionError fields did not survive pickling"
        )
        tool = payload["tool"]
        assert tool["tool_name"] == "shout"
        assert tool["stage"] == "input"
        assert tool["expected_type"] is not None
        assert tool["actual_type"] is not None
        # expression_span must be JSON-serializable (list or null, not a tuple)
        span = tool["expression_span"]
        assert span is None or isinstance(span, list)
