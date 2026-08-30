"""Phase 4 tests: provider registry, validation, installer, loader, tools CLI.

Isolation notes
---------------
Every test redirects ``DATAPIPE_USER_DATA`` to a per-test ``tmp_path`` so the
user's real registry is never read or written.

Two pieces of global state need explicit cleanup between tests:

* ``datapipe.tools.loader._loaded_providers`` caches imported providers by
  provider_id, so a stale entry would mask a re-import.
* Providers are imported into ``sys.modules`` under their file stem, so two
  tests using the same stem would collide.  The ``unique_stem`` fixture hands
  out a fresh stem per test.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from datapipe.cli.main import main
from datapipe.tools import registry as reg
from datapipe.tools.descriptor import ProviderDescriptor, ToolDescriptor
from datapipe.tools.installer import InstallationError, install_provider
from datapipe.tools.installer import remove_provider as installer_remove
from datapipe.tools.loader import ProviderLoadError, load_provider, resolve_tool
from datapipe.tools.validation import (
    MAX_SOURCE_BYTES,
    ProviderValidationError,
    StaticValidationError,
    compute_digest,
    validate_dynamic,
    validate_static,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_STEM_COUNTER = [0]


@pytest.fixture
def unique_stem() -> str:
    """Return a module stem unique to this test, avoiding sys.modules clashes."""
    _STEM_COUNTER[0] += 1
    return f"phase4_prov_{_STEM_COUNTER[0]}"


@pytest.fixture(autouse=True)
def isolated_registry(tmp_path, monkeypatch):
    """Point the registry at tmp_path and clear the loader cache."""
    data_dir = tmp_path / "dp_data"
    monkeypatch.setenv("DATAPIPE_USER_DATA", str(data_dir))

    from datapipe.tools import loader as _loader
    _loader._loaded_providers.clear()

    # Reset the compiler's built-in registry singleton so each test rebuilds
    # the full registry against its own isolated provider set.
    from datapipe.dsl import compiler as _compiler
    monkeypatch.setattr(_compiler, "_BUILTIN_REGISTRY", None, raising=False)

    yield data_dir

    _loader._loaded_providers.clear()


PROVIDER_SRC = '''\
from datapipe.tools import tool, JsonType


@tool(
    name="shout",
    target="value",
    input=JsonType.STRING,
    output=JsonType.STRING,
    description="Uppercase a string and append a suffix.",
)
def shout(value, *, suffix: str = "!") -> str:
    return value.upper() + suffix
'''


@pytest.fixture
def provider_file(tmp_path, unique_stem) -> Path:
    """Write a valid single-tool provider and return its path."""
    p = tmp_path / f"{unique_stem}.py"
    p.write_text(PROVIDER_SRC)
    return p


# ---------------------------------------------------------------------------
# registry.py
# ---------------------------------------------------------------------------


class TestRegistry:
    def _entry(self, provider_id="local:demo", alias="demo", mode="copied"):
        return reg.ProviderEntry(
            provider_id=provider_id,
            alias=alias,
            mode=mode,
            source_path="/tmp/demo.py",
            digest="sha256:" + "0" * 64,
            installed_at="2026-08-30T00:00:00Z",
            datapipe_api=1,
            tools={"demo_tool": {"name": "demo_tool"}},
        )

    def test_load_empty_when_missing(self):
        data = reg.load_registry()
        assert data.providers == {}

    def test_save_then_load_round_trip(self):
        entry = self._entry()
        reg.add_provider(entry)
        loaded = reg.load_registry()
        assert entry.provider_id in loaded.providers
        got = loaded.providers[entry.provider_id]
        assert got.alias == entry.alias
        assert got.mode == entry.mode
        assert got.digest == entry.digest
        assert got.tools == entry.tools

    def test_get_provider(self):
        entry = self._entry()
        reg.add_provider(entry)
        assert reg.get_provider(entry.provider_id) is not None

    def test_get_provider_missing_returns_none(self):
        assert reg.get_provider("local:nope") is None

    def test_list_providers(self):
        reg.add_provider(self._entry("local:a", "a"))
        reg.add_provider(self._entry("local:b", "b"))
        ids = {e.provider_id for e in reg.list_providers()}
        assert ids == {"local:a", "local:b"}

    def test_add_replaces_existing(self):
        reg.add_provider(self._entry("local:a", "a"))
        reg.add_provider(self._entry("local:a", "a2"))
        assert reg.get_provider("local:a").alias == "a2"
        assert len(reg.list_providers()) == 1

    def test_remove_provider(self):
        reg.add_provider(self._entry("local:a", "a"))
        reg.remove_provider("local:a")
        assert reg.get_provider("local:a") is None

    def test_remove_missing_raises(self):
        with pytest.raises(KeyError):
            reg.remove_provider("local:nope")

    def test_registry_file_is_valid_json(self, isolated_registry):
        reg.add_provider(self._entry())
        path = isolated_registry / "registry.json"
        doc = json.loads(path.read_text())
        assert doc["schema_version"] == 1
        assert "providers" in doc

    def test_no_temp_files_left_after_save(self, isolated_registry):
        """The atomic write must not leave its temporary file behind.

        ``registry.lock`` is expected and excluded: flock needs a stable path
        to lock against, so it persists by design.
        """
        reg.add_provider(self._entry())
        leftovers = [
            p.name for p in isolated_registry.iterdir()
            if p.is_file() and p.name not in ("registry.json", "registry.lock")
        ]
        assert leftovers == []

    def test_provider_dir_under_data_dir(self, isolated_registry):
        d = reg.provider_dir("local:demo")
        assert str(d).startswith(str(isolated_registry))


# ---------------------------------------------------------------------------
# validation.py
# ---------------------------------------------------------------------------


class TestStaticValidation:
    def test_valid_provider_returns_bytes(self, provider_file):
        data = validate_static(provider_file)
        assert isinstance(data, bytes)
        assert b"shout" in data

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(StaticValidationError):
            validate_static(tmp_path / "nope.py")

    def test_directory_raises(self, tmp_path):
        with pytest.raises(StaticValidationError):
            validate_static(tmp_path)

    def test_syntax_error_raises(self, tmp_path):
        p = tmp_path / "bad_syntax.py"
        p.write_text("def broken(\n")
        with pytest.raises(StaticValidationError):
            validate_static(p)

    def test_oversized_file_raises(self, tmp_path):
        p = tmp_path / "huge.py"
        p.write_text("# padding\n" + "x = 1\n" * (MAX_SOURCE_BYTES // 3))
        assert p.stat().st_size > MAX_SOURCE_BYTES
        with pytest.raises(StaticValidationError):
            validate_static(p)

    def test_invalid_utf8_raises(self, tmp_path):
        p = tmp_path / "binary.py"
        p.write_bytes(b"\xff\xfe\x00bad bytes")
        with pytest.raises(StaticValidationError):
            validate_static(p)


class TestComputeDigest:
    def test_prefix_and_stability(self):
        d = compute_digest(b"hello")
        assert d.startswith("sha256:")
        assert d == compute_digest(b"hello")

    def test_differs_for_different_input(self):
        assert compute_digest(b"a") != compute_digest(b"b")


class TestDynamicValidation:
    def test_discovers_declared_tool(self, provider_file):
        data = validate_static(provider_file)
        meta = validate_dynamic(provider_file, data)
        names = {t["name"] for t in meta.tools}
        assert "shout" in names

    def test_reports_parameters(self, provider_file):
        data = validate_static(provider_file)
        meta = validate_dynamic(provider_file, data)
        tool_meta = next(t for t in meta.tools if t["name"] == "shout")
        assert "suffix" in json.dumps(tool_meta)

    def test_import_error_raises(self, tmp_path, unique_stem):
        p = tmp_path / f"{unique_stem}.py"
        p.write_text("import a_module_that_does_not_exist_xyz\n")
        data = validate_static(p)
        with pytest.raises(ProviderValidationError):
            validate_dynamic(p, data)

    def test_provider_with_no_tools(self, tmp_path, unique_stem):
        p = tmp_path / f"{unique_stem}.py"
        p.write_text("x = 1\n")
        data = validate_static(p)
        meta = validate_dynamic(p, data)
        assert meta.tools == []


# ---------------------------------------------------------------------------
# installer.py
# ---------------------------------------------------------------------------


class TestInstaller:
    def test_copied_install(self, provider_file):
        entry = install_provider(provider_file, yes=True)
        assert entry is not None
        assert entry.mode == "copied"
        assert "shout" in entry.tools
        assert reg.get_provider(entry.provider_id) is not None

    def test_copied_install_snapshots_source(self, provider_file):
        entry = install_provider(provider_file, yes=True)
        snapshot = Path(entry.source_path)
        assert snapshot.exists()
        # The snapshot lives in the registry, not next to the user's file.
        assert snapshot != provider_file.resolve()
        assert snapshot.read_text() == provider_file.read_text()

    def test_copied_install_survives_source_deletion(self, provider_file):
        entry = install_provider(provider_file, yes=True)
        provider_file.unlink()
        loaded = load_provider(_descriptor_for(entry))
        assert "shout" in loaded["tools"]

    def test_editable_install_points_at_original(self, provider_file):
        entry = install_provider(provider_file, editable=True, yes=True)
        assert entry.mode == "editable"
        assert Path(entry.source_path) == provider_file.resolve()

    def test_duplicate_without_force_raises(self, provider_file):
        install_provider(provider_file, yes=True)
        with pytest.raises(InstallationError):
            install_provider(provider_file, yes=True)

    def test_duplicate_with_force_succeeds(self, provider_file):
        install_provider(provider_file, yes=True)
        entry = install_provider(provider_file, yes=True, force=True)
        assert entry is not None

    def test_builtin_name_rejected_even_with_force(self, tmp_path):
        p = tmp_path / "fromjson.py"
        p.write_text(PROVIDER_SRC)
        with pytest.raises(InstallationError):
            install_provider(p, yes=True, force=True)

    def test_non_identifier_stem_rejected(self, tmp_path):
        p = tmp_path / "not-an-identifier.py"
        p.write_text(PROVIDER_SRC)
        with pytest.raises(InstallationError):
            install_provider(p, yes=True)

    def test_declined_prompt_returns_none(self, provider_file, monkeypatch):
        monkeypatch.setattr("sys.stdin", _FakeStdin("n\n"))
        result = install_provider(provider_file, yes=False)
        assert result is None
        assert reg.list_providers() == []

    def test_accepted_prompt_installs(self, provider_file, monkeypatch):
        monkeypatch.setattr("sys.stdin", _FakeStdin("y\n"))
        result = install_provider(provider_file, yes=False)
        assert result is not None
        assert len(reg.list_providers()) == 1

    def test_syntax_error_not_registered(self, tmp_path, unique_stem):
        p = tmp_path / f"{unique_stem}.py"
        p.write_text("def broken(\n")
        with pytest.raises(InstallationError):
            install_provider(p, yes=True)
        assert reg.list_providers() == []

    def test_remove_copied_deletes_snapshot(self, provider_file):
        entry = install_provider(provider_file, yes=True)
        snapshot = Path(entry.source_path)
        installer_remove(entry.provider_id)
        assert reg.get_provider(entry.provider_id) is None
        assert not snapshot.exists()

    def test_remove_editable_keeps_user_file(self, provider_file):
        entry = install_provider(provider_file, editable=True, yes=True)
        installer_remove(entry.provider_id)
        assert reg.get_provider(entry.provider_id) is None
        assert provider_file.exists(), "editable remove must not delete user source"


class _FakeStdin:
    """Minimal stdin stand-in for confirmation-prompt tests."""

    def __init__(self, text: str) -> None:
        self._lines = text.splitlines(keepends=True)

    def readline(self) -> str:
        return self._lines.pop(0) if self._lines else ""

    def read(self) -> str:
        out, self._lines = "".join(self._lines), []
        return out

    def isatty(self) -> bool:
        return False


def _descriptor_for(entry) -> ProviderDescriptor:
    return ProviderDescriptor(
        provider_id=entry.provider_id,
        alias=entry.alias,
        mode=entry.mode,
        source_path=entry.source_path,
        sha256=entry.digest,
        api_version=entry.datapipe_api,
    )


# ---------------------------------------------------------------------------
# loader.py
# ---------------------------------------------------------------------------


class TestLoader:
    def test_load_copied_provider(self, provider_file):
        entry = install_provider(provider_file, yes=True)
        loaded = load_provider(_descriptor_for(entry))
        assert "shout" in loaded["tools"]
        assert callable(loaded["tools"]["shout"])

    def test_loaded_tool_is_callable_and_correct(self, provider_file):
        entry = install_provider(provider_file, yes=True)
        fn = resolve_tool(_descriptor_for(entry), "shout")
        assert fn("hi") == "HI!"
        assert fn("hi", suffix="?") == "HI?"

    def test_resolve_unknown_tool_raises(self, provider_file):
        entry = install_provider(provider_file, yes=True)
        with pytest.raises(ProviderLoadError):
            resolve_tool(_descriptor_for(entry), "no_such_tool")

    def test_cached_across_calls(self, provider_file):
        entry = install_provider(provider_file, yes=True)
        d = _descriptor_for(entry)
        assert load_provider(d)["module"] is load_provider(d)["module"]

    def test_copied_digest_mismatch_raises(self, provider_file):
        """A modified copied snapshot means tampering and must abort."""
        entry = install_provider(provider_file, yes=True)
        Path(entry.source_path).write_text(PROVIDER_SRC + "\n# tampered\n")
        from datapipe.tools import loader as _loader
        _loader._loaded_providers.clear()
        with pytest.raises(ProviderLoadError, match="digest"):
            load_provider(_descriptor_for(entry))

    def test_editable_ignores_digest_and_reloads(self, provider_file):
        """Editable mode must pick up edits; enforcing the digest would break it."""
        entry = install_provider(provider_file, editable=True, yes=True)
        provider_file.write_text(
            PROVIDER_SRC.replace("value.upper() + suffix", "value.lower() + suffix")
        )
        from datapipe.tools import loader as _loader
        _loader._loaded_providers.clear()
        fn = resolve_tool(_descriptor_for(entry), "shout")
        assert fn("ABC") == "abc!", "editable provider did not reload after edit"

    def test_missing_source_raises(self, provider_file):
        entry = install_provider(provider_file, editable=True, yes=True)
        provider_file.unlink()
        from datapipe.tools import loader as _loader
        _loader._loaded_providers.clear()
        with pytest.raises(Exception):
            load_provider(_descriptor_for(entry))


class TestDescriptors:
    def test_qualified_name(self):
        p = ProviderDescriptor(
            provider_id="local:x", alias="x", mode="copied",
            source_path="/tmp/x.py", sha256="sha256:" + "0" * 64,
        )
        assert ToolDescriptor(provider=p, tool_name="t").qualified_name == "x.t"

    def test_pickleable(self):
        import pickle
        p = ProviderDescriptor(
            provider_id="local:x", alias="x", mode="copied",
            source_path="/tmp/x.py", sha256="sha256:" + "0" * 64,
        )
        d = ToolDescriptor(provider=p, tool_name="t")
        assert pickle.loads(pickle.dumps(d)) == d


# ---------------------------------------------------------------------------
# datapipe tools CLI
# ---------------------------------------------------------------------------


class TestToolsCLI:
    def test_install(self, provider_file, capsys):
        rc = main(["tools", "install", "--yes", str(provider_file)])
        assert rc == 0, capsys.readouterr().err
        assert len(reg.list_providers()) == 1

    def test_install_editable(self, provider_file, capsys):
        rc = main(["tools", "install", "--yes", "--editable", str(provider_file)])
        assert rc == 0, capsys.readouterr().err
        assert reg.list_providers()[0].mode == "editable"

    def test_install_short_editable_flag(self, provider_file, capsys):
        rc = main(["tools", "install", "--yes", "-e", str(provider_file)])
        assert rc == 0, capsys.readouterr().err
        assert reg.list_providers()[0].mode == "editable"

    def test_install_bad_file_nonzero(self, tmp_path, unique_stem, capsys):
        p = tmp_path / f"{unique_stem}.py"
        p.write_text("def broken(\n")
        rc = main(["tools", "install", "--yes", str(p)])
        assert rc != 0
        assert "error" in capsys.readouterr().err.lower()

    def test_install_duplicate_nonzero(self, provider_file, capsys):
        main(["tools", "install", "--yes", str(provider_file)])
        rc = main(["tools", "install", "--yes", str(provider_file)])
        assert rc != 0

    def test_install_force_succeeds(self, provider_file, capsys):
        main(["tools", "install", "--yes", str(provider_file)])
        rc = main(["tools", "install", "--yes", "--force", str(provider_file)])
        assert rc == 0, capsys.readouterr().err

    def test_validate_does_not_install(self, provider_file, capsys):
        rc = main(["tools", "validate", str(provider_file)])
        assert rc == 0, capsys.readouterr().err
        assert "shout" in capsys.readouterr().out
        assert reg.list_providers() == []

    def test_validate_bad_file_nonzero(self, tmp_path, unique_stem):
        p = tmp_path / f"{unique_stem}.py"
        p.write_text("def broken(\n")
        assert main(["tools", "validate", str(p)]) != 0

    def test_list_empty(self, capsys):
        rc = main(["tools", "list"])
        assert rc == 0
        capsys.readouterr()

    def test_list_shows_installed(self, provider_file, capsys):
        main(["tools", "install", "--yes", str(provider_file)])
        capsys.readouterr()
        rc = main(["tools", "list"])
        assert rc == 0
        assert provider_file.stem in capsys.readouterr().out

    def test_inspect_tool(self, provider_file, capsys):
        main(["tools", "install", "--yes", str(provider_file)])
        capsys.readouterr()
        rc = main(["tools", "inspect", "shout"])
        assert rc == 0, capsys.readouterr().err
        assert "shout" in capsys.readouterr().out

    def test_inspect_json(self, provider_file, capsys):
        main(["tools", "install", "--yes", str(provider_file)])
        capsys.readouterr()
        rc = main(["tools", "inspect", "shout", "--json"])
        assert rc == 0, capsys.readouterr().err
        json.loads(capsys.readouterr().out)  # must be valid JSON

    def test_inspect_unknown_nonzero(self, capsys):
        assert main(["tools", "inspect", "no_such_tool"]) != 0

    def test_remove(self, provider_file, capsys):
        main(["tools", "install", "--yes", str(provider_file)])
        capsys.readouterr()
        rc = main(["tools", "remove", "local:" + provider_file.stem])
        assert rc == 0, capsys.readouterr().err
        assert reg.list_providers() == []

    def test_remove_unknown_nonzero(self):
        assert main(["tools", "remove", "local:nope"]) != 0

    def test_install_main_entry_point(self, provider_file, capsys):
        """datapipe-install is a shim for 'datapipe tools install'."""
        from datapipe.cli.main import install_main
        rc = install_main(["--yes", str(provider_file)])
        assert rc == 0, capsys.readouterr().err
        assert len(reg.list_providers()) == 1


# ---------------------------------------------------------------------------
# End-to-end: installed tools inside transform expressions
# ---------------------------------------------------------------------------


def _write_jsonl(path: Path, rows: list) -> None:
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")


def _read_jsonl(path: Path) -> list:
    return [json.loads(x) for x in path.read_text().splitlines() if x.strip()]


class TestInstalledToolsInTransform:
    def test_unqualified_name(self, provider_file, tmp_path, capsys):
        install_provider(provider_file, yes=True)
        src, out = tmp_path / "in.jsonl", tmp_path / "out.jsonl"
        _write_jsonl(src, [{"msg": "hello"}, {"msg": "world"}])

        rc = main([
            "transform", "shout(.msg)", str(src), str(out),
            "--executor", "sequential", "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err
        assert _read_jsonl(out) == [{"msg": "HELLO!"}, {"msg": "WORLD!"}]

    def test_qualified_name_with_argument(self, provider_file, tmp_path, capsys):
        install_provider(provider_file, yes=True)
        src, out = tmp_path / "in.jsonl", tmp_path / "out.jsonl"
        _write_jsonl(src, [{"msg": "hi"}])

        rc = main([
            "transform", f'{provider_file.stem}.shout(.msg, suffix="?")',
            str(src), str(out), "--executor", "sequential", "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err
        assert _read_jsonl(out) == [{"msg": "HI?"}]

    def test_composes_with_builtins(self, provider_file, tmp_path, capsys):
        install_provider(provider_file, yes=True)
        src, out = tmp_path / "in.jsonl", tmp_path / "out.jsonl"
        _write_jsonl(src, [{"payload": '{"msg": "hey"}'}])

        rc = main([
            "transform", "fromjson(.payload) | shout(.payload.msg)",
            str(src), str(out), "--executor", "sequential", "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err
        assert _read_jsonl(out) == [{"payload": {"msg": "HEY!"}}]

    def test_process_executor_loads_provider_in_workers(
        self, provider_file, tmp_path, capsys
    ):
        """Workers must import the provider themselves under spawn."""
        install_provider(provider_file, yes=True)
        src, out = tmp_path / "in.jsonl", tmp_path / "out.jsonl"
        _write_jsonl(src, [{"msg": f"m{i}"} for i in range(8)])

        rc = main([
            "transform", "shout(.msg)", str(src), str(out),
            "--executor", "process", "--workers", "2", "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err
        assert sorted(r["msg"] for r in _read_jsonl(out)) == [
            f"M{i}!" for i in range(8)
        ]

    def test_editable_edit_takes_effect(self, provider_file, tmp_path, capsys):
        install_provider(provider_file, editable=True, yes=True)
        src, out = tmp_path / "in.jsonl", tmp_path / "out2.jsonl"
        _write_jsonl(src, [{"msg": "abc"}])

        provider_file.write_text(
            PROVIDER_SRC.replace("value.upper() + suffix", "value + value")
        )
        from datapipe.tools import loader as _loader
        _loader._loaded_providers.clear()

        rc = main([
            "transform", "shout(.msg)", str(src), str(out),
            "--executor", "sequential", "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err
        assert _read_jsonl(out) == [{"msg": "abcabc"}]

    def test_removed_tool_no_longer_resolves(self, provider_file, tmp_path, capsys):
        entry = install_provider(provider_file, yes=True)
        installer_remove(entry.provider_id)
        src, out = tmp_path / "in.jsonl", tmp_path / "out.jsonl"
        _write_jsonl(src, [{"msg": "x"}])

        rc = main([
            "transform", "shout(.msg)", str(src), str(out),
            "--executor", "sequential", "--no-progress",
        ])
        assert rc != 0
        assert "unknown tool" in capsys.readouterr().err.lower()

    def test_broken_provider_warns_and_keeps_builtins(
        self, provider_file, tmp_path, capsys
    ):
        """A provider that fails to load must warn, not vanish silently."""
        entry = install_provider(provider_file, yes=True)
        Path(entry.source_path).write_text(PROVIDER_SRC + "\n# tampered\n")
        from datapipe.tools import loader as _loader
        _loader._loaded_providers.clear()

        src, out = tmp_path / "in.jsonl", tmp_path / "out.jsonl"
        _write_jsonl(src, [{"v": '{"a": 1}'}])

        # Built-ins still work even though the provider is broken.
        rc = main([
            "transform", "fromjson(.v)", str(src), str(out),
            "--executor", "sequential", "--no-progress",
        ])
        captured = capsys.readouterr()
        assert rc == 0, captured.err
        assert "warning" in captured.err.lower()
        assert _read_jsonl(out) == [{"v": {"a": 1}}]
