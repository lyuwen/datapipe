"""Install-time functional validation: examples, spawn smoke test, metadata rules.

Covers ``datapipe/tools/examples.py`` and its wiring into
``install_provider`` — plan §8.2/§8.3.

Isolation notes
---------------
Every test redirects ``DATAPIPE_USER_DATA`` to a per-test ``tmp_path`` and
clears ``datapipe.tools.loader._loaded_providers``, so the user's real
registry is never read or written and no stale import is reused.  Providers
are imported under their file stem, so ``unique_stem`` hands out a fresh stem
per test to avoid ``sys.modules`` collisions.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from datapipe.tools.examples import (
    MAX_DESCRIPTION_CHARS,
    MAX_TOOL_NAME_CHARS,
    ExampleValidationError,
    MetadataLimitError,
    SpawnSmokeTestError,
    check_metadata_limits,
    compare_static_dynamic,
    run_examples,
    spawn_load_smoke_test,
)
from datapipe.tools.installer import InstallationError, install_provider
from datapipe.tools.registry import load_registry
from datapipe.tools.validation import compute_digest, validate_static


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_STEM_COUNTER = [0]


@pytest.fixture
def unique_stem() -> str:
    _STEM_COUNTER[0] += 1
    return f"instval_prov_{_STEM_COUNTER[0]}"


@pytest.fixture(autouse=True)
def isolated_registry(tmp_path, monkeypatch):
    """Point the registry at tmp_path and clear the loader cache."""
    monkeypatch.setenv("DATAPIPE_USER_DATA", str(tmp_path / "share"))
    import datapipe.tools.loader as _loader

    _loader._loaded_providers.clear()
    yield tmp_path
    _loader._loaded_providers.clear()


_HEADER = """\
from datapipe.tools.decorator import tool
from datapipe.tools.types import JsonType
from datapipe.tools.contract import ToolExample
"""


def _write(tmp_path: Path, stem: str, body: str) -> Path:
    path = tmp_path / f"{stem}.py"
    path.write_text(_HEADER + body, encoding="utf-8")
    return path


PASSING_EXAMPLES = """
@tool(
    name="shout",
    api_version=1,
    target="value",
    input=JsonType.STRING,
    output=JsonType.STRING,
    description="Uppercase a string.",
    examples=[
        ToolExample(input="hi", output="HI", description="basic"),
        ToolExample(input="a b", output="A B"),
    ],
)
def shout(value, *, suffix: str = ""):
    return value.upper() + suffix
"""


# ---------------------------------------------------------------------------
# Task 1 — declared examples are executed and validated
# ---------------------------------------------------------------------------

class TestExampleExecution:
    """A declared ToolExample must actually run and match its contract."""

    def test_wrong_output_value_is_rejected(self, tmp_path, unique_stem):
        src = _write(tmp_path, unique_stem, """
@tool(
    name="shout", api_version=1, target="value",
    input=JsonType.STRING, output=JsonType.STRING,
    examples=[ToolExample(input="hi", output="bye")],
)
def shout(value):
    return value.upper()
""")
        with pytest.raises(InstallationError) as exc:
            install_provider(src, yes=True)
        message = str(exc.value)
        assert "output_value" in message
        # The diagnostic must be actionable: tool, input, expected, actual.
        assert "'shout'" in message
        assert "'hi'" in message
        assert "'bye'" in message
        assert "'HI'" in message

    def test_output_violating_declared_type_is_rejected(self, tmp_path, unique_stem):
        """Declared output type is checked, not just the example's value."""
        src = _write(tmp_path, unique_stem, """
@tool(
    name="counter", api_version=1, target="value",
    input=JsonType.STRING, output=JsonType.INTEGER,
    examples=[ToolExample(input="abc", output="3")],
)
def counter(value):
    return str(len(value))
""")
        with pytest.raises(InstallationError) as exc:
            install_provider(src, yes=True)
        message = str(exc.value)
        assert "output_type" in message
        assert "integer" in message

    def test_example_that_raises_is_rejected(self, tmp_path, unique_stem):
        src = _write(tmp_path, unique_stem, """
@tool(
    name="boom", api_version=1, target="value",
    input=JsonType.STRING, output=JsonType.STRING,
    examples=[ToolExample(input="x", output="x")],
)
def boom(value):
    raise ValueError("tool is broken")
""")
        with pytest.raises(InstallationError) as exc:
            install_provider(src, yes=True)
        message = str(exc.value)
        assert "raised" in message
        assert "tool is broken" in message

    def test_example_input_violating_input_type_is_rejected(self, tmp_path, unique_stem):
        """An example documenting a call the runtime would reject is a failure."""
        src = _write(tmp_path, unique_stem, """
@tool(
    name="shout", api_version=1, target="value",
    input=JsonType.STRING, output=JsonType.STRING,
    examples=[ToolExample(input=42, output="42")],
)
def shout(value):
    return str(value)
""")
        with pytest.raises(InstallationError) as exc:
            install_provider(src, yes=True)
        assert "input_type" in str(exc.value)

    def test_example_arguments_are_passed(self, tmp_path, unique_stem):
        """ToolExample.arguments must reach the tool as keyword arguments."""
        src = _write(tmp_path, unique_stem, """
@tool(
    name="shout", api_version=1, target="value",
    input=JsonType.STRING, output=JsonType.STRING,
    examples=[ToolExample(input="hi", output="HI!", arguments={"suffix": "!"})],
)
def shout(value, *, suffix: str = ""):
    return value.upper() + suffix
""")
        entry = install_provider(src, yes=True)
        assert entry is not None and "shout" in entry.tools

    def test_bad_arguments_report_the_arguments(self, tmp_path, unique_stem):
        src = _write(tmp_path, unique_stem, """
@tool(
    name="shout", api_version=1, target="value",
    input=JsonType.STRING, output=JsonType.STRING,
    examples=[ToolExample(input="hi", output="HI", arguments={"suffix": "?"})],
)
def shout(value, *, suffix: str = ""):
    return value.upper() + suffix
""")
        with pytest.raises(InstallationError) as exc:
            install_provider(src, yes=True)
        assert "suffix" in str(exc.value)

    def test_bool_is_not_accepted_for_an_integer_example(self, tmp_path, unique_stem):
        """True == 1 in Python, but an example declaring 1 means the integer 1."""
        src = _write(tmp_path, unique_stem, """
@tool(
    name="one", api_version=1, target="value",
    input=JsonType.STRING, output=JsonType.ANY,
    examples=[ToolExample(input="x", output=1)],
)
def one(value):
    return True
""")
        with pytest.raises(InstallationError) as exc:
            install_provider(src, yes=True)
        assert "output_value" in str(exc.value)

    def test_nothing_is_registered_when_an_example_fails(self, tmp_path, unique_stem):
        """The 'nothing registered until validation passes' invariant holds."""
        src = _write(tmp_path, unique_stem, """
@tool(
    name="shout", api_version=1, target="value",
    input=JsonType.STRING, output=JsonType.STRING,
    examples=[ToolExample(input="hi", output="bye")],
)
def shout(value):
    return value.upper()
""")
        with pytest.raises(InstallationError):
            install_provider(src, yes=True)
        assert load_registry().providers == {}

    def test_run_examples_reports_counts(self, tmp_path, unique_stem):
        src = _write(tmp_path, unique_stem, PASSING_EXAMPLES)
        report = run_examples(src, validate_static(src))
        assert report.examples_run == 2
        assert report.tools_with_examples == 1

    def test_all_failures_are_reported_together(self, tmp_path, unique_stem):
        """An author fixing a provider should see every failure in one pass."""
        src = _write(tmp_path, unique_stem, """
@tool(
    name="a", api_version=1, target="value",
    input=JsonType.STRING, output=JsonType.STRING,
    examples=[ToolExample(input="x", output="wrong1")],
)
def a(value):
    return value

@tool(
    name="b", api_version=1, target="value",
    input=JsonType.STRING, output=JsonType.STRING,
    examples=[ToolExample(input="y", output="wrong2")],
)
def b(value):
    return value
""")
        with pytest.raises(ExampleValidationError) as exc:
            run_examples(src, validate_static(src))
        message = str(exc.value)
        assert "wrong1" in message and "wrong2" in message
        assert "2 declared example(s) failed" in message

    def test_provider_stdout_does_not_corrupt_the_protocol(self, tmp_path, unique_stem):
        """A top-level print() must not be mistaken for the JSON protocol line."""
        src = _write(tmp_path, unique_stem, """
print("noise from provider import")

@tool(
    name="shout", api_version=1, target="value",
    input=JsonType.STRING, output=JsonType.STRING,
    examples=[ToolExample(input="hi", output="HI")],
)
def shout(value):
    print("noise from inside the tool")
    return value.upper()
""")
        report = run_examples(src, validate_static(src))
        assert report.examples_run == 1

    def test_examples_run_from_stdin_bytes_not_the_file_on_disk(
        self, tmp_path, unique_stem
    ):
        """Exactly the validated bytes are executed — no re-read, no TOCTOU gap."""
        src = _write(tmp_path, unique_stem, PASSING_EXAMPLES)
        good_bytes = validate_static(src)
        # Replace the file on disk with a version whose example fails.  If the
        # helper re-read the file it would report a failure; it must not.
        src.write_text(
            _HEADER
            + """
@tool(
    name="shout", api_version=1, target="value",
    input=JsonType.STRING, output=JsonType.STRING,
    examples=[ToolExample(input="hi", output="NEVER")],
)
def shout(value):
    return value.upper()
""",
            encoding="utf-8",
        )
        report = run_examples(src, good_bytes)
        assert report.examples_run == 2


class TestExamplesDoNotFalselyReject:
    """A false rejection would block valid providers — test that risk directly."""

    def test_provider_with_passing_examples_installs(self, tmp_path, unique_stem):
        src = _write(tmp_path, unique_stem, PASSING_EXAMPLES)
        entry = install_provider(src, yes=True)
        assert entry is not None
        assert "shout" in entry.tools
        assert load_registry().providers[entry.provider_id].alias == unique_stem

    def test_provider_without_examples_installs(self, tmp_path, unique_stem):
        """Examples are optional (plan §8.3) — absence must not block install."""
        src = _write(tmp_path, unique_stem, """
@tool(
    name="shout", api_version=1, target="value",
    input=JsonType.STRING, output=JsonType.STRING,
)
def shout(value):
    return value.upper()
""")
        entry = install_provider(src, yes=True)
        assert entry is not None and "shout" in entry.tools

    def test_builtin_json_provider_installs_cleanly(self, tmp_path, unique_stem):
        """The built-ins declare real examples and must still install."""
        import datapipe.tools.builtins.json as builtin_json

        src = tmp_path / f"{unique_stem}.py"
        shutil.copy(Path(builtin_json.__file__), src)

        report = run_examples(src, validate_static(src))
        assert report.examples_run == 4  # 2 on fromjson, 2 on tojson
        assert report.tools_with_examples == 2

        entry = install_provider(src, yes=True)
        assert entry is not None
        assert sorted(entry.tools) == ["fromjson", "tojson"]

    def test_editable_install_with_examples(self, tmp_path, unique_stem):
        src = _write(tmp_path, unique_stem, PASSING_EXAMPLES)
        entry = install_provider(src, yes=True, editable=True)
        assert entry is not None and entry.mode == "editable"


# ---------------------------------------------------------------------------
# Task 2 — install-time spawn/load smoke test
# ---------------------------------------------------------------------------

class TestSpawnLoadSmokeTest:
    """The provider must load in a fresh spawn worker, not just the installer."""

    def test_import_resolvable_only_at_install_time_is_rejected(
        self, tmp_path, unique_stem
    ):
        """A sibling-module import passes dynamic validation but not a worker.

        ``validate_dynamic`` seeds ``sys.path`` with the provider's parent
        directory, so ``import <sibling>`` resolves there.  A real worker does
        no such seeding, so the same provider fails on every record.  This is
        exactly the class of breakage §8.3 asks the smoke test to catch.
        """
        sibling = tmp_path / f"{unique_stem}_sidecar.py"
        sibling.write_text("VALUE = 'from sidecar'\n", encoding="utf-8")

        src = _write(tmp_path, unique_stem, f"""
import {unique_stem}_sidecar

@tool(
    name="shout", api_version=1, target="value",
    input=JsonType.STRING, output=JsonType.STRING,
)
def shout(value):
    return value.upper()
""")
        # Dynamic validation accepts it...
        from datapipe.tools.validation import validate_dynamic

        source_bytes = validate_static(src)
        assert [t["name"] for t in validate_dynamic(src, source_bytes).tools] == ["shout"]

        # ...but installation must not, because a worker cannot load it.
        with pytest.raises(InstallationError) as exc:
            install_provider(src, yes=True)
        message = str(exc.value)
        assert "fresh spawned worker" in message
        assert "ModuleNotFoundError" in message or "sidecar" in message
        assert load_registry().providers == {}

    def test_import_time_failure_reports_the_traceback(self, tmp_path, unique_stem):
        src = tmp_path / f"{unique_stem}.py"
        src.write_text("raise RuntimeError('cannot import me')\n", encoding="utf-8")
        digest = compute_digest(validate_static(src))
        with pytest.raises(SpawnSmokeTestError) as exc:
            spawn_load_smoke_test(
                provider_id="local:x",
                alias="x",
                mode="copied",
                source_path=str(src),
                digest=digest,
            )
        assert "cannot import me" in str(exc.value)

    def test_digest_mismatch_is_reported(self, tmp_path, unique_stem):
        """The smoke test runs the real loader, so digest checks apply."""
        src = _write(tmp_path, unique_stem, PASSING_EXAMPLES)
        with pytest.raises(SpawnSmokeTestError) as exc:
            spawn_load_smoke_test(
                provider_id="local:x",
                alias="x",
                mode="copied",
                source_path=str(src),
                digest="sha256:" + "0" * 64,
            )
        assert "digest mismatch" in str(exc.value)

    def test_tool_set_disagreement_is_reported(self, tmp_path, unique_stem):
        src = _write(tmp_path, unique_stem, PASSING_EXAMPLES)
        digest = compute_digest(validate_static(src))
        with pytest.raises(SpawnSmokeTestError) as exc:
            spawn_load_smoke_test(
                provider_id="local:x",
                alias="x",
                mode="copied",
                source_path=str(src),
                digest=digest,
                expected_tools=["shout", "ghost"],
            )
        message = str(exc.value)
        assert "missing in worker" in message and "ghost" in message

    def test_healthy_provider_resolves_its_tools_under_spawn(
        self, tmp_path, unique_stem
    ):
        src = _write(tmp_path, unique_stem, PASSING_EXAMPLES)
        digest = compute_digest(validate_static(src))
        found = spawn_load_smoke_test(
            provider_id="local:x",
            alias="x",
            mode="copied",
            source_path=str(src),
            digest=digest,
            expected_tools=["shout"],
        )
        assert found == ["shout"]

    def test_provider_printing_at_import_still_passes(self, tmp_path, unique_stem):
        """Provider stdout must not corrupt the smoke test's own protocol."""
        src = _write(tmp_path, unique_stem, """
print("hello from import")

@tool(
    name="shout", api_version=1, target="value",
    input=JsonType.STRING, output=JsonType.STRING,
)
def shout(value):
    return value.upper()
""")
        digest = compute_digest(validate_static(src))
        assert spawn_load_smoke_test(
            provider_id="local:x",
            alias="x",
            mode="copied",
            source_path=str(src),
            digest=digest,
        ) == ["shout"]


# ---------------------------------------------------------------------------
# Task 3 — static vs dynamic metadata comparison
# ---------------------------------------------------------------------------

class TestStaticDynamicComparison:
    """A tool the source declares but the import does not produce is an error."""

    def test_shadowed_tool_is_rejected(self, tmp_path, unique_stem):
        src = _write(tmp_path, unique_stem, """
@tool(
    name="shout", api_version=1, target="value",
    input=JsonType.STRING, output=JsonType.STRING,
)
def shout(value):
    return value.upper()

shout = None  # shadows the decorated function
""")
        with pytest.raises(InstallationError) as exc:
            install_provider(src, yes=True)
        message = str(exc.value)
        assert "inconsistent" in message
        assert "shout" in message
        assert load_registry().providers == {}

    def test_deleted_tool_is_rejected(self, tmp_path, unique_stem):
        src = _write(tmp_path, unique_stem, """
@tool(
    name="gone", api_version=1, target="value",
    input=JsonType.STRING, output=JsonType.STRING,
)
def gone(value):
    return value

del gone
""")
        with pytest.raises(InstallationError) as exc:
            install_provider(src, yes=True)
        assert "gone" in str(exc.value)

    def test_computed_name_is_not_rejected(self, tmp_path, unique_stem):
        """A name invisible to the AST is legitimate, not a mismatch."""
        src = _write(tmp_path, unique_stem, """
_PREFIX = "auto_"

@tool(
    name=_PREFIX + "shout", api_version=1, target="value",
    input=JsonType.STRING, output=JsonType.STRING,
)
def shout(value):
    return value.upper()
""")
        entry = install_provider(src, yes=True)
        assert entry is not None and "auto_shout" in entry.tools

    def test_nested_tool_definition_is_not_reported_missing(self, tmp_path, unique_stem):
        """@tool inside a function body legitimately yields no module attribute."""
        src = _write(tmp_path, unique_stem, """
@tool(
    name="shout", api_version=1, target="value",
    input=JsonType.STRING, output=JsonType.STRING,
)
def shout(value):
    return value.upper()

def factory():
    @tool(
        name="inner", api_version=1, target="value",
        input=JsonType.STRING, output=JsonType.STRING,
    )
    def inner(value):
        return value
    return inner
""")
        entry = install_provider(src, yes=True)
        assert entry is not None and sorted(entry.tools) == ["shout"]

    def test_comparison_reports_both_directions(self, tmp_path, unique_stem):
        src = _write(tmp_path, unique_stem, PASSING_EXAMPLES)
        source_bytes = validate_static(src)
        result = compare_static_dynamic(
            source_bytes, src, [{"name": "surprise"}]
        )
        assert result.missing_dynamically == ["shout"]
        assert result.only_dynamically == ["surprise"]
        assert "shout" in result.describe() and "surprise" in result.describe()


# ---------------------------------------------------------------------------
# Task 4 — name and description size/character rules
# ---------------------------------------------------------------------------

class TestMetadataLimits:
    """Names and descriptions land in CLI output and registry JSON."""

    def test_overlong_name_is_rejected(self, tmp_path, unique_stem):
        long_name = "n" * (MAX_TOOL_NAME_CHARS + 1)
        src = _write(tmp_path, unique_stem, f"""
@tool(
    name="{long_name}", api_version=1, target="value",
    input=JsonType.STRING, output=JsonType.STRING,
)
def shout(value):
    return value.upper()
""")
        with pytest.raises(InstallationError) as exc:
            install_provider(src, yes=True)
        assert str(MAX_TOOL_NAME_CHARS) in str(exc.value)
        assert load_registry().providers == {}

    def test_name_at_the_limit_is_accepted(self, tmp_path, unique_stem):
        name = "n" * MAX_TOOL_NAME_CHARS
        src = _write(tmp_path, unique_stem, f"""
@tool(
    name="{name}", api_version=1, target="value",
    input=JsonType.STRING, output=JsonType.STRING,
)
def shout(value):
    return value.upper()
""")
        entry = install_provider(src, yes=True)
        assert entry is not None and name in entry.tools

    def test_non_ascii_name_is_rejected(self, tmp_path, unique_stem):
        """'café' and homoglyphs are valid identifiers but not valid tool names."""
        src = _write(tmp_path, unique_stem, """
@tool(
    name="café", api_version=1, target="value",
    input=JsonType.STRING, output=JsonType.STRING,
)
def cafe(value):
    return value
""")
        with pytest.raises(InstallationError) as exc:
            install_provider(src, yes=True)
        assert "non-ASCII" in str(exc.value)

    def test_overlong_description_is_rejected(self, tmp_path, unique_stem):
        src = _write(tmp_path, unique_stem, f"""
@tool(
    name="shout", api_version=1, target="value",
    input=JsonType.STRING, output=JsonType.STRING,
    description="d" * {MAX_DESCRIPTION_CHARS + 1},
)
def shout(value):
    return value.upper()
""")
        with pytest.raises(InstallationError) as exc:
            install_provider(src, yes=True)
        assert str(MAX_DESCRIPTION_CHARS) in str(exc.value)

    def test_control_characters_in_description_are_rejected(self, tmp_path, unique_stem):
        """An ANSI escape lets a provider rewrite what `tools inspect` shows."""
        src = _write(tmp_path, unique_stem, r"""
@tool(
    name="shout", api_version=1, target="value",
    input=JsonType.STRING, output=JsonType.STRING,
    description="safe\x1b[2Kmalicious",
)
def shout(value):
    return value.upper()
""")
        with pytest.raises(InstallationError) as exc:
            install_provider(src, yes=True)
        assert "control characters" in str(exc.value)

    def test_multiline_description_is_accepted(self, tmp_path, unique_stem):
        """Newlines and tabs are normal in help text and must not be rejected."""
        src = _write(tmp_path, unique_stem, r"""
@tool(
    name="shout", api_version=1, target="value",
    input=JsonType.STRING, output=JsonType.STRING,
    description="line one\nline two\n\tindented",
)
def shout(value):
    return value.upper()
""")
        entry = install_provider(src, yes=True)
        assert entry is not None and "shout" in entry.tools

    def test_check_metadata_limits_collects_every_problem(self):
        with pytest.raises(MetadataLimitError) as exc:
            check_metadata_limits([
                {"name": "n" * (MAX_TOOL_NAME_CHARS + 1), "description": ""},
                {"name": "café", "description": ""},
                {"name": "ok", "description": "d" * (MAX_DESCRIPTION_CHARS + 1)},
            ])
        message = str(exc.value)
        assert message.count("  - ") == 3

    def test_builtin_contracts_satisfy_the_limits(self):
        """The limits must not be so tight that shipped tools violate them."""
        import datapipe.tools.builtins.json as builtin_json

        check_metadata_limits([
            {"name": fn.__tool_contract__.name,
             "description": fn.__tool_contract__.description}
            for fn in (builtin_json.fromjson, builtin_json.tojson)
        ])
