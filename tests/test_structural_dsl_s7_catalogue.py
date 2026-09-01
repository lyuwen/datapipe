"""Phase S7 §7 catalogue completeness (plan §15.6).

§15.6 requires every §7 use case to have *both* a CLI-level JSONL test and a
Python-level compiled-program test.  Auditing S0–S6 found the Python side well
covered but the CLI side thin: only 7.7 and 7.8 ran through ``main([...])`` on a
real JSONL file, and cases 7.4 and 7.12 had no executing test at all in either
form (S0 covers all thirteen, but S0 is a parse-only phase — it asserts the
expression *parses*, not that it does the right thing).

This file closes those gaps.  It does not restate cases whose Python-level
semantics are already covered in depth by the phase that introduced them; for
those it adds only the missing CLI-level run.  The per-case audit and its
citations are in the phase report.

Cases 7.4 and 7.12 name tools that are not built in (``finalize_record``,
``normalize_metadata``).  They are installed here as a real provider, which is
also what makes them meaningful: they exercise the record-target and
focused-pipe paths through a genuine provider rather than a built-in.
"""

from __future__ import annotations

import copy
import json

import pytest

from datapipe.cli.main import main
from datapipe.dsl.compiler import compile_program
from datapipe.stages.tool_program import CompiledProgramStage


@pytest.fixture(autouse=True)
def isolated_registry(tmp_path, monkeypatch):
    """Redirect the tool registry at tmp_path and clear the loader cache."""
    monkeypatch.setenv("DATAPIPE_USER_DATA", str(tmp_path / "dp_data"))

    from datapipe.tools import loader as _loader

    _loader._loaded_providers.clear()

    from datapipe.dsl import compiler as _compiler

    monkeypatch.setattr(_compiler, "_BUILTIN_REGISTRY", None, raising=False)


_CATALOGUE_PROVIDER = '''\
from datapipe.tools import tool, JsonType


@tool(
    name="finalize_record",
    target="record",
    input=JsonType.OBJECT,
    output=JsonType.OBJECT,
    description="Stamp a record as finalized.",
)
def finalize_record(record):
    out = dict(record)
    out["finalized"] = True
    return out


@tool(
    name="normalize_metadata",
    target="value",
    input=JsonType.OBJECT,
    output=JsonType.OBJECT,
    description="Lowercase every key of a metadata object.",
)
def normalize_metadata(value):
    return {key.lower(): item for key, item in value.items()}
'''


@pytest.fixture
def catalogue_provider(tmp_path):
    """Install the two non-built-in tools §7.4 and §7.12 refer to."""
    from datapipe.tools.installer import install_provider

    path = tmp_path / "s7_catalogue_provider.py"
    path.write_text(_CATALOGUE_PROVIDER)
    install_provider(path, yes=True)
    return path


def _run_python(expression: str, record):
    """Python-level: compile the expression and execute it on one record."""
    stage = CompiledProgramStage(compile_program(expression))
    return stage.process(copy.deepcopy(record), None)


def _run_cli(expression: str, records, tmp_path, capsys=None):
    """CLI-level: run `datapipe transform` over a real JSONL file."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    source = tmp_path / "in.jsonl"
    output = tmp_path / "out.jsonl"
    source.write_text(
        "".join(json.dumps(record) + "\n" for record in records)
    )

    code = main([
        "transform",
        expression,
        str(source),
        str(output),
        "--executor", "sequential",
        "--no-progress",
    ])
    if capsys is not None:
        assert code == 0, capsys.readouterr().err
    else:
        assert code == 0
    return [
        json.loads(line)
        for line in output.read_text().splitlines()
        if line.strip()
    ]


# ===========================================================================
# 7.1  Deserialize selected fields in place
# ===========================================================================

_7_1_EXPRESSION = "fromjson(.tools); fromjson(.metadata.annotation, recursive=true)"


def _7_1_record():
    return {
        "tools": json.dumps([{"name": "t1"}]),
        "metadata": {
            "annotation": json.dumps({"nested": json.dumps({"deep": 1})})
        },
    }


def _7_1_expected():
    return {
        "tools": [{"name": "t1"}],
        "metadata": {"annotation": {"nested": {"deep": 1}}},
    }


def test_7_1_cli_deserializes_selected_fields_in_place(tmp_path, capsys):
    rows = _run_cli(_7_1_EXPRESSION, [_7_1_record()], tmp_path, capsys)
    assert rows == [_7_1_expected()]


def test_7_1_python_deserializes_selected_fields_in_place():
    assert _run_python(_7_1_EXPRESSION, _7_1_record()) == _7_1_expected()


# ===========================================================================
# 7.2  Serialize nested tool parameters in place
# ===========================================================================

_7_2_EXPRESSION = "tojson(.tools[].function.parameters)"


def _7_2_record():
    return {
        "tools": [
            {"function": {"parameters": {"a": 1}}},
            {"function": {"parameters": {"b": 2}}},
        ]
    }


def _7_2_expected():
    return {
        "tools": [
            {"function": {"parameters": '{"a":1}'}},
            {"function": {"parameters": '{"b":2}'}},
        ]
    }


def test_7_2_cli_serializes_nested_tool_parameters(tmp_path, capsys):
    rows = _run_cli(_7_2_EXPRESSION, [_7_2_record()], tmp_path, capsys)
    assert rows == [_7_2_expected()]


def test_7_2_python_serializes_nested_tool_parameters():
    assert _run_python(_7_2_EXPRESSION, _7_2_record()) == _7_2_expected()


# ===========================================================================
# 7.3  Several independent serializations
# ===========================================================================

_7_3_EXPRESSION = "tojson(.keya); tojson(.keyb)"


def test_7_3_cli_performs_several_independent_serializations(tmp_path, capsys):
    rows = _run_cli(
        _7_3_EXPRESSION,
        [{"keya": {"x": 1}, "keyb": [1, 2], "untouched": "u"}],
        tmp_path,
        capsys,
    )
    assert rows == [
        {"keya": '{"x":1}', "keyb": "[1,2]", "untouched": "u"}
    ]


def test_7_3_python_performs_several_independent_serializations():
    result = _run_python(
        _7_3_EXPRESSION, {"keya": {"x": 1}, "keyb": [1, 2], "untouched": "u"}
    )
    assert result == {"keya": '{"x":1}', "keyb": "[1,2]", "untouched": "u"}


# ===========================================================================
# 7.4  Value operations, then a whole-record operation
# ===========================================================================

_7_4_EXPRESSION = "tojson(.keya); tojson(.keyb); finalize_record(.)"


def _7_4_record():
    return {"keya": {"x": 1}, "keyb": [1, 2], "other": 7}


def _7_4_expected():
    return {
        "keya": '{"x":1}',
        "keyb": "[1,2]",
        "other": 7,
        "finalized": True,
    }


def test_7_4_cli_value_operations_then_whole_record_operation(
    catalogue_provider, tmp_path, capsys
):
    rows = _run_cli(_7_4_EXPRESSION, [_7_4_record()], tmp_path, capsys)
    assert rows == [_7_4_expected()]


def test_7_4_python_value_operations_then_whole_record_operation(
    catalogue_provider,
):
    result = _run_python(_7_4_EXPRESSION, _7_4_record())
    assert result == _7_4_expected()
    # The whole-record tool observed the two earlier value mutations, which is
    # the point of the case: all three run in one worker invocation.
    assert result["keya"] == '{"x":1}' and result["finalized"] is True


# ===========================================================================
# 7.5  Explicitly move selected root fields into metadata
# ===========================================================================

_7_5_EXPRESSION = ".metadata << .annotation_key, .temperature, .score | tojson"


def _catalogue_record():
    return {
        "instance_id": "i1",
        "messages": [{"role": "user"}],
        "tools": [{"name": "t"}],
        "annotation_key": "k",
        "temperature": 0.7,
        "score": 3,
    }


def _nested_expected():
    return {
        "instance_id": "i1",
        "messages": [{"role": "user"}],
        "tools": [{"name": "t"}],
        "metadata": '{"annotation_key":"k","temperature":0.7,"score":3}',
    }


def test_7_5_cli_moves_explicit_root_fields_into_metadata(tmp_path, capsys):
    rows = _run_cli(_7_5_EXPRESSION, [_catalogue_record()], tmp_path, capsys)
    assert rows == [_nested_expected()]


def test_7_5_python_moves_explicit_root_fields_into_metadata():
    assert _run_python(_7_5_EXPRESSION, _catalogue_record()) == _nested_expected()


# ===========================================================================
# 7.6  Move a positive field set into metadata
# ===========================================================================

_7_6_EXPRESSION = ".metadata << .(annotation_key|temperature|score) | tojson"


def test_7_6_cli_moves_a_positive_field_set_into_metadata(tmp_path, capsys):
    rows = _run_cli(_7_6_EXPRESSION, [_catalogue_record()], tmp_path, capsys)
    assert rows == [_nested_expected()]


def test_7_6_python_moves_a_positive_field_set_into_metadata():
    assert _run_python(_7_6_EXPRESSION, _catalogue_record()) == _nested_expected()


# ===========================================================================
# 7.7  Blanket-move every field except stable root fields
# ===========================================================================

_7_7_EXPRESSION = ".metadata << .(^instance_id|messages|tools) | tojson"


def test_7_7_cli_blanket_moves_all_but_the_stable_root_fields(tmp_path, capsys):
    rows = _run_cli(_7_7_EXPRESSION, [_catalogue_record()], tmp_path, capsys)
    assert rows == [_nested_expected()]


def test_7_7_python_blanket_moves_all_but_the_stable_root_fields():
    assert _run_python(_7_7_EXPRESSION, _catalogue_record()) == _nested_expected()


# ===========================================================================
# 7.8  Equivalent configurable `nest` form
# ===========================================================================

_7_8_EXPRESSION = (
    'nest(., key="metadata", '
    'exclude=["instance_id", "messages", "tools"], jsonify=true)'
)


def test_7_8_cli_configurable_nest_matches_the_symbolic_form(tmp_path, capsys):
    rows = _run_cli(_7_8_EXPRESSION, [_catalogue_record()], tmp_path, capsys)
    assert rows == [_nested_expected()]


def test_7_8_python_configurable_nest_matches_the_symbolic_form():
    named = _run_python(_7_8_EXPRESSION, _catalogue_record())
    symbolic = _run_python(_7_7_EXPRESSION, _catalogue_record())
    assert named == symbolic == _nested_expected()


# ===========================================================================
# 7.9  Decode metadata, move selected fields out, reserialize it
# ===========================================================================

_7_9_EXPRESSION = (
    "fromjson(.metadata); . << .metadata.(temperature|score); tojson(.metadata)"
)


def _7_9_record():
    return {
        "instance_id": "i1",
        "metadata": json.dumps(
            {"temperature": 0.7, "score": 3, "note": "keep"}
        ),
    }


def _7_9_expected():
    return {
        "instance_id": "i1",
        "metadata": '{"note":"keep"}',
        "temperature": 0.7,
        "score": 3,
    }


def test_7_9_cli_moves_fields_out_of_metadata_and_reserializes(tmp_path, capsys):
    rows = _run_cli(_7_9_EXPRESSION, [_7_9_record()], tmp_path, capsys)
    assert rows == [_7_9_expected()]


def test_7_9_python_moves_fields_out_of_metadata_and_reserializes():
    assert _run_python(_7_9_EXPRESSION, _7_9_record()) == _7_9_expected()


# ===========================================================================
# 7.10  Decode a nested serialized value while moving it out
# ===========================================================================

_7_10_EXPRESSION = (
    "fromjson(.metadata); "
    ".temperature <- fromjson(.metadata.temperature); "
    "tojson(.metadata)"
)


def _7_10_record():
    return {
        "instance_id": "i1",
        "metadata": json.dumps({"temperature": "0.7", "note": "keep"}),
    }


def _7_10_expected():
    return {
        "instance_id": "i1",
        "metadata": '{"note":"keep"}',
        "temperature": 0.7,
    }


def test_7_10_cli_decodes_a_nested_value_while_moving_it_out(tmp_path, capsys):
    rows = _run_cli(_7_10_EXPRESSION, [_7_10_record()], tmp_path, capsys)
    assert rows == [_7_10_expected()]


def test_7_10_python_decodes_a_nested_value_while_moving_it_out():
    result = _run_python(_7_10_EXPRESSION, _7_10_record())
    assert result == _7_10_expected()
    # Decoded on the way out: a number at the root, not the string "0.7".
    assert result["temperature"] == 0.7 and not isinstance(
        result["temperature"], str
    )


# ===========================================================================
# 7.11  Copy a nested value rather than moving it
# ===========================================================================

_7_11_EXPRESSION = (
    "fromjson(.metadata); .temperature = .metadata.temperature; "
    "tojson(.metadata)"
)


def _7_11_record():
    return {
        "instance_id": "i1",
        "metadata": json.dumps({"temperature": 0.7, "note": "keep"}),
    }


def _7_11_expected():
    return {
        "instance_id": "i1",
        "metadata": '{"temperature":0.7,"note":"keep"}',
        "temperature": 0.7,
    }


def test_7_11_cli_copies_a_nested_value_leaving_the_source(tmp_path, capsys):
    rows = _run_cli(_7_11_EXPRESSION, [_7_11_record()], tmp_path, capsys)
    assert rows == [_7_11_expected()]


def test_7_11_python_copies_a_nested_value_leaving_the_source():
    result = _run_python(_7_11_EXPRESSION, _7_11_record())
    assert result == _7_11_expected()
    # The value is in both places, which is what distinguishes = from <-.
    assert json.loads(result["metadata"])["temperature"] == 0.7
    assert result["temperature"] == 0.7


# ===========================================================================
# 7.12  Focused structural operation followed by bare tools
# ===========================================================================

_7_12_EXPRESSION = (
    ".metadata << .(^instance_id|messages|tools) | normalize_metadata | tojson"
)


def _7_12_record():
    return {
        "instance_id": "i1",
        "messages": [{"role": "user"}],
        "tools": [{"name": "t"}],
        "Temperature": 0.7,
        "Score": 3,
    }


def _7_12_expected():
    return {
        "instance_id": "i1",
        "messages": [{"role": "user"}],
        "tools": [{"name": "t"}],
        "metadata": '{"temperature":0.7,"score":3}',
    }


def test_7_12_cli_focused_operation_then_bare_tools(
    catalogue_provider, tmp_path, capsys
):
    rows = _run_cli(_7_12_EXPRESSION, [_7_12_record()], tmp_path, capsys)
    assert rows == [_7_12_expected()]


def test_7_12_python_focused_operation_then_bare_tools(catalogue_provider):
    result = _run_python(_7_12_EXPRESSION, _7_12_record())
    assert result == _7_12_expected()
    # Both bare tools applied to .metadata (the focus), and the emitted value
    # is still the complete root record — the plan's stated guarantee for 7.12.
    assert result["instance_id"] == "i1"
    assert "Temperature" not in result and "Score" not in result


# ===========================================================================
# 7.13  Existing syntax remains expressible
# ===========================================================================

_7_13_LEGACY = "fromjson(.tools) | fromjson(.metadata.annotation, recursive=true)"
_7_13_CANONICAL = (
    "fromjson(.tools); fromjson(.metadata.annotation, recursive=true)"
)


def test_7_13_cli_legacy_and_canonical_forms_agree(tmp_path, capsys):
    legacy_rows = _run_cli(
        _7_13_LEGACY, [_7_1_record()], tmp_path / "legacy", capsys
    )
    canonical_rows = _run_cli(
        _7_13_CANONICAL, [_7_1_record()], tmp_path / "canonical", capsys
    )

    assert legacy_rows == canonical_rows == [_7_1_expected()]


def test_7_13_python_legacy_and_canonical_forms_agree():
    import warnings

    from datapipe.cli.transform import _compile_or_report
    from datapipe.dsl.compiler import CompiledProgram
    from datapipe.stages.tool_program import CompiledToolProgramStage

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        legacy_compiled = _compile_or_report(_7_13_LEGACY)

    legacy_stage = (
        CompiledProgramStage(legacy_compiled)
        if isinstance(legacy_compiled, CompiledProgram)
        else CompiledToolProgramStage(legacy_compiled)
    )
    legacy = legacy_stage.process(_7_1_record(), None)
    canonical = _run_python(_7_13_CANONICAL, _7_1_record())

    assert legacy == canonical == _7_1_expected()


def test_the_catalogue_covers_all_thirteen_cases():
    """Guards this file: a silently dropped case must not go unnoticed."""
    import re
    from pathlib import Path

    text = Path(__file__).read_text()
    for case in range(1, 14):
        cli = re.search(rf"\ndef test_7_{case}_cli_", text)
        python = re.search(rf"\ndef test_7_{case}_python_", text)
        assert cli, f"§7.{case} has no CLI-level test"
        assert python, f"§7.{case} has no Python-level test"
