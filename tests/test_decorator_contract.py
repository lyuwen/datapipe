"""First-parameter annotation vs. declared input contract (§5.4, §8.2, §15.1)."""

from __future__ import annotations

from typing import Any

import pytest

from datapipe.tools.contract import ToolContract
from datapipe.tools.decorator import ToolDecoratorError, tool
from datapipe.tools.types import JsonType, OneOf


def _decorate(annotation, input_spec):
    """Apply @tool to a function whose first parameter carries *annotation*."""

    def fn(value):
        return value

    fn.__annotations__ = {"value": annotation}
    return tool(
        name="probe",
        target="value",
        input=input_spec,
        output=JsonType.ANY,
        description="probe",
    )(fn)


# ---------------------------------------------------------------------------
# Conflicts must be rejected
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "annotation,input_spec",
    [
        (dict, JsonType.STRING),
        (str, JsonType.OBJECT),
        (list, JsonType.STRING),
        (str, JsonType.ARRAY),
        (int, JsonType.STRING),
        (str, JsonType.INTEGER),
        (bool, JsonType.STRING),
        (dict, JsonType.ARRAY),
        (list, JsonType.OBJECT),
        (str, JsonType.NULL),
        (dict, JsonType.SCALAR),
        (str, JsonType.CONTAINER),
        (bool, JsonType.INTEGER),
        (bool, JsonType.NUMBER),
        # `float` carries a second probe (1) for the numeric tower. These pin
        # that the widening reaches INTEGER only and punches no hole elsewhere.
        (float, JsonType.STRING),
        (float, JsonType.ARRAY),
        (float, JsonType.OBJECT),
        (float, JsonType.NULL),
        (float, JsonType.BOOLEAN),
        (float, JsonType.CONTAINER),
        (int, JsonType.ARRAY),
        (int, JsonType.OBJECT),
        (int, JsonType.NULL),
        (int, JsonType.BOOLEAN),
        (int, JsonType.CONTAINER),
        (dict, OneOf(JsonType.STRING, JsonType.ARRAY)),
        (str, OneOf(JsonType.ARRAY, JsonType.OBJECT)),
        (float, OneOf(JsonType.STRING, JsonType.ARRAY)),
        (int, OneOf(JsonType.STRING, JsonType.ARRAY)),
    ],
)
def test_contradicting_annotation_rejected(annotation, input_spec):
    with pytest.raises(ToolDecoratorError) as exc:
        _decorate(annotation, input_spec)
    message = str(exc.value)
    assert annotation.__name__ in message
    assert "contradicts" in message


def test_error_names_both_annotation_and_declared_type():
    with pytest.raises(ToolDecoratorError) as exc:
        _decorate(dict, JsonType.STRING)
    message = str(exc.value)
    assert "'probe'" in message
    assert "dict" in message
    assert "string" in message


def test_parameterized_generic_annotation_rejected():
    with pytest.raises(ToolDecoratorError):
        _decorate(dict[str, int], JsonType.STRING)


def test_string_annotation_is_resolved_before_comparison():
    # PEP 563: this module has `from __future__ import annotations`, so the
    # annotation below reaches the decorator as the string "dict".
    with pytest.raises(ToolDecoratorError):

        @tool(
            name="deferred",
            target="value",
            input=JsonType.STRING,
            output=JsonType.STRING,
            description="x",
        )
        def deferred(value: dict) -> str:
            return "x"


def test_record_target_annotation_conflict_rejected():
    with pytest.raises(ToolDecoratorError):

        @tool(
            name="rec",
            target="record",
            input=JsonType.OBJECT,
            output=JsonType.OBJECT,
            description="x",
        )
        def rec(record: str) -> dict:
            return {}


# ---------------------------------------------------------------------------
# Legitimate cases must keep working (false-rejection risk)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "annotation,input_spec",
    [
        (str, JsonType.STRING),
        (dict, JsonType.OBJECT),
        (list, JsonType.ARRAY),
        (int, JsonType.INTEGER),
        (int, JsonType.NUMBER),
        (float, JsonType.NUMBER),
        (bool, JsonType.BOOLEAN),
        (type(None), JsonType.NULL),
        (str, JsonType.SCALAR),
        (int, JsonType.SCALAR),
        (list, JsonType.CONTAINER),
        (dict, JsonType.CONTAINER),
        # ANY contract accepts every annotation.
        (str, JsonType.ANY),
        (dict, JsonType.ANY),
        (bool, JsonType.ANY),
        # OneOf: compatible with at least one member is enough.
        (str, OneOf(JsonType.STRING, JsonType.ARRAY)),
        (list, OneOf(JsonType.STRING, JsonType.ARRAY)),
        (dict, OneOf(JsonType.STRING, JsonType.ARRAY, JsonType.OBJECT)),
        # Unmapped / opaque annotations are documentation, not contradictions.
        (Any, JsonType.STRING),
        (object, JsonType.STRING),
        (complex, JsonType.STRING),
    ],
)
def test_compatible_annotation_accepted(annotation, input_spec):
    fn = _decorate(annotation, input_spec)
    assert isinstance(fn.__tool_contract__, ToolContract)


def test_unannotated_first_parameter_accepted():
    @tool(
        name="bare",
        target="value",
        input=JsonType.STRING,
        output=JsonType.STRING,
        description="x",
    )
    def bare(value):
        return value

    assert bare.__tool_contract__.name == "bare"


def test_float_annotation_accepts_integer_contract():
    # PEP 484 numeric tower: an int argument is acceptable where float is
    # annotated, so `value: float` with input=INTEGER is not a contradiction.
    @tool(
        name="numeric",
        target="value",
        input=JsonType.INTEGER,
        output=JsonType.NUMBER,
        description="x",
    )
    def numeric(value: float) -> float:
        return value

    assert numeric.__tool_contract__.input_type.matches(1)


def test_int_annotation_rejected_for_float_only_contexts_is_not_a_thing():
    # The widening is one-directional: int annotated against NUMBER is fine
    # (JSON NUMBER matches ints), which is what existing providers rely on.
    fn = _decorate(int, JsonType.NUMBER)
    assert fn.__tool_contract__.input_type.matches(1)


def test_numeric_annotations_accept_exactly_the_numeric_contracts():
    # Guards the blast radius of float's extra probe: `int` and `float` must
    # accept the numeric contracts and the umbrellas that contain them, and
    # nothing else.  A future probe-map edit that leaks acceptance into
    # STRING/ARRAY/OBJECT/NULL/BOOLEAN/CONTAINER fails here.
    expected = {
        JsonType.INTEGER,
        JsonType.NUMBER,
        JsonType.SCALAR,
        JsonType.ANY,
    }
    for annotation in (int, float):
        accepted = set()
        for json_type in JsonType:
            try:
                _decorate(annotation, json_type)
            except ToolDecoratorError:
                continue
            accepted.add(json_type)
        assert accepted == expected, f"{annotation.__name__} accepted {accepted}"


def test_oneof_with_a_numeric_member_accepts_float():
    fn = _decorate(float, OneOf(JsonType.STRING, JsonType.INTEGER))
    assert isinstance(fn.__tool_contract__, ToolContract)


def test_builtin_tools_decorate_cleanly():
    from datapipe.tools.builtins import json as builtin_json

    assert builtin_json.fromjson.__tool_contract__.name == "fromjson"
    assert builtin_json.tojson.__tool_contract__.name == "tojson"


def test_annotation_check_runs_after_contract_construction():
    # An invalid contract must still report the contract error, not be masked
    # by the annotation check.
    with pytest.raises(ValueError, match="cardinality"):

        @tool(
            name="bad_card",
            target="value",
            input=JsonType.STRING,
            output=JsonType.STRING,
            cardinality="one_to_many",
            description="x",
        )
        def bad_card(value: dict) -> str:
            return "x"
