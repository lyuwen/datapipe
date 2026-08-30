"""Phase 5 tests: type inference, selector rendering, ToolExecutionError, and
runtime validation modes.

Covers tasks A-G of the Phase 5 validation/diagnostics work:
  - datapipe.tools.types.infer_json_type / describe
  - CompiledSelector.render
  - datapipe.tools.errors.ToolExecutionError (message format + pickling)
  - CompiledToolProgramStage validate="always" | "sample" | "off"
  - ToolInvocation.expression_span
  - _error_payload "tool" key
  - end-to-end `datapipe transform --validate-tools ...`
"""

from __future__ import annotations

import json
import math
import pickle
from pathlib import Path

import pytest

from datapipe.cli.main import main
from datapipe.context import WorkerContext
from datapipe.dsl.compiler import CompiledExpression, ToolInvocation, compile_expression
from datapipe.stages.tool_program import (
    SAMPLE_LIMIT,
    CompiledToolProgramStage,
    _provider_id,
)
from datapipe.tools import describe, infer_json_type
from datapipe.tools.decorator import get_contract, tool
from datapipe.tools.errors import ToolExecutionError
from datapipe.tools.types import JsonType, OneOf


def _ctx(record_index: int | None = 0) -> WorkerContext:
    return WorkerContext(rank=0, world_size=1, worker_id=0, record_index=record_index)


def _write_jsonl(path: Path, rows: list) -> None:
    path.write_text("".join(json.dumps(r) + "\n" for r in rows))


def _read_jsonl(path: Path) -> list:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


# ---------------------------------------------------------------------------
# Task A — infer_json_type / describe
# ---------------------------------------------------------------------------


class TestInferJsonType:
    @pytest.mark.parametrize(
        "value, expected",
        [
            (None, JsonType.NULL),
            (True, JsonType.BOOLEAN),
            (False, JsonType.BOOLEAN),
            (0, JsonType.INTEGER),
            (1, JsonType.INTEGER),
            (-1, JsonType.INTEGER),
            (3.14, JsonType.NUMBER),
            (-0.5, JsonType.NUMBER),
            ("", JsonType.STRING),
            ("s", JsonType.STRING),
            ([], JsonType.ARRAY),
            ([1, 2], JsonType.ARRAY),
            ({}, JsonType.OBJECT),
            ({"a": 1}, JsonType.OBJECT),
        ],
    )
    def test_specific_types(self, value, expected):
        assert infer_json_type(value) is expected

    def test_bool_is_checked_before_int(self):
        # bool is an int subclass; the more specific answer must win.
        assert infer_json_type(True) is JsonType.BOOLEAN
        assert infer_json_type(True) is not JsonType.INTEGER

    @pytest.mark.parametrize(
        "value",
        [float("nan"), float("inf"), float("-inf")],
    )
    def test_non_finite_floats_are_not_json(self, value):
        assert infer_json_type(value) is None

    @pytest.mark.parametrize("value", [set(), object(), {1, 2}, (1, 2)])
    def test_non_json_values(self, value):
        assert infer_json_type(value) is None

    def test_integral_float_is_number_not_integer(self):
        # A float stays a NUMBER even when its value is integral, so the
        # int/float distinction survives a round-trip.
        assert infer_json_type(2.0) is JsonType.NUMBER

    def test_returns_only_specific_types(self):
        # The umbrella types are never inferred.
        umbrella = {JsonType.ANY, JsonType.SCALAR, JsonType.CONTAINER}
        for value in [None, True, 1, 1.5, "s", [], {}]:
            assert infer_json_type(value) not in umbrella


class TestDescribe:
    def test_plain_json_type(self):
        assert describe(JsonType.STRING) == "string"
        assert describe(JsonType.NULL) == "null"
        assert describe(JsonType.ANY) == "any"

    def test_one_of(self):
        spec = OneOf(JsonType.STRING, JsonType.ARRAY, JsonType.OBJECT)
        assert describe(spec) == "string | array | object"

    def test_two_member_one_of(self):
        assert describe(OneOf(JsonType.STRING, JsonType.NULL)) == "string | null"

    def test_type_spec_singleton(self):
        from datapipe.tools.types import as_type_spec

        assert describe(as_type_spec(JsonType.INTEGER)) == "integer"

    def test_rejects_non_type(self):
        with pytest.raises(TypeError):
            describe("string")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Task B — CompiledSelector.render
# ---------------------------------------------------------------------------


def _render(expr_selector: str) -> str:
    compiled = compile_expression(f"fromjson({expr_selector})")
    return compiled.invocations[0].selector.render()


class TestSelectorRender:
    def test_root(self):
        assert _render(".") == "."

    def test_single_field(self):
        assert _render(".data") == ".data"

    def test_nested_field(self):
        assert _render(".a.b.c") == ".a.b.c"

    def test_index(self):
        assert _render(".items[0]") == ".items[0]"

    def test_leading_index(self):
        assert _render(".[0]") == ".[0]"

    def test_quoted_key(self):
        assert _render('.["key.with.dots"]') == '.["key.with.dots"]'

    def test_leading_quoted_key(self):
        assert _render('.["a b"]') == '.["a b"]'

    def test_wildcard(self):
        assert _render(".tools[]") == ".tools[]"

    def test_mixed_path(self):
        assert _render(".tools[].function.parameters") == ".tools[].function.parameters"

    def test_render_round_trips_through_the_parser(self):
        # Rendering must produce text the parser accepts back to the same AST.
        for sel in [".", ".a", ".a.b", ".items[0]", ".tools[]", '.["k"]']:
            assert _render(_render(sel)) == _render(sel)

    def test_has_wildcard(self):
        compiled = compile_expression("fromjson(.tools[])")
        assert compiled.invocations[0].selector.has_wildcard is True
        compiled = compile_expression("fromjson(.tools)")
        assert compiled.invocations[0].selector.has_wildcard is False

    def test_repr_uses_render(self):
        compiled = compile_expression("fromjson(.a.b)")
        assert ".a.b" in repr(compiled.invocations[0].selector)

    def test_cli_no_longer_has_a_second_renderer(self):
        import datapipe.cli.transform as transform_mod

        assert not hasattr(transform_mod, "_part_str")

    def test_dry_run_output_uses_render(self, tmp_path, capsys):
        src = tmp_path / "in.jsonl"
        src.write_text('{"x": 1}\n')
        rc = main([
            "transform",
            "fromjson(.tools[].function)",
            str(src), str(tmp_path / "out.jsonl"),
            "--dry-run",
        ])
        assert rc == 0
        out = capsys.readouterr().out
        assert "fromjson(.tools[].function)" in out
        # The old renderer produced a doubled leading dot.
        assert "..tools" not in out


# ---------------------------------------------------------------------------
# Task C — ToolExecutionError
# ---------------------------------------------------------------------------


def _make_error(**overrides) -> ToolExecutionError:
    kwargs = dict(
        record_seq=1842,
        invocation_index=2,
        tool_name="fromjson",
        provider_id="builtin:json",
        expression_span=(0, 24),
        selector=".metadata.annotation",
        matched_path=".metadata.annotation",
        match_ordinal=None,
        expected_type="string",
        actual_type="null",
        stage="input",
        cause=None,
    )
    kwargs.update(overrides)
    return ToolExecutionError(**kwargs)


class TestToolExecutionErrorMessage:
    def test_input_mismatch_format(self):
        err = _make_error()
        assert str(err) == (
            "record 1842 failed in fromjson at .metadata.annotation\n"
            "provider: builtin:json\n"
            "invocation: 2\n"
            "expected input: string\n"
            "actual input: null"
        )

    def test_output_mismatch_uses_output_labels(self):
        err = _make_error(stage="output", expected_type="string", actual_type="integer")
        lines = str(err).splitlines()
        assert lines[-2] == "expected output: string"
        assert lines[-1] == "actual output: integer"

    def test_call_failure_includes_cause_and_omits_types(self):
        cause = ValueError("boom")
        err = _make_error(
            stage="call", expected_type=None, actual_type=None, cause=cause
        )
        text = str(err)
        assert "expected" not in text
        assert "actual" not in text
        assert "cause: ValueError: boom" in text

    def test_expected_and_actual_omitted_when_unset(self):
        err = _make_error(expected_type=None, actual_type=None)
        assert str(err).splitlines() == [
            "record 1842 failed in fromjson at .metadata.annotation",
            "provider: builtin:json",
            "invocation: 2",
        ]

    def test_expected_without_actual(self):
        err = _make_error(actual_type=None)
        text = str(err)
        assert "expected input: string" in text
        assert "actual input" not in text

    def test_falls_back_to_selector_when_no_matched_path(self):
        err = _make_error(matched_path=None, selector=".a[]")
        assert str(err).startswith("record 1842 failed in fromjson at .a[]")

    def test_unknown_record_seq_renders_placeholder(self):
        err = _make_error(record_seq=None)
        assert str(err).startswith("record ? failed in fromjson")

    def test_args_message_matches_str(self):
        err = _make_error()
        assert str(err) == err.args[0]


class TestToolExecutionErrorPickle:
    _FIELDS = (
        "record_seq",
        "invocation_index",
        "tool_name",
        "provider_id",
        "expression_span",
        "selector",
        "matched_path",
        "match_ordinal",
        "expected_type",
        "actual_type",
        "stage",
    )

    def test_round_trip_preserves_every_field(self):
        err = _make_error(match_ordinal=3)
        restored = pickle.loads(pickle.dumps(err))

        assert isinstance(restored, ToolExecutionError)
        for field in self._FIELDS:
            assert getattr(restored, field) == getattr(err, field), field
        assert restored.cause is None
        assert str(restored) == str(err)

    def test_round_trip_preserves_cause(self):
        err = _make_error(stage="call", expected_type=None, actual_type=None,
                          cause=ValueError("boom"))
        restored = pickle.loads(pickle.dumps(err))

        assert type(restored.cause) is ValueError
        assert str(restored.cause) == "boom"
        assert str(restored) == str(err)

    def test_round_trip_with_none_span(self):
        err = _make_error(expression_span=None)
        assert pickle.loads(pickle.dumps(err)).expression_span is None

    def test_span_is_stored_as_a_plain_tuple(self):
        err = _make_error(expression_span=[1, 2])
        assert err.expression_span == (1, 2)
        assert type(err.expression_span) is tuple

    def test_raisable_and_catchable_after_unpickling(self):
        restored = pickle.loads(pickle.dumps(_make_error()))
        with pytest.raises(ToolExecutionError):
            raise restored


# ---------------------------------------------------------------------------
# Task D — validation modes
# ---------------------------------------------------------------------------


@tool(
    name="phase5_boom",
    target="value",
    input=JsonType.ANY,
    output=JsonType.ANY,
    description="Always raises, to exercise stage='call' wrapping.",
)
def phase5_boom(value):
    raise RuntimeError("tool body failed")


@tool(
    name="phase5_bad_output",
    target="value",
    input=JsonType.ANY,
    output=JsonType.STRING,
    description="Declares a string output but returns an integer.",
)
def phase5_bad_output(value):
    return 123


def _manual_expression(fn, selector_expr: str, source: str) -> CompiledExpression:
    """Build a CompiledExpression around *fn*, bypassing the name registry."""
    reference = compile_expression(f"fromjson({selector_expr})")
    template = reference.invocations[0]
    contract = get_contract(fn)
    inv = ToolInvocation(
        tool_fn=fn,
        tool_name=contract.name,
        contract=contract,
        selector=template.selector,
        arguments={},
        expression_index=0,
        expression_span=template.expression_span,
    )
    return CompiledExpression(invocations=(inv,), source=source)


class TestValidateAlways:
    def test_input_mismatch_raises(self):
        stage = CompiledToolProgramStage(
            compile_expression("fromjson(.v)"), validate="always"
        )
        with pytest.raises(ToolExecutionError) as excinfo:
            stage.process({"v": None}, _ctx(1842))

        err = excinfo.value
        assert err.stage == "input"
        assert err.expected_type == "string | array | object"
        assert err.actual_type == "null"
        assert err.matched_path == ".v"
        assert err.selector == ".v"
        assert err.tool_name == "fromjson"
        assert err.provider_id == "builtin:json"
        assert err.record_seq == 1842
        assert err.invocation_index == 0

    def test_input_mismatch_on_number(self):
        stage = CompiledToolProgramStage(
            compile_expression("fromjson(.v)"), validate="always"
        )
        with pytest.raises(ToolExecutionError) as excinfo:
            stage.process({"v": 42}, _ctx(3))
        assert excinfo.value.actual_type == "integer"
        assert excinfo.value.stage == "input"

    def test_passing_values_do_not_raise(self):
        stage = CompiledToolProgramStage(
            compile_expression("fromjson(.v)"), validate="always"
        )
        assert stage.process({"v": '{"a": 1}'}, _ctx(0)) == {"v": {"a": 1}}

    def test_chained_expression_still_works(self):
        stage = CompiledToolProgramStage(
            compile_expression("fromjson(.tools) | tojson(.tools[].name)"),
            validate="always",
        )
        result = stage.process({"tools": '[{"name": {"x": 1}}]'}, _ctx(0))
        assert result == {"tools": [{"name": '{"x":1}'}]}

    def test_output_mismatch_raises(self):
        stage = CompiledToolProgramStage(
            _manual_expression(phase5_bad_output, ".v", "phase5_bad_output(.v)"),
            validate="always",
        )
        with pytest.raises(ToolExecutionError) as excinfo:
            stage.process({"v": "anything"}, _ctx(5))

        err = excinfo.value
        assert err.stage == "output"
        assert err.expected_type == "string"
        assert err.actual_type == "integer"
        assert "expected output: string" in str(err)

    def test_second_invocation_index_is_reported(self):
        # The failing invocation is the second one: .n is a number, and
        # fromjson rejects numbers.
        stage = CompiledToolProgramStage(
            compile_expression("fromjson(.v) | fromjson(.n)"), validate="always"
        )
        with pytest.raises(ToolExecutionError) as excinfo:
            stage.process({"v": "[]", "n": 7}, _ctx(0))
        assert excinfo.value.invocation_index == 1


class TestValidateOff:
    def test_input_mismatch_is_not_a_validation_error(self):
        stage = CompiledToolProgramStage(
            compile_expression("fromjson(.v)"), validate="off"
        )
        # The tool body still rejects None, but that is a call failure, not a
        # contract validation failure.
        with pytest.raises(ToolExecutionError) as excinfo:
            stage.process({"v": None}, _ctx(0))
        assert excinfo.value.stage == "call"
        assert excinfo.value.expected_type is None
        assert excinfo.value.actual_type is None

    def test_output_mismatch_is_not_checked(self):
        stage = CompiledToolProgramStage(
            _manual_expression(phase5_bad_output, ".v", "phase5_bad_output(.v)"),
            validate="off",
        )
        # Declared output is STRING but the tool returns an int; with checks
        # off the value is written back untouched.
        assert stage.process({"v": "x"}, _ctx(0)) == {"v": 123}

    def test_valid_records_are_unaffected(self):
        stage = CompiledToolProgramStage(
            compile_expression("fromjson(.v)"), validate="off"
        )
        assert stage.process({"v": "[1]"}, _ctx(0)) == {"v": [1]}


class TestValidateSample:
    def _stage(self):
        return CompiledToolProgramStage(
            compile_expression("fromjson(.v)"), validate="sample"
        )

    def test_early_records_are_validated(self):
        stage = self._stage()
        with pytest.raises(ToolExecutionError) as excinfo:
            stage.process({"v": None}, _ctx(0))
        assert excinfo.value.stage == "input"

    def test_stops_validating_after_sample_limit(self):
        stage = self._stage()
        # Drive exactly SAMPLE_LIMIT good records through the sampling window.
        for i in range(SAMPLE_LIMIT):
            assert stage.process({"v": "[]"}, _ctx(i)) == {"v": []}

        # The window is now closed: a record that would have failed input
        # validation is passed straight to the tool instead.
        with pytest.raises(ToolExecutionError) as excinfo:
            stage.process({"v": None}, _ctx(SAMPLE_LIMIT))
        assert excinfo.value.stage == "call"

    def test_boundary_record_is_still_validated(self):
        stage = self._stage()
        for i in range(SAMPLE_LIMIT - 1):
            stage.process({"v": "[]"}, _ctx(i))
        # Record number SAMPLE_LIMIT is the last one inside the window.
        with pytest.raises(ToolExecutionError) as excinfo:
            stage.process({"v": None}, _ctx(SAMPLE_LIMIT - 1))
        assert excinfo.value.stage == "input"

    def test_counter_is_per_record_not_per_value(self):
        stage = CompiledToolProgramStage(
            compile_expression("fromjson(.items[])"), validate="sample"
        )
        # Each record has 3 matches but must consume only one sample slot.
        for i in range(10):
            stage.process({"items": ["[]", "[]", "[]"]}, _ctx(i))
        assert stage._validated_records == 10

    def test_sampling_is_deterministic(self):
        counts = []
        for _ in range(3):
            stage = self._stage()
            validated = 0
            for i in range(SAMPLE_LIMIT + 25):
                try:
                    stage.process({"v": "[]"}, _ctx(i))
                except ToolExecutionError:  # pragma: no cover
                    pass
                validated = stage._validated_records
            counts.append(validated)
        assert counts == [SAMPLE_LIMIT] * 3


class TestValidateModeConstruction:
    @pytest.mark.parametrize("mode", ["always", "sample", "off"])
    def test_accepted_modes(self, mode):
        stage = CompiledToolProgramStage(
            compile_expression("fromjson(.v)"), validate=mode
        )
        assert stage.validate == mode

    @pytest.mark.parametrize("mode", ["Always", "on", "", "none", "sampled", None, 1])
    def test_invalid_mode_raises_value_error(self, mode):
        with pytest.raises(ValueError, match="invalid validate mode"):
            CompiledToolProgramStage(
                compile_expression("fromjson(.v)"), validate=mode
            )

    def test_default_is_always(self):
        stage = CompiledToolProgramStage(compile_expression("fromjson(.v)"))
        assert stage.validate == "always"

    def test_stage_is_pickleable(self):
        stage = CompiledToolProgramStage(
            compile_expression("fromjson(.v)"), validate="sample"
        )
        restored = pickle.loads(pickle.dumps(stage))
        assert restored.validate == "sample"


class TestToolBodyFailures:
    def test_call_failure_is_wrapped(self):
        stage = CompiledToolProgramStage(
            _manual_expression(phase5_boom, ".v", "phase5_boom(.v)"),
            validate="always",
        )
        with pytest.raises(ToolExecutionError) as excinfo:
            stage.process({"v": "anything"}, _ctx(9))

        err = excinfo.value
        assert err.stage == "call"
        assert err.expected_type is None
        assert err.actual_type is None
        assert type(err.cause) is RuntimeError
        assert str(err.cause) == "tool body failed"
        assert err.matched_path == ".v"
        assert err.record_seq == 9

    def test_original_traceback_is_preserved(self):
        stage = CompiledToolProgramStage(
            _manual_expression(phase5_boom, ".v", "phase5_boom(.v)"),
            validate="always",
        )
        with pytest.raises(ToolExecutionError) as excinfo:
            stage.process({"v": "x"}, _ctx(0))
        # `raise ... from exc` sets __cause__ and keeps the original frames.
        assert type(excinfo.value.__cause__) is RuntimeError
        assert excinfo.value.__cause__.__traceback__ is not None

    def test_builtin_body_failure(self):
        stage = CompiledToolProgramStage(
            compile_expression("fromjson(.v)"), validate="always"
        )
        with pytest.raises(ToolExecutionError) as excinfo:
            stage.process({"v": "not json"}, _ctx(4))
        assert excinfo.value.stage == "call"
        assert isinstance(excinfo.value.cause, json.JSONDecodeError)

    def test_tool_execution_error_is_not_double_wrapped(self):
        stage = CompiledToolProgramStage(
            compile_expression("fromjson(.v)"), validate="always"
        )
        with pytest.raises(ToolExecutionError) as excinfo:
            stage.process({"v": "not json"}, _ctx(0))
        assert not isinstance(excinfo.value.cause, ToolExecutionError)


class TestMatchOrdinal:
    def test_set_for_wildcard_selectors(self):
        stage = CompiledToolProgramStage(
            compile_expression("fromjson(.items[])"), validate="always"
        )
        with pytest.raises(ToolExecutionError) as excinfo:
            stage.process({"items": ['"a"', '"b"', 7]}, _ctx(0))

        err = excinfo.value
        assert err.match_ordinal == 2
        assert err.matched_path == ".items[2]"
        assert err.selector == ".items[]"

    def test_first_wildcard_match_has_ordinal_zero(self):
        stage = CompiledToolProgramStage(
            compile_expression("fromjson(.items[])"), validate="always"
        )
        with pytest.raises(ToolExecutionError) as excinfo:
            stage.process({"items": [None, '"b"']}, _ctx(0))
        assert excinfo.value.match_ordinal == 0

    def test_none_for_non_wildcard_selectors(self):
        stage = CompiledToolProgramStage(
            compile_expression("fromjson(.v)"), validate="always"
        )
        with pytest.raises(ToolExecutionError) as excinfo:
            stage.process({"v": None}, _ctx(0))
        assert excinfo.value.match_ordinal is None

    def test_none_for_fixed_index_selector(self):
        stage = CompiledToolProgramStage(
            compile_expression("fromjson(.items[1])"), validate="always"
        )
        with pytest.raises(ToolExecutionError) as excinfo:
            stage.process({"items": ['"a"', None]}, _ctx(0))
        assert excinfo.value.match_ordinal is None
        assert excinfo.value.matched_path == ".items[1]"

    def test_set_for_call_failures_under_wildcard(self):
        stage = CompiledToolProgramStage(
            compile_expression("fromjson(.items[])"), validate="always"
        )
        with pytest.raises(ToolExecutionError) as excinfo:
            stage.process({"items": ['"a"', "nope"]}, _ctx(0))
        assert excinfo.value.stage == "call"
        assert excinfo.value.match_ordinal == 1


class TestProviderId:
    def test_builtin_json_tools(self):
        compiled = compile_expression("fromjson(.v) | tojson(.v)")
        assert _provider_id(compiled.invocations[0]) == "builtin:json"
        assert _provider_id(compiled.invocations[1]) == "builtin:json"

    def test_non_builtin_tool_uses_provider_prefix(self):
        compiled = _manual_expression(phase5_boom, ".v", "phase5_boom(.v)")
        assert _provider_id(compiled.invocations[0]) == f"provider:{__name__}"


# ---------------------------------------------------------------------------
# expression_span on ToolInvocation
# ---------------------------------------------------------------------------


class TestExpressionSpan:
    def test_populated_by_compile_expression(self):
        expr = "fromjson(.data)"
        inv = compile_expression(expr).invocations[0]
        assert inv.expression_span is not None
        start, end = inv.expression_span
        assert expr[start:end] == expr

    def test_span_is_a_plain_tuple(self):
        inv = compile_expression("fromjson(.data)").invocations[0]
        assert type(inv.expression_span) is tuple
        assert len(inv.expression_span) == 2
        assert all(isinstance(x, int) for x in inv.expression_span)

    def test_each_invocation_has_its_own_span(self):
        expr = "fromjson(.a) | tojson(.b)"
        invs = compile_expression(expr).invocations
        spans = [i.expression_span for i in invs]
        assert spans[0] != spans[1]
        assert expr[spans[0][0]:spans[0][1]] == "fromjson(.a)"
        assert expr[spans[1][0]:spans[1][1]] == "tojson(.b)"

    def test_defaults_to_none_when_omitted(self):
        # Existing constructor calls without the new field must keep working.
        template = compile_expression("fromjson(.v)").invocations[0]
        inv = ToolInvocation(
            tool_fn=template.tool_fn,
            tool_name=template.tool_name,
            contract=template.contract,
            selector=template.selector,
            arguments={},
            expression_index=0,
        )
        assert inv.expression_span is None

    def test_span_reaches_the_error(self):
        expr = "fromjson(.v)"
        stage = CompiledToolProgramStage(compile_expression(expr), validate="always")
        with pytest.raises(ToolExecutionError) as excinfo:
            stage.process({"v": None}, _ctx(0))
        assert excinfo.value.expression_span == (0, len(expr))


# ---------------------------------------------------------------------------
# Task E — _error_payload carries the tool fields
# ---------------------------------------------------------------------------


def _payload_for(exc):
    from datapipe.pipeline import _error_payload
    from datapipe.result import TaskResult

    return _error_payload(TaskResult(seq=1842, value=None, error=exc))


class TestErrorPayload:
    def test_includes_tool_key(self):
        err = _make_error(match_ordinal=2)
        payload = _payload_for(err)

        assert payload["tool"] == {
            "invocation_index": 2,
            "tool_name": "fromjson",
            "provider_id": "builtin:json",
            "expression_span": [0, 24],
            "selector": ".metadata.annotation",
            "matched_path": ".metadata.annotation",
            "match_ordinal": 2,
            "expected_type": "string",
            "actual_type": "null",
            "stage": "input",
        }

    def test_existing_top_level_keys_are_unchanged(self):
        payload = _payload_for(_make_error())
        assert payload["seq"] == 1842
        assert payload["error_type"] == "ToolExecutionError"
        assert payload["error_message"] == str(_make_error())
        assert "traceback" in payload
        assert "stage_name" in payload
        assert "metadata" in payload

    def test_payload_is_json_serializable(self):
        payload = _payload_for(_make_error(match_ordinal=0))
        restored = json.loads(json.dumps(payload))
        assert restored["tool"]["expression_span"] == [0, 24]
        assert restored["tool"]["match_ordinal"] == 0

    def test_null_span_serializes_as_null(self):
        payload = _payload_for(_make_error(expression_span=None))
        assert payload["tool"]["expression_span"] is None
        assert json.loads(json.dumps(payload))["tool"]["expression_span"] is None

    def test_unwrapped_from_stage_execution_error(self):
        from datapipe.errors import StageExecutionError

        wrapped = StageExecutionError(
            stage_name="tools", record_seq=1842, cause=_make_error()
        )
        payload = _payload_for(wrapped)
        assert payload["stage_name"] == "tools"
        assert payload["tool"]["tool_name"] == "fromjson"

    def test_no_tool_key_for_other_errors(self):
        payload = _payload_for(ValueError("plain"))
        assert "tool" not in payload
        assert payload["error_type"] == "ValueError"


# ---------------------------------------------------------------------------
# Tasks F/G — end-to-end via the CLI
# ---------------------------------------------------------------------------


class TestTransformValidationEndToEnd:
    #: .v is a number, which violates fromjson's OneOf(STRING, ARRAY, OBJECT).
    ROWS = [{"v": 1}]

    def _run(self, tmp_path, mode, executor="sequential", rows=None, errors="raise"):
        src = tmp_path / "in.jsonl"
        out = tmp_path / "out.jsonl"
        err_out = tmp_path / "errors.jsonl"
        _write_jsonl(src, rows if rows is not None else self.ROWS)

        rc = main([
            "transform",
            "fromjson(.v)",
            str(src), str(out),
            "--executor", executor,
            "--no-progress",
            "--errors", errors,
            "--error-output", str(err_out),
            "--validate-tools", mode,
        ])
        return rc, out, err_out

    def test_always_surfaces_validation_failure(self, tmp_path):
        rc, _out, err_out = self._run(tmp_path, "always", errors="return")
        assert rc == 0
        payloads = _read_jsonl(err_out)
        assert len(payloads) == 1
        tool_info = payloads[0]["tool"]
        assert tool_info["stage"] == "input"
        assert tool_info["expected_type"] == "string | array | object"
        assert tool_info["actual_type"] == "integer"
        assert tool_info["tool_name"] == "fromjson"
        assert tool_info["provider_id"] == "builtin:json"
        assert tool_info["selector"] == ".v"

    def test_always_with_raise_policy_exits_nonzero(self, tmp_path):
        rc, _out, _err = self._run(tmp_path, "always", errors="raise")
        assert rc != 0

    def test_off_does_not_fail_validation(self, tmp_path):
        # With validation off the value reaches the tool body, which raises a
        # call error rather than a contract violation.
        rc, _out, err_out = self._run(tmp_path, "off", errors="return")
        assert rc == 0
        payloads = _read_jsonl(err_out)
        assert len(payloads) == 1
        assert payloads[0]["tool"]["stage"] == "call"
        assert payloads[0]["tool"]["expected_type"] is None

    def test_off_passes_valid_records_through(self, tmp_path):
        rc, out, _err = self._run(
            tmp_path, "off", rows=[{"v": '{"a": 1}'}], errors="return"
        )
        assert rc == 0
        assert _read_jsonl(out) == [{"v": {"a": 1}}]

    def test_sample_mode_runs(self, tmp_path):
        rc, out, _err = self._run(
            tmp_path, "sample", rows=[{"v": "[1]"}] * 5, errors="return"
        )
        assert rc == 0
        assert _read_jsonl(out) == [{"v": [1]}] * 5

    def test_flag_is_actually_wired_through(self, tmp_path):
        # Guards the original bug: --validate-tools was parsed and ignored.
        from datapipe.cli.transform import _build_pipeline

        pipeline = _build_pipeline(compile_expression("fromjson(.v)"), validate="off")
        stage = pipeline.stages[1]
        assert stage.validate == "off"

    def test_process_executor_preserves_tool_error(self, tmp_path):
        # The pickling path that matters most: the error is raised in a worker
        # process and must arrive intact in the coordinator.
        rc, _out, err_out = self._run(
            tmp_path, "always", executor="process", errors="return"
        )
        assert rc == 0
        payloads = _read_jsonl(err_out)
        assert len(payloads) == 1
        assert payloads[0]["error_type"] == "ToolExecutionError"
        tool_info = payloads[0]["tool"]
        assert tool_info["stage"] == "input"
        assert tool_info["expected_type"] == "string | array | object"
        assert tool_info["actual_type"] == "integer"
        assert tool_info["expression_span"] == [0, len("fromjson(.v)")]

    def test_process_executor_valid_records(self, tmp_path):
        rc, out, _err = self._run(
            tmp_path, "always", executor="process", rows=[{"v": "[1]"}] * 4
        )
        assert rc == 0
        assert _read_jsonl(out) == [{"v": [1]}] * 4
