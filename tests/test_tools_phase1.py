"""Phase 1 tool contract tests: JsonType, TypeSpec, ToolContract, @tool, fromjson, tojson."""

from __future__ import annotations

import math
import pickle

import pytest

from datapipe.tools import (
    Cardinality,
    JsonType,
    OneOf,
    ParameterSpec,
    ToolContract,
    ToolDecoratorError,
    ToolExample,
    as_type_spec,
    fromjson,
    get_contract,
    is_tool,
    make_contract,
    matches,
    tojson,
    tool,
)
from datapipe.tools.types import _SimpleTypeSpec


# ---------------------------------------------------------------------------
# JsonType and type matching
# ---------------------------------------------------------------------------


class TestJsonTypeMatching:
    def test_null_matches_none(self):
        assert matches(None, JsonType.NULL)

    def test_null_rejects_zero(self):
        assert not matches(0, JsonType.NULL)

    def test_boolean_matches_true_false(self):
        assert matches(True, JsonType.BOOLEAN)
        assert matches(False, JsonType.BOOLEAN)

    def test_boolean_rejects_int(self):
        assert not matches(1, JsonType.BOOLEAN)

    def test_integer_matches_int_not_bool(self):
        assert matches(42, JsonType.INTEGER)
        assert not matches(True, JsonType.INTEGER)
        assert not matches(3.14, JsonType.INTEGER)

    def test_number_matches_int_and_float(self):
        assert matches(42, JsonType.NUMBER)
        assert matches(3.14, JsonType.NUMBER)
        assert not matches(True, JsonType.NUMBER)
        assert not matches(float("nan"), JsonType.NUMBER)
        assert not matches(float("inf"), JsonType.NUMBER)
        assert not matches(float("-inf"), JsonType.NUMBER)

    def test_string_matches_str(self):
        assert matches("hello", JsonType.STRING)
        assert not matches(b"hello", JsonType.STRING)

    def test_array_matches_list_only(self):
        assert matches([1, 2], JsonType.ARRAY)
        assert not matches((1, 2), JsonType.ARRAY)

    def test_object_matches_dict(self):
        assert matches({"a": 1}, JsonType.OBJECT)
        assert not matches([{"a": 1}], JsonType.OBJECT)

    def test_scalar_matches_primitives(self):
        for v in [None, True, 42, 3.14, "hi"]:
            assert matches(v, JsonType.SCALAR), f"expected SCALAR to match {v!r}"
        assert not matches([1], JsonType.SCALAR)
        assert not matches({}, JsonType.SCALAR)

    def test_container_matches_list_and_dict(self):
        assert matches([1], JsonType.CONTAINER)
        assert matches({"k": "v"}, JsonType.CONTAINER)
        assert not matches("str", JsonType.CONTAINER)

    def test_any_matches_everything(self):
        for v in [None, True, 1, 3.14, "s", [], {}]:
            assert matches(v, JsonType.ANY)

    def test_any_does_not_match_non_finite(self):
        # ANY does not imply JSON-serializable; matching is structural
        assert matches(float("nan"), JsonType.ANY)


class TestOneOf:
    def test_basic_union(self):
        spec = OneOf(JsonType.STRING, JsonType.ARRAY)
        assert spec.matches("hello")
        assert spec.matches([1, 2])
        assert not spec.matches(42)

    def test_requires_two_members(self):
        with pytest.raises(ValueError):
            OneOf(JsonType.STRING)

    def test_flattens_nested_one_of(self):
        inner = OneOf(JsonType.STRING, JsonType.ARRAY)
        outer = OneOf(inner, JsonType.OBJECT)
        assert len(outer.members) == 3

    def test_accepts_type_spec_members(self):
        spec = OneOf(as_type_spec(JsonType.STRING), as_type_spec(JsonType.NULL))
        assert spec.matches("x")
        assert spec.matches(None)

    def test_equality(self):
        a = OneOf(JsonType.STRING, JsonType.ARRAY)
        b = OneOf(JsonType.STRING, JsonType.ARRAY)
        assert a == b

    def test_repr(self):
        spec = OneOf(JsonType.STRING, JsonType.ARRAY)
        r = repr(spec)
        assert "OneOf" in r
        assert "STRING" in r

    def test_rejects_non_type(self):
        with pytest.raises(TypeError):
            OneOf("not_a_type", JsonType.STRING)  # type: ignore


class TestTypeSpecPickle:
    def test_simple_type_spec_pickleable(self):
        spec = as_type_spec(JsonType.STRING)
        assert pickle.loads(pickle.dumps(spec)) == spec

    def test_one_of_pickleable(self):
        spec = OneOf(JsonType.STRING, JsonType.ARRAY)
        assert pickle.loads(pickle.dumps(spec)) == spec


# ---------------------------------------------------------------------------
# ParameterSpec and ToolContract
# ---------------------------------------------------------------------------


class TestParameterSpec:
    def test_basic(self):
        p = ParameterSpec(name="lowercase", default=False, annotation=bool)
        assert p.name == "lowercase"
        assert p.default is False
        assert p.annotation is bool

    def test_invalid_name(self):
        with pytest.raises(ValueError):
            ParameterSpec(name="not valid", default=0)

    def test_pickleable(self):
        p = ParameterSpec(name="x", default=1)
        assert pickle.loads(pickle.dumps(p)) == p


class TestToolContract:
    def _make(self, **overrides):
        defaults = dict(
            name="test_tool",
            api_version=1,
            target="value",
            input=JsonType.STRING,
            output=JsonType.STRING,
            cardinality="one_to_one",
            deterministic=True,
            description="",
            parameters=(),
        )
        defaults.update(overrides)
        return make_contract(**defaults)

    def test_basic(self):
        c = self._make()
        assert c.name == "test_tool"
        assert c.target == "value"
        assert c.cardinality == Cardinality.ONE_TO_ONE

    def test_rejects_unsupported_api_version(self):
        with pytest.raises(ValueError, match="api_version"):
            self._make(api_version=2)

    def test_rejects_invalid_target(self):
        with pytest.raises(ValueError, match="target"):
            self._make(target="both")

    def test_rejects_unsupported_cardinality(self):
        with pytest.raises(ValueError, match="cardinality"):
            self._make(cardinality="one_to_many")

    def test_rejects_empty_name(self):
        with pytest.raises(ValueError):
            self._make(name="")

    def test_parameter_defaults(self):
        c = self._make(parameters=[
            ParameterSpec("a", default=1),
            ParameterSpec("b", default="x"),
        ])
        assert c.parameter_defaults() == {"a": 1, "b": "x"}

    def test_pickleable(self):
        c = self._make()
        assert pickle.loads(pickle.dumps(c)).name == c.name


# ---------------------------------------------------------------------------
# @tool decorator
# ---------------------------------------------------------------------------


class TestToolDecorator:
    def test_basic_decoration(self):
        @tool(
            name="greet",
            target="value",
            input=JsonType.STRING,
            output=JsonType.STRING,
        )
        def greet(value, *, uppercase: bool = False) -> str:
            return value.upper() if uppercase else value

        assert is_tool(greet)
        contract = get_contract(greet)
        assert contract is not None
        assert contract.name == "greet"
        assert len(contract.parameters) == 1
        assert contract.parameters[0].name == "uppercase"
        assert contract.parameters[0].default is False

    def test_function_still_works(self):
        @tool(
            name="double",
            target="value",
            input=JsonType.INTEGER,
            output=JsonType.INTEGER,
        )
        def double(value, *, factor: int = 2) -> int:
            return value * factor

        assert double(3) == 6
        assert double(3, factor=4) == 12

    def test_rejects_no_positional_param(self):
        with pytest.raises(ToolDecoratorError, match="positional"):
            @tool(name="bad", target="value", input=JsonType.ANY, output=JsonType.ANY)
            def bad(*, kw: int = 0):
                return kw

    def test_rejects_positional_config_param(self):
        with pytest.raises(ToolDecoratorError, match="keyword-only"):
            @tool(name="bad", target="value", input=JsonType.ANY, output=JsonType.ANY)
            def bad(value, extra: int = 0):
                return value

    def test_rejects_var_args(self):
        with pytest.raises(ToolDecoratorError, match=r"\*args"):
            @tool(name="bad", target="value", input=JsonType.ANY, output=JsonType.ANY)
            def bad(value, *args):
                return value

    def test_rejects_var_kwargs(self):
        with pytest.raises(ToolDecoratorError, match=r"\*\*kwargs"):
            @tool(name="bad", target="value", input=JsonType.ANY, output=JsonType.ANY)
            def bad(value, **kwargs):
                return value

    def test_rejects_param_without_default(self):
        with pytest.raises(ToolDecoratorError, match="no default"):
            @tool(name="bad", target="value", input=JsonType.ANY, output=JsonType.ANY)
            def bad(value, *, required_kw: int):
                return value

    def test_rejects_non_json_default(self):
        with pytest.raises(ToolDecoratorError, match="JSON-serializable"):
            @tool(name="bad", target="value", input=JsonType.ANY, output=JsonType.ANY)
            def bad(value, *, fn=lambda x: x):
                return value

    def test_is_tool_false_for_plain_function(self):
        def plain(x):
            return x
        assert not is_tool(plain)

    def test_get_contract_none_for_plain(self):
        assert get_contract(lambda x: x) is None

    def test_contract_pickleable(self):
        @tool(name="p", target="value", input=JsonType.ANY, output=JsonType.ANY)
        def p(value):
            return value
        contract = get_contract(p)
        c2 = pickle.loads(pickle.dumps(contract))
        assert c2.name == "p"

    def test_one_of_input(self):
        @tool(
            name="multi_input",
            target="value",
            input=OneOf(JsonType.STRING, JsonType.ARRAY),
            output=JsonType.ANY,
        )
        def multi_input(value):
            return value

        contract = get_contract(multi_input)
        assert isinstance(contract.input_type, OneOf)

    def test_record_target(self):
        @tool(name="rec", target="record", input=JsonType.OBJECT, output=JsonType.OBJECT)
        def rec(record):
            return record
        assert get_contract(rec).target == "record"


# ---------------------------------------------------------------------------
# Built-in: fromjson
# ---------------------------------------------------------------------------


class TestFromJson:
    def test_basic_decode_object(self):
        assert fromjson('{"a": 1}') == {"a": 1}

    def test_basic_decode_array(self):
        assert fromjson("[1, 2, 3]") == [1, 2, 3]

    def test_basic_decode_null(self):
        assert fromjson("null") is None

    def test_basic_decode_string_literal(self):
        assert fromjson('"hello"') == "hello"

    def test_non_string_raises(self):
        with pytest.raises(TypeError, match="string"):
            fromjson(42)

    def test_invalid_json_raises(self):
        with pytest.raises(Exception):
            fromjson("{not valid json}")

    def test_recursive_nested_object(self):
        value = {"a": '{"b": 2}'}
        result = fromjson(value, recursive=True)
        assert result == {"a": {"b": 2}}

    def test_recursive_nested_array(self):
        value = {"items": '[1, 2, 3]'}
        result = fromjson(value, recursive=True)
        assert result == {"items": [1, 2, 3]}

    def test_recursive_non_json_string_unchanged(self):
        value = {"note": "not json"}
        result = fromjson(value, recursive=True)
        assert result == {"note": "not json"}

    def test_recursive_containers_only_true(self):
        # "42" decodes to int — with containers_only=True, stays as string
        value = {"x": "42"}
        result = fromjson(value, recursive=True, containers_only=True)
        assert result == {"x": "42"}

    def test_recursive_containers_only_false(self):
        # "42" decodes to int — with containers_only=False, becomes int
        value = {"x": "42"}
        result = fromjson(value, recursive=True, containers_only=False)
        assert result == {"x": 42}

    def test_recursive_root_must_be_string(self):
        with pytest.raises(TypeError, match="root value must be a string"):
            fromjson(42, recursive=True)

    def test_recursive_root_string_decoded_then_traversed(self):
        value = '{"a": "[1,2]"}'
        result = fromjson(value, recursive=True)
        assert result == {"a": [1, 2]}

    def test_is_tool(self):
        assert is_tool(fromjson)

    def test_contract_name(self):
        assert get_contract(fromjson).name == "fromjson"

    def test_array_passthrough_non_recursive(self):
        # Without recursive, only strings are accepted (§6.1: "must be a string")
        with pytest.raises(TypeError, match="expected a string"):
            fromjson([1, 2])  # type: ignore[arg-type]

    def test_object_passthrough_non_recursive(self):
        with pytest.raises(TypeError, match="expected a string"):
            fromjson({"a": 1})  # type: ignore[arg-type]

    def test_recursive_deeply_nested(self):
        value = {"outer": '{"inner": "[1]"}'}
        result = fromjson(value, recursive=True)
        assert result == {"outer": {"inner": [1]}}

    def test_unicode(self):
        assert fromjson('"héllo"') == "héllo"


# ---------------------------------------------------------------------------
# Built-in: tojson
# ---------------------------------------------------------------------------


class TestToJson:
    def test_basic_object(self):
        result = tojson({"a": 1})
        assert result == '{"a":1}'

    def test_compact_false(self):
        result = tojson({"a": 1}, compact=False)
        assert result == '{"a": 1}'

    def test_sort_keys(self):
        result = tojson({"b": 2, "a": 1}, sort_keys=True)
        assert result == '{"a":1,"b":2}'

    def test_string_re_serialized(self):
        assert tojson("hello") == '"hello"'

    def test_array(self):
        assert tojson([1, 2, 3]) == "[1,2,3]"

    def test_null(self):
        assert tojson(None) == "null"

    def test_boolean(self):
        assert tojson(True) == "true"
        assert tojson(False) == "false"

    def test_integer(self):
        assert tojson(42) == "42"

    def test_float(self):
        result = tojson(3.14)
        assert result == "3.14"

    def test_non_finite_raises(self):
        with pytest.raises(ValueError, match="non-finite"):
            tojson(float("nan"))
        with pytest.raises(ValueError, match="non-finite"):
            tojson(float("inf"))

    def test_non_finite_nested_raises(self):
        with pytest.raises(ValueError, match="non-finite"):
            tojson({"x": float("nan")})

    def test_ensure_ascii_false(self):
        result = tojson("héllo", ensure_ascii=False)
        assert "é" in result

    def test_ensure_ascii_true(self):
        result = tojson("héllo", ensure_ascii=True)
        assert "é" not in result
        assert "\\u" in result

    def test_is_tool(self):
        assert is_tool(tojson)

    def test_contract_name(self):
        assert get_contract(tojson).name == "tojson"

    def test_unicode_passthrough(self):
        assert tojson("日本語") == '"日本語"'

    def test_examples_run(self):
        """Run the declared ToolExamples to verify they are accurate."""
        contract = get_contract(tojson)
        for ex in contract.examples:
            result = tojson(ex.input, **ex.arguments)
            assert result == ex.output, (
                f"example failed: tojson({ex.input!r}, **{ex.arguments!r}) "
                f"returned {result!r}, expected {ex.output!r}"
            )

    def test_fromjson_examples_run(self):
        """Run fromjson's declared ToolExamples."""
        contract = get_contract(fromjson)
        for ex in contract.examples:
            result = fromjson(ex.input, **ex.arguments)
            assert result == ex.output


# ---------------------------------------------------------------------------
# Public API surface: importable from datapipe.tools
# ---------------------------------------------------------------------------


class TestPublicImports:
    def test_all_public_symbols_importable(self):
        import datapipe.tools as dt
        for name in dt.__all__:
            assert hasattr(dt, name), f"datapipe.tools.{name} not found"

    def test_fromjson_importable_from_top(self):
        from datapipe.tools import fromjson as fj
        assert callable(fj)

    def test_tojson_importable_from_top(self):
        from datapipe.tools import tojson as tj
        assert callable(tj)
