"""Phase S5 tests: the `nest` and `unnest` record-level convenience tools.

TDD: written before the implementation.  The central property is §15.4
*named/symbolic equivalence* — `nest` and `unnest` must produce byte-identical
records to the `<<` forms they sugar.  The equivalence tables below are the
reason this phase exists; everything else guards the S3/S4 semantics these
tools inherit (§8.1 atomicity, §8.4 collisions, §8.5 strict positive sets,
§8.7 destination self-exclusion, §6.4 source object order, and aliasing).

Error assertions deliberately anchor to ``StructuralExecutionError.reason`` or
to the wrapped cause's own message rather than to substrings that also appear
in an echoed expression.
"""

from __future__ import annotations

import copy
import json
import pickle

import pytest

from datapipe.tools.errors import StructuralExecutionError, ToolExecutionError


@pytest.fixture(autouse=True)
def isolated_registry(tmp_path, monkeypatch):
    """Redirect the tool registry at tmp_path and clear the loader cache."""
    monkeypatch.setenv("DATAPIPE_USER_DATA", str(tmp_path / "dp_data"))

    from datapipe.tools import loader as _loader
    _loader._loaded_providers.clear()

    from datapipe.dsl import compiler as _compiler
    monkeypatch.setattr(_compiler, "_BUILTIN_REGISTRY", None, raising=False)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _stage(expression: str, validate: str = "always"):
    from datapipe.dsl.compiler import compile_program
    from datapipe.stages.tool_program import CompiledProgramStage

    return CompiledProgramStage(compile_program(expression), validate=validate)


def _run(expression: str, record):
    """Execute *expression* against *record* in-process and return the result."""
    return _stage(expression).process(record, None)


def _run_jsonl(expression, record, tmp_path, executor):
    from datapipe.io.jsonl import JsonlSink, JsonlSource
    from datapipe.pipeline import Pipeline
    from datapipe.stage import JsonDumpStage, JsonLoadStage

    tmp_path.mkdir(parents=True, exist_ok=True)
    pipeline = Pipeline([JsonLoadStage(), _stage(expression), JsonDumpStage()])
    inp = tmp_path / "in.jsonl"
    out = tmp_path / "out.jsonl"
    inp.write_text(json.dumps(record) + "\n")
    pipeline.run(
        source=JsonlSource(str(inp), raw=True),
        sink=JsonlSink(str(out), raw=True),
        executor=executor,
    )
    return json.loads(out.read_text().strip())


def _bytes(record) -> str:
    """Serialize *record* preserving key order, so ordering differences show up."""
    return json.dumps(record, sort_keys=False)


def _cause(exc_info):
    """Return the wrapped cause of a ToolExecutionError raised through the DSL."""
    return exc_info.value.cause


# ===========================================================================
# 1-3. Contract and registration
# ===========================================================================


def test_both_tools_resolve_by_name_through_the_compiler():
    from datapipe.dsl.compiler import compile_program

    program = compile_program(
        'nest(., key="m", exclude=["a"]); unnest(., key="m", include=["b"])'
    )
    names = [stmt.operation.tool_name for stmt in program.statements]
    assert names == ["nest", "unnest"]


def test_builtin_names_reserve_nest_and_unnest():
    from datapipe.dsl.compiler import _BUILTIN_NAMES, _build_builtin_registry

    assert {"nest", "unnest"} <= _BUILTIN_NAMES
    registry = _build_builtin_registry()
    assert {"nest", "unnest"} <= set(registry)


def test_both_tools_appear_in_builtin_inspection_output(capsys):
    from datapipe.cli.main import main

    for name in ("nest", "unnest"):
        rc = main(["tools", "inspect", name, "--json"])
        assert rc == 0, capsys.readouterr().err
        data = json.loads(capsys.readouterr().out)
        assert data["tool"]["name"] == name
        assert data["tool"]["target"] == "record"
        assert data["tool"]["input"] is not None
        assert data["tool"]["output"] is not None
        params = {p["name"] for p in data["tool"]["parameters"]}
        assert {"key", "include", "exclude", "jsonify"} <= params


def test_contracts_declare_record_target_and_object_types():
    from datapipe.tools.builtins.structural import nest, unnest
    from datapipe.tools.decorator import get_contract
    from datapipe.tools.types import JsonType, as_type_spec

    obj = as_type_spec(JsonType.OBJECT)
    for fn in (nest, unnest):
        contract = get_contract(fn)
        assert contract is not None
        assert contract.target == "record"
        assert contract.input_type == obj
        assert contract.output_type == obj
        assert contract.cardinality.value == "one_to_one"
        assert contract.deterministic is True
        assert contract.description


def test_record_target_rejects_a_non_root_selector():
    from datapipe.dsl.compiler import compile_program
    from datapipe.dsl.errors import ToolConfigurationError

    with pytest.raises(ToolConfigurationError) as exc:
        compile_program('nest(.metadata, key="m")')
    assert "target='record'" in exc.value.base_message


# ===========================================================================
# 4-12. nest
# ===========================================================================


# 4. §6.7 — the exact example from the plan.
def test_plan_6_7_example():
    record = {
        "instance_id": "abc",
        "messages": [{"role": "user"}],
        "tools": ["t1"],
        "temperature": 0.7,
        "score": 0.9,
    }
    result = _run(
        'nest(., key="metadata", '
        'exclude=["instance_id", "messages", "tools"], jsonify=true)',
        record,
    )
    assert result == {
        "instance_id": "abc",
        "messages": [{"role": "user"}],
        "tools": ["t1"],
        "metadata": '{"temperature":0.7,"score":0.9}',
    }


# 5. include nests only the named fields.
def test_include_nests_only_the_named_fields():
    from datapipe.tools.builtins.structural import nest

    result = nest({"a": 1, "b": 2, "c": 3}, key="m", include=["b", "c"])
    assert result == {"a": 1, "m": {"b": 2, "c": 3}}


# 6. exclude nests everything else.
def test_exclude_nests_everything_else():
    from datapipe.tools.builtins.structural import nest

    result = nest({"a": 1, "b": 2, "c": 3}, key="m", exclude=["a"])
    assert result == {"a": 1, "m": {"b": 2, "c": 3}}


def test_no_include_or_exclude_nests_every_other_field():
    from datapipe.tools.builtins.structural import nest

    result = nest({"a": 1, "b": 2}, key="m")
    assert result == {"m": {"a": 1, "b": 2}}


# 7. jsonify controls whether the destination is serialized.
def test_jsonify_true_serializes_the_destination():
    from datapipe.tools.builtins.structural import nest

    result = nest({"a": 1, "b": 2}, key="m", exclude=["a"], jsonify=True)
    assert result == {"a": 1, "m": '{"b":2}'}


def test_jsonify_false_leaves_the_destination_an_object():
    from datapipe.tools.builtins.structural import nest

    result = nest({"a": 1, "b": 2}, key="m", exclude=["a"], jsonify=False)
    assert result["m"] == {"b": 2}


# 8. the destination is created when absent, and merged into when present.
def test_destination_auto_created_when_absent():
    from datapipe.tools.builtins.structural import nest

    result = nest({"a": 1}, key="brand_new", include=["a"])
    assert result == {"brand_new": {"a": 1}}


def test_existing_object_destination_is_merged_into():
    from datapipe.tools.builtins.structural import nest

    result = nest({"a": 1, "m": {"pre": 0}}, key="m", exclude=["a"])
    assert result == {"a": 1, "m": {"pre": 0}}
    result2 = nest({"a": 1, "m": {"pre": 0}, "b": 2}, key="m", exclude=["a"])
    assert result2 == {"a": 1, "m": {"pre": 0, "b": 2}}


# 9. §8.7 — the destination key is never nested inside itself.
def test_destination_is_self_excluded_from_a_blanket_set():
    from datapipe.tools.builtins.structural import nest

    result = nest(
        {"instance_id": "i1", "metadata": {"pre": 1}, "temperature": 0.7},
        key="metadata",
        exclude=["instance_id"],
    )
    assert result == {
        "instance_id": "i1",
        "metadata": {"pre": 1, "temperature": 0.7},
    }
    assert "metadata" not in result["metadata"]


def test_self_exclusion_also_applies_with_no_exclude_list():
    from datapipe.tools.builtins.structural import nest

    result = nest({"metadata": {"pre": 1}, "t": 0.7}, key="metadata")
    assert result == {"metadata": {"pre": 1, "t": 0.7}}
    assert "metadata" not in result["metadata"]


# 10. §8.4 — a collision under collision="error" fails and changes nothing.
def test_collision_errors_and_leaves_the_record_unmodified():
    from datapipe.tools.builtins.structural import nest

    record = {"temperature": 0.7, "metadata": {"temperature": 0.1}}
    with pytest.raises(StructuralExecutionError) as exc:
        nest(record, key="metadata", include=["temperature"])
    assert "already exists" in exc.value.reason
    assert record == {"temperature": 0.7, "metadata": {"temperature": 0.1}}


def test_collision_through_the_dsl_reports_a_structural_cause():
    record = {"temperature": 0.7, "metadata": {"temperature": 0.1}}
    with pytest.raises(ToolExecutionError) as exc:
        _run('nest(., key="metadata", include=["temperature"])', record)
    cause = _cause(exc)
    assert isinstance(cause, StructuralExecutionError)
    assert "already exists" in cause.reason
    assert record == {"temperature": 0.7, "metadata": {"temperature": 0.1}}


# 11. include and exclude are mutually exclusive.
def test_include_and_exclude_together_is_an_error():
    from datapipe.tools.builtins.structural import nest

    record = {"a": 1, "b": 2, "c": 3}
    with pytest.raises(ValueError) as exc:
        nest(record, key="m", include=["a"], exclude=["b"])
    assert "mutually exclusive" in str(exc.value)
    assert record == {"a": 1, "b": 2, "c": 3}


def test_mutual_exclusion_surfaces_through_the_dsl():
    """include and exclude are mutually exclusive; now caught at compile time."""
    from datapipe.dsl.compiler import compile_program
    from datapipe.dsl.errors import ToolConfigurationError
    with pytest.raises(ToolConfigurationError) as exc:
        compile_program('nest(., key="m", include=["a"], exclude=["b"])')
    assert "mutually exclusive" in str(exc.value)


def test_unnest_include_and_exclude_together_is_an_error():
    from datapipe.tools.builtins.structural import unnest

    with pytest.raises(ValueError) as exc:
        unnest({"m": {"a": 1}}, key="m", include=["a"], exclude=["a"])
    assert "mutually exclusive" in str(exc.value)


# 12. §6.4 — source object order is preserved.
def test_source_object_order_is_preserved():
    from datapipe.tools.builtins.structural import nest

    record = {"z": 1, "a": 2, "m_keep": 3, "b": 4}
    result = nest(record, key="m", exclude=["m_keep"])
    assert list(result["m"]) == ["z", "a", "b"]


def test_include_uses_source_order_not_the_order_written():
    from datapipe.tools.builtins.structural import nest

    result = nest({"z": 1, "a": 2, "b": 3}, key="m", include=["b", "z"])
    assert list(result["m"]) == ["z", "b"]


def test_unnest_extracts_in_source_object_order(  # §6.4
):
    from datapipe.tools.builtins.structural import unnest

    result = unnest({"m": {"z": 1, "a": 2, "b": 3}}, key="m", include=["b", "z"])
    assert list(result) == ["m", "z", "b"]


def test_unnest_order_does_not_depend_on_the_expansion_order():
    """§6.4 is enforced by S4, not by the order this tool emits names in.

    S4 re-expands a positive field set in the source object's own key order, so
    `unnest` may hand it names in any order.  Asserted directly so a future
    change to the expansion cannot silently start relying on emission order.
    """
    record = {"m": {"note": 1, "n": 2, "t": 3, "nil": 4}}
    written_order = _run(". << .m.(note|n|t|nil)", copy.deepcopy(record))
    shuffled_order = _run(". << .m.(n|nil|note|t)", copy.deepcopy(record))
    assert _bytes(written_order) == _bytes(shuffled_order)
    assert list(written_order) == ["m", "note", "n", "t", "nil"]


# Argument validation: policies and element types.
@pytest.mark.parametrize("param", ["collision", "missing"])
def test_unknown_policy_value_is_rejected(param):
    from datapipe.tools.builtins.structural import nest

    record = {"a": 1, "b": 2}
    with pytest.raises(ValueError) as exc:
        nest(record, key="m", exclude=["a"], **{param: "shrug"})
    assert "'shrug'" in str(exc.value)
    assert param in str(exc.value)
    assert record == {"a": 1, "b": 2}


def test_non_string_field_name_is_rejected():
    from datapipe.tools.builtins.structural import nest

    with pytest.raises(ValueError) as exc:
        nest({"a": 1}, key="m", include=["a", 7])
    assert "field name" in str(exc.value)


def test_duplicate_field_name_is_rejected():
    from datapipe.tools.builtins.structural import nest

    record = {"a": 1, "b": 2}
    with pytest.raises(ValueError) as exc:
        nest(record, key="m", include=["a", "a"])
    assert "duplicate" in str(exc.value)
    assert record == {"a": 1, "b": 2}


def test_a_non_list_selection_is_rejected():
    from datapipe.tools.builtins.structural import nest

    with pytest.raises(ValueError) as exc:
        nest({"a": 1}, key="m", include="a")
    assert "list of field names" in str(exc.value)


def test_an_explicitly_empty_exclude_nests_everything():
    from datapipe.tools.builtins.structural import nest

    assert nest({"a": 1, "b": 2}, key="m", exclude=[]) == {"m": {"a": 1, "b": 2}}


def test_a_key_that_is_not_an_identifier_still_works():
    """The destination key is configuration, not source text, so any string works."""
    from datapipe.tools.builtins.structural import nest, unnest

    nested = nest({"a": 1, "b": 2}, key="my key.with dots", exclude=["a"])
    assert nested == {"a": 1, "my key.with dots": {"b": 2}}
    assert unnest(nested, key="my key.with dots", include=["b"]) == {
        "a": 1, "my key.with dots": {}, "b": 2,
    }


def test_non_object_destination_is_rejected():
    from datapipe.tools.builtins.structural import nest

    record = {"metadata": '{"a": 1}', "temperature": 0.7}
    with pytest.raises(StructuralExecutionError) as exc:
        nest(record, key="metadata", include=["temperature"])
    assert "not an object" in exc.value.reason
    assert record == {"metadata": '{"a": 1}', "temperature": 0.7}


def test_nest_missing_included_field_errors_under_missing_error():
    from datapipe.tools.builtins.structural import nest

    record = {"a": 1}
    with pytest.raises(StructuralExecutionError) as exc:
        nest(record, key="m", include=["a", "nope"])
    assert "'nope'" in exc.value.reason
    assert record == {"a": 1}


def test_nest_excluding_a_missing_name_is_harmless():
    from datapipe.tools.builtins.structural import nest

    result = nest({"a": 1, "b": 2}, key="m", exclude=["a", "future_field"])
    assert result == {"a": 1, "m": {"b": 2}}


# ===========================================================================
# 13-18. unnest
# ===========================================================================


# 13. §6.8 — the exact example from the plan.
def test_plan_6_8_example():
    record = {
        "instance_id": "abc",
        "metadata": '{"temperature":0.7,"score":0.9,"annotation":"good"}',
    }
    result = _run(
        'unnest(., key="metadata", include=["temperature", "score"], '
        "parse=true, jsonify=true)",
        record,
    )
    assert result == {
        "instance_id": "abc",
        "temperature": 0.7,
        "score": 0.9,
        "metadata": '{"annotation":"good"}',
    }


# 14. parse=true decodes a JSON-string source before extracting.
def test_parse_true_decodes_the_source_first():
    from datapipe.tools.builtins.structural import unnest

    result = unnest(
        {"m": '{"a": 1, "b": 2}'}, key="m", include=["a"], parse=True
    )
    assert result == {"a": 1, "m": {"b": 2}}


def test_parse_false_requires_an_already_decoded_source():
    from datapipe.tools.builtins.structural import unnest

    result = unnest({"m": {"a": 1, "b": 2}}, key="m", include=["a"])
    assert result == {"a": 1, "m": {"b": 2}}


def test_parse_true_on_an_undecodable_source_errors():
    from datapipe.tools.builtins.structural import unnest

    record = {"m": "not json at all"}
    with pytest.raises(ToolExecutionError) as exc:
        unnest(record, key="m", include=["a"], parse=True)
    assert exc.value.tool_name == "fromjson"
    assert record == {"m": "not json at all"}


def test_absent_key_errors_and_changes_nothing():
    from datapipe.tools.builtins.structural import unnest

    # With parse=True the missing key is hit by the `fromjson` statement first,
    # so it surfaces as a ToolExecutionError before any move-into runs.
    record = {"other": 1}
    with pytest.raises(ToolExecutionError):
        unnest(record, key="m", include=["a"], parse=True)
    assert record == {"other": 1}

    # Without parse, both selection forms must report the same precondition
    # failure with the same error type: `include` reaches it inside the S4
    # move-into path, while `exclude` must expand its complement first and
    # raises it directly.
    record2 = {"other": 1}
    with pytest.raises(StructuralExecutionError):
        unnest(record2, key="m", include=["a"])
    assert record2 == {"other": 1}

    record3 = {"other": 1}
    with pytest.raises(StructuralExecutionError):
        unnest(record3, key="m", exclude=["a"])
    assert record3 == {"other": 1}


# 15. jsonify=true re-encodes the remaining source object.
def test_jsonify_true_reencodes_the_remainder():
    from datapipe.tools.builtins.structural import unnest

    result = unnest(
        {"m": {"a": 1, "b": 2}}, key="m", include=["a"], jsonify=True
    )
    assert result == {"a": 1, "m": '{"b":2}'}


def test_unnest_exclude_moves_everything_but_the_named_fields():
    from datapipe.tools.builtins.structural import unnest

    result = unnest({"m": {"a": 1, "b": 2, "c": 3}}, key="m", exclude=["b"])
    assert result == {"a": 1, "c": 3, "m": {"b": 2}}


def test_unnest_with_no_selection_empties_the_source():
    from datapipe.tools.builtins.structural import unnest

    result = unnest({"m": {"a": 1, "b": 2}}, key="m")
    assert result == {"a": 1, "b": 2, "m": {}}


# 16. a field absent from the source is an error under missing="error".
def test_unnest_missing_included_field_errors():
    from datapipe.tools.builtins.structural import unnest

    record = {"m": {"a": 1}}
    with pytest.raises(StructuralExecutionError) as exc:
        unnest(record, key="m", include=["a", "gone"])
    assert "'gone'" in exc.value.reason
    assert record == {"m": {"a": 1}}


# 17. a collision with an existing root field is an error.
def test_unnest_collision_with_a_root_field_errors():
    from datapipe.tools.builtins.structural import unnest

    record = {"a": "root", "m": {"a": "nested", "b": 2}}
    with pytest.raises(StructuralExecutionError) as exc:
        unnest(record, key="m", include=["a"])
    assert "already exists" in exc.value.reason
    assert record == {"a": "root", "m": {"a": "nested", "b": 2}}


# 18. round-trip.
def test_nest_then_unnest_round_trips():
    from datapipe.tools.builtins.structural import nest, unnest

    original = {
        "instance_id": "abc",
        "temperature": 0.7,
        "score": 0.9,
        "note": {"deep": [1, 2]},
    }
    nested = nest(
        copy.deepcopy(original), key="metadata", exclude=["instance_id"],
        jsonify=True,
    )
    assert nested == {
        "instance_id": "abc",
        "metadata": '{"temperature":0.7,"score":0.9,"note":{"deep":[1,2]}}',
    }
    restored = unnest(nested, key="metadata", parse=True)
    assert restored == {**original, "metadata": {}}
    del restored["metadata"]
    assert restored == original
    assert _bytes(restored) == _bytes(original)


# ===========================================================================
# 19. §15.4 named/symbolic equivalence — the core property of this phase
# ===========================================================================


#: (label, record, nest kwargs, equivalent symbolic program)
NEST_EQUIVALENCE_CASES = [
    (
        "plan-6.7",
        {
            "instance_id": "abc",
            "messages": [{"role": "user"}],
            "tools": ["t1"],
            "temperature": 0.7,
            "score": 0.9,
        },
        {
            "key": "metadata",
            "exclude": ["instance_id", "messages", "tools"],
            "jsonify": True,
        },
        ".metadata << .(^instance_id|messages|tools) | tojson",
    ),
    (
        "empty-after-exclusion",
        {"a": 1},
        {"key": "m", "exclude": ["a"], "jsonify": False},
        ".m << .(^a)",
    ),
    (
        "single-field",
        {"a": 1, "b": 2},
        {"key": "m", "exclude": ["a"], "jsonify": False},
        ".m << .(^a)",
    ),
    (
        "container-values",
        {"a": 1, "msgs": [{"r": "u"}, {"r": "a"}], "cfg": {"x": {"y": [1]}}},
        {"key": "m", "exclude": ["a"], "jsonify": False},
        ".m << .(^a)",
    ),
    (
        "container-values-jsonified",
        {"a": 1, "msgs": [{"r": "u"}], "cfg": {"x": {"y": [1]}}},
        {"key": "m", "exclude": ["a"], "jsonify": True},
        ".m << .(^a) | tojson",
    ),
    (
        "destination-absent",
        {"z": 1, "a": 2, "b": 3},
        {"key": "brand_new", "exclude": ["z"], "jsonify": False},
        ".brand_new << .(^z)",
    ),
    (
        "destination-present",
        {"a": 1, "m": {"pre": 0}, "b": 2, "c": 3},
        {"key": "m", "exclude": ["a"], "jsonify": False},
        ".m << .(^a)",
    ),
    (
        "destination-present-jsonified",
        {"a": 1, "m": {"pre": 0}, "b": 2},
        {"key": "m", "exclude": ["a"], "jsonify": True},
        ".m << .(^a) | tojson",
    ),
    (
        "include-form",
        {"z": 1, "a": 2, "b": 3, "c": 4},
        {"key": "m", "include": ["b", "z"], "jsonify": False},
        ".m << .(b|z)",
    ),
    (
        "include-form-jsonified",
        {"z": 1, "a": 2, "b": 3},
        {"key": "m", "include": ["b", "z"], "jsonify": True},
        ".m << .(b|z) | tojson",
    ),
    (
        "self-exclusion",
        {"instance_id": "i1", "metadata": {"pre": 1}, "temperature": 0.7},
        {"key": "metadata", "exclude": ["instance_id"], "jsonify": False},
        ".metadata << .(^instance_id)",
    ),
    (
        "exclusion-of-a-missing-name",
        {"a": 1, "b": 2},
        {"key": "m", "exclude": ["a", "future_field"], "jsonify": False},
        ".m << .(^a|future_field)",
    ),
    (
        "unicode-and-numeric-values",
        {"id": "x", "note": "héllo", "n": 3, "f": 1.5, "t": True, "nil": None},
        {"key": "m", "exclude": ["id"], "jsonify": True},
        ".m << .(^id) | tojson",
    ),
]


@pytest.mark.parametrize(
    "label,record,kwargs,symbolic",
    NEST_EQUIVALENCE_CASES,
    ids=[c[0] for c in NEST_EQUIVALENCE_CASES],
)
def test_nest_is_equivalent_to_its_symbolic_form(label, record, kwargs, symbolic):
    from datapipe.tools.builtins.structural import nest

    named = nest(copy.deepcopy(record), **kwargs)
    sugared = _run(symbolic, copy.deepcopy(record))
    assert named == sugared, label
    assert _bytes(named) == _bytes(sugared), f"{label}: key order differs"


#: (label, record, unnest kwargs, equivalent symbolic program)
UNNEST_EQUIVALENCE_CASES = [
    (
        "plan-6.8",
        {
            "instance_id": "abc",
            "metadata": '{"temperature":0.7,"score":0.9,"annotation":"good"}',
        },
        {
            "key": "metadata",
            "include": ["temperature", "score"],
            "parse": True,
            "jsonify": True,
        },
        "fromjson(.metadata); . << .metadata.(temperature|score); "
        "tojson(.metadata)",
    ),
    (
        "single-field-no-parse-no-jsonify",
        {"m": {"a": 1, "b": 2}},
        {"key": "m", "include": ["a"]},
        ". << .m.(a)",
    ),
    (
        "empty-remainder",
        {"m": {"a": 1}},
        {"key": "m", "include": ["a"], "jsonify": True},
        ". << .m.(a); tojson(.m)",
    ),
    (
        "container-value-extracted",
        {"id": "x", "m": {"deep": {"k": [1, 2]}, "b": 2}},
        {"key": "m", "include": ["deep"]},
        ". << .m.(deep)",
    ),
    # S6 narrowed the complement base check, so the reference form is now the
    # complement itself rather than a hand-expanded positive set.
    (
        "exclude-form",
        {"id": "x", "m": {"a": 1, "b": 2, "c": 3}},
        {"key": "m", "exclude": ["b"]},
        ". << .m.(^b)",
    ),
    (
        "exclude-form-parsed-and-jsonified",
        {"id": "x", "m": '{"a":1,"b":2,"c":3}'},
        {"key": "m", "exclude": ["b"], "parse": True, "jsonify": True},
        "fromjson(.m); . << .m.(^b); tojson(.m)",
    ),
    (
        "parse-only",
        {"m": '{"a":1,"b":2}'},
        {"key": "m", "include": ["a"], "parse": True},
        "fromjson(.m); . << .m.(a)",
    ),
    (
        "jsonify-only",
        {"m": {"a": 1, "b": 2}},
        {"key": "m", "include": ["a"], "jsonify": True},
        ". << .m.(a); tojson(.m)",
    ),
    (
        "source-order-preserved",
        {"m": {"z": 1, "a": 2, "b": 3}},
        {"key": "m", "include": ["b", "z"]},
        ". << .m.(b|z)",
    ),
    (
        "unicode-and-scalars",
        {"m": {"note": "héllo", "n": 3, "t": True, "nil": None, "keep": 0}},
        {"key": "m", "exclude": ["keep"], "jsonify": True},
        ". << .m.(^keep); tojson(.m)",
    ),
]


def test_nested_complement_under_a_root_destination_compiles():
    """S6 narrowed `_check_complement_base` to base == destination only.

    S4 rejected `. << .m.(^b)` because the root is an ancestor of every base.
    That was a conservative over-rejection: only a *specific* member can
    conflict, and `_check_move_entries` rejects exactly that member at runtime.
    So `unnest(exclude=...)` no longer expands its complement by hand.
    """
    from datapipe.dsl.compiler import compile_program

    compile_program(". << .m.(^b)")
    assert _run(". << .m.(^b)", {"m": {"a": 1, "b": 2, "c": 3}}) == {
        "m": {"b": 2}, "a": 1, "c": 3,
    }

    # The base *being* the destination stays a compile error: every derived key
    # would land back on the field it came from, for every possible record.
    from datapipe.dsl.errors import ToolConfigurationError

    with pytest.raises(ToolConfigurationError) as exc:
        compile_program(".m << .m.(^a)")
    assert "the source is the destination itself" in exc.value.base_message


@pytest.mark.parametrize(
    "label,record,kwargs,symbolic",
    UNNEST_EQUIVALENCE_CASES,
    ids=[c[0] for c in UNNEST_EQUIVALENCE_CASES],
)
def test_unnest_is_equivalent_to_its_symbolic_form(label, record, kwargs, symbolic):
    from datapipe.tools.builtins.structural import unnest

    named = unnest(copy.deepcopy(record), **kwargs)
    sugared = _run(symbolic, copy.deepcopy(record))
    assert named == sugared, label
    assert _bytes(named) == _bytes(sugared), f"{label}: key order differs"


def test_equivalence_holds_through_the_dsl_too():
    """The named form invoked as a DSL tool matches the symbolic program."""
    record = {
        "instance_id": "abc",
        "messages": [{"role": "user"}],
        "tools": ["t1"],
        "temperature": 0.7,
        "score": 0.9,
    }
    named = _run(
        'nest(., key="metadata", '
        'exclude=["instance_id","messages","tools"], jsonify=true)',
        copy.deepcopy(record),
    )
    sugared = _run(
        ".metadata << .(^instance_id|messages|tools) | tojson",
        copy.deepcopy(record),
    )
    assert _bytes(named) == _bytes(sugared)


# ===========================================================================
# 20-21. Atomicity — §8.1 resolve before mutation
# ===========================================================================


def test_failing_nest_leaves_the_whole_record_unmodified():
    from datapipe.tools.builtins.structural import nest

    record = {
        "instance_id": "i1",
        "a": 1,
        "b": 2,
        "metadata": {"b": "collides"},
    }
    before = copy.deepcopy(record)
    with pytest.raises(StructuralExecutionError):
        nest(record, key="metadata", exclude=["instance_id"])
    assert record == before


def test_failing_nest_on_a_late_include_name_leaves_the_record_unmodified():
    from datapipe.tools.builtins.structural import nest

    record = {"a": 1, "b": 2, "c": 3}
    before = copy.deepcopy(record)
    with pytest.raises(StructuralExecutionError):
        nest(record, key="m", include=["a", "b", "absent"])
    assert record == before


def test_failing_unnest_leaves_the_whole_record_unmodified_after_parse():
    """The `parse` step must not commit when a later step fails (§8.1)."""
    from datapipe.tools.builtins.structural import unnest

    record = {"a": "root", "m": '{"a": "nested", "b": 2}'}
    before = copy.deepcopy(record)
    with pytest.raises(StructuralExecutionError):
        unnest(record, key="m", include=["a"], parse=True, jsonify=True)
    assert record == before
    assert record["m"] == '{"a": "nested", "b": 2}'


def test_failing_unnest_on_a_missing_field_leaves_the_record_unmodified():
    from datapipe.tools.builtins.structural import unnest

    record = {"id": "x", "m": '{"a": 1}'}
    before = copy.deepcopy(record)
    with pytest.raises(StructuralExecutionError):
        unnest(record, key="m", include=["a", "absent"], parse=True)
    assert record == before


def test_failing_unnest_jsonify_leaves_the_record_unmodified():
    """A non-finite float makes the final tojson fail after the move."""
    from datapipe.tools.builtins.structural import unnest

    record = {"m": {"a": 1, "bad": float("inf")}}
    before = copy.deepcopy(record)
    with pytest.raises(ToolExecutionError):
        unnest(record, key="m", include=["a"], jsonify=True)
    # The whole call is atomic: `a` was never lifted to the root and `m` still
    # holds both of its original keys.
    assert record == before
    assert list(record) == ["m"]
    assert record["m"]["bad"] == float("inf")


# ===========================================================================
# 22. Aliasing
# ===========================================================================


def test_nested_container_is_detached_from_caller_held_data():
    from datapipe.tools.builtins.structural import nest

    live = {"deep": [1, 2]}
    record = {"id": "x", "payload": live}
    result = nest(record, key="m", exclude=["id"])
    result["m"]["payload"]["deep"].append(99)
    assert live == {"deep": [1, 2]}


def test_unnested_container_is_detached_from_the_remaining_source():
    """A moved container must not alias what stays behind in the source."""
    from datapipe.tools.builtins.structural import unnest

    source = {"deep": {"k": [1]}, "b": 2}
    result = unnest({"m": source}, key="m", include=["deep"])
    result["deep"]["k"].append(99)
    assert result["m"] == {"b": 2}
    assert "deep" not in source


def test_unnested_container_is_detached_from_caller_held_data():
    from datapipe.tools.builtins.structural import unnest

    live = {"k": [1]}
    result = unnest({"m": {"deep": live, "b": 2}}, key="m", include=["deep"])
    result["deep"]["k"].append(99)
    assert live == {"k": [1]}


def test_a_failing_unnest_never_aliases_the_callers_record():
    """The atomicity copy must not leak: a failure returns nothing at all."""
    from datapipe.tools.builtins.structural import unnest

    record = {"a": "root", "m": '{"a": "nested"}'}
    with pytest.raises(StructuralExecutionError):
        unnest(record, key="m", include=["a"], parse=True)
    assert record == {"a": "root", "m": '{"a": "nested"}'}


def test_successful_unnest_matches_the_symbolic_forms_mutation_behavior():
    """§15.4 equivalence covers mutation, not just the returned value.

    A single-statement `unnest` is exactly `. << .m.(a)`, which mutates the
    record in place and returns it.  With `parse` or `jsonify` the desugaring
    spans several statements, and those commit one at a time, so the call works
    on a copy to stay atomic (§8.1) — the returned record is then a new object.
    Either way the *content* is identical, which is what equivalence asserts.
    """
    from datapipe.tools.builtins.structural import unnest

    record = {"m": {"a": 1, "b": 2}}
    assert unnest(record, key="m", include=["a"]) is record

    symbolic_input = {"m": {"a": 1, "b": 2}}
    assert _run(". << .m.(a)", symbolic_input) is symbolic_input

    copied_input = {"m": {"a": 1, "b": 2}}
    result = unnest(copied_input, key="m", include=["a"], jsonify=True)
    assert result is not copied_input
    assert copied_input == {"m": {"a": 1, "b": 2}}


def test_successful_nest_matches_the_symbolic_forms_mutation_behavior():
    from datapipe.tools.builtins.structural import nest

    record = {"a": 1, "b": 2}
    assert nest(record, key="m", exclude=["a"]) is record

    symbolic_input = {"a": 1, "b": 2}
    assert _run(".m << .(^a)", symbolic_input) is symbolic_input


# ===========================================================================
# 23-24. Integration
# ===========================================================================


def test_e2e_sequential_executor(tmp_path):
    from datapipe.execution.sequential import SequentialExecutor

    result = _run_jsonl(
        'nest(., key="metadata", exclude=["instance_id"], jsonify=true)',
        {"instance_id": "i1", "temperature": 0.7, "score": 5},
        tmp_path,
        SequentialExecutor(),
    )
    assert result["instance_id"] == "i1"
    assert json.loads(result["metadata"]) == {"temperature": 0.7, "score": 5}


def test_e2e_process_executor(tmp_path):
    from datapipe.execution.process import ProcessExecutor

    result = _run_jsonl(
        'unnest(., key="metadata", include=["temperature"], '
        "parse=true, jsonify=true)",
        {"instance_id": "i1", "metadata": '{"temperature":0.7,"score":5}'},
        tmp_path,
        ProcessExecutor(workers=2),
    )
    assert result == {
        "instance_id": "i1",
        "temperature": 0.7,
        "metadata": '{"score":5}',
    }


def test_e2e_process_executor_surfaces_the_structural_cause(tmp_path):
    from datapipe.execution.process import ProcessExecutor
    from datapipe.io.jsonl import JsonlSink, JsonlSource
    from datapipe.pipeline import Pipeline
    from datapipe.stage import JsonDumpStage, JsonLoadStage

    inp = tmp_path / "in.jsonl"
    out = tmp_path / "out.jsonl"
    err = tmp_path / "err.jsonl"
    inp.write_text(json.dumps({"t": 1, "metadata": {"t": 2}}) + "\n")

    pipeline = Pipeline([
        JsonLoadStage(),
        _stage('nest(., key="metadata", include=["t"])'),
        JsonDumpStage(),
    ])
    pipeline.run(
        source=JsonlSource(str(inp), raw=True),
        sink=JsonlSink(str(out), raw=True),
        executor=ProcessExecutor(workers=1),
        errors="return",
        error_sink=JsonlSink(str(err)),
    )
    payload = json.loads(err.read_text().strip())
    assert payload["error_type"] == "ToolExecutionError"
    assert payload["tool"]["tool_name"] == "nest"
    assert payload["tool"]["provider_id"] == "builtin:structural"
    # The wrapped StructuralExecutionError's own reason survives the spawn
    # boundary and the error-payload serialization.
    assert "already exists" in payload["error_message"]
    assert "StructuralExecutionError" in payload["error_message"]


def test_usable_alongside_symbolic_forms_in_one_program():
    result = _run(
        'nest(., key="m", include=["a"]); tojson(.m)',
        {"a": 1, "b": 2},
    )
    assert result == {"b": 2, "m": '{"a":1}'}


def test_mixed_program_with_a_symbolic_move_into():
    result = _run(
        'unnest(., key="m", include=["a"], parse=true); .bag << .a',
        {"m": '{"a": 1, "b": 2}'},
    )
    assert result == {"m": {"b": 2}, "bag": {"a": 1}}


def test_tools_are_pickleable_across_the_spawn_boundary():
    from datapipe.tools.builtins.structural import nest, unnest

    for fn in (nest, unnest):
        assert pickle.loads(pickle.dumps(fn)) is fn


def test_compiled_stage_pickle_roundtrip_preserves_the_invocation():
    stage = _stage('nest(., key="m", exclude=["a"], jsonify=true)')
    revived = pickle.loads(pickle.dumps(stage))
    op = revived._compiled.statements[0].operation
    assert op.tool_name == "nest"
    assert op.arguments["key"] == "m"
    assert op.arguments["exclude"] == ["a"]
    assert op.arguments["jsonify"] is True
    assert revived.process({"a": 1, "b": 2}, None) == {"a": 1, "m": '{"b":2}'}


def test_transform_cli_end_to_end(tmp_path, capsys):
    from datapipe.cli.main import main

    inp = tmp_path / "in.jsonl"
    out = tmp_path / "out.jsonl"
    inp.write_text(
        json.dumps({"instance_id": "i1", "temperature": 0.7, "score": 5}) + "\n"
    )
    rc = main([
        "transform",
        'nest(., key="metadata", exclude=["instance_id"], jsonify=true)',
        str(inp), str(out),
        "--executor", "sequential",
        "--no-progress",
    ])
    assert rc == 0, capsys.readouterr().err
    result = json.loads(out.read_text().strip())
    assert result["instance_id"] == "i1"
    assert json.loads(result["metadata"]) == {"temperature": 0.7, "score": 5}
