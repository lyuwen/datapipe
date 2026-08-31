"""Registry metadata fidelity and deterministic preflight validation.

Three groups, each pinning a defect the reviews flagged:

1. ``TestTypeSpecRoundTrip`` — provider contracts used to be stored only as
   the human ``describe()`` string, so ``OneOf(STRING, ARRAY)`` decoded back
   to ``JsonType.ANY`` and every composite contract silently accepted
   anything.
2. ``TestConfigAnnotationValidation`` — unsupported config annotations
   (``Optional``, ``Union``, enums, typed containers) warned and then skipped
   validation entirely, so bad configuration reached the workers.
3. ``TestStaticCompatibility`` — §9 pass 7 did not exist: an expression whose
   adjacent tools provably disagree on the type at one concrete path compiled
   cleanly and only failed once records were being processed.

Provider tests redirect ``DATAPIPE_USER_DATA`` to ``tmp_path`` and clear the
loader cache so they never touch the real user registry.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from datapipe.dsl.compiler import (
    ContainerAnnotation,
    EnumValues,
    UnionAnnotation,
    UnsupportedAnnotation,
    _build_full_registry,
    compile_expression,
    decode_annotation,
    decode_type_spec,
)
from datapipe.dsl.errors import ToolConfigurationError
from datapipe.tools.decorator import get_contract
from datapipe.tools.installer import install_provider
from datapipe.tools.types import JsonType, OneOf, as_type_spec, matches


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_STEM_COUNTER = [0]


@pytest.fixture
def unique_stem() -> str:
    """Return a module stem unique to this test, avoiding sys.modules clashes."""
    _STEM_COUNTER[0] += 1
    return f"regmeta_prov_{_STEM_COUNTER[0]}"


@pytest.fixture(autouse=True)
def isolated_registry(tmp_path, monkeypatch):
    """Point the registry at tmp_path and clear the loader cache."""
    data_dir = tmp_path / "dp_data"
    monkeypatch.setenv("DATAPIPE_USER_DATA", str(data_dir))

    from datapipe.tools import loader as _loader
    _loader._loaded_providers.clear()

    from datapipe.dsl import compiler as _compiler
    monkeypatch.setattr(_compiler, "_BUILTIN_REGISTRY", None, raising=False)

    yield data_dir

    _loader._loaded_providers.clear()


def _install(tmp_path: Path, stem: str, source: str):
    """Write *source* to a provider file and install it."""
    provider = tmp_path / f"{stem}.py"
    provider.write_text(source)
    return install_provider(provider, yes=True)


def _tool_meta(tmp_path: Path, stem: str, source: str, tool_name: str) -> dict:
    """Install a provider and return the stored registry metadata for one tool."""
    entry = _install(tmp_path, stem, source)
    assert entry is not None
    return entry.tools[tool_name]


def _contract_for(tool_name: str):
    """Return the contract the compiler reconstructs for an installed tool."""
    registry = _build_full_registry()
    stub, _desc = registry[tool_name]
    return get_contract(stub)


# ---------------------------------------------------------------------------
# Group 1: reversible TypeSpec encoding
# ---------------------------------------------------------------------------

_ONEOF_SRC = '''
from datapipe.tools.decorator import tool
from datapipe.tools.types import JsonType, OneOf


@tool(
    name="stringify",
    target="value",
    input=OneOf(JsonType.STRING, JsonType.ARRAY),
    output=JsonType.STRING,
)
def stringify(value):
    return str(value)
'''


class TestTypeSpecRoundTrip:
    """A contract's TypeSpecs must survive the trip through registry JSON."""

    @pytest.mark.parametrize(
        "spec",
        [as_type_spec(jt) for jt in JsonType]
        + [
            OneOf(JsonType.STRING, JsonType.ARRAY),
            OneOf(JsonType.NULL, JsonType.BOOLEAN, JsonType.INTEGER),
            OneOf(JsonType.STRING, OneOf(JsonType.ARRAY, JsonType.OBJECT)),
        ],
        ids=repr,
    )
    def test_encoder_decoder_round_trip(self, spec):
        """Every scalar JsonType, ANY, and OneOf composites round-trip exactly."""
        encoded = _encode_via_helper_logic(spec)
        # The encoding must survive a JSON serialization round trip, since
        # that is how it reaches the registry file.
        encoded = json.loads(json.dumps(encoded))
        assert decode_type_spec(encoded) == spec

    def test_oneof_contract_survives_installation(self, tmp_path, unique_stem):
        """OneOf(STRING, ARRAY) must not degrade to ANY in the registry."""
        meta = _tool_meta(tmp_path, unique_stem, _ONEOF_SRC, "stringify")
        assert meta["input_spec"] == {
            "kind": "one_of",
            "members": [
                {"kind": "json_type", "name": "STRING"},
                {"kind": "json_type", "name": "ARRAY"},
            ],
        }
        assert meta["output_spec"] == {"kind": "json_type", "name": "STRING"}

    def test_compiler_reconstructs_oneof_not_any(self, tmp_path, unique_stem):
        """The compiler's stub contract must carry the real OneOf, not ANY.

        Pre-fix this asserted-equal to ``JsonType.ANY`` because ``_jt_from_desc``
        could not parse ``"string | array"``.
        """
        _install(tmp_path, unique_stem, _ONEOF_SRC)
        contract = _contract_for("stringify")
        assert contract.input_type == OneOf(JsonType.STRING, JsonType.ARRAY)
        assert contract.output_type == as_type_spec(JsonType.STRING)

    def test_reconstructed_oneof_actually_rejects(self, tmp_path, unique_stem):
        """The reconstructed contract must reject values outside the union.

        This is the behavioural consequence: with a degraded ANY contract,
        runtime validation accepted an int where only string|array is legal.
        """
        _install(tmp_path, unique_stem, _ONEOF_SRC)
        contract = _contract_for("stringify")
        assert matches("hi", contract.input_type)
        assert matches([1, 2], contract.input_type)
        assert not matches(42, contract.input_type)
        assert not matches({"a": 1}, contract.input_type)

    def test_human_description_is_retained(self, tmp_path, unique_stem):
        """``tools inspect`` reads the describe() string; it must stay."""
        meta = _tool_meta(tmp_path, unique_stem, _ONEOF_SRC, "stringify")
        assert meta["input"] == "string | array"
        assert meta["output"] == "string"

    def test_legacy_entry_without_spec_falls_back_to_description(self):
        """Registry entries predating the structured encoding still decode."""
        assert decode_type_spec(None, "string") == as_type_spec(JsonType.STRING)
        assert decode_type_spec(None, "string | array") == OneOf(
            JsonType.STRING, JsonType.ARRAY
        )
        assert decode_type_spec(None, None) == as_type_spec(JsonType.ANY)

    def test_unencodable_type_degrades_with_a_reason(self, tmp_path, unique_stem):
        """A TypeSpec the encoder cannot represent must not crash validation."""
        src = '''
from datapipe.tools.decorator import tool
from datapipe.tools.types import JsonType, TypeSpec


class Weird(TypeSpec):
    def matches(self, value):
        return True

    def __repr__(self):
        return "Weird()"


@tool(name="weird", target="value", input=Weird(), output=JsonType.STRING)
def weird(value):
    return str(value)
'''
        meta = _tool_meta(tmp_path, unique_stem, src, "weird")
        assert meta["input_spec"]["kind"] == "unsupported"
        assert "Weird" in meta["input_spec"]["reason"]
        # Degrading to ANY is correct here — it is the only sound fallback —
        # but the reason must be recorded rather than lost.
        assert decode_type_spec(meta["input_spec"]) == as_type_spec(JsonType.ANY)


def _encode_via_helper_logic(spec):
    """Mirror of the subprocess encoder, for pure round-trip testing.

    The helper template runs in a subprocess so it cannot be imported; this
    extracts the same logic by executing the template's encoder definition.
    """
    from datapipe.tools import validation

    namespace: dict = {}
    template = validation._HELPER_TEMPLATE
    start = template.index("def _encode_type_spec")
    end = template.index("_ANN_BASE_NAMES = {")
    exec(
        "import json, typing, enum, types\n"
        "from datapipe.tools.types import JsonType as _JsonType, "
        "OneOf as _OneOf, TypeSpec as _TypeSpec\n"
        + template[start:end],
        namespace,
    )
    return namespace["_encode_type_spec"](spec)


# ---------------------------------------------------------------------------
# Group 2: deterministic config-annotation validation
# ---------------------------------------------------------------------------

_ANNOTATED_SRC = '''
import enum
from typing import Optional, Union

from datapipe.tools.decorator import tool
from datapipe.tools.types import JsonType


class Mode(enum.Enum):
    FAST = "fast"
    SLOW = "slow"


@tool(name="annotated", target="value", input=JsonType.ANY, output=JsonType.STRING)
def annotated(
    value,
    *,
    opt: Optional[int] = None,
    either: Union[int, str] = 1,
    mode: Mode = "fast",
    items: list[int] = [],
    mapping: dict[str, int] = {},
):
    return str(value)
'''


class TestConfigAnnotationValidation:
    """Config annotations must be validated deterministically, not skipped."""

    @pytest.fixture(autouse=True)
    def _installed(self, tmp_path, unique_stem):
        _install(tmp_path, unique_stem, _ANNOTATED_SRC)

    def test_annotations_are_reconstructed_structurally(self):
        """The decoded annotations must be typed, not None."""
        contract = _contract_for("annotated")
        by_name = {p.name: p.annotation for p in contract.parameters}
        assert by_name["opt"] == UnionAnnotation((int, type(None)))
        assert by_name["either"] == UnionAnnotation((int, str))
        assert by_name["mode"] == EnumValues("Mode", ("fast", "slow"))
        assert by_name["items"] == ContainerAnnotation(list, (int,))
        assert by_name["mapping"] == ContainerAnnotation(dict, (str, int))

    @pytest.mark.parametrize(
        "expr",
        [
            'annotated(.a, opt=5)',
            'annotated(.a, opt=null)',
            'annotated(.a, either=3)',
            'annotated(.a, either="x")',
            'annotated(.a, mode="slow")',
            'annotated(.a, items=[1, 2])',
            'annotated(.a, items=[])',
            'annotated(.a, mapping={"k": 1})',
        ],
    )
    def test_valid_values_are_accepted(self, expr):
        """Well-typed configuration must still compile."""
        assert compile_expression(expr).invocations

    @pytest.mark.parametrize(
        "expr, needle",
        [
            ('annotated(.a, opt="nope")', "int | None"),
            ('annotated(.a, either=true)', "int | str"),
            ('annotated(.a, either=[1])', "int | str"),
            ('annotated(.a, mode="turbo")', "not one of the allowed values"),
            ('annotated(.a, items=["x"])', "list[int]"),
            ('annotated(.a, items="x")', "list[int]"),
            ('annotated(.a, mapping={"k": "v"})', "dict[str, int]"),
        ],
    )
    def test_invalid_values_are_rejected(self, expr, needle):
        """Ill-typed configuration must fail at compile time, not at runtime.

        Pre-fix every one of these compiled successfully (with a warning).
        """
        with pytest.raises(ToolConfigurationError) as exc:
            compile_expression(expr)
        assert needle in str(exc.value)

    def test_enum_error_lists_allowed_values(self):
        """The enum rejection message must name the legal values."""
        with pytest.raises(ToolConfigurationError) as exc:
            compile_expression('annotated(.a, mode="turbo")')
        message = str(exc.value)
        assert "'fast'" in message and "'slow'" in message

    def test_no_warning_is_emitted(self, recwarn):
        """Validation must be decisive: no 'not validated at compile time' warning."""
        compile_expression('annotated(.a, opt=5)')
        assert not [
            w for w in recwarn if "not validated at compile time" in str(w.message)
        ]

    def test_genuinely_unsupported_annotation_is_rejected(self, tmp_path):
        """An annotation with no structured encoding must error, not warn-and-skip."""
        src = '''
from datapipe.tools.decorator import tool
from datapipe.tools.types import JsonType


class Custom:
    pass


@tool(name="customann", target="value", input=JsonType.ANY, output=JsonType.STRING)
def customann(value, *, opt: Custom = None):
    return str(value)
'''
        _STEM_COUNTER[0] += 1
        _install(tmp_path, f"regmeta_prov_{_STEM_COUNTER[0]}", src)
        with pytest.raises(ToolConfigurationError) as exc:
            compile_expression("customann(.a)")
        assert "cannot be validated at compile time" in str(exc.value)

    def test_decode_marks_unsupported_rather_than_none(self):
        """An unsupported encoding must decode to a marker, not a blank annotation.

        ``None`` would mean 'unannotated', which disables checking — exactly
        the silent-acceptance the reviews flagged.
        """
        decoded = decode_annotation({"kind": "unsupported", "reason": "nope"})
        assert isinstance(decoded, UnsupportedAnnotation)
        assert decoded.reason == "nope"

    def test_legacy_annotation_name_still_decodes(self):
        """Registry entries with only the old type-name string keep working."""
        assert decode_annotation(None, "str") is str
        assert decode_annotation(None, "int") is int
        assert decode_annotation(None, None) is None


# ---------------------------------------------------------------------------
# Group 3: static cross-invocation compatibility (§9 pass 7)
# ---------------------------------------------------------------------------

_OBJ_ONLY_SRC = '''
from datapipe.tools.decorator import tool
from datapipe.tools.types import JsonType


@tool(name="objonly", target="value", input=JsonType.OBJECT, output=JsonType.OBJECT)
def objonly(value):
    return value


@tool(name="stronly", target="value", input=JsonType.STRING, output=JsonType.STRING)
def stronly(value):
    return value


@tool(
    name="numout", target="value", input=JsonType.ANY, output=JsonType.NUMBER
)
def numout(value):
    return 1.0
'''


class TestStaticCompatibility:
    """Adjacent tools that provably disagree at one path must fail to compile."""

    @pytest.fixture(autouse=True)
    def _installed(self, tmp_path, unique_stem):
        _install(tmp_path, unique_stem, _OBJ_ONLY_SRC)

    def test_incompatible_pair_on_identical_path_is_rejected(self):
        """The plan's example: tojson outputs string, objonly accepts only object.

        Pre-fix this compiled cleanly and blew up per-record at runtime.
        """
        with pytest.raises(ToolConfigurationError) as exc:
            compile_expression("tojson(.a) | objonly(.a)")
        message = str(exc.value)
        assert "tojson" in message and "objonly" in message
        assert "no value can satisfy both" in message

    def test_error_points_at_the_consuming_invocation(self):
        """The diagnostic must carry the span of the offending consumer."""
        expr = "tojson(.a) | objonly(.a)"
        with pytest.raises(ToolConfigurationError) as exc:
            compile_expression(expr)
        start, end = exc.value.span
        assert expr[start:end] == "objonly(.a)"

    def test_provider_to_provider_incompatibility_is_rejected(self):
        """The pass works between two installed provider tools too."""
        with pytest.raises(ToolConfigurationError):
            compile_expression("numout(.a) | stronly(.a)")

    @pytest.mark.parametrize(
        "expr",
        [
            # Compatible: tojson outputs string, fromjson accepts string.
            "tojson(.a) | fromjson(.a)",
            # Different paths — nothing is provable.
            "tojson(.a) | objonly(.b)",
            # Non-adjacent; the intervening tool may fix the type.
            "tojson(.a) | fromjson(.a) | objonly(.a)",
            # ANY on either side is never a contradiction.
            "objonly(.a) | tojson(.a)",
            # Same path, same types.
            "stronly(.a) | stronly(.a)",
        ],
    )
    def test_compatible_and_unprovable_pairs_still_compile(self, expr):
        """The pass is conservative: only provable contradictions are flagged."""
        assert compile_expression(expr).invocations

    def test_wildcard_paths_are_never_flagged(self):
        """A wildcard match set is not a single concrete path — stay silent."""
        assert compile_expression("tojson(.a[]) | objonly(.a[])").invocations

    def test_record_and_value_targets_are_not_compared(self, tmp_path):
        """A record-target tool rewrites the row; its output says nothing about a path."""
        src = '''
from datapipe.tools.decorator import tool
from datapipe.tools.types import JsonType


@tool(name="recstr", target="record", input=JsonType.ANY, output=JsonType.STRING)
def recstr(record):
    return "x"
'''
        _STEM_COUNTER[0] += 1
        _install(tmp_path, f"regmeta_prov_{_STEM_COUNTER[0]}", src)
        assert compile_expression("recstr(.) | tojson(.)").invocations
