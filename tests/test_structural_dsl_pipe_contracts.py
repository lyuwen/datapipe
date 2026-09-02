"""Contract checks that apply to bare pipe calls inside a structural statement.

Two review findings, both in ``compile_program``:

* Finding 2 — ``target="record"`` was enforced only on the first operation's
  explicit selector, so a record-level tool reached by ``|`` was applied to
  whatever field the statement focused.
* Finding 3 — the conservative type-flow check was skipped entirely, including
  *within* a statement where the base operation and its bare calls are joined
  by explicit value flow.

Error assertions anchor to ``ToolConfigurationError.base_message`` and to the
reported span, never to text that also appears in the echoed expression.
"""

from __future__ import annotations

import pytest

from datapipe.dsl.errors import ToolConfigurationError
from datapipe.tools.decorator import tool
from datapipe.tools.types import JsonType


# Module-level so the contracts behave exactly like real registered tools.
@tool(name="objout", target="value", input=JsonType.ANY, output=JsonType.OBJECT)
def objout(value):
    return {}


@tool(name="stronly", target="value", input=JsonType.STRING, output=JsonType.STRING)
def stronly(value):
    return value


@tool(name="anytool", target="value", input=JsonType.ANY, output=JsonType.ANY)
def anytool(value):
    return value


@pytest.fixture(autouse=True)
def isolated_registry(tmp_path, monkeypatch):
    """Redirect the tool registry and expose the typed probe tools as built-ins."""
    monkeypatch.setenv("DATAPIPE_USER_DATA", str(tmp_path / "dp_data"))

    from datapipe.tools import loader as _loader
    _loader._loaded_providers.clear()

    from datapipe.dsl import compiler as _compiler
    extended = dict(
        _compiler._build_builtin_registry(),
        objout=objout,
        stronly=stronly,
        anytool=anytool,
    )
    monkeypatch.setattr(_compiler, "_BUILTIN_REGISTRY", extended)


def _compile(expression):
    from datapipe.dsl.compiler import compile_program

    return compile_program(expression)


# ---------------------------------------------------------------------------
# Finding 2: target="record" must hold for trailing bare calls too
# ---------------------------------------------------------------------------


class TestRecordTargetInPipeChain:
    """A record-level tool piped onto a field focus must be rejected."""

    @pytest.mark.parametrize(
        "expression, bare_text",
        [
            # (a) after an assignment: focus is the destination
            ('.b = .a | nest(key="m")', 'nest(key="m")'),
            ('.b <- .a | nest(key="m")', 'nest(key="m")'),
            # (b) after a move-into: focus is the destination
            ('.metadata << .x | nest(key="m")', 'nest(key="m")'),
            # (c) after an invocation-first operation: focus is its selector
            ('tojson(.a) | nest(key="m")', 'nest(key="m")'),
            # and after a focused operation
            ('.a | tojson | unnest(key="m")', 'unnest(key="m")'),
        ],
    )
    def test_record_target_on_field_focus_is_rejected(self, expression, bare_text):
        with pytest.raises(ToolConfigurationError) as exc:
            _compile(expression)
        assert (
            "has target='record' and requires the root selector"
            in exc.value.base_message
        )
        start, end = exc.value.span
        assert expression[start:end] == bare_text

    def test_diagnostic_matches_the_explicit_selector_wording(self):
        """The bare-call path reuses _compile_selector's message verbatim."""
        with pytest.raises(ToolConfigurationError) as explicit:
            _compile('nest(.metadata, key="m")')
        with pytest.raises(ToolConfigurationError) as piped:
            _compile('.metadata << .x | nest(key="m")')
        assert piped.value.base_message == explicit.value.base_message

    @pytest.mark.parametrize(
        "expression",
        [
            # invocation-first operation already focused on the root
            'nest(., key="m") | unnest(key="m")',
            # §6.6 root move-into
            '. << .metadata.x | nest(key="w")',
        ],
    )
    def test_record_target_is_accepted_when_the_focus_is_the_root(self, expression):
        program = _compile(expression)
        assert len(program.statements[0].pipes) == 1


# ---------------------------------------------------------------------------
# Finding 3: type flow within a statement
# ---------------------------------------------------------------------------


class TestStaticFlowWithinStatement:
    """Base operation → bare call → bare call is a real value chain."""

    def test_incompatible_chain_is_rejected(self):
        expression = ".a | objout | stronly"
        with pytest.raises(ToolConfigurationError) as exc:
            _compile(expression)
        message = exc.value.base_message
        assert "objout" in message and "stronly" in message
        assert "no value can satisfy both" in message
        start, end = exc.value.span
        assert expression[start:end] == "stronly"

    def test_incompatibility_is_found_after_an_assignment_transform(self):
        """The chain head of a `=` statement is its transform's output."""
        expression = ".b = .a | objout | stronly"
        with pytest.raises(ToolConfigurationError) as exc:
            _compile(expression)
        assert "no value can satisfy both" in exc.value.base_message
        # The path label is the focus — the destination, not the source.
        assert " at .b," in exc.value.base_message

    def test_chain_diagnostic_matches_the_legacy_explicit_selector_path(self):
        """Same wording as the adjacent-invocation pass for the same tool pair."""
        from datapipe.dsl.compiler import compile_expression

        with pytest.raises(ToolConfigurationError) as legacy:
            compile_expression("objout(.a) | stronly(.a)")
        with pytest.raises(ToolConfigurationError) as chained:
            _compile(".a | objout | stronly")
        assert chained.value.base_message == legacy.value.base_message

    def test_any_typed_chain_still_compiles(self):
        program = _compile(".a | anytool | stronly | anytool")
        assert len(program.statements[0].pipes) == 2

    def test_chain_through_a_structural_operation_still_compiles(self):
        """`<<` declares no output type, so nothing downstream is provable."""
        program = _compile(".b << .x | stronly")
        assert len(program.statements[0].pipes) == 1

    def test_move_without_a_transform_still_compiles(self):
        program = _compile(".b <- .a | stronly")
        assert len(program.statements[0].pipes) == 1

    def test_separate_statements_are_not_chained(self):
        """Independent statements share no value flow, so no check applies."""
        program = _compile("objout(.a); stronly(.a)")
        assert len(program.statements) == 2
