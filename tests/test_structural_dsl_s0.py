"""Phase S0 fixture tests: parser baseline for every Section 7 example.

Currently-valid expressions assert that parse() succeeds.
Expressions that require new structural syntax (`;`, `<<`, `<-`, `^`,
field-set selectors) are marked xfail(strict=False) so the suite stays
green today and they automatically promote to passing when those tokens
land in later phases.
"""

from __future__ import annotations

import pytest

from datapipe.dsl.parser import parse
from datapipe.dsl.ast import Expression


# ---------------------------------------------------------------------------
# §7.1  Sequence via `;` — two fromjson invocations
# ---------------------------------------------------------------------------

@pytest.mark.xfail(strict=False, reason="structural syntax `;` not yet implemented")
def test_7_1_fromjson_sequence_semicolon():
    result = parse("fromjson(.tools); fromjson(.metadata.annotation, recursive=true)")
    assert result is not None


# ---------------------------------------------------------------------------
# §7.2  Single tojson invocation — currently valid
# ---------------------------------------------------------------------------

def test_7_2_tojson_single_currently_valid():
    result = parse("tojson(.tools[].function.parameters)")
    assert result is not None  # placeholder until Program node exists


# ---------------------------------------------------------------------------
# §7.3  Sequence via `;` — two tojson invocations
# ---------------------------------------------------------------------------

@pytest.mark.xfail(strict=False, reason="structural syntax `;` not yet implemented")
def test_7_3_tojson_sequence_semicolon():
    result = parse("tojson(.keya); tojson(.keyb)")
    assert result is not None


# ---------------------------------------------------------------------------
# §7.4  Three-statement sequence via `;`
# ---------------------------------------------------------------------------

@pytest.mark.xfail(strict=False, reason="structural syntax `;` not yet implemented")
def test_7_4_three_statement_sequence():
    result = parse("tojson(.tools); tojson(.metadata); finalize_record(.)")
    assert result is not None


# ---------------------------------------------------------------------------
# §7.5  Struct-merge `<<` with comma-separated field list
# ---------------------------------------------------------------------------

@pytest.mark.xfail(strict=False, reason="structural syntax `<<` not yet implemented")
def test_7_5_struct_merge_comma_fields():
    result = parse(".metadata << .annotation_key, .temperature, .score | tojson")
    assert result is not None


# ---------------------------------------------------------------------------
# §7.6  Struct-merge `<<` with grouped field-set selector `.(a|b|c)`
# ---------------------------------------------------------------------------

@pytest.mark.xfail(
    strict=False,
    reason="structural syntax `<<` and field-set selector not yet implemented",
)
def test_7_6_struct_merge_field_set():
    result = parse(".metadata << .(annotation_key|temperature|score) | tojson")
    assert result is not None


# ---------------------------------------------------------------------------
# §7.7  Struct-merge `<<` with complement selector `^`
# ---------------------------------------------------------------------------

@pytest.mark.xfail(
    strict=False,
    reason="structural syntax `<<` and complement selector `^` not yet implemented",
)
def test_7_7_struct_merge_complement():
    result = parse(".metadata << .(^instance_id|messages|tools) | tojson")
    assert result is not None


# ---------------------------------------------------------------------------
# §7.8  `nest()` built-in — syntax is already valid; tool not yet registered
# ---------------------------------------------------------------------------

@pytest.mark.xfail(
    strict=False,
    reason="nest/unnest tools not yet registered; expression cannot be compiled",
)
def test_7_8_nest_builtin_unregistered():
    result = parse(
        'nest(., key="metadata", exclude=["instance_id","messages","tools"], jsonify=true)'
    )
    assert result is not None


# ---------------------------------------------------------------------------
# §7.9  Multi-statement with `<<` nesting
# ---------------------------------------------------------------------------

@pytest.mark.xfail(
    strict=False,
    reason="structural syntax `;` and `<<` not yet implemented",
)
def test_7_9_multistatement_with_struct_merge():
    result = parse(
        "fromjson(.metadata); . << .metadata.(temperature|score); tojson(.metadata)"
    )
    assert result is not None


# ---------------------------------------------------------------------------
# §7.10  Multi-statement with `<-` field assignment
# ---------------------------------------------------------------------------

@pytest.mark.xfail(
    strict=False,
    reason="structural syntax `;` and `<-` field assignment not yet implemented",
)
def test_7_10_field_assignment_arrow():
    result = parse(
        "fromjson(.metadata); .temperature <- fromjson(.metadata.temperature); tojson(.metadata)"
    )
    assert result is not None


# ---------------------------------------------------------------------------
# §7.11  Multi-statement with `=` as structural assignment
# ---------------------------------------------------------------------------

@pytest.mark.xfail(
    strict=False,
    reason="structural syntax `;` and `=` as structural assignment not yet implemented",
)
def test_7_11_structural_equals_assignment():
    result = parse(
        "fromjson(.metadata); .temperature = .metadata.temperature; tojson(.metadata)"
    )
    assert result is not None


# ---------------------------------------------------------------------------
# §7.12  Complement merge piped into unregistered tool
# ---------------------------------------------------------------------------

@pytest.mark.xfail(
    strict=False,
    reason="structural syntax `<<` and complement `^` not yet implemented; normalize_metadata not registered",
)
def test_7_12_complement_merge_pipe_tool():
    result = parse(
        ".metadata << .(^instance_id|messages|tools) | normalize_metadata | tojson"
    )
    assert result is not None


# ---------------------------------------------------------------------------
# §7.13a  Legacy pipe sequencing — currently valid
# ---------------------------------------------------------------------------

def test_7_13a_legacy_pipe_currently_valid():
    result = parse(
        "fromjson(.tools) | fromjson(.metadata.annotation, recursive=true)"
    )
    assert result is not None  # placeholder until Program node exists


# ---------------------------------------------------------------------------
# §7.13b  Same as 7.13a but using `;` — not yet valid
# ---------------------------------------------------------------------------

@pytest.mark.xfail(strict=False, reason="structural syntax `;` not yet implemented")
def test_7_13b_semicolon_sequencing():
    result = parse(
        "fromjson(.tools); fromjson(.metadata.annotation, recursive=true)"
    )
    assert result is not None
