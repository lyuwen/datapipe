from __future__ import annotations

from datapipe.dsl.parser import parse, parse_program
from datapipe.dsl.ast import Expression, Program


# ---------------------------------------------------------------------------
# §7.1  Sequence via `;` — two fromjson invocations
# ---------------------------------------------------------------------------

def test_7_1_fromjson_sequence_semicolon():
    result = parse_program("fromjson(.tools); fromjson(.metadata.annotation, recursive=true)")
    assert isinstance(result, Program)


# ---------------------------------------------------------------------------
# §7.2  Single tojson invocation — currently valid
# ---------------------------------------------------------------------------

def test_7_2_tojson_single_currently_valid():
    expression = "tojson(.tools[].function.parameters)"

    legacy = parse(expression)
    assert isinstance(legacy, Expression)
    assert len(legacy.invocations) == 1
    assert legacy.invocations[0].qualified_name.name == "tojson"

    program = parse_program(expression)
    assert isinstance(program, Program)
    assert len(program.statements) == 1
    assert program.statements[0].operation.qualified_name.name == "tojson"


# ---------------------------------------------------------------------------
# §7.3  Sequence via `;` — two tojson invocations
# ---------------------------------------------------------------------------

def test_7_3_tojson_sequence_semicolon():
    result = parse_program("tojson(.keya); tojson(.keyb)")
    assert isinstance(result, Program)


# ---------------------------------------------------------------------------
# §7.4  Three-statement sequence via `;`
# ---------------------------------------------------------------------------

def test_7_4_three_statement_sequence():
    result = parse_program("tojson(.tools); tojson(.metadata); finalize_record(.)")
    assert isinstance(result, Program)


# ---------------------------------------------------------------------------
# §7.5  Struct-merge `<<` with comma-separated field list
# ---------------------------------------------------------------------------

def test_7_5_struct_merge_comma_fields():
    result = parse_program(".metadata << .annotation_key, .temperature, .score | tojson")
    assert isinstance(result, Program)


# ---------------------------------------------------------------------------
# §7.6  Struct-merge `<<` with grouped field-set selector `.(a|b|c)`
# ---------------------------------------------------------------------------

def test_7_6_struct_merge_field_set():
    result = parse_program(".metadata << .(annotation_key|temperature|score) | tojson")
    assert isinstance(result, Program)


# ---------------------------------------------------------------------------
# §7.7  Struct-merge `<<` with complement selector `^`
# ---------------------------------------------------------------------------

def test_7_7_struct_merge_complement():
    result = parse_program(".metadata << .(^instance_id|messages|tools) | tojson")
    assert isinstance(result, Program)


# ---------------------------------------------------------------------------
# §7.8  `nest()` built-in — registered since S5
# ---------------------------------------------------------------------------

def test_7_8_nest_builtin():
    expression = (
        'nest(., key="metadata", exclude=["instance_id","messages","tools"], jsonify=true)'
    )

    legacy = parse(expression)
    assert isinstance(legacy, Expression)
    assert len(legacy.invocations) == 1
    assert legacy.invocations[0].qualified_name.name == "nest"

    program = parse_program(expression)
    assert isinstance(program, Program)
    assert len(program.statements) == 1
    assert program.statements[0].operation.qualified_name.name == "nest"


# ---------------------------------------------------------------------------
# §7.9  Multi-statement with `<<` nesting
# ---------------------------------------------------------------------------

def test_7_9_multistatement_with_struct_merge():
    result = parse_program(
        "fromjson(.metadata); . << .metadata.(temperature|score); tojson(.metadata)"
    )
    assert isinstance(result, Program)


# ---------------------------------------------------------------------------
# §7.10  Multi-statement with `<-` field assignment
# ---------------------------------------------------------------------------

def test_7_10_field_assignment_arrow():
    result = parse_program(
        "fromjson(.metadata); .temperature <- fromjson(.metadata.temperature); tojson(.metadata)"
    )
    assert isinstance(result, Program)
    assert len(result.statements) == 3


# ---------------------------------------------------------------------------
# §7.11  Multi-statement with `=` as structural assignment
# ---------------------------------------------------------------------------

def test_7_11_structural_equals_assignment():
    result = parse_program(
        "fromjson(.metadata); .temperature = .metadata.temperature; tojson(.metadata)"
    )
    assert isinstance(result, Program)
    assert len(result.statements) == 3


# ---------------------------------------------------------------------------
# §7.12  Complement merge piped into unregistered tool
# ---------------------------------------------------------------------------

def test_7_12_complement_merge_pipe_tool():
    result = parse_program(
        ".metadata << .(^instance_id|messages|tools) | normalize_metadata | tojson"
    )
    assert isinstance(result, Program)


# ---------------------------------------------------------------------------
# §7.13a  Legacy pipe sequencing — currently valid
# ---------------------------------------------------------------------------

def test_7_13a_legacy_pipe_currently_valid():
    result = parse(
        "fromjson(.tools) | fromjson(.metadata.annotation, recursive=true)"
    )
    assert result is not None  # placeholder until Program node exists


# ---------------------------------------------------------------------------
# §7.13b  Same as 7.13a but using `;` — now valid via parse_program()
# ---------------------------------------------------------------------------

def test_7_13b_semicolon_sequencing():
    result = parse_program(
        "fromjson(.tools); fromjson(.metadata.annotation, recursive=true)"
    )
    assert isinstance(result, Program)
