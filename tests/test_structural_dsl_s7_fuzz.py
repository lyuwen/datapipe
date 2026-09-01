"""Phase S7 fuzzing: parser robustness, precedence, and structural overlap.

No new dependency — ``random`` with a fixed seed, so any failure reproduces.
Every failure message prints the seed and the offending expression.

The overlap and record-invariance sections are the point of this file.  Two
real data-corruption bugs reached review in S3 and neither was caught by an
example-based suite:

- ``.items[1] <- .items[0]`` deleted a list element by index, which shifted
  later elements out from under an already-resolved destination reference;
- ``.a = .b`` inserted a live alias, so a later ``.a.x = .c`` visibly mutated
  ``.b``.

Both were "the compiler accepted it and the runtime silently corrupted the
record".  The properties here are written to catch that class systematically:
anything the compiler accepts must either execute cleanly and leave a record
with no aliasing and no lost data, or fail with a structured error and leave
the record byte-identical to its input.

Vacuity guard: every generator asserts a floor on the number of cases it
actually produced.  A property that holds because nothing was generated is the
same failure as a property that is not tested.
"""

from __future__ import annotations

import copy
import dataclasses
import itertools
import json
import random
import signal
import sys

import pytest

from datapipe.dsl import ast as _ast
from datapipe.dsl.compiler import compile_program
from datapipe.dsl.errors import (
    ExpressionSyntaxError,
    Span,
    ToolConfigurationError,
    ToolResolutionError,
)
from datapipe.dsl.parser import parse_program
from datapipe.stages.tool_program import CompiledProgramStage
from datapipe.tools.errors import StructuralExecutionError, ToolExecutionError

#: Fixed so a failure reproduces exactly.  Printed in every failure message.
SEED = 20260901

#: Wall-clock bound for a single parse.  A parser that exceeds this is hung as
#: far as a caller is concerned, whatever it is doing internally.
PARSE_TIMEOUT_SECONDS = 5


@pytest.fixture(autouse=True)
def isolated_registry(tmp_path, monkeypatch):
    """Redirect the tool registry at tmp_path and clear the loader cache."""
    monkeypatch.setenv("DATAPIPE_USER_DATA", str(tmp_path / "dp_data"))

    from datapipe.tools import loader as _loader

    _loader._loaded_providers.clear()

    from datapipe.dsl import compiler as _compiler

    monkeypatch.setattr(_compiler, "_BUILTIN_REGISTRY", None, raising=False)


# ===========================================================================
# Helpers
# ===========================================================================


def _ctx(expression: str, extra: str = "") -> str:
    """Failure context that always names the seed and the input."""
    suffix = f"\n  {extra}" if extra else ""
    return f"\n  seed={SEED}\n  expression={expression!r}{suffix}"


class _Timeout(Exception):
    pass


def _parse_bounded(expression: str):
    """Parse *expression*, failing the test if it takes longer than the bound.

    ``SIGALRM`` is POSIX-only; where it is unavailable the bound is skipped and
    the other parser properties still apply.
    """
    if not hasattr(signal, "SIGALRM"):
        return parse_program(expression)

    def _fire(_signum, _frame):
        raise _Timeout()

    previous = signal.signal(signal.SIGALRM, _fire)
    signal.alarm(PARSE_TIMEOUT_SECONDS)
    try:
        return parse_program(expression)
    except _Timeout:
        pytest.fail(f"parse did not terminate within bound{_ctx(expression)}")
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


def _spans(node, out=None):
    """Collect ``(node_type_name, Span)`` for every AST node reachable from *node*."""
    if out is None:
        out = []
    if dataclasses.is_dataclass(node) and not isinstance(node, type):
        span = getattr(node, "span", None)
        if isinstance(span, Span):
            out.append((type(node).__name__, span))
        for field in dataclasses.fields(node):
            _spans(getattr(node, field.name), out)
    elif isinstance(node, tuple):
        for item in node:
            _spans(item, out)
    return out


def _shared_containers(value, seen=None, path=()):
    """Return ``(first_path, second_path)`` for any container reachable twice.

    A dict or list reachable by two distinct paths is an alias: mutating it
    through one path is visible through the other.  That is exactly the S3 copy
    bug, and a self-referential cycle shows up here too (the container is
    reachable from inside itself).
    """
    if seen is None:
        seen = {}
    found = []
    if isinstance(value, (dict, list)):
        marker = id(value)
        if marker in seen:
            return [(seen[marker], path)]
        seen[marker] = path
        items = value.items() if isinstance(value, dict) else enumerate(value)
        for key, sub in items:
            found += _shared_containers(sub, seen, path + (key,))
    return found


def _leaf_count(value) -> int:
    """Number of scalar leaves in *value* — a move must conserve this."""
    if isinstance(value, dict):
        return sum(_leaf_count(v) for v in value.values())
    if isinstance(value, list):
        return sum(_leaf_count(v) for v in value)
    return 1


def _run(expression: str, record, validate: str = "off"):
    """Compile and execute *expression* against a fresh copy of *record*."""
    stage = CompiledProgramStage(compile_program(expression), validate=validate)
    return stage.process(record, None)


# ===========================================================================
# 1. Parser fuzzing: nothing but ExpressionSyntaxError may escape
# ===========================================================================

#: Tokens drawn from the structural grammar, including every operator S0-S6
#: added.  Random sequences of these are overwhelmingly malformed, which is the
#: point: the parser must reject them as syntax errors, not crash.
_FUZZ_TOKENS = (
    ".", "a", "b", "metadata", "tools", ";", "|", "=", "<-", "<<", "^",
    "(", ")", ",", "[", "]", "{", "}", ":", "0", "1", '"k"', "'s'",
    "fromjson", "tojson", "nest", "unnest", "true", "null", "-", "_x",
)

#: Exceptions a caller of ``parse_program`` is expected to handle.  Anything
#: else — IndexError, RecursionError, AttributeError, a bare Exception — is a
#: parser bug that would surface as a crash rather than a diagnostic.
_ALLOWED_PARSE_ERRORS = (ExpressionSyntaxError,)


def test_random_token_sequences_only_raise_expression_syntax_error():
    """No random token sequence may make the parser raise anything else.

    Random token soup is almost never well-formed, so a handful of well-formed
    expressions are mixed in: a test that only ever walks the error path proves
    nothing about the parser on valid input.
    """
    rng = random.Random(SEED)
    wellformed = _generate_wellformed(rng, count=2_000)
    generated = 0
    parsed_ok = 0

    for index in range(20_000):
        if index % 10 == 0:
            expression = wellformed[(index // 10) % len(wellformed)]
        else:
            length = rng.randint(1, 12)
            expression = " ".join(rng.choice(_FUZZ_TOKENS) for _ in range(length))
        generated += 1
        try:
            _parse_bounded(expression)
            parsed_ok += 1
        except _ALLOWED_PARSE_ERRORS:
            pass
        except Exception as exc:  # noqa: BLE001
            pytest.fail(
                f"parser leaked {type(exc).__name__}: {exc}"
                + _ctx(expression)
            )

    assert generated == 20_000, "generator floor: expected 20,000 sequences"
    # Vacuity guard the other way: a generator that never produces a *valid*
    # expression is only testing the error path.
    assert parsed_ok >= 1_500, (
        f"generator produced only {parsed_ok} parseable expressions; it is not "
        f"reaching the success path (seed={SEED})"
    )


def test_random_character_noise_only_raises_expression_syntax_error():
    """Byte-level noise, including unbalanced delimiters, must not crash the lexer."""
    rng = random.Random(SEED + 1)
    alphabet = ".|;=<->^(),[]{}:\"'abc_ 019\\\t\n"
    generated = 0

    for _ in range(20_000):
        length = rng.randint(1, 40)
        expression = "".join(rng.choice(alphabet) for _ in range(length))
        generated += 1
        try:
            _parse_bounded(expression)
        except _ALLOWED_PARSE_ERRORS:
            pass
        except Exception as exc:  # noqa: BLE001
            pytest.fail(
                f"parser leaked {type(exc).__name__}: {exc}" + _ctx(expression)
            )

    assert generated == 20_000, "generator floor: expected 20,000 noise strings"


@pytest.mark.parametrize("depth", [80, 500, 5_000])
def test_deeply_nested_literals_raise_syntax_error_not_recursion_error(depth):
    """A deeply nested literal is a syntax error, never a ``RecursionError``.

    Literals are the parser's only unbounded recursion.  Before S7 a literal
    nested past roughly 496 levels exhausted the interpreter stack and raised
    ``RecursionError``, which is not an ``ExpressionSyntaxError`` and therefore
    escaped every caller that handles malformed expressions — including the
    CLI's ``_compile_or_report``.
    """
    for opening, closing in (("[", "]"), ('{"k":', "}")):
        body = opening * depth + ("1" if opening != "[" else "") + closing * depth
        expression = f"fromjson(.a, x={body})"
        with pytest.raises(ExpressionSyntaxError) as caught:
            parse_program(expression)
        assert "nested more than" in caught.value.base_message, (
            f"expected a depth diagnostic, got {caught.value.base_message!r}"
            + _ctx(expression[:60] + "...")
        )


def test_literals_just_under_the_depth_bound_still_parse():
    """The depth bound must not reject configurations anyone would actually write."""
    from datapipe.dsl.parser import _MAX_LITERAL_DEPTH

    depth = _MAX_LITERAL_DEPTH
    expression = f"fromjson(.a, x={'[' * depth}{']' * depth})"
    program = parse_program(expression)
    argument = program.statements[0].operation.arguments[0]

    # Walk the parsed value down to confirm the full depth survived, rather
    # than trusting that "it parsed" means "it parsed correctly".
    value = argument.value.value
    for _ in range(depth - 1):
        assert isinstance(value, list) and len(value) == 1, _ctx(expression[:40])
        value = value[0]
    assert value == [], _ctx(expression[:40])


def test_every_ast_span_falls_inside_the_source_string():
    """No node may carry a span pointing outside the expression it came from."""
    rng = random.Random(SEED + 2)
    checked_expressions = 0
    checked_spans = 0

    for expression in _generate_wellformed(rng, count=2_000):
        try:
            program = _parse_bounded(expression)
        except ExpressionSyntaxError:
            continue
        checked_expressions += 1
        for name, span in _spans(program):
            checked_spans += 1
            assert 0 <= span.start <= span.end <= len(expression), (
                f"{name} span {span} escapes a {len(expression)}-char source"
                + _ctx(expression)
            )

    assert checked_expressions >= 1_500, (
        f"generator floor: only {checked_expressions} well-formed expressions "
        f"parsed (seed={SEED})"
    )
    assert checked_spans >= 10_000, (
        f"generator floor: only {checked_spans} spans checked (seed={SEED})"
    )


# ===========================================================================
# 2. Precedence fuzzing: the parse must be predictable from the shape
# ===========================================================================


def _generate_wellformed(rng: random.Random, count: int) -> list[str]:
    """Generate syntactically well-formed structural expressions.

    Each is built from a known shape, so the expected parse is known before
    parsing — which is what makes the precedence assertions meaningful rather
    than "it parsed, therefore it is right".
    """
    selectors = [".a", ".b", ".m", ".m.x", ".tools", ".items[0]", "."]
    names = ["a", "b", "c", "temperature", "score"]
    tools = ["fromjson", "tojson"]

    out: list[str] = []
    for _ in range(count):
        statements = []
        for _ in range(rng.randint(1, 3)):
            kind = rng.choice(("call", "assign", "move", "move_into", "focused"))
            if kind == "call":
                statements.append(f"{rng.choice(tools)}({rng.choice(selectors)})")
            elif kind == "assign":
                statements.append(f"{rng.choice(selectors)} = {rng.choice(selectors)}")
            elif kind == "move":
                statements.append(f"{rng.choice(selectors)} <- {rng.choice(selectors)}")
            elif kind == "focused":
                pipes = " | ".join(
                    rng.choice(tools) for _ in range(rng.randint(1, 3))
                )
                statements.append(f"{rng.choice(selectors)} | {pipes}")
            else:
                sources = []
                for _ in range(rng.randint(1, 3)):
                    if rng.random() < 0.4:
                        caret = "^" if rng.random() < 0.5 else ""
                        chosen = rng.sample(names, rng.randint(1, 3))
                        base = rng.choice(["", ".m", ".a"])
                        sources.append(f"{base}.({caret}{'|'.join(chosen)})")
                    else:
                        sources.append(rng.choice(selectors))
                text = f"{rng.choice(selectors)} << {', '.join(sources)}"
                if rng.random() < 0.4:
                    text += " | " + " | ".join(
                        rng.choice(tools) for _ in range(rng.randint(1, 2))
                    )
                statements.append(text)
        expression = "; ".join(statements)
        if rng.random() < 0.1:
            expression += ";"
        out.append(expression)
    return out


def test_semicolon_binds_loosest_so_it_sets_the_statement_count():
    """``;`` separates statements; nothing inside a statement may split it (§9.1)."""
    rng = random.Random(SEED + 3)
    checked = 0

    for expression in _generate_wellformed(rng, count=2_000):
        try:
            program = _parse_bounded(expression)
        except ExpressionSyntaxError:
            continue
        # A trailing `;` is optional and adds no statement.
        expected = len([p for p in expression.split(";") if p.strip()])
        assert len(program.statements) == expected, (
            f"expected {expected} statements, parsed {len(program.statements)}"
            + _ctx(expression)
        )
        checked += 1

    assert checked >= 1_500, (
        f"generator floor: only {checked} expressions checked (seed={SEED})"
    )


def test_pipe_binds_tighter_than_semicolon_and_attaches_to_its_statement():
    """A ``|`` chain belongs to one statement; it never spans a ``;`` (§9.1)."""
    rng = random.Random(SEED + 4)
    with_pipes = 0

    for expression in _generate_wellformed(rng, count=2_000):
        try:
            program = _parse_bounded(expression)
        except ExpressionSyntaxError:
            continue
        for index, statement in enumerate(program.statements):
            # Every pipe of a statement must lie within that statement's own
            # span — the structural proof that `|` did not cross a `;`.
            for pipe in statement.pipes:
                assert (
                    statement.span.start <= pipe.span.start
                    and pipe.span.end <= statement.span.end
                ), (
                    f"statement {index} pipe span {pipe.span} escapes statement "
                    f"span {statement.span}" + _ctx(expression)
                )
            if statement.pipes:
                with_pipes += 1

    assert with_pipes >= 300, (
        f"generator floor: only {with_pipes} statements carried pipes; the "
        f"precedence property is nearly vacuous (seed={SEED})"
    )


def test_comma_binds_tighter_than_pipe_in_a_move_source_list():
    """``a, b | tool`` is one move of two sources plus one pipe, not two moves (§9.1)."""
    rng = random.Random(SEED + 5)
    checked = 0

    for _ in range(500):
        count = rng.randint(2, 4)
        sources = ", ".join(f".s{i}" for i in range(count))
        pipes = rng.randint(1, 3)
        tail = " | " + " | ".join("tojson" for _ in range(pipes))
        expression = f".dest << {sources}{tail}"

        program = _parse_bounded(expression)
        assert len(program.statements) == 1, _ctx(expression)
        operation = program.statements[0].operation
        assert isinstance(operation, _ast.MoveInto), (
            f"expected MoveInto, got {type(operation).__name__}" + _ctx(expression)
        )
        # The comma-joined sources all belong to the move...
        assert len(operation.sources) == count, (
            f"expected {count} sources, got {len(operation.sources)}"
            + _ctx(expression)
        )
        # ...and every pipe attached to the statement, not to the last source.
        assert len(program.statements[0].pipes) == pipes, (
            f"expected {pipes} pipes, got {len(program.statements[0].pipes)}"
            + _ctx(expression)
        )
        checked += 1

    assert checked == 500, "generator floor: expected 500 comma/pipe cases"


def test_pipe_inside_field_set_parentheses_is_always_a_field_union():
    """§9.2: inside ``.()`` a ``|`` joins names; outside it chains tools."""
    rng = random.Random(SEED + 6)
    checked = 0

    for _ in range(500):
        inner = rng.randint(1, 5)
        outer = rng.randint(0, 3)
        names = [f"f{i}" for i in range(inner)]
        complement = rng.random() < 0.5
        caret = "^" if complement else ""
        field_set = f".({caret}{'|'.join(names)})"
        tail = "".join(" | tojson" for _ in range(outer))
        expression = f".dest << {field_set}{tail}"

        program = _parse_bounded(expression)
        statement = program.statements[0]
        operation = statement.operation
        assert isinstance(operation, _ast.MoveInto), _ctx(expression)
        assert len(operation.sources) == 1, (
            "a field union must stay one source" + _ctx(expression)
        )
        source = operation.sources[0]
        assert isinstance(source, _ast.FieldSet), (
            f"expected FieldSet, got {type(source).__name__}" + _ctx(expression)
        )
        # Every inner `|` became a name, and no inner `|` leaked out as a pipe.
        assert list(source.names) == names, (
            f"expected names {names}, got {list(source.names)}" + _ctx(expression)
        )
        assert source.complement is complement, _ctx(expression)
        assert len(statement.pipes) == outer, (
            f"expected {outer} pipes outside the parentheses, got "
            f"{len(statement.pipes)}" + _ctx(expression)
        )
        checked += 1

    assert checked == 500, "generator floor: expected 500 contextual-pipe cases"


# ===========================================================================
# 3. Overlap-detection fuzzing
# ===========================================================================

#: Records chosen to exercise the shapes the overlap rules care about: nested
#: objects, arrays of objects, a key whose name repeats at two depths, a
#: non-object where an object is expected, and empty containers.
_FUZZ_RECORDS = (
    {
        "a": {"x": 1, "y": {"z": 2}},
        "b": {"x": 10, "w": 20},
        "m": {"p": 1, "q": 2, "r": 3},
        "items": [{"p": 1}, {"p": 2}, {"p": 3}],
        "s": "text",
        "n": 5,
    },
    {"a": 1, "b": [1, 2], "m": "not-an-object", "items": [], "s": "", "n": 0},
    {"m": {"a": 1, "m": {"deep": 1}}, "a": {"m": 2}, "items": [{"p": 1}], "s": "t"},
    {"a": {}, "b": {}, "m": {}, "items": [], "s": "x", "n": 1},
)

_FUZZ_PATHS = (
    ".a", ".a.x", ".a.y", ".b", ".b.x", ".m", ".m.p", ".m.m",
    ".items", ".items[0]", ".items[1]", ".items[0].p", ".items[1].p",
    ".s", ".n", ".new", ".a.new", ".",
)

_FUZZ_FIELD_SETS = (
    ".(a|b)", ".(^a)", ".(^a|b|m)", ".m.(p)", ".m.(^p)", ".m.(a)",
    ".m.(^a)", ".a.(x)", ".a.(m)",
)


def _statement_templates() -> list[str]:
    """Every single-statement structural form over the fuzz paths."""
    templates: list[str] = []
    for operator in ("=", "<-"):
        for dest, src in itertools.product(_FUZZ_PATHS, _FUZZ_PATHS):
            templates.append(f"{dest} {operator} {src}")
            templates.append(f"{dest} {operator} fromjson({src})")
    for dest in _FUZZ_PATHS:
        for src in _FUZZ_PATHS + _FUZZ_FIELD_SETS:
            templates.append(f"{dest} << {src}")
            templates.append(f"{dest} << {src} | tojson")
        for src, second in itertools.product(_FUZZ_FIELD_SETS, _FUZZ_PATHS[:6]):
            templates.append(f"{dest} << {src}, {second}")
    return templates


def test_accepted_single_statements_never_corrupt_a_record():
    """Anything the compiler accepts executes cleanly or fails atomically.

    This is the property that would have caught both S3 bugs.  For every
    accepted statement and every fuzz record, exactly one of these must hold:

    - it succeeds, and the result has no aliased container, no cycle, and is
      JSON-serializable; a move additionally conserves the leaf count;
    - it raises a structured error, and the record is unchanged.

    A leaked exception type, a surviving alias, or a mutated record on failure
    is a failure of the test.
    """
    templates = _statement_templates()
    compiled = 0
    succeeded = 0
    failed = 0
    conserved = 0

    for expression in templates:
        try:
            program = compile_program(expression)
        except (ToolConfigurationError, ToolResolutionError, ExpressionSyntaxError):
            continue
        compiled += 1
        stage = CompiledProgramStage(program, validate="off")

        for original in _FUZZ_RECORDS:
            record = copy.deepcopy(original)
            before = copy.deepcopy(original)
            try:
                result = stage.process(record, None)
            except (StructuralExecutionError, ToolExecutionError):
                failed += 1
                assert record == before, (
                    "a failed statement mutated the record"
                    + _ctx(expression, f"before={before!r}\n  after={record!r}")
                )
                continue
            except Exception as exc:  # noqa: BLE001
                pytest.fail(
                    f"unstructured {type(exc).__name__} from an accepted "
                    f"statement: {exc}" + _ctx(expression)
                )

            succeeded += 1
            shared = _shared_containers(result)
            assert not shared, (
                f"result contains an aliased container reachable at {shared[0][0]} "
                f"and {shared[0][1]}" + _ctx(expression, f"result={result!r}")
            )
            json.dumps(result)  # a cycle or a non-JSON value raises here

            # A pipeless `<<` relocates fields without transforming them, so
            # every leaf that went in must come out.  `<-` is excluded: it
            # overwrites its destination, so a move onto an occupied path
            # legitimately drops what was there.  A trailing pipe is excluded
            # too — `| tojson` collapses a subtree into one string leaf.
            if "<<" in expression and "|" not in expression:
                conserved += 1
                assert _leaf_count(result) == _leaf_count(before), (
                    f"a move-into changed the leaf count "
                    f"{_leaf_count(before)} -> {_leaf_count(result)}"
                    + _ctx(expression, f"before={before!r}\n  after={result!r}")
                )

    assert compiled >= 1_500, (
        f"generator floor: only {compiled} statements compiled (seed={SEED})"
    )
    assert succeeded >= 1_000, (
        f"generator floor: only {succeeded} statements executed successfully; "
        f"the success-path assertions are nearly vacuous (seed={SEED})"
    )
    assert failed >= 1_000, (
        f"generator floor: only {failed} statements failed; the atomicity "
        f"assertion is nearly vacuous (seed={SEED})"
    )
    assert conserved >= 200, (
        f"generator floor: only {conserved} pipeless move-intos ran; the "
        f"leaf-conservation property is nearly vacuous (seed={SEED})"
    )


#: Statements chosen so that a random *composition* of them frequently
#: succeeds.  Drawing statements uniformly from ``_statement_templates()``
#: almost always produces a program that dies on its first collision, which
#: leaves the alias property — a cross-statement property that needs the
#: program to actually run — measured on a handful of cases.  These use
#: distinct sources and destinations so the composition survives.
_COMPOSABLE_STATEMENTS = (
    ".new = .a", ".new = .b", ".new = .m", ".new <- .a", ".new <- .b",
    ".a.new = .b", ".a.new <- .b", ".b.new = .a", ".a.k = .s", ".b.k = .n",
    ".m << .a", ".m << .b", ".m << .a, .b", ".m << .s", ".m << .n",
    ".new = .s", ".new = .n", ".m << .(^m)", ". << .m.(^deep)",
    ".m << .a | tojson", "tojson(.a)", "tojson(.b)", "tojson(.m)",
    ".a | tojson", ".b | tojson",
)

#: Records rich enough that the composable statements above resolve.
_COMPOSABLE_RECORDS = (
    {
        "a": {"x": 1, "y": {"z": 2}},
        "b": {"x": 10, "w": 20},
        "m": {"p": 1, "q": 2, "r": 3},
        "items": [{"p": 1}, {"p": 2}],
        "s": "text",
        "n": 5,
    },
    {
        "m": {"a": 1, "m": {"deep": 1}},
        "a": {"m": 2},
        "b": {"k": 1},
        "items": [{"p": 1}],
        "s": "t",
        "n": 2,
    },
    {"a": {"x": 1}, "b": {"w": 2}, "m": {}, "items": [{"p": 1}], "s": "x", "n": 1},
)


def test_accepted_multi_statement_programs_never_alias_or_cycle():
    """Aliasing is cross-statement: it only shows once a later statement writes.

    ``.a = .b`` alone looks fine; the corruption appears when a following
    ``.a.x = .c`` mutates what turned out to be ``.b``.  So the alias property
    has to be checked over whole programs, not single statements — and the
    programs have to actually run, which is why half the draws come from the
    composable pool rather than all from the exhaustive one.
    """
    rng = random.Random(SEED + 7)
    templates = _statement_templates()
    compiled = 0
    succeeded = 0

    for index in range(20_000):
        pool = _COMPOSABLE_STATEMENTS if index % 2 == 0 else templates
        records = _COMPOSABLE_RECORDS if index % 2 == 0 else _FUZZ_RECORDS
        count = rng.randint(2, 4)
        expression = "; ".join(rng.choice(pool) for _ in range(count))
        try:
            program = compile_program(expression)
        except (ToolConfigurationError, ToolResolutionError, ExpressionSyntaxError):
            continue
        compiled += 1

        original = rng.choice(records)
        record = copy.deepcopy(original)
        try:
            result = CompiledProgramStage(program, validate="off").process(
                record, None
            )
        except (StructuralExecutionError, ToolExecutionError):
            continue
        except Exception as exc:  # noqa: BLE001
            pytest.fail(
                f"unstructured {type(exc).__name__} from an accepted program: "
                f"{exc}" + _ctx(expression)
            )

        succeeded += 1
        shared = _shared_containers(result)
        assert not shared, (
            f"program result contains an aliased container reachable at "
            f"{shared[0][0]} and {shared[0][1]}"
            + _ctx(expression, f"result={result!r}")
        )
        json.dumps(result)

    assert compiled >= 5_000, (
        f"generator floor: only {compiled} programs compiled (seed={SEED})"
    )
    assert succeeded >= 4_000, (
        f"generator floor: only {succeeded} programs ran to completion; the "
        f"alias property is nearly vacuous (seed={SEED})"
    )


def test_a_copy_leaves_the_source_independently_mutable():
    """§6.1: ``=`` copies.  Writing through the destination must not touch the source.

    The direct statement of the S3 aliasing bug, checked over every
    source/destination pair the compiler accepts rather than one example.
    """
    checked = 0

    for dest, src in itertools.product(_FUZZ_PATHS, _FUZZ_PATHS):
        expression = f"{dest} = {src}"
        try:
            program = compile_program(expression)
        except (ToolConfigurationError, ExpressionSyntaxError):
            continue

        for original in _FUZZ_RECORDS:
            record = copy.deepcopy(original)
            try:
                result = CompiledProgramStage(program, validate="off").process(
                    record, None
                )
            except (StructuralExecutionError, ToolExecutionError):
                continue

            # Resolve both endpoints in the result and confirm that mutating
            # one is invisible through the other.
            from datapipe.dsl.selector import CompiledSelector
            from datapipe.dsl.errors import SelectorResolutionError
            from datapipe.dsl.parser import parse_program as _pp

            def _resolve(text):
                node = _pp(f"tojson({text})").statements[0].operation.selector
                try:
                    refs = CompiledSelector(node).resolve(result)
                except SelectorResolutionError:
                    return None
                return refs[0].value if len(refs) == 1 else None

            dest_value = _resolve(dest)
            src_value = _resolve(src)
            if not isinstance(dest_value, dict) or not isinstance(src_value, dict):
                continue
            if dest_value is src_value:
                pytest.fail(
                    "copy produced a live alias: destination and source are the "
                    "same object" + _ctx(expression, f"result={result!r}")
                )

            sentinel = "__s7_probe__"
            dest_value[sentinel] = 1
            assert sentinel not in src_value, (
                "writing to the copy destination mutated the copy source"
                + _ctx(expression, f"result={result!r}")
            )
            del dest_value[sentinel]
            checked += 1

    assert checked >= 20, (
        f"generator floor: only {checked} container copies were reachable; the "
        f"aliasing property is nearly vacuous (seed={SEED})"
    )


def test_statically_rejected_overlaps_would_also_fail_at_runtime():
    """A compile-time rejection must describe a real runtime hazard, not a guess.

    The two checks share ``overlap_reason``, so a statement rejected statically
    on a fully-static path pair must be rejected for the same reason when the
    runtime evaluates the concrete paths.  If the static check were stricter
    than the runtime one, it would be rejecting expressions that are actually
    safe — which is what S4 did to complements before S6 narrowed it.
    """
    from datapipe.dsl.compiler import overlap_reason, _static_path
    from datapipe.dsl.parser import parse_program as _pp

    rejected = 0
    for operator, is_move in (("=", False), ("<-", True)):
        for dest, src in itertools.product(_FUZZ_PATHS, _FUZZ_PATHS):
            expression = f"{dest} {operator} {src}"
            try:
                compile_program(expression)
                continue
            except ExpressionSyntaxError:
                continue
            except ToolConfigurationError:
                pass

            statement = _pp(expression).statements[0].operation
            dest_path = _static_path(statement.destination)
            src_path = _static_path(statement.rhs.source)
            if dest_path is None or src_path is None:
                continue

            # The same predicate the runtime applies to concrete paths.
            reason = overlap_reason(dest_path, src_path, is_move=is_move)
            assert reason is not None, (
                "compiler rejected a pair the runtime overlap rule considers "
                "safe" + _ctx(expression)
            )
            rejected += 1

    assert rejected >= 40, (
        f"generator floor: only {rejected} static rejections examined "
        f"(seed={SEED})"
    )


def test_a_move_out_of_an_array_by_index_is_rejected_not_silently_wrong():
    """The exact S3 list-index bug: ``.items[1] <- .items[0]`` must not compile.

    Deleting a *list element* by index renumbers every later element, so a
    destination resolved before the delete no longer names the slot it was
    resolved against.  Both endpoints in the same array is unfixable in either
    order, so it is rejected.

    The rule is about the deletion renumbering siblings, not about indices
    appearing in the path.  ``.items[0].p <- .items[1].p`` deletes a *dict key*
    inside an element, which shifts nothing, so it stays legal — and the
    assertion below pins that distinction rather than over-rejecting.
    """
    unstable = [
        ".items[1] <- .items[0]",
        ".items[0] <- .items[1]",
    ]
    for expression in unstable:
        with pytest.raises(ToolConfigurationError) as caught:
            compile_program(expression)
        assert "renumber" in str(caught.value), (
            f"expected the renumbering diagnostic, got {caught.value}"
            + _ctx(expression)
        )

    # A move between two array elements that deletes an object field renumbers
    # nothing, so it compiles and must execute correctly.
    record = {"items": [{"p": 1, "q": 9}, {"p": 2, "q": 8}]}
    result = _run(".items[0].p <- .items[1].p", copy.deepcopy(record))
    assert result == {"items": [{"p": 2, "q": 9}, {"q": 8}]}, result

    # The legal counterpart from §8.9 must still work, and must actually move.
    record = {"metadata": {}, "values": ["first", "second"]}
    result = _run(".metadata.first <- .values[0]", copy.deepcopy(record))
    assert result == {"metadata": {"first": "first"}, "values": ["second"]}, result


# ===========================================================================
# 4. Record invariance: a failing statement leaves the record untouched
# ===========================================================================


def test_every_runtime_failure_leaves_the_record_byte_identical():
    """The atomicity guarantee, over randomly composed programs.

    §8.1 requires that nothing is written until every precondition has passed.
    Checked here against the *serialized* record, so a difference in key order
    or a mutated nested container counts as a violation just as a changed value
    does.
    """
    rng = random.Random(SEED + 8)
    templates = _statement_templates()
    observed_failures = 0
    observed_successes = 0

    for index in range(20_000):
        # Half the draws come from the composable pool so the success path is
        # exercised too; a suite that only ever fails proves only that failing
        # is safe, not that succeeding is correct.
        pool = _COMPOSABLE_STATEMENTS if index % 2 == 0 else templates
        records = _COMPOSABLE_RECORDS if index % 2 == 0 else _FUZZ_RECORDS
        count = rng.randint(1, 3)
        expression = "; ".join(rng.choice(pool) for _ in range(count))
        try:
            program = compile_program(expression)
        except (ToolConfigurationError, ToolResolutionError, ExpressionSyntaxError):
            continue

        original = rng.choice(records)
        record = copy.deepcopy(original)
        # Serialize before, so the comparison catches key-order changes too.
        before = json.dumps(original, sort_keys=False)

        try:
            CompiledProgramStage(program, validate="always").process(record, None)
            observed_successes += 1
        except (StructuralExecutionError, ToolExecutionError):
            observed_failures += 1
            if count == 1:
                # Single statements are atomic (§8.1).  A multi-statement
                # program commits each statement as it goes, so only the
                # single-statement case carries the whole-record guarantee.
                assert json.dumps(record, sort_keys=False) == before, (
                    "a failed single statement left the record modified"
                    + _ctx(expression, f"before={before}\n  after={json.dumps(record)}")
                )
        except Exception as exc:  # noqa: BLE001
            pytest.fail(
                f"unstructured {type(exc).__name__}: {exc}" + _ctx(expression)
            )

    assert observed_failures >= 3_000, (
        f"generator floor: only {observed_failures} runtime failures observed; "
        f"the invariance property is nearly vacuous (seed={SEED})"
    )
    assert observed_successes >= 3_000, (
        f"generator floor: only {observed_successes} programs succeeded "
        f"(seed={SEED})"
    )


def test_a_collision_on_the_last_source_rolls_back_the_whole_move():
    """§8.1 concretely: a three-source move that collides writes nothing.

    A move-into resolves and validates every source before it writes any of
    them, so a collision found on the third source must leave the first two
    where they were.
    """
    record = {
        "alpha": 1,
        "beta": 2,
        "gamma": 3,
        "metadata": {"gamma": "already here"},
    }
    before = copy.deepcopy(record)

    with pytest.raises(StructuralExecutionError) as caught:
        _run(".metadata << .alpha, .beta, .gamma", record)

    assert record == before, (
        f"a collision on the third source left a partial move: {record!r}"
    )
    error = caught.value
    assert error.operation == "move-into", error.operation
    assert "already exists" in (error.reason or ""), error.reason
    # The first two sources are still at the root, untouched.
    assert record["alpha"] == 1 and record["beta"] == 2, record
