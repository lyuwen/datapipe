"""Built-in structural tools: ``nest`` and ``unnest`` (§6.7, §6.8).

These are named, programmatically-configurable alternatives to the symbolic
``<<`` form.  Their defining property (§15.4) is **equivalence**: they must
produce byte-identical records to the symbolic statements they sugar.

The way that property is kept is by construction rather than by testing.
Neither tool reimplements the move-into semantics.  Each one desugars its
arguments into the very same compiled IR the symbolic form produces — an
``_ast.MoveInto`` run through ``_compile_move_into`` and executed by
``CompiledProgramStage`` — so §8.1 atomicity, §8.4 collisions, §8.5 strict
positive sets, §8.6 harmless exclusion misses, §8.7 destination
self-exclusion, §6.4 source ordering, and the ``_detached()`` aliasing rule
are all inherited, not restated.

Where equivalence by construction stops
---------------------------------------
Two degenerate configurations have no symbolic counterpart to be equivalent
*to*, because the grammar has no empty field set — ``.m << .()`` is a syntax
error.  ``nest(include=[])`` and ``unnest(include=[])`` therefore fall back to
the "not supplied" reading (see ``_selection``) and behave as the complement of
nothing: nest everything, extract everything.  For those two inputs the
desugared source string this module reports is the program it actually ran,
but no user could have typed it.

``nest`` (§6.7)
---------------
``nest(., key="metadata", exclude=["a","b"], jsonify=true)`` desugars to
``.metadata << .(^a|b) | tojson``.  ``include`` produces a positive field set
instead; supplying neither nests every other field.

``unnest`` (§6.8)
-----------------
``unnest(., key="metadata", include=["x"], parse=true, jsonify=true)``
desugars to ``fromjson(.metadata); . << .metadata.(x); tojson(.metadata)``.
``exclude`` produces the complement form ``. << .metadata.(^x)`` directly; it
used to expand the complement against the live record because S4 rejected a
complement under a root destination, but S6 narrowed that check to the one
case it can actually prove (base == destination), so the sugar and the
symbolic form are now the same program.

Atomicity across a multi-statement desugaring
---------------------------------------------
``nest`` desugars to a *single* move-into statement (``jsonify`` rides along as
that statement's trailing pipe), so S4's deferred-write ``place`` closure
already makes it all-or-nothing: nothing is written until every precondition
and the pipe have succeeded.  It needs no copy of its own.

``unnest`` cannot always be one statement.  Its move destination is the root,
so ``jsonify`` has to re-serialize ``.key`` as a separate statement rather than
as a trailing pipe — a trailing pipe there would serialize the whole record.
``parse`` is likewise a statement that runs first.  Either one makes the
desugaring multi-statement, and a multi-statement program is not atomic on its
own: ``fromjson`` would commit its decode before a later collision was found,
leaving a record whose metadata had been silently decoded.  So ``unnest`` runs
that case against a detached working copy and returns it only once every
statement has succeeded.  A single-statement ``unnest`` skips the copy, because
S4 already gives it the same guarantee ``nest`` gets.

Policy parameters
-----------------
``collision`` and ``missing`` accept only ``"error"``.  §8.4 specifies
``collision="error"`` for move operations and names ``"replace"`` only as
future syntax on a hypothetical ``move`` tool; implementing it here would mean
forking ``_check_move_entries`` into a second, divergent copy of the collision
rules — exactly the drift these tools exist to avoid.  The parameters are
accepted and validated so expressions that state the default explicitly work
and so an unsupported policy fails loudly instead of being ignored.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any

from datapipe.tools.decorator import tool
from datapipe.tools.contract import ToolExample
from datapipe.tools.types import JsonType

#: The only collision policy `<<` implements (§8.4).
_COLLISION_POLICIES = ("error",)

#: The only missing-field policy positive field sets implement (§8.5).
_MISSING_POLICIES = ("error",)


@tool(
    name="nest",
    api_version=1,
    target="record",
    input=JsonType.OBJECT,
    output=JsonType.OBJECT,
    cardinality="one_to_one",
    deterministic=True,
    description=(
        "Move record fields into a nested object. "
        "Equivalent to the symbolic `.<key> << .(...)` form: `include` names "
        "the fields to nest, `exclude` nests every other field, and "
        "jsonify=true serializes the destination afterwards."
    ),
    examples=[
        ToolExample(
            input={"id": "abc", "temperature": 0.7},
            output={"id": "abc", "metadata": {"temperature": 0.7}},
            arguments={"key": "metadata", "exclude": ["id"]},
            description="blanket nesting of everything but the excluded fields",
        ),
        ToolExample(
            input={"a": 1, "b": 2},
            output={"a": 1, "m": '{"b":2}'},
            arguments={"key": "m", "include": ["b"], "jsonify": True},
            description="nest named fields and serialize the destination",
        ),
    ],
)
def nest(
    record: dict,
    *,
    key: str = "metadata",
    include: list | None = None,
    exclude: list | None = None,
    jsonify: bool = False,
    collision: str = "error",
    missing: str = "error",
) -> dict:
    """Move fields of *record* into the nested object at *key*.

    Parameters
    ----------
    key:
        Destination field.  Created as ``{}`` when absent (§8.2); an existing
        destination must already be an object (§8.3).
    include:
        Field names to nest.  Strict: a named field that the record does not
        have is an error (§8.5).  Mutually exclusive with ``exclude``.
    exclude:
        Field names to leave at the root; every other field is nested.  Names
        the record does not have are harmless (§8.6), and *key* itself is
        always left out of the set so the destination is never nested inside
        itself (§8.7).  Mutually exclusive with ``include``.
    jsonify:
        Serialize the destination with the built-in ``tojson`` once the move
        is complete.
    collision:
        Only ``"error"``: a derived key that already exists at the destination
        fails the whole call (§8.4).
    missing:
        Only ``"error"``: see ``include``.
    """
    names, complement = _selection(include, exclude, "nest")
    _check_policy("collision", collision, _COLLISION_POLICIES, "nest")
    _check_policy("missing", missing, _MISSING_POLICIES, "nest")

    # One statement, so S4's deferred write is already all-or-nothing (§8.1).
    return _run(_nest_program(key, names, complement, jsonify), record)


@tool(
    name="unnest",
    api_version=1,
    target="record",
    input=JsonType.OBJECT,
    output=JsonType.OBJECT,
    cardinality="one_to_one",
    deterministic=True,
    description=(
        "Move fields out of a nested object up to the record root. "
        "Equivalent to `fromjson(.<key>); . << .<key>.(...); tojson(.<key>)`: "
        "parse=true decodes a JSON-encoded source first and jsonify=true "
        "re-encodes whatever remains in it."
    ),
    examples=[
        ToolExample(
            input={"id": "abc", "metadata": '{"temperature":0.7,"note":"n"}'},
            output={"id": "abc", "temperature": 0.7, "metadata": '{"note":"n"}'},
            arguments={
                "key": "metadata",
                "include": ["temperature"],
                "parse": True,
                "jsonify": True,
            },
            description="extract a field from serialized metadata",
        ),
        ToolExample(
            input={"m": {"a": 1, "b": 2}},
            output={"a": 1, "m": {"b": 2}},
            arguments={"key": "m", "include": ["a"]},
            description="extract from an already-decoded object",
        ),
    ],
)
def unnest(
    record: dict,
    *,
    key: str = "metadata",
    include: list | None = None,
    exclude: list | None = None,
    parse: bool = False,
    jsonify: bool = False,
    collision: str = "error",
    missing: str = "error",
) -> dict:
    """Move fields out of the nested object at *key* up to the record root.

    Parameters
    ----------
    key:
        Source field.  It must exist, and — once ``parse`` has run — hold an
        object.
    include:
        Field names to extract.  Strict (§8.5).  Mutually exclusive with
        ``exclude``.
    exclude:
        Field names to leave in the source; every other field is extracted.
        Mutually exclusive with ``include``.
    parse:
        Decode the source with the built-in ``fromjson`` before extracting.
    jsonify:
        Re-encode whatever remains in the source with ``tojson`` afterwards.
    collision:
        Only ``"error"``: an extracted key that already exists at the root
        fails the whole call (§8.4).
    missing:
        Only ``"error"``: see ``include``.
    """
    names, complement = _selection(include, exclude, "unnest")
    _check_policy("collision", collision, _COLLISION_POLICIES, "unnest")
    _check_policy("missing", missing, _MISSING_POLICIES, "unnest")

    # `parse` and `jsonify` each add a statement around the move, and a
    # multi-statement program commits each statement as it goes.  Working on a
    # copy is what keeps the whole call atomic (§8.1) in that case; a bare move
    # is a single statement and already atomic, so it skips the copy.
    working = record if not (parse or jsonify) else _detached(record)
    if parse:
        working = _run(_parse_program(key), working)

    return _run(_unnest_program(key, names, complement, jsonify), working)


# ---------------------------------------------------------------------------
# Argument validation
# ---------------------------------------------------------------------------


def _selection(
    include: "list | None", exclude: "list | None", tool_name: str
) -> "tuple[tuple[str, ...], bool]":
    """Normalize ``include``/``exclude`` into ``(names, complement)``.

    ``None`` is the real default — the plan's sketch writes ``list = []``, but
    a shared mutable default is a footgun, and ``None`` also distinguishes
    "not supplied" from an explicitly empty list.  An empty list is treated as
    not supplied on either side, so ``exclude=[]`` still means "nest everything".
    """
    if include and exclude:
        raise ValueError(
            f"{tool_name}: 'include' and 'exclude' are mutually exclusive; "
            f"got include={list(include)!r} and exclude={list(exclude)!r}"
        )
    if include:
        return _field_names(include, "include", tool_name), False
    return _field_names(exclude or (), "exclude", tool_name), True


def _field_names(
    values: "list | tuple", param: str, tool_name: str
) -> "tuple[str, ...]":
    """Validate that *values* is a sequence of unique field-name strings."""
    if not isinstance(values, (list, tuple)):
        raise ValueError(
            f"{tool_name}: {param!r} must be a list of field names, got "
            f"{type(values).__name__}"
        )
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, str):
            raise ValueError(
                f"{tool_name}: {param!r} must contain field name strings, got "
                f"{type(value).__name__} ({value!r})"
            )
        if value in seen:
            raise ValueError(
                f"{tool_name}: duplicate field name {value!r} in {param!r}"
            )
        seen.add(value)
    return tuple(values)


def _check_policy(
    param: str, value: str, allowed: "tuple[str, ...]", tool_name: str
) -> None:
    """Reject a policy value this implementation does not honor."""
    if value in allowed:
        return
    raise ValueError(
        f"{tool_name}: {value!r} is not a supported {param!r} policy; "
        f"expected {' or '.join(repr(a) for a in allowed)}"
    )


# ---------------------------------------------------------------------------
# Desugaring: build the same compiled IR the symbolic form produces
# ---------------------------------------------------------------------------


def _detached(value: Any) -> Any:
    """Copy *value* so nothing written shares structure with live record data."""
    from datapipe.stages.tool_program import _detached as _s3_detached

    return _s3_detached(value)


def _lexes_as_field(key: str) -> bool:
    """Whether ``.key`` would lex as a bare ``Field`` part.

    Mirrors the identifier rule in ``datapipe/dsl/lexer.py`` — leading
    ``isalpha()`` or ``_``, then ``isalnum()`` or ``_`` — rather than using
    ``str.isidentifier()``, which accepts a slightly different unicode set.
    """
    if not key or not (key[0].isalpha() or key[0] == "_"):
        return False
    return all(c.isalnum() or c == "_" for c in key[1:])


def _key_selector(*keys: str) -> Any:
    """Build a selector AST for *keys*.

    A key that would lex as a bare field becomes a ``Field`` part so
    diagnostics render ``.m`` exactly as the symbolic form does; anything else
    falls back to a quoted key, which is the only form that can carry an
    arbitrary string.  Both canonicalize to the same path, so the compiled
    selector is identical either way — the choice only affects display.
    """
    from datapipe.dsl import ast as _ast
    from datapipe.dsl.errors import Span

    span = Span(0, 0)
    parts = tuple(
        _ast.Field(name=k, span=span)
        if _lexes_as_field(k)
        else _ast.QuotedKey(key=k, span=span)
        for k in keys
    )
    return _ast.Selector(parts=parts, span=span)


def _move_into_statement(
    destination: Any,
    base: Any,
    names: "tuple[str, ...]",
    complement: bool,
    pipes: tuple,
) -> Any:
    """Compile ``destination << base.(names)`` into a ``CompiledStatement``."""
    from datapipe.dsl import ast as _ast
    from datapipe.dsl.compiler import CompiledStatement, _compile_move_into
    from datapipe.dsl.errors import Span

    span = Span(0, 0)
    node = _ast.MoveInto(
        destination=destination,
        sources=(
            _ast.FieldSet(
                base=base, names=names, complement=complement, span=span
            ),
        ),
        span=span,
    )
    operation, focus = _compile_move_into(node, "")
    return CompiledStatement(
        operation=operation, pipes=pipes, focus_selector=focus, span=(0, 0)
    )


def _tool_statement(fn: Any, selector: Any, index: int) -> Any:
    """Compile a whole-statement built-in call such as ``tojson(.metadata)``."""
    from datapipe.dsl.compiler import CompiledStatement, ToolInvocation
    from datapipe.dsl.selector import CompiledSelector
    from datapipe.tools.decorator import get_contract

    contract = get_contract(fn)
    invocation = ToolInvocation(
        tool_descriptor=None,
        builtin_fn=fn,
        tool_name=contract.name,
        contract=contract,
        selector=CompiledSelector(selector),
        arguments={},
        expression_index=index,
        expression_span=(0, 0),
    )
    return CompiledStatement(
        operation=invocation, pipes=(), focus_selector=None, span=(0, 0)
    )


def _bare_pipe(fn: Any, index: int) -> Any:
    """Build the trailing ``| tojson`` of a move-into statement."""
    from datapipe.dsl.compiler import CompiledBareCall

    return CompiledBareCall(
        expression_index=index,
        callable=fn,
        descriptor=None,
        bound_args={},
        span=(0, 0),
    )


def _stage(statements: tuple, source: str) -> Any:
    """Wrap compiled statements in the executing stage.

    Built at ``validate="always"``; the effective decision is applied per call
    by ``_run`` so one cached stage serves every mode.
    """
    from datapipe.dsl.compiler import CompiledProgram
    from datapipe.stages.tool_program import CompiledProgramStage

    return CompiledProgramStage(
        CompiledProgram(statements=statements, source=source), name=source
    )


def _run(stage: Any, record: Any) -> Any:
    """Execute *stage* under the enclosing stage's decision for this record.

    A ``target="record"`` tool is handed only its record, so the outer
    ``--validate`` mode reaches these desugared inner programs through the
    ``_ACTIVE_VALIDATE`` ContextVar rather than through the signature.

    What the ContextVar carries is already resolved to ``"always"`` or
    ``"off"`` — the outer stage decides per record and publishes the outcome,
    so a ``sample`` run turns the inner program off once the outer window
    closes.  The inner stage must not do its own counting: it is
    ``lru_cache``d and shared, so per-record mutable state on it would leak
    across records, and the cheap per-call view ``with_validate`` returns
    starts a fresh counter that would never reach ``SAMPLE_LIMIT``.  Because
    the inner program runs exactly once per outer record, inheriting the
    outer decision reproduces the outer sampling window precisely.

    The enclosing ``WorkerContext`` travels the same way, so a failure inside
    the desugared program attributes itself to the same record number the
    equivalent symbolic statement would report instead of ``record ?``.
    """
    from datapipe.stages.tool_program import (
        active_validate_mode,
        active_worker_context,
    )

    mode = active_validate_mode()
    if mode != stage.validate:
        stage = stage.with_validate(mode)
    return stage.process(record, active_worker_context())


# The desugared program depends only on the configuration, never on the record,
# so it is built once per distinct configuration rather than once per record.
# The resulting stage carries no per-record state.


@lru_cache(maxsize=256)
def _nest_program(
    key: str, names: "tuple[str, ...]", complement: bool, jsonify: bool
) -> Any:
    """``.<key> << .(names)`` with an optional trailing ``| tojson``."""
    from datapipe.tools.builtins.json import tojson

    pipes = (_bare_pipe(tojson, 1),) if jsonify else ()
    statement = _move_into_statement(
        _key_selector(key), _key_selector(), names, complement, pipes
    )
    caret = "^" if complement else ""
    source = f".{key} << .({caret}{'|'.join(names)})" + (" | tojson" if jsonify else "")
    return _stage((statement,), source)


@lru_cache(maxsize=256)
def _parse_program(key: str) -> Any:
    """``fromjson(.<key>)`` — the ``parse=true`` step of ``unnest``."""
    from datapipe.tools.builtins.json import fromjson

    return _stage(
        (_tool_statement(fromjson, _key_selector(key), 0),), f"fromjson(.{key})"
    )


@lru_cache(maxsize=256)
def _unnest_program(
    key: str, names: "tuple[str, ...]", complement: bool, jsonify: bool
) -> Any:
    """``. << .<key>.(names)`` followed by an optional ``tojson(.<key>)``."""
    from datapipe.tools.builtins.json import tojson

    statements = [
        _move_into_statement(
            _key_selector(), _key_selector(key), names, complement, ()
        )
    ]
    caret = "^" if complement else ""
    source = f". << .{key}.({caret}{'|'.join(names)})"
    if jsonify:
        statements.append(_tool_statement(tojson, _key_selector(key), 1))
        source += f"; tojson(.{key})"
    return _stage(tuple(statements), source)
