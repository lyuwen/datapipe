"""Compile-time validation of `nest`/`unnest` configuration must match runtime.

These pin three defects introduced by the first attempt at moving structural-tool
validation to compile time.  All three shipped green: the suite passed, CI passed,
and the behavior was still wrong.

1. **Duplicates escaped.** The hand-written compiler check tested element *types*
   but not uniqueness, while the runtime rejected duplicates.  So
   `nest(., include=["a","a"])` compiled, then failed once per record -- and under
   `--errors skip` that silently dropped every record and exited 0.  Exactly the
   data-loss shape the compile-time move was meant to eliminate.

2. **Empty lists diverged.** The compiler treated any two non-None selections as
   mutually exclusive, but `_selection` documents an empty list as "not supplied".
   `nest(include=[], exclude=["id"])` therefore worked as a direct call and was
   rejected through the DSL -- two definitions of valid configuration disagreeing.

3. **Validation keyed on a bare name.** `contract.name` carries no namespace, so
   a provider tool legitimately named `nest` inherited the built-in's rules and
   died with a raw `TypeError: 'int' object is not iterable`.

The fix deletes the second copy of the rules: the compiler calls
`structural.validate_configuration`, which is the same `_selection`/`_check_policy`
pair the tool bodies use, and keys on function *identity* rather than name.  The
agreement tests below are the real guard -- they compare the two paths directly, so
any future divergence fails rather than being noticed by a reviewer.
"""

from __future__ import annotations

import pytest

from datapipe.dsl.compiler import compile_program
from datapipe.dsl.errors import ToolConfigurationError
from datapipe.tools.builtins.structural import nest, unnest


@pytest.fixture(autouse=True)
def isolated_registry(tmp_path, monkeypatch):
    """Redirect the tool registry so provider installs cannot touch the real one."""
    monkeypatch.setenv("DATAPIPE_USER_DATA", str(tmp_path / "dp_data"))
    from datapipe.tools import loader as _loader
    _loader._loaded_providers.clear()
    from datapipe.dsl import compiler as _compiler
    monkeypatch.setattr(_compiler, "_BUILTIN_REGISTRY", None, raising=False)


# ---------------------------------------------------------------------------
# 1. Duplicates are rejected before the source opens
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "expression",
    [
        'nest(., include=["a", "a"])',
        'nest(., exclude=["x", "x"])',
        'unnest(., key="m", include=["a", "a"])',
        'unnest(., key="m", exclude=["x", "x"])',
        'nest(., include=["a", "b", "a"])',
    ],
)
def test_duplicate_field_names_are_rejected_at_compile_time(expression):
    """Compiled successfully before the fix, then dropped every record."""
    with pytest.raises(ToolConfigurationError) as excinfo:
        compile_program(expression)
    assert "duplicate field name" in str(excinfo.value)


def test_duplicates_do_not_reach_the_data_path(tmp_path):
    """The whole point: an invalid config must not silently empty the output.

    Under `--errors skip` the pre-fix behavior was exit 0 with zero records
    written, which is indistinguishable from a successful run over empty input.
    """
    import json
    import subprocess
    import sys

    src = tmp_path / "in.jsonl"
    out = tmp_path / "out.jsonl"
    src.write_text("\n".join(json.dumps({"a": i}) for i in range(3)) + "\n")

    proc = subprocess.run(
        [
            sys.executable, "-c",
            "import sys; from datapipe.cli.main import main; sys.exit(main())",
            "transform", 'nest(., include=["a","a"])',
            str(src), str(out), "--errors", "skip", "--no-progress",
        ],
        capture_output=True, text=True,
    )

    assert proc.returncode != 0, (
        "invalid configuration exited 0; a silent empty output is the data-loss "
        f"failure this guards.\nstderr:\n{proc.stderr}"
    )
    assert "duplicate field name" in proc.stderr


# ---------------------------------------------------------------------------
# 2. An empty list means "not supplied", identically on both paths
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "expression",
    [
        'nest(., include=[], exclude=["id"])',
        'nest(., include=["a"], exclude=[])',
        'nest(., include=[], exclude=[])',
        'unnest(., key="m", include=[], exclude=["x"])',
    ],
)
def test_empty_list_is_not_a_supplied_selection(expression):
    """`_selection` documents empty as absent; the DSL must agree."""
    compile_program(expression)


def test_two_real_selections_are_still_mutually_exclusive():
    with pytest.raises(ToolConfigurationError) as excinfo:
        compile_program('nest(., include=["a"], exclude=["b"])')
    assert "mutually exclusive" in str(excinfo.value)


# ---------------------------------------------------------------------------
# 3. Compile-time and runtime agree -- the guard against a second divergence
# ---------------------------------------------------------------------------

_AGREEMENT_CASES = [
    ({"include": [], "exclude": ["id"]}, 'nest(., key="m", include=[], exclude=["id"])'),
    ({"include": [], "exclude": []}, 'nest(., key="m", include=[], exclude=[])'),
    ({"include": ["a"]}, 'nest(., key="m", include=["a"])'),
    ({"exclude": ["id"]}, 'nest(., key="m", exclude=["id"])'),
    ({"include": ["a", "a"]}, 'nest(., key="m", include=["a","a"])'),
    ({"include": ["a"], "exclude": ["b"]}, 'nest(., key="m", include=["a"], exclude=["b"])'),
    ({"include": [1]}, 'nest(., key="m", include=[1])'),
    ({"collision": "replace"}, 'nest(., key="m", collision="replace")'),
    ({"missing": "skip"}, 'nest(., key="m", missing="skip")'),
]


@pytest.mark.parametrize("kwargs,expression", _AGREEMENT_CASES)
def test_direct_call_and_dsl_agree_on_validity(kwargs, expression):
    """Whatever the tool accepts, the compiler accepts -- and vice versa.

    This is the test that would have caught the empty-list divergence, which
    every other test missed because each path was only ever checked alone.
    """
    try:
        nest({"id": 1, "a": 2}, **kwargs)
        direct_ok = True
    except (ValueError, TypeError):
        direct_ok = False

    try:
        compile_program(expression)
        dsl_ok = True
    except ToolConfigurationError:
        dsl_ok = False

    assert direct_ok == dsl_ok, (
        f"direct call {'accepted' if direct_ok else 'rejected'} but the DSL "
        f"{'accepted' if dsl_ok else 'rejected'} the same configuration: {expression}"
    )


# ---------------------------------------------------------------------------
# 4. Validation is keyed on identity, not on the tool's bare name
# ---------------------------------------------------------------------------

_PROVIDER_SRC = '''
from datapipe.tools.decorator import tool
from datapipe.tools.types import JsonType

@tool(name="nest", api_version=1, target="value",
      input=JsonType.ANY, output=JsonType.ANY,
      cardinality="one_to_one", deterministic=True,
      description="A provider tool that happens to be named nest")
def nest(value, *, include: int = 0):
    return value
'''


def test_a_provider_tool_named_nest_does_not_inherit_builtin_rules(tmp_path):
    """`contract.name` has no namespace, so name-keyed validation leaked.

    Before the fix this raised `TypeError: 'int' object is not iterable` from
    inside the built-in's selection logic, for a tool that has nothing to do
    with it.
    """
    import subprocess
    import sys

    provider = tmp_path / "vendor_nest.py"
    provider.write_text(_PROVIDER_SRC)

    install = subprocess.run(
        [
            sys.executable, "-c",
            "import sys; from datapipe.cli.main import main; sys.exit(main())",
            "tools", "install", str(provider), "--yes",
        ],
        capture_output=True, text=True,
    )
    if install.returncode != 0:
        pytest.skip(f"provider install unavailable here: {install.stderr[:200]}")

    check = subprocess.run(
        [
            sys.executable, "-c",
            "import sys; from datapipe.dsl.compiler import compile_program;"
            "compile_program('vendor_nest.nest(.a, include=1)');"
            "print('COMPILED')",
        ],
        capture_output=True, text=True,
    )

    assert "TypeError" not in check.stderr, (
        "the built-in's rules leaked onto a same-named provider tool:\n"
        + check.stderr
    )
    assert check.returncode == 0, check.stderr


def test_the_builtins_are_still_validated_by_identity():
    """The identity check must not be so narrow that it validates nothing."""
    for tool_fn, name in ((nest, "nest"), (unnest, "unnest")):
        with pytest.raises(ToolConfigurationError):
            compile_program(f'{name}(., include=["a","a"])')
