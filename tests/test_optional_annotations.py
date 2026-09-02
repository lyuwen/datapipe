"""Optional/Union parameter annotations must validate on every supported Python.

Two defects met here, and CI caught them only on Python 3.10:

1. ``_validate_argument_type`` rejected any *live* union annotation with
   "annotation Optional which is not supported for compile-time validation" --
   while the same message advertised "Optional/Union of those" as supported.
   Provider contracts arrive decoded from the registry (already a
   ``UnionAnnotation``), but a built-in's annotation comes straight off the
   live signature, so no branch recognised it.  Any tool author writing
   ``mode: str | None = None`` hit this on *every* version.

2. ``nest``/``unnest`` were annotated ``include: list = None`` -- a ``list``
   annotation with a ``None`` default.  Python 3.10's ``get_type_hints`` still
   applied PEP 484's implicit-Optional rule and rewrote that to
   ``Optional[list]``; 3.11 dropped the rule.  So identical source validated on
   3.11+ and failed on 3.10 with 40 test failures.

The tests below pin the mechanism rather than the symptom, so they exercise the
3.10 path on any interpreter: an explicitly-Optional ``ParameterSpec`` is what
3.10 would have handed the validator.
"""

from __future__ import annotations

import dataclasses
import sys
import typing

import pytest

from datapipe.dsl.compiler import (
    UnionAnnotation,
    _normalize_live_union,
    _validate_argument_type,
    compile_program,
)
from datapipe.dsl.errors import Span, ToolConfigurationError
from datapipe.tools.builtins.structural import nest, unnest
from datapipe.tools.decorator import get_contract


# ---------------------------------------------------------------------------
# 1. Live union annotations normalize, whichever spelling the author used
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "annotation,expected_members",
    [
        (typing.Optional[list], (list, type(None))),
        (typing.Union[list, None], (list, type(None))),
        (list | None, (list, type(None))),
        (typing.Union[str, int], (str, int)),
        (typing.Optional[str], (str, type(None))),
    ],
)
def test_live_unions_become_union_annotation(annotation, expected_members):
    normalized = _normalize_live_union(annotation)
    assert isinstance(normalized, UnionAnnotation)
    assert normalized.members == expected_members


@pytest.mark.parametrize("annotation", [list, str, int, bool, dict, typing.Any])
def test_non_unions_pass_through_untouched(annotation):
    """Normalization must not disturb the annotations that already worked."""
    assert _normalize_live_union(annotation) is annotation


# ---------------------------------------------------------------------------
# 2. The 3.10 path: an Optional-annotated parameter validates correctly
# ---------------------------------------------------------------------------

def _as_python310_would(tool, param_name: str):
    """Return *tool*'s parameter spec as 3.10's implicit-Optional rule yields it."""
    contract = get_contract(tool)
    spec = next(p for p in contract.parameters if p.name == param_name)
    return dataclasses.replace(spec, annotation=typing.Optional[list])


@pytest.mark.parametrize("tool,param", [(nest, "exclude"), (nest, "include"),
                                        (unnest, "exclude"), (unnest, "include")])
def test_optional_annotated_parameter_accepts_a_list(tool, param):
    """This raised ToolConfigurationError on 3.10, failing 40 tests."""
    spec = _as_python310_would(tool, param)
    _validate_argument_type(["id"], spec, tool.__name__, "expr", Span(0, 1))


@pytest.mark.parametrize("tool,param", [(nest, "exclude"), (unnest, "include")])
def test_optional_annotated_parameter_accepts_none(tool, param):
    spec = _as_python310_would(tool, param)
    _validate_argument_type(None, spec, tool.__name__, "expr", Span(0, 1))


@pytest.mark.parametrize("bad", [5, "text", 1.5, True, {"k": 1}])
def test_optional_annotation_still_rejects_the_wrong_type(bad):
    """Normalizing must not weaken validation into accepting anything."""
    spec = _as_python310_would(nest, "exclude")
    with pytest.raises(ToolConfigurationError) as excinfo:
        _validate_argument_type(bad, spec, "nest", "expr", Span(0, 1))
    assert "expected list | None" in str(excinfo.value)


# ---------------------------------------------------------------------------
# 3. The expressions CI actually failed on
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "expression",
    [
        'nest(., key="m", exclude=["instance_id"], jsonify=true)',
        'nest(., key="m", include=["a"])',
        'nest(., key="metadata", exclude=["instance_id", "messages", "tools"], jsonify=true)',
        'unnest(., key="m", include=["a"], parse=true)',
        'unnest(., key="m", exclude=["y"], parse=true, jsonify=true)',
        'fromjson(.tools); nest(., key="metadata", exclude=["instance_id", "tools"])',
    ],
)
def test_the_expressions_that_failed_on_python_310_compile(expression):
    compile_program(expression)


# ---------------------------------------------------------------------------
# 4. The source annotations are honest, so no version has to infer them
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("tool", [nest, unnest])
@pytest.mark.parametrize("param", ["include", "exclude"])
def test_none_defaulted_parameters_are_annotated_optional(tool, param):
    """A `list` annotation with a `None` default is what diverged across versions.

    Declaring the union explicitly means `get_type_hints` has nothing to infer,
    so 3.10 and 3.11+ resolve the signature identically.
    """
    hints = typing.get_type_hints(tool)
    normalized = _normalize_live_union(hints[param])

    assert isinstance(normalized, UnionAnnotation), (
        f"{tool.__name__}.{param} is annotated {hints[param]!r}; a None default "
        "needs an explicit `| None` or 3.10 and 3.11+ disagree"
    )
    assert type(None) in normalized.members


def test_python_version_is_actually_supported():
    """Guards the premise: the matrix and requires-python both include 3.10."""
    assert sys.version_info >= (3, 10)
