"""Pipeline loader: resolve ``module:object`` and ``file.py:object`` references.

Both the ``run`` and ``inspect`` commands need to load an arbitrary
Python-defined ``Pipeline`` from a user-supplied reference string.  The two
supported forms are:

  module.submodule:object_name    standard importlib path
  ./relative/file.py:object_name  file-system path (any valid .py extension)
  /absolute/file.py:object_name   absolute path

The separator is always ``:``.  The object name must be a simple dotted
attribute path (no calls or indexing).
"""

from __future__ import annotations

import importlib
import importlib.util
import os
import sys
from typing import Any


class PipelineLoadError(Exception):
    """Raised when a pipeline reference cannot be resolved."""


def load_pipeline_ref(ref: str) -> Any:
    """Load an object from a ``module:attr`` or ``file.py:attr`` reference.

    Returns the resolved object, which should be a :class:`datapipe.Pipeline`
    instance when used with ``datapipe run`` or ``datapipe inspect``.

    Raises :class:`PipelineLoadError` with a human-readable message on any
    resolution failure.
    """
    if ":" not in ref:
        raise PipelineLoadError(
            f"invalid pipeline reference {ref!r}: expected 'module:object' "
            "or 'file.py:object'"
        )
    module_part, _, attr_part = ref.partition(":")
    if not module_part or not attr_part:
        raise PipelineLoadError(
            f"invalid pipeline reference {ref!r}: both module/file and "
            "object name must be non-empty"
        )

    if _looks_like_file(module_part):
        module = _import_from_file(module_part)
    else:
        module = _import_by_name(module_part)

    obj = _resolve_attr(module, attr_part)
    return obj


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _looks_like_file(part: str) -> bool:
    """True if the module part looks like a filesystem path."""
    return (
        part.endswith(".py")
        or os.sep in part
        or part.startswith("./")
        or part.startswith("../")
        or os.path.isabs(part)
    )


def _import_by_name(module_name: str):
    """Import a module by its dotted Python name."""
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        raise PipelineLoadError(
            f"cannot import module {module_name!r}: {exc}"
        ) from exc
    except Exception as exc:
        raise PipelineLoadError(
            f"error importing module {module_name!r}: {exc}"
        ) from exc


def _import_from_file(file_path: str):
    """Import a module from a .py file path."""
    path = os.path.abspath(file_path)
    if not os.path.isfile(path):
        raise PipelineLoadError(f"pipeline file not found: {path!r}")
    if not path.endswith(".py"):
        raise PipelineLoadError(
            f"pipeline file must be a .py file, got: {path!r}"
        )

    module_name = _module_name_from_path(path)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise PipelineLoadError(
            f"cannot create module spec for {path!r}"
        )

    # Make the file's directory importable so relative imports inside it work.
    directory = os.path.dirname(path)
    if directory not in sys.path:
        sys.path.insert(0, directory)

    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)  # type: ignore[union-attr]
    except Exception as exc:
        raise PipelineLoadError(
            f"error executing {path!r}: {exc}"
        ) from exc
    return module


def _module_name_from_path(path: str) -> str:
    """Derive a stable, unique module name from an absolute file path."""
    # Use the bare filename (without extension) as the module name.  Prefix
    # with a synthetic package to avoid clobbering any real top-level module.
    stem = os.path.splitext(os.path.basename(path))[0]
    return f"_datapipe_loader.{stem}"


def _resolve_attr(module, attr_path: str) -> Any:
    """Walk a dotted attribute path on *module* and return the final object."""
    parts = attr_path.split(".")
    obj = module
    for part in parts:
        if not part:
            raise PipelineLoadError(
                f"invalid attribute path {attr_path!r}: empty segment"
            )
        if not hasattr(obj, part):
            raise PipelineLoadError(
                f"object {type(obj).__name__!r} has no attribute {part!r} "
                f"(full path: {attr_path!r})"
            )
        obj = getattr(obj, part)
    return obj
