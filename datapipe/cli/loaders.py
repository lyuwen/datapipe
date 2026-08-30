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
    """Import a module from a .py file path.

    The module is registered in ``sys.modules`` under its synthetic name so
    that ``pickle`` (used by ``ProcessExecutor`` under the ``spawn`` start
    method) can resolve functions and classes defined in the file.  Without
    this registration, pickling any callable from the file raises
    ``AttributeError: Can't pickle … import of module '…' failed``.
    """
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
    # Register before exec so that any module-level self-imports resolve, and
    # so that pickle can find the module by name in spawned worker processes.
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)  # type: ignore[union-attr]
    except Exception as exc:
        # Unregister on failure so a retry or a different file doesn't see
        # a half-initialised module under the same name.
        sys.modules.pop(module_name, None)
        raise PipelineLoadError(
            f"error executing {path!r}: {exc}"
        ) from exc
    return module


def _module_name_from_path(path: str) -> str:
    """Return the module name under which this file should be registered.

    Requirements:

    1. Spawned worker processes must be able to re-import it.  Under the
       ``spawn`` start method workers start fresh; pickle resolves the module
       by calling ``import <name>``, so the name must be importable via
       ``sys.path``.  ``_import_from_file`` adds the file's directory to
       ``sys.path``, so the bare stem is always importable from there.

    2. The name must not clash with an existing *package* (e.g. a stem of
       ``datapipe`` would shadow the whole library).  We detect that case and
       raise a clear error.  For modules (single-file entries), re-registration
       under the same stem is intentional — the caller always wants the newly
       loaded file.

    We deliberately do NOT use a synthetic dotted namespace such as
    ``_datapipe_loader.X`` because dotted names require the parent package to
    also exist in ``sys.modules`` and to be importable by workers, which a
    synthetic prefix cannot satisfy under ``spawn``.
    """
    stem = os.path.splitext(os.path.basename(path))[0]

    # Guard against shadowing a real *package* (has __path__ but no __file__),
    # such as accidentally naming a pipeline file ``datapipe.py``.
    existing = sys.modules.get(stem)
    if existing is not None and hasattr(existing, "__path__") and not getattr(existing, "__file__", None):
        raise PipelineLoadError(
            f"cannot load {path!r} as module {stem!r}: that name is already "
            f"used by a package. Rename your file to avoid the collision."
        )
    return stem


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
