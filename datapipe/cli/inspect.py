"""``datapipe inspect``: display the stage structure of a Pipeline definition.

Example output::

    Pipeline 'normalize_and_score'
      0  json_load    JsonLoadStage
      1  normalize    GenericStage   process=mypkg.steps.normalize
      2  enrich       GenericStage   setup=mypkg.steps.load_resources  process=mypkg.steps.enrich
      3  is_valid     FilterStage    predicate=mypkg.steps.is_valid
      4  json_dump    JsonDumpStage
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from datapipe.cli.loaders import PipelineLoadError, load_pipeline_ref

if TYPE_CHECKING:
    import argparse


# ---------------------------------------------------------------------------
# Argument-parser fragment (registered by main.py)
# ---------------------------------------------------------------------------


def add_inspect_parser(subparsers) -> None:
    """Attach the ``inspect`` sub-command to *subparsers*."""
    p = subparsers.add_parser(
        "inspect",
        help="inspect a pipeline definition",
        description=(
            "Load a Python-defined Pipeline and display its stage structure "
            "without executing any data."
        ),
    )
    p.add_argument(
        "pipeline_ref",
        help="'module.submodule:object' or './file.py:object'",
    )
    p.add_argument(
        "--json", dest="as_json", action="store_true",
        help="emit inspection output as JSON instead of human-readable text",
    )


# ---------------------------------------------------------------------------
# Command implementation
# ---------------------------------------------------------------------------


def inspect_command(args: "argparse.Namespace") -> int:
    """Execute the ``inspect`` sub-command.  Returns an exit code."""
    try:
        pipeline = load_pipeline_ref(args.pipeline_ref)
    except PipelineLoadError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    from datapipe.pipeline import Pipeline

    if not isinstance(pipeline, Pipeline):
        print(
            f"error: {args.pipeline_ref!r} resolved to "
            f"{type(pipeline).__name__}, expected a Pipeline instance",
            file=sys.stderr,
        )
        return 1

    if getattr(args, "as_json", False):
        _print_json(pipeline)
    else:
        _print_human(pipeline)
    return 0


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def _stage_detail(stage) -> dict[str, str]:
    """Extract human-readable key/value pairs from a stage instance."""
    from datapipe.stage import GenericStage, FilterStage, TransformStage, TapStage

    attrs: dict[str, str] = {}

    if isinstance(stage, GenericStage):
        for attr in ("setup_fn", "input_fn", "process_fn", "output_fn", "teardown_fn"):
            fn = getattr(stage, attr, None)
            if fn is not None:
                key = attr.removesuffix("_fn")
                attrs[key] = _callable_name(fn)
    elif isinstance(stage, FilterStage):
        attrs["predicate"] = _callable_name(stage.predicate)
    elif isinstance(stage, TransformStage):
        attrs["fn"] = _callable_name(stage.fn)
    elif isinstance(stage, TapStage):
        attrs["fn"] = _callable_name(stage.fn)

    return attrs


def _callable_name(fn) -> str:
    """Return a human-readable qualified name for a callable."""
    module = getattr(fn, "__module__", None) or ""
    qualname = getattr(fn, "__qualname__", None) or getattr(fn, "__name__", None) or repr(fn)
    if module and module not in ("builtins", "__main__"):
        return f"{module}.{qualname}"
    return qualname


def _print_human(pipeline) -> None:
    """Render the pipeline in human-readable text form."""
    name = getattr(pipeline, "name", "pipeline")
    stages = list(pipeline)
    print(f"Pipeline {name!r}")
    if not stages:
        print("  (no stages)")
        return

    idx_w = len(str(len(stages) - 1))
    name_w = max(len(s.name) for s in stages)
    type_w = max(len(type(s).__name__) for s in stages)

    for i, stage in enumerate(stages):
        detail = _stage_detail(stage)
        detail_str = "  ".join(f"{k}={v}" for k, v in detail.items())
        parts = [
            f"  {i:{idx_w}d}",
            f"  {stage.name:{name_w}}",
            f"  {type(stage).__name__:{type_w}}",
        ]
        if detail_str:
            parts.append(f"  {detail_str}")
        print("".join(parts))


def _print_json(pipeline) -> None:
    """Render the pipeline as a JSON document."""
    import json

    name = getattr(pipeline, "name", "pipeline")
    stages = []
    for i, stage in enumerate(pipeline):
        entry: dict = {
            "index": i,
            "name": stage.name,
            "type": type(stage).__name__,
            "detail": _stage_detail(stage),
        }
        stages.append(entry)

    doc = {"pipeline": name, "stages": stages}
    print(json.dumps(doc, indent=2))
