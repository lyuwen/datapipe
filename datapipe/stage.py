"""Stage model: per-record transformations.

A ``Stage`` is a single, worker-local transformation. Stages are composed
into a ``Pipeline`` and run sequentially inside one worker; the runtime never
inserts queues between stages.
"""

from __future__ import annotations

import json as _json
from typing import Any, Callable

from datapipe.context import WorkerContext
from datapipe.errors import PipelineValidationError
from datapipe.sentinels import DROP


class Stage:
    """Base class for all pipeline stages.

    Subclasses implement ``setup``/``process``/``teardown``. ``setup`` runs
    once per worker, ``process`` once per record, and ``teardown`` once per
    worker (best-effort under process executors).

    ``_name_explicit`` records whether ``name`` was user-provided; it is used
    by ``Pipeline`` to decide how strict duplicate-name validation should be.
    """

    name: str = "stage"
    _name_explicit: bool = False

    def setup(self, ctx: WorkerContext) -> None:
        """Initialize per-worker state once."""

    def process(self, value: Any, ctx: WorkerContext) -> Any:
        """Transform one record value."""
        raise NotImplementedError

    def teardown(self, ctx: WorkerContext) -> None:
        """Release per-worker state (best-effort)."""

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.name!r})"


def _call_with_ctx(
    fn: Callable,
    value: Any,
    ctx: WorkerContext,
    with_context: bool,
) -> Any:
    if with_context:
        return fn(value, ctx)
    return fn(value)


class GenericStage(Stage):
    """The primary user-facing stage.

    Wraps three optional callables into one stage:

    .. code-block:: python

        GenericStage(
            input=json.loads,
            process=normalize,
            output=json.dumps,
            name="normalize",
            setup=load_resources,
            teardown=release_resources,
            with_context=False,
        )

    Semantics per record: ``x = output(process(input(x)))``.
    ``DROP`` returned by any callable drops the record.
    """

    def __init__(
        self,
        *,
        process: Callable,
        input: Callable | None = None,  # noqa: A002 - matches plan API
        output: Callable | None = None,  # noqa: A002
        setup: Callable | None = None,
        teardown: Callable | None = None,
        name: str | None = None,
        with_context: bool = False,
    ) -> None:
        if not callable(process):
            raise PipelineValidationError(
                "GenericStage requires a callable `process`"
            )
        self.process_fn = process
        self.input_fn = input
        self.output_fn = output
        self.setup_fn = setup
        self.teardown_fn = teardown
        self.with_context = bool(with_context)
        self._name_explicit = name is not None
        self.name = name or getattr(process, "__name__", "generic")

    def setup(self, ctx: WorkerContext) -> None:
        if self.setup_fn is not None:
            if self.with_context:
                self.setup_fn(ctx)
            else:
                self.setup_fn()

    def process(self, value: Any, ctx: WorkerContext) -> Any:
        if self.input_fn is not None:
            value = _call_with_ctx(self.input_fn, value, ctx, self.with_context)
            if value is DROP:
                return DROP

        value = _call_with_ctx(self.process_fn, value, ctx, self.with_context)
        if value is DROP:
            return DROP

        if self.output_fn is not None:
            value = _call_with_ctx(self.output_fn, value, ctx, self.with_context)
            if value is DROP:
                return DROP

        return value

    def teardown(self, ctx: WorkerContext) -> None:
        if self.teardown_fn is not None:
            if self.with_context:
                self.teardown_fn(ctx)
            else:
                self.teardown_fn()

    def __repr__(self) -> str:
        return (
            f"GenericStage("
            f"name={self.name!r}, "
            f"process={getattr(self.process_fn, '__qualname__', self.process_fn)!r})"
        )


class TransformStage(Stage):
    """One-to-one mapping stage: ``x -> fn(x)``."""

    def __init__(self, fn: Callable, *, name: str | None = None) -> None:
        if not callable(fn):
            raise PipelineValidationError("TransformStage requires a callable")
        self.fn = fn
        self._name_explicit = name is not None
        self.name = name or getattr(fn, "__name__", "transform")

    def process(self, value: Any, ctx: WorkerContext) -> Any:
        return self.fn(value)


class FilterStage(Stage):
    """Keeps records for which ``predicate(value)`` is truthy, else drops."""

    def __init__(
        self, predicate: Callable, *, name: str | None = None
    ) -> None:
        if not callable(predicate):
            raise PipelineValidationError("FilterStage requires a callable")
        self.predicate = predicate
        self._name_explicit = name is not None
        self.name = name or getattr(predicate, "__name__", "filter")

    def process(self, value: Any, ctx: WorkerContext) -> Any:
        if self.predicate(value):
            return value
        return DROP


class TapStage(Stage):
    """Side-effect stage: ``fn(x)`` is called, then ``x`` is passed through."""

    def __init__(self, fn: Callable, *, name: str | None = None) -> None:
        if not callable(fn):
            raise PipelineValidationError("TapStage requires a callable")
        self.fn = fn
        self._name_explicit = name is not None
        self.name = name or getattr(fn, "__name__", "tap")

    def process(self, value: Any, ctx: WorkerContext) -> Any:
        self.fn(value)
        return value


class JsonLoadStage(Stage):
    """Parses a JSON string into a Python object."""

    name = "json_load"

    def __init__(self, *, loads: Callable = _json.loads) -> None:
        self.loads = loads

    def process(self, value: Any, ctx: WorkerContext) -> Any:
        return self.loads(value)


class JsonDumpStage(Stage):
    """Serializes a Python object to a JSON string."""

    name = "json_dump"

    def __init__(self, *, dumps: Callable = _json.dumps) -> None:
        self.dumps = dumps

    def process(self, value: Any, ctx: WorkerContext) -> Any:
        return self.dumps(value)


def coerce_stage(entry: Any, index: int | None = None) -> Stage:
    """Coerce a pipeline entry into a Stage.

    ``Stage`` instances pass through; plain callables become ``TransformStage``.
    """
    if isinstance(entry, Stage):
        return entry
    if callable(entry):
        return TransformStage(entry)
    where = f" (entry {index})" if index is not None else ""
    raise PipelineValidationError(
        f"pipeline entries must be Stage instances or callables{where}, "
        f"got {type(entry).__name__}"
    )



