"""Runtime selector evaluation: resolve paths and replace values in records.

A ``CompiledSelector`` wraps the AST ``Selector`` and provides two operations:

resolve(record) -> list[Reference]
    Walk the record according to the selector path and return one ``Reference``
    per matched value. A ``Reference`` holds enough information to assign a
    new value back to the same location.

replace(record, references, new_values) -> record
    Apply replacements to *record* in stable traversal order and return the
    updated record. The original record is mutated in-place (records are
    already isolated per task) and returned.

Selector semantics (§4.2 of the CLI plan):
  .                  root — one match: the whole record
  .field             dict key lookup
  ["key.with.dots"]  dict key lookup via quoted string
  [0]                list index lookup (negative indices are errors)
  []                 every element of a list (wildcard); empty list → zero matches, success
  []                 applied to non-list → SelectorResolutionError
  missing field      SelectorResolutionError (strict by default)
  out-of-range index SelectorResolutionError
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from datapipe.dsl import ast as _ast
from datapipe.dsl.errors import SelectorResolutionError


@dataclass
class Reference:
    """A located value within a record, ready for replacement.

    Attributes
    ----------
    parent:
        The container (dict or list) that holds ``value`` at ``key``.
        ``None`` for the root reference (the whole record).
    key:
        The key or index within ``parent``.  ``None`` for root.
    value:
        The current value at this location.
    path:
        Human-readable path string for diagnostics, e.g. ``.tools[0].name``.
    """
    parent: Any          # dict | list | None (root)
    key: "int | str | None"
    value: Any
    path: str

    def replace(self, new_value: Any) -> None:
        """Write *new_value* back to the location described by this reference."""
        if self.parent is None:
            # Root replacement is handled by the caller via the return value.
            return
        self.parent[self.key] = new_value


class _RootSentinel:
    """Marker so we can distinguish a root reference from a None parent."""
    pass


_ROOT = _RootSentinel()


class CompiledSelector:
    """Compiled form of an AST ``Selector`` ready for repeated evaluation.

    Caches the selector's part list so resolve() does not re-inspect the AST
    on every record.
    """

    def __init__(self, selector: "_ast.Selector") -> None:
        self._parts = selector.parts
        self._is_root = selector.is_root

    def resolve(self, record: Any) -> list[Reference]:
        """Return a list of ``Reference`` objects for all selector matches.

        For the root selector ``"."``, always returns exactly one reference.
        For wildcard ``"[]"`` on an empty list, returns zero references (success).
        Raises ``SelectorResolutionError`` on missing fields or type mismatches.
        """
        if self._is_root:
            return [Reference(parent=None, key=None, value=record, path=".")]

        # Iterative descent: maintain a list of (parent, key, value, path)
        # tuples.  Each selector part may expand or filter the list.
        current: list[tuple[Any, Any, Any, str]] = [(None, None, record, "")]

        for part in self._parts:
            next_: list[tuple[Any, Any, Any, str]] = []

            for parent, _key, value, path in current:
                if isinstance(part, _ast.Field):
                    if not isinstance(value, dict):
                        raise SelectorResolutionError(
                            f"selector .{part.name} expects a dict at {path or '.'!r}, "
                            f"got {type(value).__name__}",
                            path=path,
                        )
                    if part.name not in value:
                        raise SelectorResolutionError(
                            f"missing field {part.name!r} at {path or '.'!r}",
                            path=path,
                        )
                    new_path = f"{path}.{part.name}"
                    next_.append((value, part.name, value[part.name], new_path))

                elif isinstance(part, _ast.QuotedKey):
                    if not isinstance(value, dict):
                        raise SelectorResolutionError(
                            f"selector [\"{part.key}\"] expects a dict at {path or '.'!r}, "
                            f"got {type(value).__name__}",
                            path=path,
                        )
                    if part.key not in value:
                        raise SelectorResolutionError(
                            f"missing field {part.key!r} at {path or '.'!r}",
                            path=path,
                        )
                    new_path = f'{path}["{part.key}"]'
                    next_.append((value, part.key, value[part.key], new_path))

                elif isinstance(part, _ast.Index):
                    if not isinstance(value, list):
                        raise SelectorResolutionError(
                            f"selector [{part.index}] expects a list at {path or '.'!r}, "
                            f"got {type(value).__name__}",
                            path=path,
                        )
                    if part.index < 0 or part.index >= len(value):
                        raise SelectorResolutionError(
                            f"index {part.index} out of range (length {len(value)}) "
                            f"at {path or '.'!r}",
                            path=path,
                        )
                    new_path = f"{path}[{part.index}]"
                    next_.append((value, part.index, value[part.index], new_path))

                elif isinstance(part, _ast.Each):
                    if not isinstance(value, list):
                        raise SelectorResolutionError(
                            f"selector [] expects a list at {path or '.'!r}, "
                            f"got {type(value).__name__}",
                            path=path,
                        )
                    # Empty list → zero matches, which is a success (no-op).
                    for i, item in enumerate(value):
                        new_path = f"{path}[{i}]"
                        next_.append((value, i, item, new_path))

            current = next_

        return [
            Reference(parent=p, key=k, value=v, path=path or ".")
            for p, k, v, path in current
        ]

    def apply(self, record: Any, references: list[Reference], new_values: list[Any]) -> Any:
        """Replace each reference's value and return the (possibly new) record.

        For non-root references, mutation happens in-place through the
        ``Reference.replace`` method. For the root reference, the new value
        is returned directly.

        ``references`` and ``new_values`` must have the same length.
        """
        assert len(references) == len(new_values)

        for ref, new_val in zip(references, new_values):
            if ref.parent is None:
                # Root replacement: the whole record becomes new_val.
                return new_val
            ref.replace(new_val)

        return record
