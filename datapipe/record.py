"""Core record envelope, sentinels, and exceptions for datapipe."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class _Sentinel:
    """A singleton sentinel with a readable repr."""

    __slots__ = ("_name",)

    def __init__(self, name: str) -> None:
        object.__setattr__(self, "_name", name)

    def __repr__(self) -> str:  # pragma: no cover - trivial
        return self._name

    def __reduce__(self):
        # Keep sentinels pickle-stable across process boundaries.
        return (_get_sentinel, (self._name,))


def _get_sentinel(name: str) -> _Sentinel:
    from datapipe.sentinels import _SENTINELS

    return _SENTINELS[name]


#: Marker returned by a stage to drop the current record from the output.
DROP = _Sentinel("DROP")

#: Private marker: a record that was skipped because it is not owned by this
#: rank. Never surfaces to user stages.
_NOT_OWNED = _Sentinel("_NOT_OWNED")

#: Private marker: the source has been exhausted.
_EOF = _Sentinel("_EOF")


@dataclass
class Record:
    """A record envelope carrying a stable sequence identifier.

    ``value`` holds the user payload; ``metadata`` holds optional
    bookkeeping the runtime may attach (never mutated by user stages).
    """

    seq: int
    value: Any
    metadata: dict[str, Any] = field(default_factory=dict)

    def __iter__(self):
        yield self.value

    def __len__(self) -> int:
        return 1
