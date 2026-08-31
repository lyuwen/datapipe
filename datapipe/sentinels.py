"""Sentinel registry for datapipe.

Sentinels are module-level singletons that must survive pickling across
process boundaries. This module keeps a single source of truth so that
``pickle``/``copy`` always resolve to the same object.
"""

from __future__ import annotations

from datapipe.record import _Sentinel, _get_sentinel, DROP  # noqa: F401

#: Private: record not owned by this rank.
_NOT_OWNED = _Sentinel("_NOT_OWNED")

#: Private: source exhausted.
_EOF = _Sentinel("_EOF")

_SENTINELS = {
    "DROP": DROP,
    "_NOT_OWNED": _NOT_OWNED,
    "_EOF": _EOF,
}
