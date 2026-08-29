"""Python-native IO adapters: iterables, lists, and callables."""

from __future__ import annotations

from typing import Any, Callable, Iterable, Iterator

from datapipe.io.base import Source, Sink


class IterableSource(Source):
    """Wraps any Python iterable as a source."""

    supports_physical_sharding = False

    def __init__(self, iterable: Iterable[Any]) -> None:
        self._iterable = iterable
        self._seq_offset = 0
        # Compute the total eagerly for sized iterables so it is available
        # before iteration begins (needed by RangeSharding total injection).
        if isinstance(iterable, (list, tuple, range, set, frozenset)):
            self._total_hint: int | None = len(iterable)
        else:
            self._total_hint = None

    def __iter__(self) -> Iterator[Any]:
        return iter(self._iterable)

    @property
    def total(self) -> int | None:
        return self._total_hint


class CallableSink(Sink):
    """Calls ``fn(record)`` for each written record."""

    def __init__(self, fn: Callable[[Any], Any]) -> None:
        self.fn = fn
        self.count = 0

    def write(self, record: Any) -> None:
        self.fn(record)
        self.count += 1


class ListSink(Sink):
    """Accumulates written records into a list. Ideal for tests."""

    def __init__(self) -> None:
        self.items: list[Any] = []

    def write(self, record: Any) -> None:
        self.items.append(record)
