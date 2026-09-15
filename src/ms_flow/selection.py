"""Lazy, typed collections returned by application query APIs.

``Selection`` deliberately owns only collection operations.  The application
API still owns query construction and domain actions; ``scope`` remains the
plain, serializable object a job receives.
"""
from __future__ import annotations

from dataclasses import dataclass
from itertools import islice
from typing import Any, Callable, Generic, Iterator, TypeVar


T = TypeVar("T")


class NoResultFound(LookupError):
    """Raised when :meth:`Selection.one` finds no item."""


class MultipleResultsFound(LookupError):
    """Raised when :meth:`Selection.one` finds more than one item."""


@dataclass(frozen=True)
class Selection(Generic[T]):
    """A re-iterable lazy collection bound to an application's query reader.

    ``scope`` is intentionally public: callers pass it to asynchronous jobs
    when they need the transport-safe query description rather than this
    runtime-bound convenience object.
    """

    scope: Any
    _stream: Callable[[], Iterator[T]]
    _count: Callable[[], int]

    def __iter__(self) -> Iterator[T]:
        return self._stream()

    def stream(self) -> Iterator[T]:
        """Return a fresh lazy iterator over the selected items."""
        return iter(self)

    def count(self) -> int:
        """Return the number of selected items without materializing them."""
        return self._count()

    def first(self) -> T | None:
        """Return the first selected item, or ``None`` when empty."""
        return next(iter(self), None)

    def one(self) -> T:
        """Return exactly one item, raising when the selection is not singular."""
        rows = list(islice(iter(self), 2))
        if not rows:
            raise NoResultFound("Selection contains no items.")
        if len(rows) > 1:
            raise MultipleResultsFound("Selection contains more than one item.")
        return rows[0]

    def one_or_none(self) -> T | None:
        """Return one item or ``None``; raise when more than one exists."""
        rows = list(islice(iter(self), 2))
        if len(rows) > 1:
            raise MultipleResultsFound("Selection contains more than one item.")
        return rows[0] if rows else None


__all__ = ["MultipleResultsFound", "NoResultFound", "Selection"]
