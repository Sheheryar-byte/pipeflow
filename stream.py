"""
Stream: a lazy, chainable wrapper around any Python iterable.
"""

from __future__ import annotations

import functools
import itertools
from typing import (
    Any,
    Callable,
    Generic,
    Iterable,
    Iterator,
    Optional,
    TypeVar,
    Union,
    overload,
)

T = TypeVar("T")
U = TypeVar("U")
R = TypeVar("R")


class Stream(Generic[T]):
    """
    A lazy, chainable data pipeline for any Python iterable.

    Examples:
        >>> Stream([1, 2, 3, 4, 5])
        ...     .map(lambda x: x ** 2)
        ...     .filter(lambda x: x > 5)
        ...     .to(list)
        [9, 16, 25]

        >>> Stream(range(100)).take(5).to(list)
        [0, 1, 2, 3, 4]
    """

    def __init__(self, iterable: Iterable[T]) -> None:
        self._iter: Iterable[T] = iterable

    def __iter__(self) -> Iterator[T]:
        return iter(self._iter)

    def __repr__(self) -> str:
        return f"Stream({self._iter!r})"

    # ------------------------------------------------------------------ #
    # Transformations                                                       #
    # ------------------------------------------------------------------ #

    def map(self, func: Callable[[T], U]) -> "Stream[U]":
        """Apply *func* to every element. Lazy."""
        return Stream(map(func, self._iter))

    def filter(self, func: Callable[[T], bool]) -> "Stream[T]":
        """Keep only elements for which *func* returns True. Lazy."""
        return Stream(filter(func, self._iter))

    def flatmap(self, func: Callable[[T], Iterable[U]]) -> "Stream[U]":
        """Map then flatten one level. Lazy."""
        return Stream(itertools.chain.from_iterable(map(func, self._iter)))

    def flatten(self) -> "Stream[Any]":
        """Flatten one level of nesting. Lazy."""
        return Stream(itertools.chain.from_iterable(self._iter))  # type: ignore[arg-type]

    def take(self, n: int) -> "Stream[T]":
        """Keep only the first *n* elements. Lazy."""
        return Stream(itertools.islice(self._iter, n))

    def skip(self, n: int) -> "Stream[T]":
        """Drop the first *n* elements. Lazy."""
        return Stream(itertools.islice(self._iter, n, None))

    def take_while(self, func: Callable[[T], bool]) -> "Stream[T]":
        """Take elements while *func* is True, then stop. Lazy."""
        return Stream(itertools.takewhile(func, self._iter))

    def drop_while(self, func: Callable[[T], bool]) -> "Stream[T]":
        """Drop elements while *func* is True, then yield the rest. Lazy."""
        return Stream(itertools.dropwhile(func, self._iter))

    def enumerate(self, start: int = 0) -> "Stream[tuple[int, T]]":
        """Pair each element with its index. Lazy."""
        return Stream(enumerate(self._iter, start=start))  # type: ignore[arg-type]

    def zip(self, *others: Iterable[Any]) -> "Stream[tuple[Any, ...]]":
        """Zip this stream with one or more iterables. Lazy."""
        return Stream(zip(self._iter, *others))

    def chain(self, *others: Iterable[T]) -> "Stream[T]":
        """Append other iterables after this stream. Lazy."""
        return Stream(itertools.chain(self._iter, *others))

    def batch(self, size: int) -> "Stream[list[T]]":
        """
        Yield non-overlapping batches of *size* elements. Lazy.

        >>> Stream(range(7)).batch(3).to(list)
        [[0, 1, 2], [3, 4, 5], [6]]
        """
        def _batched(it: Iterable[T], n: int):
            it = iter(it)
            while True:
                chunk = list(itertools.islice(it, n))
                if not chunk:
                    break
                yield chunk

        return Stream(_batched(self._iter, size))

    def unique(self, key: Optional[Callable[[T], Any]] = None) -> "Stream[T]":
        """
        Yield each element once, preserving order. Lazy.
        An optional *key* function selects the comparison value.
        """
        def _unique(it: Iterable[T]):
            seen: set[Any] = set()
            for item in it:
                k = key(item) if key else item
                if k not in seen:
                    seen.add(k)
                    yield item

        return Stream(_unique(self._iter))

    def sort(
        self,
        key: Optional[Callable[[T], Any]] = None,
        reverse: bool = False,
    ) -> "Stream[T]":
        """
        Sort the stream (forces evaluation). Returns a new lazy Stream.
        """
        return Stream(sorted(self._iter, key=key, reverse=reverse))  # type: ignore[type-var]

    def reverse(self) -> "Stream[T]":
        """Reverse the stream (forces evaluation). Returns a new lazy Stream."""
        return Stream(reversed(list(self._iter)))

    def tap(self, func: Callable[[T], None]) -> "Stream[T]":
        """
        Call *func* for side-effects on each element without changing it.
        Useful for debugging mid-pipeline.

        >>> Stream([1, 2, 3]).tap(print).to(list)
        1
        2
        3
        [1, 2, 3]
        """
        def _tap(it: Iterable[T]):
            for item in it:
                func(item)
                yield item

        return Stream(_tap(self._iter))

    def starmap(self, func: Callable[..., U]) -> "Stream[U]":
        """Like map but unpacks tuples as arguments. Lazy."""
        return Stream(itertools.starmap(func, self._iter))  # type: ignore[arg-type]

    # ------------------------------------------------------------------ #
    # Reductions / Terminals                                               #
    # ------------------------------------------------------------------ #

    def to(self, collector: Callable[[Iterable[T]], R]) -> R:
        """
        Collect the stream into any container.

        >>> Stream([1, 2, 3]).to(list)
        [1, 2, 3]
        >>> Stream([1, 2, 3]).to(set)
        {1, 2, 3}
        >>> Stream([1, 2, 3]).to(tuple)
        (1, 2, 3)
        >>> Stream([(\"a\", 1), (\"b\", 2)]).to(dict)
        {'a': 1, 'b': 2}
        """
        return collector(self._iter)

    def reduce(self, func: Callable[[T, T], T], initial: Any = None) -> T:
        """Reduce the stream to a single value."""
        if initial is None:
            return functools.reduce(func, self._iter)
        return functools.reduce(func, self._iter, initial)

    def fold(self, func: Callable[[U, T], U], initial: U) -> U:
        """
        Like reduce but always requires an explicit initial value and
        supports a different accumulator type.

        >>> Stream([1, 2, 3, 4]).fold(lambda acc, x: acc + x, 0)
        10
        """
        acc = initial
        for item in self._iter:
            acc = func(acc, item)
        return acc

    def count(self) -> int:
        """Count elements (forces evaluation)."""
        return sum(1 for _ in self._iter)

    def first(self, default: Any = None) -> Optional[T]:
        """Return the first element or *default*."""
        return next(iter(self._iter), default)

    def last(self, default: Any = None) -> Optional[T]:
        """Return the last element or *default* (forces evaluation)."""
        result = default
        for item in self._iter:
            result = item
        return result

    def any(self, func: Optional[Callable[[T], bool]] = None) -> bool:
        """True if any element satisfies *func* (or is truthy if no func)."""
        if func:
            return any(func(x) for x in self._iter)
        return any(self._iter)

    def all(self, func: Optional[Callable[[T], bool]] = None) -> bool:
        """True if all elements satisfy *func* (or are truthy if no func)."""
        if func:
            return all(func(x) for x in self._iter)
        return all(self._iter)

    def sum(self) -> Any:
        """Sum all elements."""
        return sum(self._iter)  # type: ignore[call-overload]

    def min(self, key: Optional[Callable[[T], Any]] = None) -> T:
        """Return the minimum element."""
        return min(self._iter, key=key) if key else min(self._iter)  # type: ignore[type-var]

    def max(self, key: Optional[Callable[[T], Any]] = None) -> T:
        """Return the maximum element."""
        return max(self._iter, key=key) if key else max(self._iter)  # type: ignore[type-var]

    def for_each(self, func: Callable[[T], None]) -> None:
        """Call *func* for every element. Forces evaluation."""
        for item in self._iter:
            func(item)

    def group_by(self, key: Callable[[T], Any]) -> "Stream[tuple[Any, list[T]]]":
        """
        Group consecutive elements by *key*. For full grouping, sort first.

        >>> (Stream([1, 2, 2, 3, 3, 3])
        ...     .group_by(lambda x: x)
        ...     .to(list))
        [(1, [1]), (2, [2, 2]), (3, [3, 3, 3])]
        """
        def _group(it: Iterable[T]):
            for k, g in itertools.groupby(it, key):
                yield k, list(g)

        return Stream(_group(self._iter))

    # ------------------------------------------------------------------ #
    # Pipe operator (|)                                                    #
    # ------------------------------------------------------------------ #

    def __or__(self, other: "PipeStep[T, R]") -> "R":
        """
        Enable | syntax:
            Stream([1, 2, 3]) | to(list)
        """
        if callable(other):
            return other(self)  # type: ignore[return-value]
        return NotImplemented
