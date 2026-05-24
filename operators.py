"""
Functional helpers that work with the | operator on plain iterables.

    from pipeflow import pipe, to

    result = [1, 2, 3, 4, 5] | pipe(filter, lambda x: x % 2 == 0) | to(list)
    # [2, 4]
"""

from __future__ import annotations

from typing import Any, Callable, Iterable, TypeVar

from pipeflow.stream import Stream

T = TypeVar("T")
R = TypeVar("R")


class _PipeStep:
    """
    A callable that, when used with |, wraps the left-hand iterable
    in a Stream and applies the given stdlib function with its args.

    pipe(map, func)  →  _PipeStep that calls map(func, data)
    pipe(filter, pred) →  _PipeStep that calls filter(pred, data)
    """

    def __init__(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        self._func = func
        self._args = args
        self._kwargs = kwargs

    def __call__(self, data: Any) -> Stream[Any]:
        iterable = data._iter if isinstance(data, Stream) else data
        result = self._func(*self._args, iterable, **self._kwargs)
        return Stream(result)

    def __ror__(self, other: Any) -> Stream[Any]:
        """Support plain_list | pipe(...)"""
        return self(other)


class _ToStep:
    """
    Terminal step that collects the stream into a container.

        stream | to(list)   →  list(stream)
        stream | to(set)    →  set(stream)
        stream | to(dict)   →  dict(stream)
    """

    def __init__(self, collector: Callable[[Iterable[Any]], R]) -> None:
        self._collector = collector

    def __call__(self, data: Any) -> R:
        iterable = data._iter if isinstance(data, Stream) else data
        return self._collector(iterable)

    def __ror__(self, other: Any) -> R:
        return self(other)


def pipe(func: Callable[..., Iterable[Any]], *args: Any, **kwargs: Any) -> _PipeStep:
    """
    Wrap a stdlib transformation for use with the | operator.

    The wrapped function must accept the iterable as its LAST positional
    argument (map, filter, itertools.* all follow this convention).

    Examples:
        [1, 2, 3] | pipe(map, lambda x: x * 2) | to(list)
        # [2, 4, 6]

        range(10) | pipe(filter, lambda x: x % 3 == 0) | to(list)
        # [0, 3, 6, 9]
    """
    return _PipeStep(func, *args, **kwargs)


def to(collector: Callable[[Iterable[Any]], R]) -> _ToStep:
    """
    Terminal operator — collect a stream or piped iterable into a container.

    Examples:
        Stream([1, 2, 3]) | to(list)     # [1, 2, 3]
        Stream([1, 2, 3]) | to(set)      # {1, 2, 3}
        Stream([1, 2, 3]) | to(tuple)    # (1, 2, 3)
        Stream([(\"a\", 1)]) | to(dict)    # {'a': 1}
        Stream([1, 2, 3]) | to(sum)      # 6
    """
    return _ToStep(collector)
