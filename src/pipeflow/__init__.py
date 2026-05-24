"""
pipeflow — readable data transformation pipelines for Python.

Usage:
    from pipeflow import Stream

    result = (
        Stream([1, 2, 3, 4, 5])
        .map(lambda x: x * 2)
        .filter(lambda x: x > 4)
        .to(list)
    )
    # [6, 8, 10]

Or with the pipe operator:
    from pipeflow import pipe, to

    result = [1, 2, 3] | pipe(map, lambda x: x * 2) | to(list)
"""

from pipeflow.stream import Stream
from pipeflow.operators import pipe, to

__all__ = ["Stream", "pipe", "to"]
__version__ = "0.1.0"
