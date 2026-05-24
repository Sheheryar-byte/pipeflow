# pipeflow

> Readable, lazy data-transformation pipelines for Python — no more nested function calls.

```python
# Before pipeflow
result = list(filter(lambda x: x > 5, map(lambda x: x ** 2, range(10))))

# With pipeflow
result = (
    Stream(range(10))
    .map(lambda x: x ** 2)
    .filter(lambda x: x > 5)
    .to(list)
)
```

## Installation

```bash
pip install pipeflow
```

## Quick Start

```python
from pipeflow import Stream, pipe, to

# Method chaining style
result = (
    Stream([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    .filter(lambda x: x % 2 == 0)
    .map(lambda x: x ** 2)
    .take(3)
    .to(list)
)
# [4, 16, 36]

# Pipe operator style (works on plain lists too)
result = (
    range(10)
    | pipe(filter, lambda x: x % 2 == 0)
    | pipe(map, lambda x: x ** 2)
    | to(list)
)
# [0, 4, 16, 36, 64]
```

## Why pipeflow?

Python lacks a native pipe operator (`|>`). The workaround — nesting function calls — is hard to read:

```python
# Hard to follow — you read inside-out
list(filter(pred, map(func, filter(other_pred, data))))

# pipeflow — you read top-to-bottom, left-to-right
Stream(data).filter(other_pred).map(func).filter(pred).to(list)
```

**Zero dependencies.** pipeflow uses only the Python standard library.  
**Fully lazy.** Elements are computed on demand — no intermediate lists.  
**Fully typed.** Every method ships with precise type annotations.

---

## API Reference

### `Stream(iterable)`

Wrap any iterable in a lazy pipeline.

#### Transformations

| Method | Description |
|---|---|
| `.map(func)` | Apply *func* to every element |
| `.filter(func)` | Keep elements where *func* is True |
| `.flatmap(func)` | Map then flatten one level |
| `.flatten()` | Flatten one level of nesting |
| `.take(n)` | Keep the first *n* elements |
| `.skip(n)` | Drop the first *n* elements |
| `.take_while(func)` | Take while *func* is True |
| `.drop_while(func)` | Drop while *func* is True |
| `.enumerate(start=0)` | Pair elements with their index |
| `.zip(*others)` | Zip with other iterables |
| `.chain(*others)` | Append other iterables |
| `.batch(n)` | Yield lists of *n* elements |
| `.unique(key=None)` | Deduplicate (order-preserving) |
| `.sort(key=None, reverse=False)` | Sort (forces evaluation) |
| `.reverse()` | Reverse (forces evaluation) |
| `.tap(func)` | Side-effects without changing elements |
| `.starmap(func)` | Map with tuple unpacking |
| `.group_by(key)` | Group consecutive elements |

#### Terminals

| Method | Description |
|---|---|
| `.to(collector)` | Collect into list, set, dict, tuple… |
| `.reduce(func, initial=None)` | Fold to a single value |
| `.fold(func, initial)` | Fold with explicit initial value |
| `.count()` | Count elements |
| `.first(default=None)` | First element |
| `.last(default=None)` | Last element |
| `.any(func=None)` | Any element satisfies *func* |
| `.all(func=None)` | All elements satisfy *func* |
| `.sum()` | Sum of elements |
| `.min(key=None)` | Minimum element |
| `.max(key=None)` | Maximum element |
| `.for_each(func)` | Call *func* for side-effects |

### `pipe(func, *args)` and `to(collector)`

Use the `|` operator on plain lists or generators:

```python
from pipeflow import pipe, to

result = [1, 2, 3, 4, 5] | pipe(filter, lambda x: x % 2 == 0) | to(list)
# [2, 4]
```

---

## Real-world Examples

### ETL Pipeline

```python
users = [
    {"name": "Alice", "age": 30, "active": True},
    {"name": "Bob",   "age": 17, "active": True},
    {"name": "Carol", "age": 25, "active": False},
]

result = (
    Stream(users)
    .filter(lambda u: u["active"])
    .filter(lambda u: u["age"] >= 18)
    .map(lambda u: u["name"].upper())
    .sort()
    .to(list)
)
# ["ALICE"]
```

### Word Count

```python
text = "hello world hello python world hello"

counts = (
    Stream(text.split())
    .group_by(lambda w: w)
    .map(lambda kv: (kv[0], len(kv[1])))
    .sort(key=lambda kv: kv[1], reverse=True)
    .to(dict)
)
# {"hello": 3, "world": 2, "python": 1}
```

### Debugging mid-pipeline

```python
result = (
    Stream(range(20))
    .filter(lambda x: x % 3 == 0)
    .tap(lambda x: print(f"after filter: {x}"))
    .map(lambda x: x * 10)
    .to(list)
)
```

### Batch processing

```python
Stream(records).batch(100).for_each(bulk_insert)
```

---

## License

MIT
