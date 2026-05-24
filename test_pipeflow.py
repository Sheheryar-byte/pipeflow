"""Tests for pipeflow."""

import itertools
import pytest
from pipeflow import Stream, pipe, to


# ------------------------------------------------------------------ #
# Stream — Transformations                                             #
# ------------------------------------------------------------------ #

class TestMap:
    def test_basic(self):
        assert Stream([1, 2, 3]).map(lambda x: x * 2).to(list) == [2, 4, 6]

    def test_string(self):
        assert Stream(["a", "b"]).map(str.upper).to(list) == ["A", "B"]

    def test_chained(self):
        result = Stream([1, 2, 3]).map(lambda x: x + 1).map(lambda x: x * 3).to(list)
        assert result == [6, 9, 12]


class TestFilter:
    def test_basic(self):
        assert Stream([1, 2, 3, 4]).filter(lambda x: x % 2 == 0).to(list) == [2, 4]

    def test_all_filtered(self):
        assert Stream([1, 3, 5]).filter(lambda x: x % 2 == 0).to(list) == []

    def test_none_filtered(self):
        assert Stream([2, 4]).filter(lambda x: x % 2 == 0).to(list) == [2, 4]


class TestFlatmap:
    def test_basic(self):
        result = Stream([[1, 2], [3, 4]]).flatmap(lambda x: x).to(list)
        assert result == [1, 2, 3, 4]

    def test_with_transform(self):
        result = Stream([1, 2, 3]).flatmap(lambda x: [x, x * 10]).to(list)
        assert result == [1, 10, 2, 20, 3, 30]


class TestFlatten:
    def test_basic(self):
        assert Stream([[1, 2], [3, 4], [5]]).flatten().to(list) == [1, 2, 3, 4, 5]


class TestTakeSkip:
    def test_take(self):
        assert Stream(range(100)).take(3).to(list) == [0, 1, 2]

    def test_skip(self):
        assert Stream([1, 2, 3, 4, 5]).skip(3).to(list) == [4, 5]

    def test_take_more_than_available(self):
        assert Stream([1, 2]).take(10).to(list) == [1, 2]

    def test_take_while(self):
        assert Stream([1, 2, 3, 4, 1]).take_while(lambda x: x < 4).to(list) == [1, 2, 3]

    def test_drop_while(self):
        assert Stream([1, 2, 3, 4]).drop_while(lambda x: x < 3).to(list) == [3, 4]


class TestEnumerateZip:
    def test_enumerate(self):
        result = Stream(["a", "b"]).enumerate().to(list)
        assert result == [(0, "a"), (1, "b")]

    def test_enumerate_start(self):
        result = Stream(["a", "b"]).enumerate(start=5).to(list)
        assert result == [(5, "a"), (6, "b")]

    def test_zip(self):
        result = Stream([1, 2, 3]).zip([4, 5, 6]).to(list)
        assert result == [(1, 4), (2, 5), (3, 6)]


class TestChain:
    def test_basic(self):
        assert Stream([1, 2]).chain([3, 4]).to(list) == [1, 2, 3, 4]


class TestBatch:
    def test_even(self):
        assert Stream(range(6)).batch(2).to(list) == [[0, 1], [2, 3], [4, 5]]

    def test_remainder(self):
        assert Stream(range(7)).batch(3).to(list) == [[0, 1, 2], [3, 4, 5], [6]]


class TestUnique:
    def test_basic(self):
        assert Stream([1, 2, 2, 3, 1]).unique().to(list) == [1, 2, 3]

    def test_with_key(self):
        result = Stream(["a", "A", "b"]).unique(key=str.lower).to(list)
        assert result == ["a", "b"]


class TestSortReverse:
    def test_sort(self):
        assert Stream([3, 1, 2]).sort().to(list) == [1, 2, 3]

    def test_sort_key(self):
        result = Stream(["banana", "apple", "kiwi"]).sort(key=len).to(list)
        assert result == ["kiwi", "apple", "banana"]

    def test_sort_reverse_flag(self):
        assert Stream([1, 3, 2]).sort(reverse=True).to(list) == [3, 2, 1]

    def test_reverse(self):
        assert Stream([1, 2, 3]).reverse().to(list) == [3, 2, 1]


class TestTap:
    def test_passthrough(self):
        seen = []
        result = Stream([1, 2, 3]).tap(seen.append).to(list)
        assert result == [1, 2, 3]
        assert seen == [1, 2, 3]


class TestStarmap:
    def test_basic(self):
        result = Stream([(1, 2), (3, 4)]).starmap(lambda a, b: a + b).to(list)
        assert result == [3, 7]


# ------------------------------------------------------------------ #
# Stream — Terminals                                                   #
# ------------------------------------------------------------------ #

class TestTo:
    def test_list(self):
        assert Stream([1, 2, 3]).to(list) == [1, 2, 3]

    def test_set(self):
        assert Stream([1, 2, 2]).to(set) == {1, 2}

    def test_tuple(self):
        assert Stream([1, 2, 3]).to(tuple) == (1, 2, 3)

    def test_dict(self):
        assert Stream([("a", 1), ("b", 2)]).to(dict) == {"a": 1, "b": 2}

    def test_sum(self):
        assert Stream([1, 2, 3]).to(sum) == 6


class TestReduce:
    def test_sum(self):
        assert Stream([1, 2, 3, 4]).reduce(lambda a, b: a + b) == 10

    def test_with_initial(self):
        assert Stream([1, 2, 3]).reduce(lambda a, b: a + b, 10) == 16


class TestFold:
    def test_basic(self):
        assert Stream([1, 2, 3, 4]).fold(lambda acc, x: acc + x, 0) == 10

    def test_build_dict(self):
        result = Stream([("a", 1), ("b", 2)]).fold(
            lambda acc, kv: {**acc, kv[0]: kv[1]}, {}
        )
        assert result == {"a": 1, "b": 2}


class TestAggregates:
    def test_count(self):
        assert Stream([1, 2, 3]).count() == 3

    def test_first(self):
        assert Stream([10, 20, 30]).first() == 10

    def test_first_default(self):
        assert Stream([]).first(default=-1) == -1

    def test_last(self):
        assert Stream([10, 20, 30]).last() == 30

    def test_any_true(self):
        assert Stream([1, 2, 3]).any(lambda x: x > 2) is True

    def test_any_false(self):
        assert Stream([1, 2, 3]).any(lambda x: x > 10) is False

    def test_all_true(self):
        assert Stream([2, 4, 6]).all(lambda x: x % 2 == 0) is True

    def test_all_false(self):
        assert Stream([2, 3, 6]).all(lambda x: x % 2 == 0) is False

    def test_sum(self):
        assert Stream([1, 2, 3]).sum() == 6

    def test_min(self):
        assert Stream([3, 1, 2]).min() == 1

    def test_max(self):
        assert Stream([3, 1, 2]).max() == 3


class TestForEach:
    def test_side_effects(self):
        results = []
        Stream([1, 2, 3]).for_each(results.append)
        assert results == [1, 2, 3]


class TestGroupBy:
    def test_consecutive(self):
        result = Stream([1, 1, 2, 3, 3]).group_by(lambda x: x).to(list)
        assert result == [(1, [1, 1]), (2, [2]), (3, [3, 3])]


# ------------------------------------------------------------------ #
# Pipe operator                                                         #
# ------------------------------------------------------------------ #

class TestPipeOperator:
    def test_stream_pipe_to(self):
        result = Stream([1, 2, 3]) | to(list)
        assert result == [1, 2, 3]

    def test_plain_list_pipe(self):
        result = [1, 2, 3, 4] | pipe(filter, lambda x: x % 2 == 0) | to(list)
        assert result == [2, 4]

    def test_map_pipe(self):
        result = [1, 2, 3] | pipe(map, lambda x: x * 10) | to(list)
        assert result == [10, 20, 30]

    def test_chained_pipes(self):
        result = (
            range(10)
            | pipe(filter, lambda x: x % 2 == 0)
            | pipe(map, lambda x: x ** 2)
            | to(list)
        )
        assert result == [0, 4, 16, 36, 64]

    def test_to_set(self):
        result = Stream([1, 1, 2]) | to(set)
        assert result == {1, 2}

    def test_to_sum(self):
        result = Stream([1, 2, 3]) | to(sum)
        assert result == 6


# ------------------------------------------------------------------ #
# Real-world pipelines                                                 #
# ------------------------------------------------------------------ #

class TestRealWorldPipelines:
    def test_word_count(self):
        text = "hello world hello python world hello"
        # group_by groups *consecutive* equal elements, so sort first
        result = (
            Stream(text.split())
            .sort()
            .group_by(lambda w: w)
            .map(lambda kv: (kv[0], len(kv[1])))
            .to(dict)
        )
        assert result["hello"] == 3
        assert result["world"] == 2

    def test_etl_pipeline(self):
        raw = [
            {"name": "Alice", "age": 30, "active": True},
            {"name": "Bob", "age": 17, "active": True},
            {"name": "Carol", "age": 25, "active": False},
            {"name": "Dave", "age": 40, "active": True},
        ]
        result = (
            Stream(raw)
            .filter(lambda u: u["active"])
            .filter(lambda u: u["age"] >= 18)
            .map(lambda u: u["name"].upper())
            .sort()
            .to(list)
        )
        assert result == ["ALICE", "DAVE"]

    def test_batch_processing(self):
        total_batches = Stream(range(10)).batch(3).count()
        assert total_batches == 4  # [0,1,2], [3,4,5], [6,7,8], [9]

    def test_debug_with_tap(self):
        log = []
        result = (
            Stream([1, 2, 3, 4, 5])
            .filter(lambda x: x % 2 != 0)
            .tap(log.append)
            .map(lambda x: x ** 2)
            .to(list)
        )
        assert result == [1, 9, 25]
        assert log == [1, 3, 5]
