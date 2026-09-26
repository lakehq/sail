"""Lifecycle tests independent of a Spark server or storage provider."""

from collections import deque

import pytest

from pysail.spark.datasource.object_store import _reset_current, _set_current, get_object_store


class _Cursor:
    def __init__(self):
        self.closed = False
        self.calls = 0

    def next_batch(self):
        self.calls += 1
        return [(f"memory:///item-{i}", 1, "", None, None) for i in range(3)]

    def close(self):
        self.closed = True


class _Native:
    def __init__(self, value):
        self.value = value
        self.closed = False
        self.cursor = _Cursor()

    def read(self, _location, _limit):
        return self.value

    def iter_objects(self, _location, _size):
        return self.cursor

    def close(self):
        self.closed = True


def test_nested_scopes_restore_outer_and_invalidate_saved_inner():
    a = _Native(b"a")
    previous = _set_current(a)
    outer = get_object_store()
    try:
        b = _Native(b"b")
        saved = _set_current(b)
        inner = get_object_store()
        iterator = inner.iter_objects("memory:///")
        assert next(iterator).location == "memory:///item-0"
        _reset_current(saved)
        assert b.closed
        assert b.cursor.closed
        assert not a.closed
        assert get_object_store() is outer
        assert outer.read("memory:///data") == b"a"
        with pytest.raises(RuntimeError, match="ended"):
            inner.read("memory:///data")
        with pytest.raises(RuntimeError, match="ended"):
            next(iterator)
        assert not iterator._buffer  # noqa: SLF001 - verify teardown releases buffered metadata
    finally:
        _reset_current(previous)
    assert a.closed
    with pytest.raises(RuntimeError, match="ended"):
        outer.head("memory:///data")


def test_listing_fetches_batches_on_demand_and_closes_early():
    native = _Native(b"")
    previous = _set_current(native)
    try:
        with get_object_store().iter_objects("memory:///") as items:
            assert native.cursor.calls == 0
            assert next(items).size == 1
            assert next(items).size == 1
            assert native.cursor.calls == 1
        assert native.cursor.closed
        assert list(items) == []
    finally:
        _reset_current(previous)


def test_eager_listing_limit_closes_cursor():
    native = _Native(b"")
    previous = _set_current(native)
    try:
        with pytest.raises(ValueError, match="max_entries"):
            get_object_store().list("memory:///", max_entries=2)
        assert native.cursor.closed
    finally:
        _reset_current(previous)


def test_byte_iterator_closes_after_error():
    class FailingCursor(_Cursor):
        def next_chunk(self):
            message = "denied"
            raise PermissionError(message)

    class Native(_Native):
        def iter_bytes(self, _location, _size):
            return self.cursor

    native = Native(b"")
    native.cursor = FailingCursor()
    previous = _set_current(native)
    try:
        chunks = get_object_store().iter_bytes("memory:///data")
        with pytest.raises(PermissionError):
            next(chunks)
        assert native.cursor.closed
        assert list(chunks) == []
    finally:
        _reset_current(previous)


def test_byte_iterator_yields_without_collecting():
    class Cursor(_Cursor):
        def __init__(self):
            super().__init__()
            self.chunks = deque([b"abc", b"def", None])

        def next_chunk(self):
            self.calls += 1
            return self.chunks.popleft()

    class Native(_Native):
        def iter_bytes(self, _location, _size):
            return self.cursor

    native = Native(b"")
    native.cursor = Cursor()
    previous = _set_current(native)
    try:
        chunks = get_object_store().iter_bytes("memory:///data")
        assert native.cursor.calls == 0
        assert next(chunks) == b"abc"
        assert native.cursor.calls == 1
        assert list(chunks) == [b"def"]
        assert native.cursor.closed
    finally:
        _reset_current(previous)


def test_glob_returns_metadata_and_rejects_expired_scope():
    requested_limit = 2
    expected_size = 5

    class Native(_Native):
        def glob(self, pattern, limit):
            assert pattern == "memory:///part-*.txt"
            assert limit == requested_limit
            return [("memory:///part-1.txt", expected_size, "", None, None)]

    previous = _set_current(Native(b""))
    store = get_object_store()
    try:
        assert store.glob("memory:///part-*.txt", max_entries=requested_limit)[0].size == expected_size
        with pytest.raises(ValueError, match="max_entries"):
            store.glob("memory:///part-*.txt", max_entries=-1)
    finally:
        _reset_current(previous)
    with pytest.raises(RuntimeError, match="ended"):
        store.glob("memory:///part-*.txt")
