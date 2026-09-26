"""Callback-scoped access to the current process's Sail object-store registry.

Storage paths are literal; use glob() explicitly for file discovery.
URL keys must be percent-encoded;
metadata locations are already encoded and can be passed back to all operations.
"""

from __future__ import annotations

from collections import deque
from threading import local
from typing import NamedTuple
from weakref import WeakSet

DEFAULT_MAX_BYTES = 64 * 1024 * 1024
DEFAULT_MAX_RANGES = 1024
DEFAULT_MAX_ENTRIES = 10000


class ObjectMeta(NamedTuple):
    """Object metadata with a fully qualified, percent-encoded location."""

    location: str
    size: int
    last_modified: str
    e_tag: str | None
    version: str | None


class _StorageIterator:
    """Closeable, single-consumer iterator owned by one callback."""

    def __init__(self, owner, native, *, metadata):
        self._owner = owner
        self._native = native
        self._metadata = metadata
        self._buffer = deque()
        owner._iterators.add(self)  # noqa: SLF001 - iterator and owner share this module's lifecycle

    def __iter__(self):
        return self

    def __next__(self):
        self._owner._check()  # noqa: SLF001 - enforce the owning callback scope
        if self._native is None:
            raise StopIteration
        try:
            if self._metadata:
                if not self._buffer:
                    self._buffer.extend(self._native.next_batch())
                if self._buffer:
                    return ObjectMeta(*self._buffer.popleft())
            else:
                chunk = self._native.next_chunk()
                if chunk is not None:
                    return chunk
        except BaseException:
            self.close()
            raise
        self.close()
        raise StopIteration

    def close(self):
        native, self._native = self._native, None
        self._buffer.clear()
        if native is not None:
            native.close()
        self._owner._iterators.discard(self)  # noqa: SLF001 - unregister from the owning scope

    def __enter__(self):
        self._owner._check()  # noqa: SLF001 - enforce the owning callback scope
        return self

    def __exit__(self, *_args):
        self.close()

    def __del__(self):
        self.close()


class ObjectStore:
    """Synchronous storage facade valid only during its originating callback.

    Missing objects raise FileNotFoundError, denied access raises PermissionError,
    other provider errors raise OSError, and canceled I/O raises InterruptedError.
    Iterators support ``with`` and ``close()`` for early termination.
    """

    def __init__(self, native):
        self._native = native
        self._iterators = WeakSet()

    def _check(self):
        if self._native is None:
            message = "Sail object-store callback has ended"
            raise RuntimeError(message)
        return self._native

    def _close(self):
        native, self._native = self._native, None
        if native is not None:
            native.close()
        for iterator in list(self._iterators):
            iterator.close()

    def read(self, location: str, *, max_bytes: int = DEFAULT_MAX_BYTES) -> bytes:
        """Read a whole object, rejecting results larger than max_bytes.

        max_bytes is a per-call byte budget, defaulting to 64 MiB; it can be raised.
        """
        return self._check().read(location, max_bytes)

    def read_range(self, location: str, start: int, end: int, *, max_bytes: int = DEFAULT_MAX_BYTES) -> bytes:
        """Read the half-open byte range [start, end).

        max_bytes defaults to 64 MiB per call and can be raised.
        """
        return self._check().read_range(location, start, end, max_bytes)

    def read_ranges(
        self,
        location: str,
        ranges: list[tuple[int, int]],
        *,
        max_bytes: int = DEFAULT_MAX_BYTES,
        max_ranges: int = DEFAULT_MAX_RANGES,
    ) -> list[bytes]:
        """Read half-open byte ranges in one native call.

        max_bytes defaults to 64 MiB for the sum of requested range lengths.
        max_ranges defaults to 1,024 ranges per call. Both can be overridden.
        """
        native = self._check()
        if len(ranges) > max_ranges:
            message = "too many byte ranges"
            raise ValueError(message)
        return native.read_ranges(location, ranges, max_bytes, max_ranges)

    def iter_bytes(self, location: str, *, chunk_size: int = 1024 * 1024):
        """Stream one response in chunks no larger than chunk_size bytes.

        The default is 1 MiB; the native implementation accepts 1 byte to 64 MiB.
        This limits each yielded chunk, not the total object size.

        Rust can also retain the current provider chunk and provider-side buffers.
        Retaining yielded chunks in Python will still accumulate memory.
        """
        return _StorageIterator(self, self._check().iter_bytes(location, chunk_size), metadata=False)

    def iter_objects(self, location: str, *, batch_size: int = 128):
        """List a literal prefix, transferring at most batch_size entries per call.

        The default is 128 entries; the native implementation accepts 1 to 4,096.
        This bounds each transfer, not the total number of listed objects.
        """
        return _StorageIterator(self, self._check().iter_objects(location, batch_size), metadata=True)

    def glob(self, pattern: str, *, max_entries: int = DEFAULT_MAX_ENTRIES) -> list[ObjectMeta]:
        """Discover files using Sail's Rust globbing and hidden-file filtering.

        Supports Hadoop-style *, ?, character classes, and brace alternatives.
        Returns unique metadata sorted by encoded location; no matches yields [].
        Directory paths follow native Sail discovery and session listing settings.
        max_entries defaults to 10,000 objects per call and can be overridden.
        Exceeding it raises ValueError instead of truncating results.
        Native listing caches may retain more entries than this result limit.
        """
        native = self._check()
        if max_entries < 0:
            message = "max_entries must be non-negative"
            raise ValueError(message)
        return [ObjectMeta(*item) for item in native.glob(pattern, max_entries)]

    def list(self, location: str, *, max_entries: int = DEFAULT_MAX_ENTRIES) -> list[ObjectMeta]:
        """Collect a literal prefix listing; use iter_objects for larger listings.

        max_entries defaults to 10,000 objects per call and can be overridden.
        Exceeding it raises ValueError instead of returning a partial listing.
        """
        if max_entries < 0:
            message = "max_entries must be non-negative"
            raise ValueError(message)
        result = []
        with self.iter_objects(location) as objects:
            for item in objects:
                if len(result) >= max_entries:
                    message = "listing exceeds max_entries; use iter_objects"
                    raise ValueError(message)
                result.append(item)
        return result

    def write(self, location: str, data: bytes) -> None:
        """Write an object in one put. Cancellation does not imply remote rollback."""
        self._check().write(location, data)

    def delete(self, location: str) -> None:
        """Delete an object."""
        self._check().delete(location)

    def head(self, location: str) -> ObjectMeta:
        """Return object metadata."""
        return ObjectMeta(*self._check().head(location))


_STATE = local()


def get_object_store() -> ObjectStore:
    """Get storage during a Sail datasource callback, including construction.

    Driver and worker registries are independent. Custom stores must be configured
    in every process that executes callbacks. Do not save this proxy on a reader,
    writer, or partition; retrieve it inside each callback instead.
    """
    store = getattr(_STATE, "store", None)
    if store is None:
        message = "Sail object-store access is only available while executing a Python DataSource callback"
        raise RuntimeError(message)
    return store


def _set_current(native) -> ObjectStore | None:
    previous = getattr(_STATE, "store", None)
    _STATE.store = ObjectStore(native)
    return previous


def _reset_current(previous: ObjectStore | None) -> None:
    current = getattr(_STATE, "store", None)
    if current is not None:
        current._close()  # noqa: SLF001 - only callback teardown may invalidate a proxy
    if previous is None:
        if hasattr(_STATE, "store"):
            del _STATE.store
    else:
        _STATE.store = previous
