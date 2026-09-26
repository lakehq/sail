---
title: Python
rank: 3
---

# Python Data Sources

The Python data source allows you to extend the `SparkSession.read` and `DataFrame.write` APIs to support custom formats and external system integrations.
It optionally supports Arrow for zero-copy data exchange between the Python process and the Sail execution engine. This gives you flexibility in data source implementations without incurring performance penalties.

You can define a Python class that inherits from the `pyspark.sql.datasource.DataSource` abstract class, and register it to the Spark session to create a custom data source that can be used in the standard PySpark API. The `DataSource` class provides methods for defining the name and schema of the data source, as well as methods for creating readers and writers.

Currently, Sail supports Python data sources for batch reading and writing.

## Examples

<!--@include: ../../_common/spark-session.md-->

### Batch Reader

<<< @/../python/pysail/tests/spark/datasource/test_python_read.txt{python-console}

### Batch Arrow Reader

<<< @/../python/pysail/tests/spark/datasource/test_python_read_arrow.txt{python-console}

## Object Storage

Python data sources can access the same object-store registry used by Sail's
native readers and writers. This keeps credentials, endpoint configuration, and
custom object-store registrations in one place rather than configuring a second
Python storage client.

```python
from pysail.spark.datasource.object_store import get_object_store

class MyReader(DataSourceReader):
    def read(self, partition):
        store = get_object_store()
        header, footer = store.read_ranges(
            partition.path,
            [(0, 4096), (partition.footer_start, partition.footer_end)],
        )
        ...
```

`get_object_store()` is available only while Sail invokes a Python data-source
callback, including construction, schema inference, partition planning, reads, writes, commit,
and abort. The proxy is runtime-scoped and is not serialized with Python readers
or writers, so distributed workers use their own DataFusion `RuntimeEnv`.
Custom registrations must exist in every process that executes the callbacks;
registering a store on the driver does not register it on workers. Retrieve the
proxy inside each callback. A saved proxy or iterator becomes invalid when its
callback ends, and releases its native storage reference.

For formats that need several byte ranges from the same object, prefer
`read_ranges()` over repeated `read_range()` calls. Vectored reads cross the
Python/Rust boundary once and let the Rust object-store implementation coalesce
or parallelize requests.

### Paths and errors

Storage operations use literal paths: `*`, `?`, and brackets never expand into glob patterns.
Use `glob()` explicitly for file discovery.
Filesystem paths are accepted directly; keys in URLs must be percent-encoded.
Locations returned by `head()`, `list()`, and `iter_objects()` are encoded URLs
that can be passed back to storage operations, including for keys containing
`#`, `?`, `%`, or spaces. Listing follows object-store prefix semantics. Spark
glob expansion and hidden-file filtering remain in Sail's listing utilities.

Missing objects raise `FileNotFoundError`, denied access raises `PermissionError`,
and other provider failures raise `OSError`. Invalid ranges or exceeded limits
raise `ValueError`.

### File discovery with glob patterns

Use `store.glob(pattern)` while planning input partitions to reuse Sail's Rust
file discovery. It supports `*`, `?`, character classes, and brace alternatives,
for example `s3://bucket/events/part-*.{json,csv}`. Both filesystem paths and URLs
are supported. Directories, hidden-file filtering, and the session's subdirectory
listing setting follow the same rules as native Sail file sources.

```python
def partitions(self):
    store = get_object_store()
    return [InputPartition(meta.location) for meta in store.glob(self.options["path"])]
```

Results are `ObjectMeta` values, deduplicated and sorted by encoded location.
Their locations can be passed directly to `read()` or `read_ranges()` in worker
callbacks. No matches returns an empty list. Invalid patterns raise `ValueError`.
`max_entries` defaults to 10,000; exceeding it raises `ValueError` instead of
returning a partial result. The limit bounds returned metadata, while the shared
native listing cache may retain additional entries. Discovery uses the existing
callback cancellation scope. `list()` and `iter_objects()` retain literal prefix
semantics and do not apply file-discovery filtering.

### Bounded reads and listing

Each keyword argument controls one operation or iterator; defaults do not change
with CPU count or available RAM. Override them explicitly on the call:

| Operation                                 | Argument      | Default | Meaning and enforcement                                                                                                               |
| ----------------------------------------- | ------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `read()`, `read_range()`, `read_ranges()` | `max_bytes`   | 64 MiB  | Maximum returned bytes per call; for vectored reads, the sum of requested range lengths. Can be raised; 64 MiB is not a hard ceiling. |
| `read_ranges()`                           | `max_ranges`  | 1,024   | Maximum ranges per call; can be raised.                                                                                               |
| `list()`, `glob()`                        | `max_entries` | 10,000  | Maximum collected objects per call; can be raised. Exceeding it raises `ValueError`, rather than returning partial results.           |
| `iter_bytes()`                            | `chunk_size`  | 1 MiB   | Maximum bytes per yielded chunk; must be between 1 byte and the native hard ceiling of 64 MiB.                                        |
| `iter_objects()`                          | `batch_size`  | 128     | Maximum objects transferred from Rust per batch; must be between 1 and the native hard ceiling of 4,096 entries.                      |

One MiB is 1,048,576 bytes. Ranges use byte offsets and are half-open `[start, end)`;
empty ranges return empty bytes and reversed ranges are rejected.
Iterator chunk and batch ceilings do not limit the total object or listing size.
The native implementation enforces these ceilings even when a caller overrides
the Python defaults.

Use iterators when results may exceed those limits:

```python
store = get_object_store()
with store.iter_objects(prefix, batch_size=128) as objects:
    for obj in objects:
        with store.iter_bytes(obj.location, chunk_size=1024 * 1024) as chunks:
            for chunk in chunks:
                consume(chunk)
```

Listing transfers at most `batch_size` entries per call (maximum 4,096).
Byte iteration consumes one get response and yields at most `chunk_size` bytes
per call (maximum 64 MiB). Iterators close on exhaustion, error, or callback exit;
use `with` or `close()` when stopping early.

These bounds limit collected results, not total process memory. Provider buffers,
range coalescing, the current provider chunk, Python copies, and concurrent calls
can require additional memory. Retaining yielded objects or chunks also consumes
memory. Validate peak memory and throughput with representative providers,
object sizes, prefix sizes, and concurrency before increasing defaults.

### Choosing limits and concurrency

Pass storage limits directly to the operation that needs them. For example,
`store.read_ranges(path, ranges, max_bytes=128 * 1024 * 1024)` allows up to
128 MiB of requested ranges in that call. There are no application-wide defaults
for these storage arguments; datasource authors own the request size and how
partitions group files or row groups.

Application settings such as `python.data_source_write_channel_capacity` control
Sail's execution pipeline instead: that setting bounds the number of batches
queued for a Python writer, not the bytes returned by a storage read. Storage
access still uses Sail's runtime and registered clients; per-call tuning does
not create a second storage client or override its credentials.

A worker with 20 cores does not thereby enforce a limit of 20 in-flight storage
operations or an aggregate read budget. Even exactly 20 simultaneous reads at
64 MiB each can retain 1,280 MiB of returned data, before provider buffers,
Parquet decoding, and queued Arrow batches. Size requests and partition
concurrency together; the per-call limits are not worker-wide memory reservations.

### Cancellation and cleanup

Dropping a callback's execution future cancels its bridge I/O. Dropping a reader
stream also cancels its bridge I/O, and closing an iterator drops its storage
stream. Sail's runtime wrapper propagates cancellation to spawned I/O tasks.
Canceled bridge calls raise `InterruptedError`; this cannot forcibly stop arbitrary
Python code or guarantee rollback of a remote write that has already completed.
Writer abort runs in a fresh callback scope with a 30-second wait limit, so cleanup
can use storage even after the failing write scope has ended.

The bridge is validated with a Parquet reader using a small file-like adapter in
the tests. It is not yet wired into the Vortex data source: Vortex's Python API
accepts its own concrete store types, so using this bridge there needs a separate
compatible integration.

### More Examples

Please refer to the [Spark documentation](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html) for more Python data source examples, including how to define a batch writer. We will also add more examples to this guide in the future.
