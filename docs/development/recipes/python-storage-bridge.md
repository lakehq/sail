---
title: Internal Python Storage Bridge
rank: 85
---

# Internal Python Storage Bridge

`pysail.spark.datasource._object_store` is private machinery for developing Sail
storage adapters. Its `_get_object_store()`, `_ObjectStore`, and `_ObjectMeta`
interfaces may change with the implementation. They are not a supported public
Python filesystem API. Future fsspec, PyArrow, or format-specific adapters own
their consumer-facing interfaces.

## Ownership and installation

`sail-object-store` owns store registration, path resolution, and storage execution.
`sail-data-source` binds Python callbacks to the session or task `RuntimeEnv`.
Shared listing utilities own glob expansion, filtering, and listing-cache keys.
The bridge uses existing registered clients rather than constructing Python clients.

The private module must ship in the matching `pysail` package in each process
using the bridge, including standalone servers and workers. Ordinary PySpark
datasources can run without it. Binding skips an absent module or PySail parent
package, while unrelated import failures propagate.

## Callback lifetime

The bridge is bound during construction, schema inference, partition planning,
reads, writes, commit, and abort. Obtain the proxy within each callback; do not
serialize it in readers, writers, or partitions. Driver and worker registries are
process-local, so custom registrations must exist wherever callbacks execute.

Callback exit invalidates the proxy and closes its iterators. Close iterators
explicitly when stopping early. Dropping the enclosing execution future cancels
bridge I/O; it cannot preempt arbitrary running Python code or roll back completed
remote writes. Abort runs in a fresh scope and is awaited before reporting the
original write or commit failure, unless the enclosing operation is canceled.

## Adapter implementation constraints

Exact operations use literal paths; URL keys are percent-encoded and returned
metadata locations round-trip through storage operations. Use glob discovery
during partition planning. Format adapters own file and row-group partitioning;
ranged reads do not automatically split scans.

Reads materialize their requested bytes. Per-call byte and entry limits and
iterator transfer bounds are not aggregate worker memory budgets. Provider
buffers, listing caches, concurrent calls, and retained Python results can consume
additional memory. Defaults and native ceilings are documented beside the private
implementation.

The scope tests and Spark datasource tests cover lifecycle, paths, errors,
cancellation, cleanup, and cross-store discovery. The Parquet test's file-like
adapter is an internal integration fixture, not a shipped filesystem adapter.
