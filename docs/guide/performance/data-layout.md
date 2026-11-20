---
title: Data Layout and Caches
rank: 4
---

# Data Layout and Caches

<!--@include: ./_common/options.md-->

The way Parquet files are written affects how fast Sail can read them.
The [configuration reference](/reference/configuration/#parquet) lists every Parquet option.

## Row Groups

A Parquet file stores rows in one or more **row groups**.
Sail divides a Parquet scan into partitions at row group boundaries.
The exceptions are a scan with a known sort order and a scan that takes a `LIMIT` directly, such as `df.limit(10)` on a read.
A scan runs with at most the smallest of the following numbers of partitions.

- the number of row groups in the files that it reads
- `execution.default_parallelism`
- the number of MiB of compressed data in the row groups that it reads, rounded up

The compressed data size counts all columns, not only the columns that the query reads.
Sail reads a file with a single large row group in a single task.
Sail also removes the round-robin repartitioning that would follow such a scan.
The operators after the scan then run with the same number of partitions until the next repartitioning, such as for a join or an aggregation.
If any file in a Delta Lake scan has a deletion vector, Sail reads each file in one partition instead of dividing it at row group boundaries.

`parquet.max_row_group_size` sets the maximum number of rows in each row group that Sail writes, and defaults to `1048576`.
Larger row groups require more memory to write, but can get better compression and be faster to read.
Smaller row groups give a later scan more units to read in parallel.
You can set the row group size for a single write with the `maxRowGroupSize` option.

Unlike Spark, when you write files to a path with `df.write.parquet(path)` or a similar method, the number of files does not follow the number of partitions.
Such a write merges all partitions into a single stream.
In cluster mode, a single task writes the files.
Without `partitionBy`, Sail sends the batches of that stream round-robin to up to four files, which it writes in parallel.
Sail replaces each file with a new one after about 50,000,000 rows.
For example, `df.repartition(16).write.parquet(path)` produces at most four files rather than sixteen, and very small data can produce fewer.
Delta Lake and Iceberg writes instead write files from each partition in parallel.
An Iceberg copy-on-write `DELETE`, `UPDATE`, or `MERGE` writes from a single partition when the write distribution mode of the table is `range`.

```python
df.write.option("maxRowGroupSize", 131072).parquet("/data/orders")
```

## Compression

`parquet.compression` sets the compression codec for the Parquet files that Sail writes, and defaults to `zstd(3)`.
The `compression` option, or its alias `codec`, sets the codec for a single write.
A codec name without a level uses the default level of the Parquet writer, so `zstd` writes with level 1 rather than level 3.
Specify the level explicitly, such as `zstd(3)`, to keep the same level as the default codec.

```python
df.write.option("compression", "zstd(3)").parquet("/data/orders")
```

Sail does not use the `spark.sql.parquet.compression.codec` Spark configuration.

## Reading

`parquet.pushdown_filters` applies filters while the Parquet reader decodes data, which reduces the number of rows decoded.
This optimization is sometimes called late materialization, and it is disabled by default.
When you enable it, you can also enable `parquet.reorder_filters`, which lets the reader reorder the filters to reduce the cost of evaluating them.

`parquet.enable_page_index` reads the page index of a file if the file has one, which can reduce I/O and the number of rows decoded.
`parquet.bloom_filter_on_read` uses the bloom filters stored in a file to skip row groups.
Both options are enabled by default.
Sail writes bloom filters only when you enable `parquet.bloom_filter_on_write`, or the `bloom_filter_on_write` option for a single write.

## Read Caches

Sail keeps three configurable caches to avoid repeating the same storage requests for every query.

| Option                               | Default  | Contents                                       |
| ------------------------------------ | -------- | ---------------------------------------------- |
| `execution.file_listing_cache.type`  | `none`   | File listings of the paths a query reads       |
| `parquet.file_statistics_cache.type` | `global` | Statistics collected from Parquet files        |
| `parquet.file_metadata_cache.type`   | `global` | Metadata read from the footer of Parquet files |

The `type` option of each cache takes one of the following values.

- `none` disables the cache.
- `global` uses one cache for all sessions in a process.
- `session` uses a separate cache for each session.

In `local-cluster` mode, the workers of a session share one set of caches, separate from the caches of the server.
In cluster mode, the server and each worker keep their own caches.

The two Parquet caches compare each entry with the size and modification time of the file in the current file listing.
Sail therefore reads a file again after it changes, unless the file listing cache still holds the old listing.
The file listing cache does not check the storage.
Sail refreshes an entry only after it expires.
Entries expire after `execution.file_listing_cache.ttl` seconds, which defaults to `1800`.
Until then, a query does not see new files under a cached path.
A query that reads a deleted file fails.
Enable the file listing cache only for data that changes less often than its TTL.

For each cache, setting `ttl`, `max_entries`, or `size_limit` to `0` removes that limit rather than disabling the cache.
`parquet.file_metadata_cache.size_limit` defaults to `0`.
The metadata cache therefore has no size limit by default.

Refer to the [Catalog](/guide/catalog/) guide for the separate caches that catalogs keep.
