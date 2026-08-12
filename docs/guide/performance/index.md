---
title: Performance Tuning
rank: 12
---

# Performance Tuning

Sail is designed to provide high performance for data processing workloads. However, several factors can affect the performance of your Sail applications. In this guide, we discuss common performance tuning techniques and best practices to help you optimize your Sail applications.

## Configuration Options

::: info
Here, we refer to configuration options by their names, such as `execution.batch_size`, but you can typically set these options via environment variables, such as `SAIL_EXECUTION__BATCH_SIZE`.
Refer to the [Configuration Guide](../configuration/) for more information on how Sail configuration works in general.
:::

`execution.batch_size` controls the number of rows processed in each batch during execution.
Increasing this value can improve performance by reducing the overhead of processing many small batches.
However, setting it too high may lead to potential out-of-memory errors.
Experiment with different batch sizes to find the optimal value for your workload.

`execution.default_parallelism` determines the default number of partitions for physical operators.
The default value `0` indicates that parallelism is determined based on the available CPU cores, which is a good default for local mode.
However, when running in cluster mode, this default parallelism is determined by the available CPU cores on the driver, which typically has only a small number of CPU cores.
Since this configuration option is in effect during physical planning on the driver, the default parallelism does not take into account the actual size of the cluster.
Therefore, in cluster mode, set this option explicitly so that the parallelism is appropriate for distributed task execution on the workers.

`cluster.worker_task_slots` controls the number of tasks that can run concurrently on each worker.
Note that within a task region, some tasks can share a slot if their stages belong to the same slot-sharing group.
The task slots track only logical task assignments rather than physical resource isolation. All tasks running on a worker compete for the same pool of CPU and memory resources.
We believe this simplification in resource management works well in cloud environments, where resource isolation can be achieved at the worker level using containers.
Setting this option to a lower value can help reduce contention for CPU and memory resources but may result in underutilization. It may also increase scheduling overhead, as more workers may be needed to run the same workload. Setting this to a higher value can improve resource sharing, but it means each worker will need more CPU and memory resources.

## Object Store Read Cache

Sail has an experimental, opt-in, per-process read-through cache for object stores. It is useful when a worker repeatedly reads the same remote Parquet, Iceberg, or Delta files. The cache splits reads into fixed-size pages, so overlapping byte ranges share cached data, and it coalesces concurrent misses for the same page.

Enable it before starting Sail:

```shell
export SAIL_OBJECT_STORE_CACHE=true
```

The following environment variables control the cache:

| Variable                                    |      Default | Meaning                                                                                             |
| ------------------------------------------- | -----------: | --------------------------------------------------------------------------------------------------- |
| `SAIL_OBJECT_STORE_CACHE`                   |     disabled | Enable with `1`, `true`, `yes`, or `on` (case-insensitive).                                         |
| `SAIL_OBJECT_STORE_CACHE_PAGE_SIZE`         |    `1048576` | Page size in bytes. Larger pages reduce remote requests; smaller pages reduce read amplification.   |
| `SAIL_OBJECT_STORE_CACHE_MEMORY`            | `1073741824` | Maximum weighted capacity of cached data pages, in bytes.                                           |
| `SAIL_OBJECT_STORE_CACHE_METADATA`          |   `67108864` | Maximum combined weighted capacity of cached object metadata and compact path identities, in bytes. |
| `SAIL_OBJECT_STORE_CACHE_METADATA_TTL_SECS` |         `60` | Seconds before Sail revalidates an object's metadata. Set to `0` to revalidate on every read.       |

The data and metadata capacities apply to each Sail process. In cluster mode, every worker has an independent cache, so budget memory as `capacity × worker count`. Cache contents are not shared between workers and do not survive worker termination.

### Consistency Model

Writes performed through the same Sail object-store wrapper invalidate the affected object before and after ordinary writes, after multipart completion, and around copy or rename operations. The second invalidation closes a race in which a reader could otherwise repopulate an old value while a write is in flight.

For writes performed by another process, Sail revalidates object size, modification time, ETag, and version after the metadata TTL. If the identity is unchanged, Sail retains the cached pages. If it changed, Sail changes the object's compact cache identity, making every old page unreachable in constant time. The TTL is therefore the maximum expected stale interval for external overwrites. Data-lake data and metadata files are normally immutable; use a shorter TTL for workloads that overwrite paths in place.

Conditional requests, version-specific requests, and requests with backend-specific extensions bypass the cache so the underlying object store preserves their exact semantics. The current implementation uses Foyer's in-memory cache only; it does not persist data to disk.

### Tuning Guidance

- Keep the 1 MiB page default for mixed Parquet workloads unless request metrics show either many tiny reads or excessive request counts.
- Increase the page size for high-latency remote storage and broad sequential scans. Decrease it for narrow projections that repeatedly read only a few kilobytes from each file.
- Size the data cache for the reusable working set, leaving headroom for query execution, Arrow batches, and the operating system.
- Use a nonzero metadata TTL for immutable data-lake files. Use `0` only when every read must observe an external in-place overwrite immediately, since it adds one metadata request per object read.
- The cache is unlikely to help one-pass scans, frequently replaced workers, or workloads whose reusable working set is much larger than the configured capacity.
