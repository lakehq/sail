---
title: Parallelism and Batching
rank: 1
---

# Parallelism and Batching

<!--@include: ./_common/options.md-->

Sail splits the data of a query into **partitions** and processes them in parallel.
It processes each partition in **batches** of rows.

## Batch Size

`execution.batch_size` controls the number of rows processed in each batch during execution.
Increasing this value can improve performance by reducing the overhead of processing many small batches.
However, setting it too high may lead to potential out-of-memory errors.
Experiment with different batch sizes to find the optimal value for your workload.

On Kubernetes, the server passes this value to the workers it launches.
See [Cluster Configuration and Sizing](/guide/deployment/configuration#settings-passed-to-workers).

## Default Parallelism

`execution.default_parallelism` determines the default number of partitions for physical operators.
The default value `0` indicates that parallelism is determined based on the available CPU cores, which is a good default for local mode.
However, when running in cluster mode, this default parallelism is determined by the available CPU cores on the driver, which typically has only a small number of CPU cores.
Since this configuration option is in effect during physical planning on the driver, the default parallelism does not take into account the actual size of the cluster.
Therefore, in cluster mode, set this option explicitly so that the parallelism is appropriate for distributed task execution on the workers.

This option also sets the number of partitions that Sail creates when it repartitions data for joins and aggregations.
Sail does not use `spark.sql.shuffle.partitions` for this purpose.
In cluster mode, some joins use other partition counts.
See [Join Strategies](./joins.md#cluster-mode).

## Explicit Repartitioning

You can change the partitioning of a DataFrame with the following methods.

| Method                     | Result                                                                    |
| -------------------------- | ------------------------------------------------------------------------- |
| `df.repartition(n)`        | `n` partitions, with rows distributed in round-robin order                |
| `df.repartition(n, *cols)` | `n` partitions, with rows hash-partitioned by `cols`                      |
| `df.repartition(*cols)`    | Rows hash-partitioned by `cols`, with as many partitions as the input has |
| `df.coalesce(n)`           | Existing partitions merged into at most `n` partitions                    |

Unlike Spark, `df.repartition(*cols)` keeps the partition count of its input instead of using `spark.sql.shuffle.partitions`.
Sail takes this count when it plans the query, before it adds partitions for parallelism.
So `df.repartition(*cols)` on data read from a single file produces one partition, even after filters, projections, or aggregations.
`df.repartitionByRange()` currently hash-partitions the data, so the output partitions are not ordered by range.

In SQL, Sail supports the `REPARTITION` and `COALESCE` hints.

```sql
SELECT /*+ REPARTITION(16, customer_id) */ * FROM orders
```

The `REPARTITION` hint accepts an optional partition count followed by column names.
Without a partition count, it uses `spark.sql.shuffle.partitions`, which defaults to `200`.
The `COALESCE` hint requires a partition count.
Sail ignores other partitioning hints, such as `REPARTITION_BY_RANGE` and `REBALANCE`, and logs a warning on the server.

## Task Slots

`cluster.worker_task_slots` controls the number of tasks that can run concurrently on each worker.
Note that within a task region, some tasks can share a slot if their stages belong to the same slot-sharing group.
The task slots track only logical task assignments rather than physical resource isolation. All tasks running on a worker compete for the same pool of CPU and memory resources.
We believe this simplification in resource management works well in cloud environments, where resource isolation can be achieved at the worker level using containers.
Setting this option to a lower value can help reduce contention for CPU and memory resources but may result in underutilization. It may also increase scheduling overhead, as more workers may be needed to run the same workload. Setting this to a higher value can improve resource sharing, but it means each worker will need more CPU and memory resources.

Refer to the [Shuffle Backends](/guide/deployment/shuffle) page for task regions and how each shuffle backend schedules them.

## Row Count Estimates

`execution.use_row_number_estimates_to_optimize_partitioning` controls whether Sail trusts estimated row counts when it decides to spread an input across more partitions.
The option is disabled by default.
When it is disabled, Sail treats any estimated row count, such as the row count of a filter output, as large enough to justify repartitioning.
When the option is enabled, Sail considers such an input worth repartitioning only if the estimate exceeds `execution.batch_size`.
Enabling it can reduce overhead for queries that read little data.
For Parquet scans, Sail removes this repartitioning regardless of the option.
The option still decides whether Sail splits a scan with an estimated row count into more partitions.
