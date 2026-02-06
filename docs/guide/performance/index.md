---
title: Performance Tuning
rank: 11
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
It is recommended to experiment with different batch sizes to find the optimal value for your workload.

`execution.default_parallelism` determines the default number of partitions for physical operators.
The default value `0` indicates that parallelism is determined based on the available CPU cores, which is a good default for local mode.
However, when running in cluster mode, this default parallelism is determined by the available CPU cores on the driver, which typically has only a small number of CPU cores.
Since this configuration option is in effect during physical planning on the driver, the default parallelism does not take into account the actual size of the cluster.
Therefore, in cluster mode, it is recommended to set this option explicitly so that the parallelism is appropriate for distributed task execution on the workers.

`cluster.worker_task_slots` controls the number of tasks that can run concurrently on each worker.
Note that within a task region, some tasks can share a slot if their stages belong to the same slot-sharing group.
The task slots track only logical task assignments rather than physical resource isolation. All tasks running on a worker compete for the same pool of CPU and memory resources.
We believe this simplification in resource management works well in cloud environments, where resource isolation can be achieved at the worker level using containers.
Setting this option to a lower value can help reduce contention for CPU and memory resources but may result in underutilization. It may also increase scheduling overhead, as more workers may be needed to run the same workload. Setting this to a higher value can improve resource sharing, but it means each worker will need more CPU and memory resources.
