---
title: Performance Tuning
rank: 12
---

# Performance Tuning

Sail is designed to provide high performance for data processing workloads. However, several factors can affect the performance of your Sail applications. In this guide, we discuss common performance tuning techniques and best practices to help you optimize your Sail applications.

<!--@include: ./_common/options.md-->

## Where Settings Take Effect

Sail reads its configuration from environment variables when the Sail server starts, or in Python, when you create a `SparkConnectServer` instance.
Set the environment variables before that point.
In Python, Sail reads the following settings only once per process, when you create the first server.

- `runtime.stack_size`
- the `telemetry.*` options
- the `catalog.system.*` options
- `RUST_LOG`

In local mode, a single process plans and runs each query.
In cluster mode, the driver of each session plans the query inside the server process, and the workers run its tasks.
Options that shape the plan, such as `execution.default_parallelism` and the `optimizer.*` options, take effect on the server.
Options that limit resources, such as the memory pool and temporary files, apply to each process that runs tasks.
Set them for the workers as well.
On Kubernetes, refer to the [Cluster Configuration and Sizing](/guide/deployment/configuration) page for the options that the server passes to workers and how to set the others.
To confirm which options the server process uses, query the `system.session.options` table.
See [Observability](/guide/observability/#system-catalog).

## Differences from Spark

Some Spark tuning options and hints work differently in Sail.

| Spark option or hint                                       | Behavior in Sail                                                                                                   |
| ---------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `spark.sql.shuffle.partitions`                             | Sets only the default partition count of the `REPARTITION` hint. See [Parallelism and Batching](./parallelism.md). |
| `spark.sql.autoBroadcastJoinThreshold`                     | Has no effect. See [Join Strategies](./joins.md).                                                                  |
| `BROADCAST`, `MERGE`, `SHUFFLE_HASH`, and other join hints | Ignored. See [Join Strategies](./joins.md#join-hints).                                                             |
| `spark.sql.adaptive.*`                                     | Has no effect. Sail does not change the plan of a query while it runs.                                             |
| `spark.sql.parquet.compression.codec`                      | Has no effect. See [Data Layout and Caches](./data-layout.md#compression).                                         |

## Topics

<PageList :data="data" :prefix="['guide', 'performance']" />

For Python UDFs, the [Performance](/guide/udf/performance) page of the UDF guide compares the UDF types.
To build Sail with all features of your CPU enabled, refer to the [Installation](/introduction/installation/) guide.

<script setup>
import PageList from "@theme/components/PageList.vue";
import { data } from "./index.data.ts";
</script>
