---
title: Benchmark Results
rank: 5
---

# Benchmark Results

We ran a derived TPC-H benchmark to compare the performance and resource efficiency of Sail and Apache Spark.
The benchmark consists of 22 queries that cover a wide range of SQL operations, including filters, joins, aggregations, and subqueries.

## Setup

- **Dataset Size:** Scale factor 100 (100 GB raw data)
- **Dataset Format:** Parquet
- **Host:** AWS EC2 `r8g.4xlarge` (16 vCPU, 128 GB RAM)
- **Disk:** Separate EBS volumes for data and Spark temporary files (4000 IOPS, 1000 MB/s throughput)

The experiments were run using Sail 0.7.0 and Spark 4.2.0.
The data was generated using [`tpchgen-cli`](https://pypi.org/project/tpchgen-cli). The partition count is 64, though we did not observe performance differences with other partition counts.

::: info

- The `optimizer.enable_join_reorder` configuration option is turned on for Sail. This option is currently experimental, and we plan to enable this by default once the optimizer is more stable.
- The Sail server is built from source as a [standalone binary](/development/recipes/standalone-binary).
- Previously, the TPC-H data was generated using the `dbgen` tool followed by conversion to Parquet. We now use the `tpchgen-cli` tool, and we noticed that the dataset size in Parquet format is different. This is a reason why the baseline Spark query times are different from the previous benchmark results.

:::

## Key Findings

| Metric                     | Spark          | Sail              |
| -------------------------- | -------------- | ----------------- |
| Total Query Time           | 534.78 seconds | **52.81 seconds** |
| Query Speed-Up             | 0% (baseline)  | **176% – 2819%**  |
| Peak Memory Usage          | 72 GB          | **26 GB**         |
| Disk Write (Shuffle Spill) | 115 GB         | **0 GB**          |

From the results, we can see that Sail completes the workload 10x faster than Spark. The timing difference alone translates to a 90% cost reduction. Moreover, note that Sail can comfortably fit within an instance of 32 GB memory for this experiment. If we assume that memory is the dominant instance-sizing and pricing factor, the hardware size contributes another factor of 4x so the total cost reduction is 1 − 1/10 × 1/4, or nearly 98%.

## Detailed Results

### Query Time

The following figure shows a query time comparison between Sail and Spark for individual queries.

<SvgDiagram :svg="data['query-time.vega.json']" />

The following figure shows sorted relative improvements of Sail over Spark for each query.

<SvgDiagram :svg="data['query-speed-up.vega.json']" />

### Resource Utilization

We analyze memory and disk usage during query execution, using AWS CloudWatch metrics with 1-second resolution.

The following figure shows that Spark peaked at approximately 72 GB of memory during query execution and spilled to disk for shuffle operations. Despite abundant available memory, Spark wrote approximately 115 GB of temporary data, peaking at over 32 GB in a rolling minute.

<SvgDiagram :svg="data['resource-utilization.vega.json']['spark']" />

In contrast, the following figure shows drastically different resource consumption characteristics of Sail. At peak, Sail utilized approximately 26 GB of memory. Sail released memory after executing each query and recorded zero disk writes, relying solely on the available memory for computation.

<SvgDiagram :svg="data['resource-utilization.vega.json']['sail']" />

<script setup lang="ts">
import SvgDiagram from "@theme/components/SvgDiagram.vue";
import { data } from "./index.data.ts";
</script>
