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
- **Disk:** Separate EBS volumes for data and Spark temporary files (4,000 IOPS, 1000 MB/s throughput)

## Key Findings

| Metric                     | Spark            | Sail                 |
| -------------------------- | ---------------- | -------------------- |
| Total Query Time           | 387.36 seconds   | **102.75 seconds**   |
| Query Speed-Up             | 0% (baseline)    | **43% - 727%**       |
| Peak Memory Usage          | 54 GB (constant) | **22 GB** (1 second) |
| Disk Write (Shuffle Spill) | > 110 GB         | **0 GB**             |

From the results, we can see that Sail completes the workload nearly 4x faster than Spark,
and Sail can run on 1/4 the instance size, leading to up to 94% cost reduction.
Sail can handle larger datasets on the same hardware or achieve similar performance on smaller, cheaper infrastructure.

## Detailed Results

### Query Time

The following figure shows query time comparison between Sail and Spark for individual queries.

<SvgDiagram :svg="data['query-time.vega.json']" />

The following figure shows sorted relative improvements of Sail over Spark for each query.

<SvgDiagram :svg="data['query-speed-up.vega.json']" />

### Resource Utilization

We analyze memory and disk usage during query execution, using AWS CloudWatch metrics with 1-second resolution.

The following figure shows that Spark consumed about 54 GB of memory during query execution, and spilled to disk for shuffle operations. Despite of abundant available memory, Spark wrote over 110 GB of temporary data, peaking at over 46 GB in a rolling minute.

<SvgDiagram :svg="data['resource-utilization.vega.json']['spark']" />

In contrast, the following figure shows drastically different resource consumption characteristics of Sail. At peak, Sail utilized approximately 22 GB of memory, but this usage lasted for only one second. Sail released memory after executing each query and had zero disk usage, relying solely on the available memory for computation.

<SvgDiagram :svg="data['resource-utilization.vega.json']['sail']" />

<script setup lang="ts">
import SvgDiagram from "@theme/components/SvgDiagram.vue";
import { data } from "./index.data.ts";
</script>
