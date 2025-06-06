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

More details on the benchmark results are available in the [blog post](https://lakesail.com/blog/supercharge-spark/).
