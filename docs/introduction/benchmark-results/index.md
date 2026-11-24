---
title: Benchmark Results
rank: 5
---

# Benchmark Results

We publish results for two derived TPC benchmarks, which are not official TPC results.

- [TPC-H on a single machine](#tpc-h-on-a-single-machine) compares Sail and Apache Spark on the same host, with 22 queries over 100 GB of data. Sail completes the workload 10x faster than Spark, with lower peak memory usage and zero disk writes.
- [TPC-DS on a cluster](#tpc-ds-on-a-cluster) measures distributed Sail on eight workers, with 99 queries over 1 TB and 3 TB of data. Sail completes the workload in 444 seconds at 1 TB and 1289 seconds at 3 TB.

## TPC-H on a Single Machine

We ran a derived TPC-H benchmark to compare the performance and resource efficiency of Sail and Apache Spark.
The benchmark consists of 22 queries that cover a wide range of SQL operations, including filters, joins, aggregations, and subqueries.

### Setup

- **Dataset Size:** Scale factor 100 (100 GB raw data)
- **Dataset Format:** Parquet
- **Host:** AWS EC2 `r8g.4xlarge` (16 vCPU, 128 GB RAM)
- **Disk:** Separate EBS volumes for data and Spark temporary files (4000 IOPS, 1000 MB/s throughput)

The experiments were run using Sail 0.7.0 and Spark 4.2.0.
The data was generated using [`tpchgen-cli`](https://pypi.org/project/tpchgen-cli). The partition count is 64, though we did not observe performance differences with other partition counts.

::: info

- The `optimizer.enable_join_reorder` configuration option is turned on for Sail.
- The Sail server is built from source as a [standalone binary](/development/recipes/standalone-binary).
- Previously, the TPC-H data was generated using the `dbgen` tool followed by conversion to Parquet. We now use the `tpchgen-cli` tool, and we noticed that the dataset size in Parquet format is different. This is a reason why the baseline Spark query times are different from the previous benchmark results.

:::

### Key Findings

| Metric                     | Spark          | Sail              |
| -------------------------- | -------------- | ----------------- |
| Total Query Time           | 534.78 seconds | **52.81 seconds** |
| Query Speed-Up             | 0% (baseline)  | **176% – 2819%**  |
| Peak Memory Usage          | 72 GB          | **26 GB**         |
| Disk Write (Shuffle Spill) | 115 GB         | **0 GB**          |

From the results, we can see that Sail completes the workload 10x faster than Spark. The timing difference alone translates to a 90% cost reduction. Moreover, note that Sail can comfortably fit within an instance of 32 GB memory for this experiment. If we assume that memory is the dominant instance-sizing and pricing factor, the hardware size contributes another factor of 4x so the total cost reduction is 1 − 1/10 × 1/4, or nearly 98%.

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

## TPC-DS on a Cluster

We ran a derived TPC-DS benchmark to measure the distributed performance of Sail at terabyte scale.
The benchmark consists of 99 queries that cover a wide range of SQL operations, including multi-way joins, window functions, rollups, and common table expressions.

### Setup

- **Dataset Size:** Scale factors 1000 and 3000 (1 TB and 3 TB raw data)
- **Dataset Format:** Parquet on Amazon S3 (Snappy compression, 128 MB row groups)
- **Workers:** Eight AWS EC2 `r8gn.8xlarge` instances (32 vCPU, 256 GB RAM, 100 Gbps network each)
- **Driver:** AWS EC2 `r8g.xlarge` (4 vCPU, 32 GB RAM)
- **Disk:** One gp3 root volume per node, with no instance storage or separate volume for temporary files (16 GB, 3000 IOPS, 125 MB/s throughput)
- **Network:** One availability zone, with ENA Express enabled and no placement group

The experiments were run using Sail built from commit [`99ee46f`](https://github.com/lakehq/sail/commit/99ee46f69a97342e91bf7d4eaedb4509f1d8a9c2), on Amazon EKS through the LakeSail Platform.
The data was generated using a modified [`tpchgen-rs`](https://github.com/clflushopt/tpchgen-rs) that writes partitioned data. The fact tables and the `customer` table are partitioned, and the other dimension tables have one partition each.

| Scale Factor | Parquet Data Size | Partition Count | File Count |
| ------------ | ----------------- | --------------- | ---------- |
| 1000         | 430.9 GB          | 256             | 2065       |
| 3000         | 1.3 TB            | 768             | 6161       |

The queries are from [SQLBench-DS](https://github.com/sql-benchmarks/sqlbench-ds). Queries q14, q23, q24, and q39 have two statements each, and their time is the sum of both.
The queries ran sequentially from a PySpark client over Spark Connect, on an `r8g.xlarge` instance in the same availability zone.
Each time is measured at the client from submission until the results arrive with `toArrow()`, so it includes planning, execution, and result transfer.

::: info

- The `optimizer.enable_join_reorder` and `optimizer.enable_window_topn` configuration options are turned on for Sail.
- Shuffle data is streamed between workers over Arrow Flight without being written to disk, as described in the [blog post](https://lakesail.com/blog/tpcds-benchmark-2026/).
- Directory listings, Parquet metadata, and Parquet statistics are cached between queries. Table data is read from S3 for every query.
- The results are from the best complete run at each scale. Only query time was measured, with no memory, disk, or network metrics.

:::

::: details Sail Configuration

These settings differ from the defaults, and the shuffle backend is the default `flight`. See the [configuration reference](/reference/configuration/) for details.

| Setting                                                         | This Run | Default |
| --------------------------------------------------------------- | -------- | ------- |
| `cluster.shuffle_backend.flight.compression`                    | `lz4`    | `zstd`  |
| `cluster.shuffle_backend.flight.connection_count`               | 4        | 1       |
| `cluster.shuffle_backend.flight.initial_connection_window_size` | 67108864 | 0       |
| `cluster.shuffle_backend.flight.initial_stream_window_size`     | 8388608  | 0       |
| `cluster.task_stream_creation_timeout_secs`                     | 300      | 60      |
| `cluster.worker_max_idle_time_secs`                             | 7200     | 60      |
| `cluster.worker_task_slots`                                     | 96       | 8       |
| `execution.batch_size`                                          | 262144   | 8192    |
| `execution.default_parallelism`                                 | 256      | 0       |
| `execution.file_listing_cache.max_entries`                      | 0        | 10000   |
| `execution.file_listing_cache.ttl`                              | 7200     | 1800    |
| `execution.file_listing_cache.type`                             | `global` | `none`  |
| `optimizer.enable_join_reorder`                                 | `true`   | `false` |
| `optimizer.enable_window_topn`                                  | `true`   | `false` |
| `parquet.file_metadata_cache.ttl`                               | 7200     | 1800    |
| `parquet.file_statistics_cache.max_entries`                     | 0        | 10000   |
| `parquet.file_statistics_cache.ttl`                             | 7200     | 1800    |
| `spark.session_timeout_secs`                                    | 7200     | 900     |

A value of 0 means the transport default for the window sizes, the number of CPU cores for the parallelism, and no limit for `max_entries`.

:::

### Key Findings

| Metric                   | 1 TB                | 3 TB                 |
| ------------------------ | ------------------- | -------------------- |
| Total Query Time         | 444.32 seconds      | 1288.77 seconds      |
| Median Query Time        | 2.62 seconds        | 7.80 seconds         |
| Queries Under 10 Seconds | 90 of 99            | 69 of 99             |
| Longest Query            | 38.12 seconds (q23) | 137.58 seconds (q85) |

From the results, we can see that the total query time grows 2.9x when the data grows 3x. At 1 TB, 78 of the 99 queries finish in under 5 seconds. At 3 TB, q85, q23, and q14 together take 336 seconds, about a quarter of the total.

### Query Time

The following figure shows the query time for individual queries at 1 TB.

<SvgDiagram :svg="data['tpcds-query-time.vega.json']['1tb']" />

The following figure shows the query time for individual queries at 3 TB.

<SvgDiagram :svg="data['tpcds-query-time.vega.json']['3tb']" />

::: details Per-Query Times

| Query | 1 TB (seconds) | 3 TB (seconds) |
| ----- | -------------- | -------------- |
| q1    | 1.27           | 2.74           |
| q2    | 1.87           | 5.98           |
| q3    | 2.46           | 8.14           |
| q4    | 13.76          | 36.82          |
| q5    | 3.89           | 11.15          |
| q6    | 1.75           | 5.43           |
| q7    | 3.27           | 10.79          |
| q8    | 2.24           | 5.84           |
| q9    | 6.32           | 15.42          |
| q10   | 1.57           | 4.59           |
| q11   | 6.79           | 18.50          |
| q12   | 1.36           | 3.80           |
| q13   | 4.44           | 12.26          |
| q14   | 33.75          | 92.09          |
| q15   | 1.48           | 5.74           |
| q16   | 3.36           | 0.91           |
| q17   | 8.35           | 5.09           |
| q18   | 3.54           | 7.99           |
| q19   | 2.62           | 7.80           |
| q20   | 1.85           | 5.51           |
| q21   | 0.75           | 1.11           |
| q22   | 0.69           | 0.77           |
| q23   | 38.12          | 106.09         |
| q24   | 10.78          | 24.50          |
| q25   | 4.93           | 12.11          |
| q26   | 2.57           | 6.77           |
| q27   | 3.40           | 9.27           |
| q28   | 6.17           | 17.26          |
| q29   | 4.53           | 10.10          |
| q30   | 1.38           | 2.26           |
| q31   | 4.64           | 8.76           |
| q32   | 1.81           | 5.44           |
| q33   | 2.58           | 7.83           |
| q34   | 1.62           | 4.40           |
| q35   | 1.61           | 3.76           |
| q36   | 2.34           | 8.39           |
| q37   | 0.88           | 3.93           |
| q38   | 2.05           | 5.27           |
| q39   | 1.36           | 1.74           |
| q40   | 3.34           | 7.57           |
| q41   | 0.24           | 0.42           |
| q42   | 1.74           | 7.21           |
| q43   | 1.67           | 6.14           |
| q44   | 3.32           | 8.64           |
| q45   | 1.35           | 3.84           |
| q46   | 3.26           | 7.33           |
| q47   | 8.15           | 19.75          |
| q48   | 2.64           | 9.49           |
| q49   | 4.65           | 12.75          |
| q50   | 4.85           | 13.04          |
| q51   | 3.91           | 9.36           |
| q52   | 1.79           | 7.32           |
| q53   | 1.76           | 6.60           |
| q54   | 7.00           | 22.47          |
| q55   | 5.23           | 7.13           |
| q56   | 2.35           | 7.86           |
| q57   | 4.66           | 10.04          |
| q58   | 7.24           | 16.09          |
| q59   | 1.83           | 6.64           |
| q60   | 2.77           | 8.43           |
| q61   | 2.67           | 8.18           |
| q62   | 0.90           | 2.82           |
| q63   | 1.71           | 6.24           |
| q64   | 16.24          | 47.56          |
| q65   | 4.64           | 13.62          |
| q66   | 2.33           | 6.93           |
| q67   | 12.80          | 29.41          |
| q68   | 3.84           | 11.06          |
| q69   | 1.56           | 4.25           |
| q70   | 9.04           | 32.04          |
| q71   | 2.59           | 8.58           |
| q72   | 11.70          | 9.21           |
| q73   | 1.58           | 4.14           |
| q74   | 5.67           | 12.67          |
| q75   | 9.81           | 26.70          |
| q76   | 3.53           | 9.95           |
| q77   | 3.46           | 8.23           |
| q78   | 17.69          | 52.02          |
| q79   | 3.10           | 8.86           |
| q80   | 15.63          | 41.38          |
| q81   | 2.32           | 2.79           |
| q82   | 1.72           | 3.35           |
| q83   | 1.20           | 2.27           |
| q84   | 0.80           | 1.11           |
| q85   | 1.95           | 137.58         |
| q86   | 1.17           | 3.20           |
| q87   | 2.20           | 5.35           |
| q88   | 3.62           | 6.75           |
| q89   | 2.48           | 7.66           |
| q90   | 1.47           | 2.50           |
| q91   | 0.78           | 1.26           |
| q92   | 1.22           | 2.72           |
| q93   | 5.37           | 16.71          |
| q94   | 2.64           | 5.74           |
| q95   | 4.99           | 14.81          |
| q96   | 1.09           | 3.34           |
| q97   | 2.25           | 8.47           |
| q98   | 2.67           | 8.90           |
| q99   | 1.94           | 5.97           |

:::

<script setup lang="ts">
import SvgDiagram from "@theme/components/SvgDiagram.vue";
import { data } from "./index.data.ts";
</script>
