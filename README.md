# Sail

[![Build Status](https://github.com/lakehq/sail/actions/workflows/build.yml/badge.svg?branch=main&event=push)](https://github.com/lakehq/sail/actions)
[![Codecov](https://codecov.io/gh/lakehq/sail/graph/badge.svg)](https://app.codecov.io/gh/lakehq/sail)
[![PyPI Release](https://img.shields.io/pypi/v/pysail)](https://pypi.org/project/pysail/)
[![Static Slack Badge](https://img.shields.io/badge/slack-LakeSail_Community-3762E0?logo=slack)](https://www.launchpass.com/lakesail-community/free)

Sail is a **drop-in Apache Spark replacement** written in Rust, unifying batch processing, stream processing, and compute-intensive AI workloads on a distributed, multimodal compute engine.

- **Compatible** with the Spark Connect protocol, supporting the Spark SQL and DataFrame API with no code rewrites required.
- **100% Rust-native** with no JVM overhead, delivering memory safety, instant startup, and predictable performance.
- **~4× faster** (up to 8× in specific workloads) than Spark and **94% cheaper** on infrastructure costs. See **[derived TPC-H benchmarks](#benchmark-results)**.
- **Proven on [ClickBench](https://go.lakesail.com/clickbench)**, outperforming Spark, popular Spark accelerators, Databricks, and Snowflake.

## Documentation

The documentation of the latest Sail version can be found [here](https://docs.lakesail.com/sail/latest/).

## Installation

### Quick Start

Sail is available as a Python package on PyPI. You can install it along with PySpark in your Python environment.

```bash
pip install pysail
pip install "pyspark-client"
```

### Advanced Use Cases

You can install Sail from source to optimize performance for your specific hardware architecture. The detailed [Installation Guide](https://docs.lakesail.com/sail/latest/introduction/installation/) walks you through this process step-by-step.

If you need to deploy Sail in production environments, the [Deployment Guide](https://docs.lakesail.com/sail/latest/guide/deployment/) provides comprehensive instructions for deploying Sail on Kubernetes clusters and other infrastructure configurations.

## Getting Started

### Starting the Sail Server

**Option 1: Command Line Interface.** You can start the local Sail server using the `sail` command.

```bash
sail spark server --port 50051
```

**Option 2: Python API.** You can start the local Sail server using the Python API.

```python
from pysail.spark import SparkConnectServer

server = SparkConnectServer(port=50051)
server.start(background=False)
```

**Option 3: Kubernetes.** You can deploy Sail on Kubernetes and run Sail in cluster mode for distributed processing.
Please refer to the [Kubernetes Deployment Guide](https://docs.lakesail.com/sail/latest/guide/deployment/kubernetes.html) for instructions on building the Docker image and writing the Kubernetes manifest YAML file.

```bash
kubectl apply -f sail.yaml
kubectl -n sail port-forward service/sail-spark-server 50051:50051
```

### Connecting to the Sail Server

Once you have a running Sail server, you can connect to it in PySpark.
No changes are needed in your PySpark code!

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
spark.sql("SELECT 1 + 1").show()
```

Please refer to the [Getting Started](https://docs.lakesail.com/sail/latest/introduction/getting-started/) guide for further details.

## Spark Compatibility

Sail is designed to be compatible with Spark 3.5.x, Spark 4.x, and later versions. Existing PySpark code works out of the box once you connect your Spark client session to Sail over the Spark Connect protocol.

As a starting point, Sail ships with an **experimental** [PySpark function compatibility check script](https://docs.lakesail.com/sail/latest/introduction/migrating-from-spark/#check-your-code-for-compatibility) that scans your codebase for PySpark functions and reports their Sail support status.

```bash
python -m pysail.examples.spark.compatibility_check <directory>
```

> **Experimental** Use the script as a rough first pass only. The script checks whether referenced PySpark functions are _implemented_ in Sail. It does **not** verify behavioral parity. It looks for functions used in DataFrame operations but does **not** cover Spark SQL strings.

See the [Migration Guide](https://docs.lakesail.com/sail/latest/introduction/migrating-from-spark/) for recommended migration practices.

## Feature Highlights

### Lakehouse Formats and Catalog Providers

Sail provides native support for the [Delta Lake](https://docs.lakesail.com/sail/latest/guide/sources/delta/) and [Apache Iceberg](https://docs.lakesail.com/sail/latest/guide/sources/iceberg/) table formats.
It integrates with catalog providers including Apache Iceberg REST Catalog, AWS Glue, Unity Catalog, Hive Metastore, and Microsoft OneLake.

For more details on usage and best practices, see the [Data Sources Guide](https://docs.lakesail.com/sail/latest/guide/sources/) and [Catalog Guide](https://docs.lakesail.com/sail/latest/guide/catalog/).

### Storage

Sail supports a variety of storage backends for reading and writing data, including:

- AWS S3
- Azure
- Hugging Face
- Cloudflare R2
- Google Cloud Storage
- HDFS
- File systems
- HTTP/HTTPS
- In-memory storage

See the [Storage Guide](https://docs.lakesail.com/sail/latest/guide/storage/) for more details.

## Why Choose Sail?

For over 15 years, Spark has been the default engine for distributed data processing, powering ETL, machine learning, and analytics pipelines across nearly every industry.

But the JVM foundation that made Spark possible is now what holds it back. Sail is built to be a familiar, performant alternative without the JVM tax.

### Sail is Spark-compatible

Sail offers a drop-in replacement for Spark SQL and the Spark DataFrame API. Existing PySpark code works out of the box once you connect your Spark client session to Sail over the Spark Connect protocol.

- **Spark SQL Dialect Support.** A custom Rust parser (built with parser combinators and Rust procedural macros) covers Spark SQL syntax with production-grade accuracy.
- **DataFrame API Support.** Spark DataFrame operations run on Sail with identical semantics.
- **Python UDF, UDAF, UDWF, and UDTF Support.** Python, Pandas, and Arrow UDFs all follow the same conventions as Spark.

### Sail’s Advantages over Spark

- **Rust-Native Engine.** No garbage collection pauses, no JVM memory tuning, and low memory footprint.
- **Columnar Format and Vectorized Execution.** Built on top of [Apache Arrow](https://github.com/apache/arrow) and [Apache DataFusion](https://github.com/apache/datafusion), the columnar in-memory format and SIMD instructions unlock blazing-fast query execution.
- **Lightning-Fast Python UDFs.** Python code runs inside Sail with zero serialization overhead as Arrow array pointers enable zero-copy data sharing.
- **Performant Data Shuffling.** Workers exchange Arrow columnar data directly, minimizing shuffle costs for joins and aggregations.
- **Lightweight, Stateless Workers.** Workers start in seconds, consume only a few megabytes of memory at idle, and scale elastically to cut cloud costs and simplify operations.
- **Concurrency and Memory Safety You Can Trust.** Rust’s ownership model prevents null pointers, race conditions, and unsafe memory access for unmatched reliability.

Ready to bring your existing workloads over? Our [Migration Guide](https://docs.lakesail.com/sail/latest/introduction/migrating-from-spark/) shows you how.

## Benchmark Results

Derived TPC-H results show that Sail outperforms Apache Spark in every query:

- **Execution Time**: ~4× faster across diverse SQL workloads.
- **Hardware Cost**: 94% lower with significantly lower peak memory usage and zero shuffle spill.

| Metric                     | Spark    | Sail            |
| -------------------------- | -------- | --------------- |
| Total Query Time           | 387.36 s | **102.75 s**    |
| Query Speed-Up             | Baseline | **43% – 727%**  |
| Peak Memory Usage          | 54 GB    | **22 GB (1 s)** |
| Disk Write (Shuffle Spill) | > 110 GB | **0 GB**        |

These results come from a derived TPC-H benchmark (22 queries, scale factor 100, Parquet format) on AWS `r8g.4xlarge` instances.

![Query Time Comparison](https://github.com/lakehq/sail/raw/46d0520532f22e99de6d9ade6373a117216484ca/.github/images/query-time.svg)

See the full analysis and graphs on our [Benchmark Results](https://docs.lakesail.com/sail/latest/introduction/benchmark-results/) page.

## Further Reading

- [Architecture](https://docs.lakesail.com/sail/latest/concepts/architecture/) – Overview of Sail’s design for both local and cluster modes, and how it transitions seamlessly between them.
- [Query Planning](https://docs.lakesail.com/sail/latest/concepts/query-planning/) – Detailed explanation of how Sail parses SQL and Spark relations, builds logical and physical plans, and handles execution for local and cluster modes.
- [SQL](https://docs.lakesail.com/sail/latest/guide/sql/) and [DataFrame](https://docs.lakesail.com/sail/latest/guide/dataframe/) Features – Complete reference for Spark SQL and DataFrame API compatibility.
- [LakeSail Blog](https://lakesail.com/blog/) – Updates on Sail releases, benchmarks, and technical insights.
