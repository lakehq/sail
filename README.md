# Sail

[![Build Status](https://github.com/lakehq/sail/actions/workflows/build.yml/badge.svg?branch=main&event=push)](https://github.com/lakehq/sail/actions)
[![Codecov](https://codecov.io/gh/lakehq/sail/graph/badge.svg)](https://app.codecov.io/gh/lakehq/sail)
[![PyPI Release](https://img.shields.io/pypi/v/pysail)](https://pypi.org/project/pysail/)
[![Static Slack Badge](https://img.shields.io/badge/slack-LakeSail_Community-3762E0?logo=slack)](https://www.launchpass.com/lakesail-community/free)

Sail is an open-source **unified and distributed multimodal computation framework** created by [LakeSail](https://lakesail.com/).

Our mission is to **unify batch processing, stream processing, and compute-intensive AI workloads**. Sail is a compute engine that is:

- **Compatible** with the Spark Connect protocol, supporting the Spark SQL and DataFrame API with no code rewrites required.
- **~4x faster** than Spark in benchmarks (up to 8x in specific workloads).
- **94% cheaper** on infrastructure costs.
- **100% Rust-native** with no JVM overhead, delivering memory safety, instant startup, and predictable performance.

**🚀 Sail outperforms Spark, popular Spark accelerators, Databricks, and Snowflake on [ClickBench](https://go.lakesail.com/clickbench).**

## Documentation

The documentation of the latest Sail version can be found [here](https://docs.lakesail.com/sail/latest/).

## Installation

### Quick Start

Sail is available as a Python package on PyPI. You can install it along with PySpark in your Python environment.

```bash
pip install pysail
pip install "pyspark[connect]"
```

Alternatively, you can install the lightweight client package `pyspark-client` since Spark 4.0.
The `pyspark-connect` package, which is equivalent to `pyspark[connect]`, is also available since Spark 4.0.

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

## Feature Highlights

### Storage

Sail supports a variety of storage backends for reading and writing data. You can read more details in our [Storage Guide](https://docs.lakesail.com/sail/latest/guide/storage/).

Here are the storage options supported:

- AWS S3
- Cloudflare R2
- Azure
- Google Cloud Storage
- Hugging Face
- HDFS
- File systems
- HTTP/HTTPS
- In-memory storage

### Lakehouse Formats

Sail provides native support for modern lakehouse table formats, offering reliable storage layers with strong data management guarantees and ensuring interoperability with existing datasets.

Please refer to the following guides for the supported formats:

- [Delta Lake Guide](https://docs.lakesail.com/sail/latest/guide/formats/delta.html)
- [Apache Iceberg Guide](https://docs.lakesail.com/sail/latest/guide/formats/iceberg.html)

### Catalog Providers

Sail supports multiple catalog providers, such as the Apache Iceberg REST Catalog and Unity Catalog. You can manage datasets as external tables and integrate with broader data-platform ecosystems.

For more details on usage and best practices, see the [Catalog Guide](https://docs.lakesail.com/sail/latest/guide/catalog/).

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

## Contributing

Contributions are more than welcome!

Please submit [GitHub issues](https://github.com/lakehq/sail/issues) for bug reports and feature requests.
You are also welcome to ask questions in [GitHub discussions](https://github.com/lakehq/sail/discussions).

Feel free to create a [pull request](https://github.com/lakehq/sail/pulls) if you would like to make a code change.
You can refer to the [Development Guide](https://docs.lakesail.com/sail/main/development/) to get started.

Additionally, please join our [Slack Community](https://www.launchpass.com/lakesail-community/free) if you haven’t already!

## Why Choose Sail?

When Spark was invented over 15 years ago, it was revolutionary. It redefined distributed data processing and powered ETL, machine learning, and analytics pipelines across industries.

But Spark’s JVM-based architecture now struggles to meet modern demands for performance and cloud efficiency:

- **Garbage collection pauses** introduce latency spikes.
- **Serialization overhead** slows data exchange between JVM and Python.
- **Heavy executors** drive up cloud costs and complicate scaling.
- **Row-based processing** performs poorly on analytical workloads and leaves hardware efficiency untapped.
- **Slow startup** delays workloads, hurting interactive and on-demand use cases.

Sail solves these problems with a modern, Rust-native design.

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

Curious about how Sail stacks up against Spark? Explore our [Why Sail?](https://lakesail.com/why-sail/) page. Ready to bring your existing workloads over? Our [Migration Guide](https://docs.lakesail.com/sail/latest/introduction/migrating-from-spark/) shows you how.

## Further Reading

- [Architecture](https://docs.lakesail.com/sail/latest/concepts/architecture/) – Overview of Sail’s design for both local and cluster modes, and how it transitions seamlessly between them.
- [Query Planning](https://docs.lakesail.com/sail/latest/concepts/query-planning/) – Detailed explanation of how Sail parses SQL and Spark relations, builds logical and physical plans, and handles execution for local and cluster modes.
- [SQL](https://docs.lakesail.com/sail/latest/guide/sql/) and [DataFrame](https://docs.lakesail.com/sail/latest/guide/dataframe/) Features – Complete reference for Spark SQL and DataFrame API compatibility.
- [LakeSail Blog](https://lakesail.com/blog/) – Updates on Sail releases, benchmarks, and technical insights.

**✨Using Sail? [Tell us your story](https://lakesail.com/share-story/) and get free merch!✨**
