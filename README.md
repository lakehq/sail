# Sail

[![Build Status](https://github.com/lakehq/sail/actions/workflows/build.yml/badge.svg?branch=main&event=push)](https://github.com/lakehq/sail/actions)
[![PyPI Release](https://img.shields.io/pypi/v/pysail)](https://pypi.org/project/pysail/)
[![PyPI Downloads](https://img.shields.io/pepy/dt/pysail.svg?label=PyPI%20Downloads)](https://pypi.org/project/pysail/)
[![Static Slack Badge](https://img.shields.io/badge/slack-LakeSail_Community-3695BF?logo=slack)](https://www.launchpass.com/lakesail-community/free)

The mission of Sail is to unify stream processing, batch processing, and compute-intensive (AI) workloads.
Currently, Sail features a drop-in replacement for Spark SQL and the Spark DataFrame API in both single-host and distributed settings.

## Installation

Sail is available as a Python package on PyPI. You can install it using `pip`.

```bash
pip install "pysail[spark]"
```

Alternatively, you can install Sail from source for better performance for your hardware architecture.
You can follow the [Installation](https://docs.lakesail.com/sail/latest/guide/installation/) guide for more information.

## Getting Started

### Starting the Sail Server

**Option 1: Command Line Interface** You can start the local Sail server using the `sail` command.

```bash
sail spark server --port 50051
```

**Option 2: Python API** You can start the local Sail server using the Python API.

```python
from pysail.spark import SparkConnectServer

server = SparkConnectServer(port=50051)
server.start(background=False)
```

**Option 3: Kubernetes** You can deploy Sail on Kubernetes and run Sail in cluster mode for distributed processing.
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

Please refer to the [Getting Started](https://docs.lakesail.com/sail/latest/guide/getting-started/) guide for further details.

## Documentation

The documentation of the latest Sail version can be found [here](https://docs.lakesail.com/sail/latest/).

## Further Reading

- [Supercharge Spark: Quadruple Speed, Cut Costs by 94%](https://lakesail.com/blog/supercharge-spark/) - This post presents detailed **benchmark results** comparing Sail with Spark.
- [Sail 0.2 and the Future of Distributed Processing](https://lakesail.com/blog/the-future-of-distributed-processing/) - This post discusses the Sail **distributed processing architecture**.

## Contributing

Contributions are more than welcome!

Please submit [GitHub issues](https://github.com/lakehq/sail/issues) for bug reports and feature requests.
You are also welcome to ask questions in [GitHub discussions](https://github.com/lakehq/sail/discussions).

Feel free to create a [pull request](https://github.com/lakehq/sail/pulls) if you would like to make a code change.
You can refer to the [development guide](https://docs.lakesail.com/sail/main/development/) to get started.

## Support

LakeSail offers flexible enterprise support options for Sail. Please [contact us](https://lakesail.com/support/) to learn more.
