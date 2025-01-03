# Sail

[![Build Status](https://github.com/lakehq/sail/actions/workflows/build.yml/badge.svg?branch=main&event=push)](https://github.com/lakehq/sail/actions)
[![PyPI Release](https://img.shields.io/pypi/v/pysail)](https://pypi.org/project/pysail/)
[![PyPI Downloads](https://img.shields.io/pypi/dm/pysail.svg?label=PyPI%20Downloads)](https://pypi.org/project/pysail/)

The mission of Sail is to unify stream processing, batch processing, and compute-intensive (AI) workloads.
Currently, Sail features a drop-in replacement for Spark SQL and the Spark DataFrame API in both single-host and distributed settings.

## Kubernetes Deployment

Please refer to the [Kubernetes Deployment Guide](https://docs.lakesail.com/sail/latest/guide/deployment/kubernetes.html) for instructions on deploying Sail on Kubernetes.

## Installation

Sail is available as a Python package on PyPI. You can install it using `pip`.

```bash
pip install "pysail==0.2.0"
```

Alternatively, you can install Sail from source for better performance for your hardware architecture.
You would need [rustup](https://rustup.rs/) and [protoc](https://protobuf.dev/) in your environment for this.

```bash
env RUSTFLAGS="-C target-cpu=native" pip install "pysail==0.2.0" -v --no-binary pysail
```

You can follow the [Getting Started](https://docs.lakesail.com/sail/latest/guide/getting-started/) guide to learn more about Sail.

## Documentation

The documentation of the latest Sail version can be found [here](https://docs.lakesail.com/sail/latest/).

## Benchmark Results

Check out our blog post, [Supercharge Spark: Quadruple Speed, Cut Costs by 94%](https://lakesail.com/blog/supercharge-spark/), for detailed benchmark results comparing Sail with Spark.

## Contributing

Contributions are more than welcome!

Please submit GitHub issues for bug reports and feature requests.

Feel free to create a pull request if you would like to make a code change.
You can refer to the [development guide](https://docs.lakesail.com/sail/main/development/) to get started.

## Support

See the [Support Options Page](https://lakesail.com/#support) for more information.
