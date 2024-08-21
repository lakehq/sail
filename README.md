# Sail

[![Build Status](https://github.com/lakehq/sail/actions/workflows/build.yml/badge.svg?branch=main&event=push)](https://github.com/lakehq/sail/actions)
[![PyPI Release](https://img.shields.io/pypi/v/pysail)](https://pypi.org/project/pysail/)

The mission of Sail is to unify stream processing, batch processing, and compute-intensive (AI) workloads.
Currently, Sail features a drop-in replacement for Spark SQL and the Spark DataFrame API in single-process settings.

## Installation

Sail is available as a Python package on PyPI. You can install it using `pip`.

```bash
pip install pysail
```

You can follow the [Getting Started](https://docs.lakesail.com/sail/latest/guide/getting-started/) guide to learn more about Sail.

**Note**: Currently only a source-only pre-release is available, so you need the Rust toolchain to install Sail.
A proper release with pre-built wheels will be available soon.

## Documentation

The documentation of the latest Sail version can be found [here](https://docs.lakesail.com/sail/latest/).

## Contributing

Contributions are welcome!

Please submit GitHub issues for bug reports and feature requests.

Feel free to create a pull request if you would like to make a code change.
You can refer to the `main` branch [development guide](https://docs.lakesail.com/sail/main/development/) to get started.
Note that the development guide in the latest released version may not be up-to-date.
