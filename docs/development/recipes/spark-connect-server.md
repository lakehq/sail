---
title: Standalone Spark Connect Server
rank: 80
---

# Standalone Spark Connect Server

By default, you can run the Spark Connect server using the `pysail` Python library.
In some situations, however, you may want to build and run the server as a standalone binary.

You can build the server with the `release` profile in Cargo.

```bash
cargo build -r -p sail-spark-connect --bins
```

You can then run the server with the following command.

```bash
env RUST_LOG="sail_spark_connect=info" target/release/sail-spark-connect
```

The `--help` option can be used to show all the supported arguments of the server.

## Python Dependency

The server binary is dynamically linked to the Python library.
The Python version is determined at build time.
PyO3 infers the Python version from the environment, or you can explicitly configure the
Python interpreter via the `PYO3_PYTHON` environment variable.
You can refer to the [PyO3 documentation](https://pyo3.rs/) for more information.

You can inspect the dynamic library dependencies with command line tools.

::: code-group

```bash [Linux]
ldd target/release/sail-spark-connect
```

```bash [macOS]
otool -L target/release/sail-spark-connect
```

:::

The presence of the dynamic Python library dependency means that you must ensure the same Python environment
is present at both build time and runtime.
Therefore, it is recommended to package the server binary into a Docker image.
To keep the image size small, you can consider [multi-stage builds](https://docs.docker.com/build/building/multi-stage/) when authoring
the `Dockerfile`.
