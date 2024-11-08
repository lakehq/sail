---
title: Standalone Binary
rank: 80
---

# Standalone Binary

By default, you can run the Sail CLI (the `sail` command) by installing the `pysail` Python library.
In some situations, however, you may want to build and run the CLI as a standalone binary.

You can build the CLI with the `release` profile in Cargo.

```bash
env \
  RUSTFLAGS="-C target-cpu=native" \
  cargo build -r -p sail-cli --bins
```

You can then run the Spark Connect server with the following command.

```bash
target/release/sail spark server
```

The `--help` option can be used to show all the supported arguments of the `sail` command.

## Python Dependency

The Sail CLI binary is dynamically linked to the Python library.
The Python version is determined at build time.
PyO3 infers the Python version from the environment, or you can explicitly configure the
Python interpreter via the `PYO3_PYTHON` environment variable.
You can refer to the [PyO3 documentation](https://pyo3.rs/) for more information.

You can inspect the dynamic library dependencies with command line tools.

::: code-group

```bash [Linux]
ldd target/release/sail
```

```bash [macOS]
otool -L target/release/sail
```

:::

The presence of the dynamic Python library dependency means that you must ensure the same Python environment
is present at both build time and runtime.
Therefore, it is recommended to package the server binary into a Docker image.
To keep the image size small, you can consider [multi-stage builds](https://docs.docker.com/build/building/multi-stage/) when authoring
the `Dockerfile`.
