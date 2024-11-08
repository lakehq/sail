---
title: Installation
rank: 10
---

# Installation

Sail is available as a Python package on PyPI. You can install it using `pip`.

```bash
pip install pysail
```

## Installation from Source

::: info
This installation method is recommended when performance is critical for your application.
:::

When the pre-built wheels is not available for your platform, the `pip install` command downloads the source distribution
and builds Sail from source. You need the following build tools for this process.

1. A recent version of the stable Rust toolchain. You can manage the Rust toolchain using [rustup](https://rustup.rs/).
2. The [Protocol Buffers](https://protobuf.dev/) compiler (`protoc`).

Installation from source may take 10 - 30 minutes, and the actual build time depends on your system configuration.
You can pass the `-v` option to the `pip install` command to get more insights into the build process.

Sometimes, you may want to build Sail from source even when the pre-built wheels are available.
This can be useful when you want to have Sail optimized for your hardware architecture.
For example, the following command builds Sail with all features of the current CPU enabled.

```bash
env RUSTFLAGS="-C target-cpu=native" pip install pysail -v --no-binary pysail
```

::: info
You can refer to the [Rust documentation](https://doc.rust-lang.org/rustc/codegen-options/index.html)
for more information about the compiler options that can be specified via the `RUSTFLAGS` environment variable.
:::

## Standalone Binary

Please refer to the [Standalone Binary](/development/recipes/standalone-binary) guide for more information about building and running the Sail CLI as a standalone binary.
