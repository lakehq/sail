---
title: Installation
rank: 2
---

# Installation

Sail is available as a Python package on PyPI. You can install it using `pip`.

```bash-vue
pip install "pysail=={{ libVersion }}"
```

## Installation from Source

You can install Sail from source when performance is critical for your application.
Sail can be distributed as a Docker image or a standalone binary, besides being available as a Python package.

### Building the Docker Image

You can build the Sail Docker image for deployment in a containerized environment such as Kubernetes.
More information can be found in the [Docker Images](/guide/deployment/docker-images/) guide.

### Building the Standalone Binary

You can build and run the Sail CLI as a standalone binary.
Please refer to the [Standalone Binary](/development/recipes/standalone-binary) developer guide for more information.

### Building the Python Package

When the pre-built wheels is not available for your platform, the `pip install` command downloads the source distribution
and builds Sail from source.

::: details

You need the following build tools for building the Python package from source.

1. A recent version of the stable Rust toolchain. You can manage the Rust toolchain using [rustup](https://rustup.rs/).
2. The [Protocol Buffers](https://protobuf.dev/) compiler (`protoc`).

Installation from source may take 10 - 30 minutes, and the actual build time depends on your system configuration.
You can pass the `-v` option to the `pip install` command to get more insights into the build process.

Sometimes, you may want to build Sail from source even when the pre-built wheels are available.
This can be useful when you want to have Sail optimized for your hardware architecture.
For example, the following command builds Sail with all features of the current CPU enabled.

```bash-vue
env RUSTFLAGS="-C target-cpu=native" pip install "pysail=={{ libVersion }}" -v --no-binary pysail
```

You can refer to the [Rust documentation](https://doc.rust-lang.org/rustc/codegen-options/index.html)
for more information about the compiler options that can be specified via the `RUSTFLAGS` environment variable.

:::

<script setup>
import { useData } from "vitepress";
import { computed } from "vue";

const { site } = useData();

const libVersion = computed(() => site.value.contentProps?.libVersion);
</script>
