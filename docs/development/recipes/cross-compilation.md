---
title: Cross-compilation
rank: 40
---

# Cross-compilation of the Python Package

Cross-compilation of the Python package is straightforward with Maturin and Zig.
For example, to build the package for the `aarch64-unknown-linux-gnu` target, run the following command to install
the target toolchain.

```bash
rustup target add aarch64-unknown-linux-gnu
```

Then run the following command to build the Python package for the target.

```bash
hatch run maturin build --release --target aarch64-unknown-linux-gnu --zig
```

::: info

1. To build the Windows target on Linux/macOS, set the following environment variable to work around the issue with the
   `blake3` crate.
   ```bash
   export CARGO_FEATURE_PURE=1
   ```
2. Cross-compilation of the binary executable is more complicated since it requires a cross-compiled Python interpreter
   library. Currently, we do not provide support for this. However, if you are interested in exploring this, you can
   look at the PyO3 [documentation](https://pyo3.rs/main/building-and-distribution) and the
   [cargo zigbuild](https://github.com/rust-cross/cargo-zigbuild) tool.

:::
