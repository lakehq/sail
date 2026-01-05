---
title: Building the Rust Code
rank: 10
---

# Building the Rust Code

You can build the Rust code using the `cargo` command.

Run the following commands to verify the code before committing changes.

```bash
cargo +nightly fmt && \
  cargo clippy --all-targets --all-features && \
  cargo build && \
  env SAIL_UPDATE_GOLD_DATA=1 cargo test
```

::: info

1. The code can be built and tested using the stable toolchain,
   while the nightly toolchain is required for formatting the code.

2. Please make sure there are no warnings in the output.
   The GitHub Actions workflow runs `cargo clippy` with the `-D warnings` option,
   so that the build will fail if there are any warnings from either the compiler or the linter.

3. The `SAIL_UPDATE_GOLD_DATA` environment variable update the test gold data files to match the behavior of the code.
   Running `cargo test` without the environment variable will validate the gold data. The test would fail if the gold data is outdated.

:::

::: warning
On Windows you need to use the LLVM linker and not MSVC to successfully compile. You can do this by first installing llvm:

```bash
scoop install llvm
```

then creating a `.cargo/config.toml` file with:

```toml
[target.'cfg(all(windows, target_env = "msvc"))']
linker = "lld-link"
```

:::
