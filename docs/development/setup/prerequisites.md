---
title: Prerequisites
rank: 10
---

# Prerequisites

You need the Rust toolchain (both stable and nightly) to build the project.
You can use [rustup](https://rustup.rs/) to manage the Rust toolchain in your local environment.

You need Node.js to build the documentation.

On macOS, you can install rustup and the Node.js runtime via Homebrew.

<!-- TODO: make it clear that the following is an alternative way to install rustup -->

```bash
brew install rustup
brew install node
```

Use the following commands to install the Rust toolchains via rustup.

```bash
rustup toolchain install stable --profile default --component llvm-tools-preview
rustup toolchain install nightly --profile default
```

You also need the following tools when working on the project.

1. The [Protocol Buffers](https://protobuf.dev/) compiler (`protoc`).
2. [Hatch](https://hatch.pypa.io/latest/).
3. [Maturin](https://www.maturin.rs/).
4. [Zig](https://ziglang.org/).
5. [pnpm](https://pnpm.io/).

On macOS, you can install these tools via Homebrew.

<!-- TODO: investigate if we can install Maturin locally -->

```bash
brew install protobuf hatch maturin zig pnpm
```

If Homebrew overrides your default Rust installation,
you can prioritize the rustup-managed Rust by adding the following line to your shell profile.

```bash
export PATH="$HOME/.cargo/bin:$PATH"
```
