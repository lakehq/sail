#!/usr/bin/env bash

set -euo pipefail

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- --default-toolchain none -y

. /root/.cargo/env

RUST_STABLE_TOOLCHAIN="1.90.0"
RUST_NIGHTLY_TOOLCHAIN="nightly-2025-09-01"

rustup toolchain install "${RUST_STABLE_TOOLCHAIN}" --profile default --component llvm-tools-preview
rustup toolchain install "${RUST_NIGHTLY_TOOLCHAIN}" --profile default
