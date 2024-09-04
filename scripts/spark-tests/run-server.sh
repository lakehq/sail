#!/bin/bash

set -euo 'pipefail'

source "$(dirname "$0")/prepare-server.sh"

if [[ -z "${BENCHMARK:-}" ]]; then
  cargo run -p sail-spark-connect -- -C opt/spark
else
  # We build for the current CPU to get the best performance.
  # See also: https://crates.io/crates/arrow
  env RUSTFLAGS="-C target-cpu=native" cargo run -r -p sail-spark-connect
fi
