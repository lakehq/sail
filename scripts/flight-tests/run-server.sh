#!/bin/bash

set -euo 'pipefail'

echo "Starting Flight SQL server..."

export PYARROW_IGNORE_TIMEZONE="1"
export SAIL_EXECUTION__DEFAULT_PARALLELISM="4"
export SAIL_CATALOG__DEFAULT_CATALOG='"spark_catalog"'
export SAIL_CATALOG__DEFAULT_DATABASE='["default"]'
export SAIL_CATALOG__LIST='[{name="spark_catalog", type="memory", initial_database=["default"], initial_database_comment="default database"}]'

if [ -z "${CI:-}" ]; then
  export RUST_LOG="${RUST_LOG:-sail=debug}"
  export RUST_BACKTRACE="${RUST_BACKTRACE:-full}"

  cargo run -p sail-cli -- flight server --port 32010 --ip 127.0.0.1
else
  # In CI, use the built binary from target/debug
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
  "${REPO_ROOT}/target/debug/sail" flight server --port 32010 --ip 127.0.0.1
fi
