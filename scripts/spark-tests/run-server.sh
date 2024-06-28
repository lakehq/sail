#!/bin/bash

set -euo 'pipefail'

source "$(dirname "$0")/prepare-server.sh"

cargo build -p framework-spark-connect

# The `CI` environment variable is set by GitHub Actions.
if [ -z "${CI:-}" ]; then
  cargo run -p framework-spark-connect
else
  nohup cargo run -p framework-spark-connect > /dev/null 2>&1 < /dev/null &
fi
