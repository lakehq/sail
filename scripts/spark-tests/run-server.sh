#!/bin/bash

set -euo 'pipefail'

source "$(dirname "$0")/prepare-server.sh"

if [[ -z "${BENCHMARK:-}" ]]; then
  cargo run -p framework-spark-connect
else
  cargo run -p framework-spark-connect -r
fi
