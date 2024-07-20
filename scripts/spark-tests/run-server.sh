#!/bin/bash

set -euo 'pipefail'

source "$(dirname "$0")/prepare-server.sh"

cargo build -r -p framework-spark-connect
cargo run -r -p framework-spark-connect
