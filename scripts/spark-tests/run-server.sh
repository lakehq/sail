#!/bin/bash

set -euo 'pipefail'

source "$(dirname "$0")/prepare-server.sh"

cargo build -p framework-spark-connect
cargo run -p framework-spark-connect
