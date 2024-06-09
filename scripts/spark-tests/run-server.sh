#!/bin/bash

set -euo 'pipefail'

source "$(dirname "$0")/prepare-server.sh"
cargo run -p framework-spark-connect
