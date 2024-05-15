#!/bin/bash

set -euo 'pipefail'

project_path="$(dirname "$0")/../.."

source "${project_path}"/examples/python/.venv/bin/activate

python_version=$(python -c 'import sys; print("%s.%s" % (sys.version_info.major, sys.version_info.minor))')

export RUST_LOG="${RUST_LOG:-spark_connect_server=trace}"
export RUST_BACKTRACE="${RUST_BACKTRACE:-full}"
export RUST_LIB_BACKTRACE="${RUST_LIB_BACKTRACE:-full}"
# We have to set `PYTHONPATH` even if we are using the virtual environment.
# This is because the Python executable is the Rust program itself, and there is
# no `pyvenv.cfg` at its required location (one directory above the executable).
export PYTHONPATH="${project_path}/examples/python/.venv/lib/python${python_version}/site-packages"

cargo run -p spark-connect-server
