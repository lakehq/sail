#!/bin/bash

set -euo 'pipefail'

project_path="$(git rev-parse --show-toplevel)"

source "${project_path}"/python/.venv/bin/activate
python_version=$(python -c 'import sys; print("%s.%s" % (sys.version_info.major, sys.version_info.minor))')
deactivate

export PYO3_PYTHON="${project_path}/python/.venv/bin/python"
export RUST_LOG="${RUST_LOG:-framework_spark_connect=debug}"
export RUST_BACKTRACE="${RUST_BACKTRACE:-full}"
export RUST_MIN_STACK="${RUST_MIN_STACK:-8388608}"
# We have to set `PYTHONPATH` even if we are using the virtual environment.
# This is because the Python executable is the Rust program itself, and there is
# no `pyvenv.cfg` at its required location (one directory above the executable).
export PYTHONPATH="${project_path}/python/.venv/lib/python${python_version}/site-packages"
