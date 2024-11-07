#!/bin/bash

set -euo 'pipefail'

python_env_path="$(hatch env find)"
python_version="$(hatch run python -c 'import sys; print("%s.%s" % (sys.version_info.major, sys.version_info.minor))')"

export PYO3_PYTHON="${python_env_path}/bin/python"
export RUST_LOG="${RUST_LOG:-sail_spark_connect=debug}"
export RUST_BACKTRACE="${RUST_BACKTRACE:-full}"
# We have to set `PYTHONPATH` even if we are using the virtual environment.
# This is because the Python executable is the Rust program itself, and there is
# no `pyvenv.cfg` at its required location (one directory above the executable).
export PYTHONPATH="${python_env_path}/lib/python${python_version}/site-packages"
# TODO: Add the logic in Rust
export PYARROW_IGNORE_TIMEZONE="1"
