#!/bin/bash

set -euo 'pipefail'

python_version_script='import sys; print("%s.%s" % (sys.version_info.major, sys.version_info.minor))'

if [[ -n "${VIRTUAL_ENV:-}" ]]; then
  python_env="${VIRTUAL_ENV}"
  python_version="$(python -c "${python_version_script}")"
else
  python_env="$(hatch env find)"
  python_version="$(hatch run python -c "${python_version_script}")"
fi

export PYO3_PYTHON="${python_env}/bin/python"
export RUST_LOG="${RUST_LOG:-sail_spark_connect=debug}"
export RUST_BACKTRACE="${RUST_BACKTRACE:-full}"
# We have to set `PYTHONPATH` even if we are using the virtual environment.
# This is because the Python executable is the Rust program itself, and there is
# no `pyvenv.cfg` at its required location (one directory above the executable).
export PYTHONPATH="${python_env}/lib/python${python_version}/site-packages"
# TODO: Add the logic in Rust
export PYARROW_IGNORE_TIMEZONE="1"
