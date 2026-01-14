#!/bin/bash

set -euo 'pipefail'

if [ -z "${VIRTUAL_ENV:-}" ]; then
  echo "The server must be run in a Python virtual environment."
  echo "Please run the script via \`hatch run [<env>:]<command> <options>\`."
  exit 1
fi

work_dir="$(python -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')"
python_version="$(python -c 'import sys; print("%s.%s" % (sys.version_info.major, sys.version_info.minor))')"

echo "Python environment: ${VIRTUAL_ENV}"
echo "Python version: ${python_version}"
echo "Sail working directory: ${work_dir}"

export PYARROW_IGNORE_TIMEZONE="1"
# We use a fixed default parallelism to ensure deterministic execution plans
# when we run the server to develop snapshot tests.
export SAIL_EXECUTION__DEFAULT_PARALLELISM="4"
export SAIL_CATALOG__DEFAULT_CATALOG='"spark_catalog"'
export SAIL_CATALOG__DEFAULT_DATABASE='["default"]'
export SAIL_CATALOG__LIST='[{name="spark_catalog", type="memory", initial_database=["default"], initial_database_comment="default database"}]'

if [ -z "${CI:-}" ]; then
  export PYO3_PYTHON="${VIRTUAL_ENV}/bin/python"
  export RUST_LOG="${RUST_LOG:-sail=debug}"
  export RUST_BACKTRACE="${RUST_BACKTRACE:-full}"
  # We have to set `PYTHONPATH` even if we are using the virtual environment.
  # This is because the Python executable is the Rust program itself, and there is
  # no `pyvenv.cfg` at its required location (one directory above the executable).
  export PYTHONPATH="${VIRTUAL_ENV}/lib/python${python_version}/site-packages"

  cargo run -p sail-cli -- spark server -C "${work_dir}"
else
  sail spark server -C "${work_dir}"
fi
