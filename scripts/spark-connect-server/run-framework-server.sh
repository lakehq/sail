#!/bin/bash

cd "${FRAMEWORK_PROJECT_PATH}" || exit

source examples/python/.venv/bin/activate

export RUST_LOG=spark_connect_server=debug
export RUST_BACKTRACE=full
# We have to set `PYTHONPATH` even if we are using the virtual environment.
# This is because the Python executable is the Rust program itself, and there is
# no `pyvenv.cfg` at its required location (one directory above the executable).
export PYTHONPATH="examples/python/.venv/lib/python3.11/site-packages"
cargo run -p spark-connect-server