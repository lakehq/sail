#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."

export RUST_BACKTRACE=1

echo "Running MRE: calls PyUnicode_* before Python init and intentionally leaks allocations."
echo "Tip: watch RSS in the output; it should increase over time."
echo

cargo run -p sail-cli --example python_preinit_argv_ub -- "$@"

