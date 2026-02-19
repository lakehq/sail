#!/bin/bash
#
# Patch Ibis to remove xfail markers for tests that Sail passes.
#
# These tests are marked as xfail in Ibis for the PySpark backend because
# standard PySpark doesn't support them. Sail passes them because it
# implements additional capabilities (JSON unwrap, file I/O, Decimal256,
# big timestamps, string quantiles, divide-by-zero with ANSI toggle, etc.).
#
# This patch skips the xfail conversion so these tests count as regular passes.
#
# Can be removed when Ibis adds Sail-specific backend support.
#
set -euo pipefail

IBIS_CONFTEST=$(python -c "import ibis.backends.conftest as c; print(c.__file__)")

if [[ ! -f "$IBIS_CONFTEST" ]]; then
    echo "Error: Could not find ibis backends/conftest.py at $IBIS_CONFTEST"
    exit 1
fi

echo "Patching Ibis xfail markers in: $IBIS_CONFTEST"

# Check if already patched
if grep -q "_SAIL_XPASS_TESTS" "$IBIS_CONFTEST"; then
    echo "Ibis is already patched, skipping."
    exit 0
fi

# Use Python to apply the patch (more reliable than sed for multi-line injection)
export IBIS_CONFTEST
python3 << 'PYEOF'
import os
import sys

conftest_path = os.environ["IBIS_CONFTEST"]

with open(conftest_path) as f:
    content = f.read()

# --- 1. The set of test names that Sail passes but Ibis marks xfail for pyspark ---
xpass_block = '''
# Sail: tests that pass against Sail but are xfail in Ibis for pyspark.
# Injected by patch-ibis-xpass.sh — do not edit manually.
_SAIL_XPASS_TESTS = {
    # JSON unwrap (8)
    "test_json_unwrap[pyspark-getattr-str]",
    "test_json_unwrap[pyspark-getattr-float]",
    "test_json_unwrap[pyspark-getattr-bool]",
    "test_json_unwrap[pyspark-getattr-int]",
    "test_json_unwrap[pyspark-unwrap_as-str]",
    "test_json_unwrap[pyspark-unwrap_as-float]",
    "test_json_unwrap[pyspark-unwrap_as-int]",
    "test_json_unwrap[pyspark-unwrap_as-bool]",
    # File I/O (8)
    "test_read_csv_gz[pyspark]",
    "test_read_csv_with_dotted_name[pyspark]",
    "test_read_csv_glob[pyspark]",
    "test_read_csv_schema[pyspark]",
    "test_read_parquet[pyspark-functional_alltypes.parquet-funk_all]",
    "test_read_parquet[pyspark-functional_alltypes.parquet-None]",
    "test_read_parquet_glob[pyspark]",
    "test_read_json_glob[pyspark]",
    # Decimal256 (2)
    "test_decimal_literal[pyspark-decimal-big]",
    "test_to_pyarrow_decimal[pyspark-decimal256]",
    # Big timestamps (2)
    "test_big_timestamp[pyspark]",
    "test_large_timestamp[pyspark]",
    # String quantiles (2)
    "test_string_quantile[pyspark-median]",
    "test_string_quantile[pyspark-quantile]",
    # Divide by zero — xpass because ANSI mode fixture toggles it off (2)
    "test_divide_by_zero[pyspark-double_col-0.0]",
    "test_divide_by_zero[pyspark-float_col-0.0]",
    # UDF named destruct (1)
    "test_elementwise_udf_named_destruct[pyspark]",
    # Other (2)
    "test_first_last[pyspark-True-False-first]",
    "test_first_last[pyspark-True-False-last]",
}
'''

# --- 2. The early return to inject inside pytest_runtest_call ---
early_return = (
    '\n'
    '    # Sail: skip xfail markers for tests that Sail passes\n'
    '    if backend == "pyspark" and item.name in _SAIL_XPASS_TESTS:\n'
    '        return\n'
)

# --- 3. Find insertion points ---

# Insert the set before "def pytest_runtest_call(item):"
func_marker = "def pytest_runtest_call(item):"
func_idx = content.find(func_marker)
if func_idx == -1:
    print("Error: Could not find 'def pytest_runtest_call' in conftest.py")
    sys.exit(1)

# Insert early return after "    backend = next(iter(backend))\n"
backend_line = "    backend = next(iter(backend))\n"
backend_idx = content.find(backend_line, func_idx)
if backend_idx == -1:
    print("Error: Could not find 'backend = next(iter(backend))' in pytest_runtest_call")
    sys.exit(1)
backend_end = backend_idx + len(backend_line)

# --- 4. Build patched content ---
patched = (
    content[:func_idx]
    + xpass_block + "\n\n"
    + content[func_idx:backend_end]
    + early_return
    + content[backend_end:]
)

# --- 5. Write back ---
with open(conftest_path, "w") as f:
    f.write(patched)
PYEOF

# Verify the patch was applied
if grep -q "_SAIL_XPASS_TESTS" "$IBIS_CONFTEST"; then
    echo "Patch applied successfully."
else
    echo "Error: Patch failed to apply."
    exit 1
fi
