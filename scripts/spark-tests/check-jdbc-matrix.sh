#!/usr/bin/env bash

# Reproduce the CI checks for the JDBC data source locally, before pushing.
#
# The "Python Tests" CI job runs the suite against every supported Spark version.
# A test module that imports the Python DataSource API without a version gate
# collects fine on the local 4.1 env but errors on 3.5.7 / 4.0.1 — invisible until
# CI. This script runs lint + the non-Docker JDBC tests across the whole matrix so
# that class of failure surfaces locally.
#
# Usage:  scripts/spark-tests/check-jdbc-matrix.sh
# Requires: hatch (the test.spark-<version> envs are provisioned on first run).

set -euo 'pipefail'

project_path="$(git rev-parse --show-toplevel)"
cd "${project_path}"

spark_versions=("3.5.7" "4.0.1" "4.1.1")
tests=(
  "python/pysail/tests/spark/datasource/test_jdbc_unit.py"
  "python/pysail/tests/spark/datasource/test_version_gating.py"
)

# Prefer hatch on PATH; fall back to `uvx hatch` so this runs without a global install.
if command -v hatch >/dev/null 2>&1; then
  hatch=(hatch)
else
  hatch=(uvx hatch)
fi

echo "==> Lint (ruff)"
"${hatch[@]}" fmt --check

for version in "${spark_versions[@]}"; do
  echo "==> Python Tests (Spark ${version})"
  "${hatch[@]}" run "test.spark-${version}:pytest" -q "${tests[@]}"
done

echo "==> JDBC matrix green on: ${spark_versions[*]}"
