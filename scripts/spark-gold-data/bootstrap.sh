#!/bin/bash

set -euo 'pipefail'

if [ -z "${JAVA_HOME:-}" ]; then
  echo "Please set JAVA_HOME to avoid potential issues with sbt."
  exit 1
fi

project_path="$(git rev-parse --show-toplevel)"

scripts_path="${project_path}/scripts/spark-gold-data"
logs_path="${project_path}/tmp/spark-gold-data"
output_path="${project_path}/crates/sail-spark-connect/tests/gold_data"

source "${project_path}/scripts/shell-tools/git-patch.sh"

apply_git_patch "${project_path}"/opt/spark "v4.1.0" "${scripts_path}/spark-4.1.0.patch"

cd "${project_path}"/opt/spark

echo "Removing existing test logs..."
rm -rf "${logs_path}"
mkdir -p "${logs_path}"

sbt_command=""
# The patch is not thread-safe, so we must run the tests sequentially.
sbt_command+='set Test / parallelExecution := false; '
sbt_command+='catalyst/testOnly org.apache.spark.sql.catalyst.parser.*; '
sbt_command+='sql/testOnly org.apache.spark.sql.FunctionCollectorSuite; '
sbt_command+='exit'

env SPARK_LOCAL_IP=127.0.0.1 \
  SPARK_SUITE_OUTPUT_DIR="${logs_path}" \
  build/sbt "${sbt_command}"

echo "Removing existing test data..."
rm -rf "${output_path}"
mkdir -p "${output_path}"

cat <<EOF > "${output_path}/README.md"
# Gold Data for Spark Tests

This directory contains the gold data for the Spark tests.
All the files in this directory, including this README file, are auto-generated.
Please do not modify them manually.
EOF

cargo run -p sail-gold-test --bin spark-gold-data -- \
  --input "${logs_path}" \
  --output "${output_path}"
