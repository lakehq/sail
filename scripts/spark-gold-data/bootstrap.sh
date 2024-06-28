#!/bin/bash

set -euo 'pipefail'

if [ -z "${JAVA_HOME:-}" ]; then
  echo "Please set JAVA_HOME to avoid potential issues with sbt."
  exit 1
fi

project_path="$(git rev-parse --show-toplevel)"

cd "${project_path}"/opt/spark

# Define paths relative to the working directory (Spark project root directory).
logs_path="logs/gold-data"
scripts_path="../../scripts/spark-gold-data"
output_path="../../crates/framework-spark-connect/tests/gold_data"

function git_enter() {
  status="$(git status --porcelain)"
  if [ -n "$status" ]; then
    echo "The working directory of the Spark project is not clean."
    exit 1
  fi

  head_commit="$(git rev-parse HEAD)"
  tag_commit="$(git rev-parse v3.5.1)"
  if [ "${head_commit}" != "${tag_commit}" ]; then
    echo "The current commit of the Spark project is not v3.5.1."
    exit 1
  fi

  echo "Applying the patch for gold data generation..."
  git apply "${scripts_path}"/spark-3.5.1.patch

  trap 'git_exit' EXIT
}

function git_exit() {
  git apply -R "${scripts_path}"/spark-3.5.1.patch
  echo "Reverted the patch for gold data generation."
}

git_enter

rm -rf "${logs_path}"
mkdir -p "${logs_path}"

sbt_command=""
# The patch is not thread-safe, so we must run the tests sequentially.
sbt_command+='set Test / parallelExecution := false; '
sbt_command+='catalyst/testOnly org.apache.spark.sql.catalyst.parser.*; '
sbt_command+='sql/testOnly org.apache.spark.sql.FunctionCollectorSuite; '
sbt_command+='exit'

env SPARK_LOCAL_IP=127.0.0.1 \
  SPARK_SUITE_OUTPUT_DIR="${PWD}/${logs_path}" \
  build/sbt "${sbt_command}"

echo "Removing existing test data..."
rm -rf "${output_path}"
mkdir -p "${output_path}"

function write_grouped_tests() {
  local kind="$1"
  local suite="$2"
  local file_prefix="$3"
  local script_file="${scripts_path}/process_${kind}.jq"
  local input_file="${logs_path}/${suite}.jsonl"

  mkdir -p "${output_path}/${kind}"
  echo "Processing $(basename "${input_file}")..."
  # Write each group of test cases to a separate file.
  # The idea is that we run `jq` to get a list of unique groups first,
  # and then use `xargs` to run `jq` again for each group to produce the output file.
  # shellcheck disable=SC2016
  jq -r -f "${script_file}" --arg group "" "${input_file}" \
    | xargs -I {} bash -c 'jq -f "$0" --arg group "$1" "$2" > "$3"' \
    "${script_file}" {} "${input_file}" "${output_path}/${kind}/${file_prefix}"{}.json
}

function write_tests() {
  local kind="$1"
  local suite="$2"
  local script_file="${scripts_path}/process_${kind}.jq"
  local input_file="${logs_path}/${suite}.jsonl"

  echo "Processing $(basename "${input_file}")..."
  jq -f "${script_file}" "${input_file}" > "${output_path}/${kind}.json"
}

cat <<EOF > "${output_path}/README.md"
# Gold Data for Spark Tests

This directory contains the gold data for the Spark tests.
All the files in this directory, including this README file, are auto-generated.
Please do not modify them manually.
EOF

write_grouped_tests "plan" "DDLParserSuite" "ddl_"
write_grouped_tests "plan" "ErrorParserSuite" "error_"
write_grouped_tests "plan" "PlanParserSuite" "plan_"
write_grouped_tests "plan" "UnpivotParserSuite" "unpivot_"
write_grouped_tests "expression" "ExpressionParserSuite" ""
write_grouped_tests "function" "FunctionCollectorSuite" ""
write_tests "data_type" "DataTypeParserSuite"
write_tests "table_schema" "TableSchemaParserSuite"
