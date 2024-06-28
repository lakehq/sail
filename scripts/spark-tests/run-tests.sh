#!/bin/bash

set -euo 'pipefail'

project_path="$(git rev-parse --show-toplevel)"

cd "${project_path}"/opt/spark

source venv/bin/activate

# Define the directory for test logs. The `logs/` directory is in `.gitignore`.
logs_dir="logs/${TEST_RUN_NAME:-latest}"

echo "Removing existing test logs..."
rm -rf "${logs_dir}"
mkdir -p "${logs_dir}"

# Define a few environment variables if they are not set.
# This does not override an environment variable if it is set but empty.
# If the remote port is empty, the test runs against the original Spark Connect server.
export SPARK_TESTING_REMOTE_PORT="${SPARK_TESTING_REMOTE_PORT-50051}"
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP-127.0.0.1}"

function run_pytest() {
  name="$1"
  args=("${@:2}")

  echo "Test suite: ${name}"
  # We ignore the pytext exit code so that the command can complete successfully.
  python/run-pytest.sh \
    --disable-warnings \
    --report-log="${logs_dir}/${name}.jsonl" \
    "${args[@]}" \
    | tee "${logs_dir}/${name}.log" || true

  # Failed tests are acceptable, but we return a non-zero exit code when there are errors,
  # which indicate issues with the test setup.
  tail -n 1 "${logs_dir}/${name}.log" | grep -q -v 'error' || (echo "Found errors in test results: ${name}.log" && false)
}

echo "${TEST_RUN_GIT_COMMIT:-unknown}" > "${logs_dir}/commit"
echo "${TEST_RUN_GIT_REF:-unknown}" > "${logs_dir}/ref"

pytest_args=("$@")

if [ "${#pytest_args[@]}" -ne 0 ]; then
  run_pytest "test" "${pytest_args[@]}"
else
  pytest_args=("--tb=no" "-rN")
  run_pytest "test-connect" "python/pyspark/sql/tests/connect/" "${pytest_args[@]}"
  run_pytest "doctest-column" "--doctest-modules" "python/pyspark/sql/column.py" "${pytest_args[@]}"
  run_pytest "doctest-dataframe" "--doctest-modules" "python/pyspark/sql/dataframe.py" "${pytest_args[@]}"
  run_pytest "doctest-functions" "--doctest-modules" "python/pyspark/sql/functions.py" "${pytest_args[@]}"
fi
