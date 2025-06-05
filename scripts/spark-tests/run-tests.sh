#!/bin/bash

set -euo 'pipefail'

spark_version="${SPARK_VERSION:-4.0.0}"

project_path="$(git rev-parse --show-toplevel)"

# Define the directory for test logs. The `tmp/` directory is in `.gitignore`.
logs_dir="${project_path}/tmp/spark-tests/${TEST_RUN_NAME:-latest}"
pytest_tmp_dir="${project_path}/tmp/pytest"

cd "${project_path}"

echo "Removing existing test logs..."
rm -rf "${logs_dir}"
mkdir -p "${logs_dir}"

# Force color output for local test runs.
if [ -z "${CI:-}" ]; then
  export FORCE_COLOR="1"
fi
# The test name can be long, so we set the terminal width
# for better test log readability.
export COLUMNS="120"

export SPARK_TESTING="1"
export PYARROW_IGNORE_TIMEZONE="1"
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
  # The plugins are available on `PYTHONPATH` for the `test` environment configured in `pyproject.toml`.
  hatch run test.spark-"${spark_version}":pytest \
    -p plugins.spark \
    -p plugins.ibis \
    -o "doctest_optionflags=ELLIPSIS NORMALIZE_WHITESPACE IGNORE_EXCEPTION_DETAIL NUMBER" \
    -o "faulthandler_timeout=30" \
    --basetemp="${pytest_tmp_dir}" \
    --disable-warnings \
    --strict-markers \
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
  run_pytest test "${pytest_args[@]}"
else
  pytest_args=("--tb=no" "-rN")
  run_pytest test-connect --pyargs pyspark.sql.tests.connect "${pytest_args[@]}"
  # The Ibis tests are not run for now due to setup errors related to Spark streaming.
  # run_pytest test-ibis --pyargs ibis.backends -m pyspark "${pytest_args[@]}"
  run_pytest doctest-catalog --doctest-modules --pyargs pyspark.sql.catalog "${pytest_args[@]}"
  run_pytest doctest-column --doctest-modules --pyargs pyspark.sql.column "${pytest_args[@]}"
  run_pytest doctest-dataframe --doctest-modules --pyargs pyspark.sql.dataframe "${pytest_args[@]}"
  run_pytest doctest-functions --doctest-modules --pyargs pyspark.sql.functions "${pytest_args[@]}"
fi
