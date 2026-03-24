#!/bin/bash

set -euo 'pipefail'

if [ -z "${VIRTUAL_ENV:-}" ]; then
  echo "The tests must be run in a Python virtual environment."
  echo "Please run the script via \`hatch run <env>:<command> <options>\`."
  exit 1
fi

echo "Python environment: ${VIRTUAL_ENV}"

project_path="$(git rev-parse --show-toplevel)"

case "$(basename "${VIRTUAL_ENV}")" in
  test-spark.*)
    plugin_args=("-p" "plugins.spark")
    test_run_name="${TEST_RUN_NAME:-latest}"
    export SPARK_TESTING="1"
    export PYARROW_IGNORE_TIMEZONE="1"
    # Define a few environment variables if they are not set.
    # This does not override an environment variable if it is set but empty.
    # If the remote port is empty, the test runs against the original Spark Connect server.
    export SPARK_TESTING_REMOTE_PORT="${SPARK_TESTING_REMOTE_PORT-50051}"
    export SPARK_LOCAL_IP="${SPARK_LOCAL_IP-127.0.0.1}"
    ;;
  test-ibis)
    plugin_args=("-p" "plugins.ibis")
    test_run_name="${TEST_RUN_NAME:-ibis}"
    export IBIS_TESTING="1"
    export IBIS_TESTING_DATA_DIR="${project_path}/opt/ibis-testing-data"
    ;;
  *)
    echo "Error: This is not a valid test environment."
    exit 1
    ;;
esac

# Define the directory for test logs. The `tmp/` directory is in `.gitignore`.
logs_dir="${project_path}/tmp/spark-tests/${test_run_name}"
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

# Make the plugins available on `PYTHONPATH`.
export PYTHONPATH="${project_path}/scripts/spark-tests"

function run_pytest() {
  name="$1"
  args=("${@:2}")

  echo "Test suite: ${name}"
  # We ignore the pytext exit code so that the command can complete successfully.
  pytest \
    "${plugin_args[@]}" \
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
  case "$(basename "${VIRTUAL_ENV}")" in
    test-spark.*)
      run_pytest test-connect --pyargs pyspark.sql.tests.connect -n 4 --tb=no -rN
      run_pytest doctest-catalog --doctest-modules --pyargs pyspark.sql.catalog --tb=no -rN
      run_pytest doctest-column --doctest-modules --pyargs pyspark.sql.column --tb=no -rN
      run_pytest doctest-dataframe --doctest-modules --pyargs pyspark.sql.dataframe --tb=no -rN
      run_pytest doctest-functions --doctest-modules --pyargs pyspark.sql.functions --tb=no -rN
      ;;
    test-ibis)
      # The Ibis tests are not run in CI for now due to setup errors related to Spark streaming.
      run_pytest test-ibis --pyargs ibis.backends -m pyspark --tb=no -rN
      ;;
  esac
fi
