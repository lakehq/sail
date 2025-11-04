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
    -p no:randomly \
    -o "doctest_optionflags=ELLIPSIS NORMALIZE_WHITESPACE IGNORE_EXCEPTION_DETAIL NUMBER" \
    -o "faulthandler_timeout=30" \
    -vv \
    --durations=20 -s \
    --basetemp="${pytest_tmp_dir}" \
    --disable-warnings \
    --strict-markers \
    --report-log="${logs_dir}/${name}.jsonl" \
    -k "not test_array and not test_aggregation and not test_join and not test_client and not test_generic" \
    "${args[@]}" \
    | tee "${logs_dir}/${name}.log" || true


#    -k "(test_array and not (test_array_flatten or test_unnest_simple or test_array_position \
#    or test_range_start_stop_step or test_array_unique or test_array_agg_numeric \
#    or test_array_concat_variadic or test_array_column or test_array_map or test_array_filter or test_timestamp_range_zero_step)) \
#    or (test_aggregation and not (test_group_concat or test_approx_quantile or test_group_by_expr)) \
#    or (test_client and not (test_create_table_in_memory or test_insert_no_overwrite_from_expr)) \
#    or (test_generic and not (test_sample_with_seed or test_simple_memtable_construct or test_select_mutate_with_dict))" \

#    -k "not test_array and not test_aggregation and not test_join and not test_client and not test_generic" \
#    -k "test_array and not test_array_flatten and not test_unnest_simple and not test_array_position and not test_range_start_stop_step and not test_array_unique and not test_array_agg_numeric and not test_array_concat_variadic and not test_array_column and not test_array_map and not test_array_filter" \
#    -k "test_aggregation and not test_group_concat and not test_approx_quantile" \
#    -k "test_join" \ # funciona solo
#    -k "test_client and not test_create_table_in_memory" \
#    -k "test_generic and not test_sample_with_seed and not test_simple_memtable_construct and not test_select_mutate_with_dict" \

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
      run_pytest test-connect --pyargs pyspark.sql.tests.connect --tb=no -rN
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
