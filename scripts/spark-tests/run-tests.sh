#!/bin/bash

set -euo 'pipefail'

project_path="$(dirname "$0")/../.."

cd "${project_path}"/opt/spark

source venv/bin/activate

# Define the directory for test logs. The `logs/` directory is in `.gitignore`.
logs_dir="logs/${TEST_RUN_NAME:-latest}"

mkdir -p "${logs_dir}"

export SPARK_TESTING_REMOTE_PORT=50051
export SPARK_LOCAL_IP=127.0.0.1

pytest_args=("$@")

if [ "${#pytest_args[@]}" -eq 0 ]; then
  # We run Spark Connect tests by default.
  pytest_args+=("python/pyspark/sql/tests/connect/")
  pytest_args+=("--tb=no")
  pytest_args+=("-rN")
fi

# We ignore the pytext exit code so that the command can complete successfully.
python/run-pytest.sh \
  --disable-warnings \
  --report-log="${logs_dir}/test.jsonl" \
  "${pytest_args[@]}" \
  | tee "${logs_dir}/test.log" || true

echo "${TEST_RUN_GIT_COMMIT:-unknown}" > "${logs_dir}/commit"
echo "${TEST_RUN_GIT_REF:-unknown}" > "${logs_dir}/ref"

# Failed tests are acceptable, but we return a non-zero exit code when there are errors,
# which indicate issues with the test setup.
tail -n 1 "${logs_dir}/test.log" | grep -q -v 'error' || (echo "Errors were found in test results." && false)
