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

# We ignore the pytext exit code so that the job can complete successfully.
python/run-pytest.sh \
  --tb=no -rN --disable-warnings \
  --report-log="${logs_dir}/test.jsonl" \
  python/pyspark/sql/tests/connect/ \
  | tee "${logs_dir}/test.log" || true

echo "${TEST_RUN_GIT_COMMIT:-unknown}" > "${logs_dir}/commit"
echo "${TEST_RUN_GIT_REF:-unknown}" > "${logs_dir}/ref"

# Failed tests are acceptable, but we return a non-zero exit code when there are errors,
# which indicate issues with the test setup.
tail -n 1 "${logs_dir}/test.log" | grep -q -v 'error' || (echo "Errors were found in test results." && false)
