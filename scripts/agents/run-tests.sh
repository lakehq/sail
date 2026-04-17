#!/bin/bash

set -euo 'pipefail'

if [ -z "${TEST_PROVIDER:-}" ]; then
  echo "Missing required environment variable: TEST_PROVIDER"
  exit 1
fi

if [ "$#" -eq 0 ]; then
  echo "At least one test argument is required"
  echo "Usage: TEST_PROVIDER=<provider> $0 <args...>"
  exit 1
fi

case "${TEST_PROVIDER}" in
  spark-3.5.7|spark-4.1.1)
    hatch_env="test-spark.${TEST_PROVIDER}"
    test_envs=()
    ;;
  ibis)
    hatch_env="test-ibis"
    test_envs=("SPARK_REMOTE=sc://localhost:50051")
    ;;
  *)
    echo "Invalid value for TEST_PROVIDER: ${TEST_PROVIDER}"
    exit 1
    ;;
esac

project_path="$(git rev-parse --show-toplevel)"

clean_up() {
  local status=$?
  trap - EXIT INT TERM
  "${project_path}/scripts/spark-tests/stop-server.sh" || true
  exit "${status}"
}

trap clean_up EXIT INT TERM

cd "${project_path}"

echo "Building editable package..."
hatch run "${hatch_env}:maturin develop"

echo "Starting Spark Connect server..."
hatch run "${hatch_env}:env" CI=1 scripts/spark-tests/run-server.sh &

"${project_path}/scripts/spark-tests/wait-for-server.sh"

echo "Running tests..."
hatch run "${hatch_env}:env" TEST_RUN_NAME=selected "${test_envs[@]}" scripts/spark-tests/run-tests.sh "$@"
