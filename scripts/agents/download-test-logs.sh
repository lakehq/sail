#!/bin/bash

set -euo 'pipefail'

if [ -z "${TEST_PROVIDER:-}" ]; then
  echo "Missing required environment variable: TEST_PROVIDER"
  exit 1
fi

case "${TEST_PROVIDER}" in
  spark-3.5.7|spark-4.1.1|ibis)
    artifact_name="${TEST_PROVIDER}-test-logs"
    ;;
  *)
    echo "Invalid value for TEST_PROVIDER: ${TEST_PROVIDER}"
    exit 1
    ;;
esac

project_path="$(git rev-parse --show-toplevel)"

echo "Finding the latest successful build workflow run..."
run_id="$(gh run list \
  --workflow build.yml \
  --branch main \
  --status success \
  --limit 1 \
  --json databaseId \
  --jq '.[] | .databaseId' \
  | head -n 1)"

if [ -z "${run_id}" ]; then
  echo "Could not find a successful build workflow run"
  exit 1
fi

download_path="${project_path}/tmp/spark-tests/${TEST_PROVIDER}-${run_id}"
mkdir -p "${download_path}"

echo "Downloading ${artifact_name} from build workflow run ${run_id}..."
gh run download "${run_id}" --name "${artifact_name}" --dir "${download_path}"

echo "Downloaded test logs to ${download_path}"
