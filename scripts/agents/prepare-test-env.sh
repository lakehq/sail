#!/bin/bash

set -euo 'pipefail'

if [ -z "${TEST_PROVIDER:-}" ]; then
  echo "Missing required environment variable: TEST_PROVIDER"
  exit 1
fi

project_path="$(git rev-parse --show-toplevel)"

download_spark_artifact() {
  local spark_version="$1"
  local output_path="$2"

  echo "Finding the latest successful upload of Spark package artifacts..."
  local run_id
  run_id="$(gh run list \
    --workflow spark-package-artifacts.yml \
    --status success \
    --limit 1 \
    --json databaseId \
    --jq '.[] | .databaseId' \
    | head -n 1)"

  if [ -z "${run_id}" ]; then
    echo "Could not find a successful upload of Spark package artifacts"
    return 1
  fi

  echo "Downloading pyspark-${spark_version} from run ${run_id}..."
  gh run download "${run_id}" --name "pyspark-${spark_version}" --dir "${output_path}"
}

prepare_spark_env() {
  local spark_version="$1"
  local spark_tag="v${spark_version}"
  local repo_path="${project_path}/opt/spark"
  local artifact_path="${repo_path}/python/dist"

  mkdir -p "${project_path}/opt"

  echo "Cloning apache/spark..."
  git clone --branch "${spark_tag}" --depth 1 https://github.com/apache/spark.git "${repo_path}"

  mkdir -p "${artifact_path}"
  download_spark_artifact "${spark_version}" "${artifact_path}"

  cd "${project_path}"

  echo "Installing patched PySpark package into Hatch environment..."
  hatch run "test-spark.spark-${spark_version}:install-pyspark"
}

prepare_ibis_env() {
  local repo_path="${project_path}/opt/ibis-testing-data"

  mkdir -p "${project_path}/opt"

  echo "Cloning ibis-project/testing-data..."
  git clone --depth 1 https://github.com/ibis-project/testing-data.git "${repo_path}"
}

case "${TEST_PROVIDER}" in
  spark-3.5.7)
    prepare_spark_env 3.5.7
    ;;
  spark-4.1.1)
    prepare_spark_env 4.1.1
    ;;
  ibis)
    prepare_ibis_env
    ;;
  *)
    echo "Invalid value for TEST_PROVIDER: ${TEST_PROVIDER}"
    exit 1
    ;;
esac
