#!/bin/bash

set -euo 'pipefail'

if [ -z "${SPARK_VERSION:-}" ]; then
  echo "Missing environment variable: SPARK_VERSION"
  exit 1
fi

project_path="$(git rev-parse --show-toplevel)"

scripts_path="${project_path}/scripts/spark-tests"

source "${project_path}/scripts/shell-tools/git-patch.sh"

apply_git_patch "${project_path}"/opt/spark "v${SPARK_VERSION}" "${scripts_path}/spark-${SPARK_VERSION}.patch"

cd "${project_path}"/opt/spark

maven_opts=(
  --batch-mode
  -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn
  -DskipTests
)

case "${SPARK_VERSION}" in
  4.*)
    ./build/mvn "${maven_opts[@]}" -Phive clean package
    cd python
    python packaging/classic/setup.py sdist
    ;;
  3.*)
    ./build/mvn "${maven_opts[@]}" -Phive -Pconnect clean package
    cd python
    python setup.py sdist
    ;;
  *)
    echo "unsupported Spark version: ${SPARK_VERSION}"
    exit 1
    ;;
esac
