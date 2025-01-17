#!/bin/bash

set -euo 'pipefail'

project_path="$(git rev-parse --show-toplevel)"

scripts_path="${project_path}/scripts/spark-tests"

source "${project_path}/scripts/shell-tools/git-patch.sh"

cd "${project_path}"/opt/spark

apply_git_patch "v3.5.4" "${scripts_path}"/spark-3.5.4.patch

./build/mvn \
  --batch-mode \
  -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn \
  -DskipTests \
  -Phive -Pconnect clean package

cd python
python setup.py sdist
