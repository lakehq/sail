#!/bin/bash

set -euo 'pipefail'

project_path="$(dirname "$0")/../.."

cd "${project_path}"/opt/spark

./build/mvn \
  --batch-mode \
  -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn \
  -DskipTests \
  -Phive clean package
