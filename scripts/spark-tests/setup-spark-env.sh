#!/bin/bash

set -euo 'pipefail'

project_path="$(dirname "$0")/../.."

cd "${project_path}"/opt/spark

# Build the Spark project.
# We do not include the "clean" goal so that the build output can be reused.
./build/mvn \
  --batch-mode \
  -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn \
  -Pconnect -Phive -DskipTests package

# Create a directory for test logs. This directory is in `.gitignore`.
mkdir -p logs

python -m venv venv
echo '*' > venv/.gitignore
source venv/bin/activate
pip install --upgrade pip
pip install --upgrade -r python/requirements.txt
