#!/bin/bash

set -euo 'pipefail'

project_path="$(dirname "$0")/../.."

cd "${project_path}"/opt/spark

# Build the Spark project.
./build/mvn \
  --batch-mode \
  -T 1C \
  -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn \
  -DskipTests \
  -Phive clean package

# Create a virtual environment with dependencies to run PySpark tests.
# Note that we do not install the "pyspark" library itself,
# since we will use the PySpark source code instead.
python -m venv venv
echo '*' > venv/.gitignore
source venv/bin/activate
pip install --upgrade pip
pip install --upgrade -r python/requirements.txt
