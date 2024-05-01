#!/bin/bash

set -euo 'pipefail'

cd "${SPARK_PROJECT_PATH}" || exit

git checkout v3.5.1 && git add . && git stash
git apply "${FRAMEWORK_PROJECT_PATH}"/scripts/spark-connect-server/spark-3.5.1.patch

# Build the Spark project.
./build/mvn -Pconnect -Phive -DskipTests clean package

# Create a directory for test logs. This directory is in `.gitignore`.
mkdir -p logs

python -m venv venv
echo '*' > venv/.gitignore
source venv/bin/activate
pip install --upgrade pip
pip install --upgrade -r python/requirements.txt