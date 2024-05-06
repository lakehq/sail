#!/bin/bash

set -euo 'pipefail'

project_path="$(dirname "$0")/../.."

cd "${project_path}"/opt/spark

# Build the Spark project.
./build/mvn -Pconnect -Phive -DskipTests clean package

# Create a directory for test logs. This directory is in `.gitignore`.
mkdir -p logs

python -m venv venv
echo '*' > venv/.gitignore
source venv/bin/activate
pip install --upgrade pip
pip install --upgrade -r python/requirements.txt
