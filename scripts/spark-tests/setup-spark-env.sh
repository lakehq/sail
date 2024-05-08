#!/bin/bash

set -euo 'pipefail'

project_path="$(dirname "$0")/../.."

cd "${project_path}"/opt/spark

if [ -d venv ]; then
  echo "The virtual environment already exists."
  exit 1
fi

# Create a virtual environment with dependencies to run PySpark tests.
# Note that we do not install the "pyspark" library itself,
# since we will use the PySpark source code instead.
python -m venv venv
echo '*' > venv/.gitignore
source venv/bin/activate
pip install --upgrade pip
pip install --upgrade -r python/requirements.txt
