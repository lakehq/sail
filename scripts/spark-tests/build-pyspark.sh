#!/bin/bash

set -euo 'pipefail'

project_path="$(git rev-parse --show-toplevel)"
scripts_path="${project_path}/scripts/spark-tests"

command -v hatch >/dev/null 2>&1 || { echo >&2 "please install hatch before continuing"; exit 1; }


# does nothing if the directory already exists
mkdir -p "${project_path}/opt/log"

if [ ! -d "${project_path}/opt/spark" ]; then
  git clone https://github.com/apache/spark.git "${project_path}/opt/spark"
fi

if [ ! -d "${project_path}/opt/ibis-testing-data" ]; then
  git clone https://github.com/ibis-project/testing-data.git "${project_path}/opt/ibis-testing-data"
fi

source "${project_path}/scripts/shell-tools/git-patch.sh"

if [ ! -f "${project_path}/.venvs/default/bin/activate" ]; then
  cd "${project_path}"
  hatch env create
fi

cd "${project_path}"/opt/spark
vers="3.5.1"
# check if a lock file exists, and apply the patch if not
if [ ! -f "${project_path}/opt/log/v${vers}-patchlock.txt" ]; then
  #logfile="${project_path}/opt/v${vers}-patcherr.log"
  apply_git_patch "v${vers}" "${scripts_path}/spark-${vers}.patch"
  touch "${project_path}/opt/log/v${vers}-patchlock.txt"
fi



if [ ! -f "${project_path}/opt/log/v${vers}-mvnlock.txt" ]; then
  ./build/mvn \
    --batch-mode --threads $(nproc) \
    -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn \
    -DskipTests \
    -Phive -Pconnect clean package

    rc=$?
  # tried capturing stderr (ie 2> $logfile)
  # but it would produce output below even if successful
  # using `mvn` from path: /opt/spark/path/to/mvn
  if [ $rc -ne 0 ]; then
    echo "Error building Spark"
    exit 1
  else
    touch "${project_path}/opt/log/v${vers}-mvnlock.txt"
  fi

fi


source "${project_path}/.venvs/default/bin/activate"
cd python
python setup.py sdist
