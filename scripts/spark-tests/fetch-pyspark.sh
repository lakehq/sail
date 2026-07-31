#!/usr/bin/env bash

# Download the patched PySpark packages used by the Spark tests, instead of
# building them locally (~30 minutes each).
#
# The packages are published by the `spark-package-artifacts.yml` workflow as
# GitHub Actions artifacts. Those are not anonymously downloadable, so we fetch
# them through nightly.link, which proxies the artifacts without authentication
# (no `gh` and no token required). Only the versions whose tarball is missing
# under `opt/spark/python/dist/` are downloaded, mirroring the auto-clone of
# `ibis-testing-data`.
#
# nightly.link only indexes `push` runs, while the artifacts workflow is
# `workflow_dispatch`, so we pin the run id explicitly. Bump it after
# republishing the artifacts (or override via SAIL_SPARK_ARTIFACTS_RUN_ID).

set -euo 'pipefail'

repo="${SAIL_REPO:-lakehq/sail}"
run_id="${SAIL_SPARK_ARTIFACTS_RUN_ID:-26805537217}"

# Versions to ensure. Pass one or more as arguments (e.g. `fetch-pyspark.sh 3.5.7`)
# to fetch only those; with no arguments, ensure all known versions.
known_versions=("3.5.7" "4.1.1")
if [ "$#" -gt 0 ]; then
  versions=("$@")
else
  versions=("${known_versions[@]}")
fi

project_path="$(git rev-parse --show-toplevel)"
dist_dir="${project_path}/opt/spark/python/dist"

missing=()
for version in "${versions[@]}"; do
  if [ ! -f "${dist_dir}/pyspark-${version}.tar.gz" ]; then
    missing+=("${version}")
  fi
done

if [ "${#missing[@]}" -eq 0 ]; then
  echo "⛵ PySpark packages already present: ${versions[*]}"
  exit 0
fi

for tool in curl unzip; do
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "Error: '${tool}' is required to download the PySpark packages." >&2
    exit 1
  fi
done

mkdir -p "${dist_dir}"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

for version in "${missing[@]}"; do
  url="https://nightly.link/${repo}/actions/runs/${run_id}/pyspark-${version}.zip"
  zip_path="${tmp_dir}/pyspark-${version}.zip"

  echo "⛵ Downloading pyspark-${version} from nightly.link..."
  if ! curl -fSL --retry 3 -o "${zip_path}" "${url}"; then
    echo "Error: failed to download ${url}" >&2
    echo "The run may be wrong/expired. Set SAIL_SPARK_ARTIFACTS_RUN_ID to a" >&2
    echo "successful 'spark-package-artifacts.yml' run, or build locally via" >&2
    echo "scripts/spark-tests/build-pyspark.sh." >&2
    exit 1
  fi

  echo "⛵ Extracting pyspark-${version}.tar.gz..."
  unzip -o -j "${zip_path}" "pyspark-${version}.tar.gz" -d "${dist_dir}"
done

echo "⛵ Done. Tarballs in ${dist_dir}:"
ls -1 "${dist_dir}"/pyspark-*.tar.gz
