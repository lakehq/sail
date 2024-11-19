#!/bin/bash

set -euo pipefail

# Build the Docker image using a Git release tag.
# The script must be run from the directory containing the Dockerfile.
# Usage: build.sh TAG [OPTIONS]
#   TAG: the Git release tag to use
#   OPTIONS: optional arguments to the `docker build` command

release_tag="$1"

if [ -z "${release_tag}" ]; then
    echo "Error: the release tag argument is required"
    exit 1
fi

# We create an empty temporary directory to emulate an empty build context.
build_context_dir="$(mktemp -d)"

trap finalize EXIT

function finalize() {
  rm -rvf "${build_context_dir}" >&2
  echo "The temporary build context directory has been removed: ${build_context_dir}" >&2
}

docker build \
  -t sail:latest \
  --build-arg RELEASE_TAG="${release_tag}" \
  "$@" "${build_context_dir}"
