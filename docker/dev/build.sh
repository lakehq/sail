#!/bin/bash

# Build the Docker image for the local development environment.
# The script must be run inside the project directory.
# Usage: build.sh [OPTIONS]
#   OPTIONS: optional arguments to the `docker build` command

set -euo pipefail

project_path="$(git rev-parse --show-toplevel)"

docker build \
  -t sail:latest \
  -f "${project_path}/docker/dev/Dockerfile" \
  "$@" "${project_path}"
