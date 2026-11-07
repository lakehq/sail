#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(git -C "${script_dir}" rev-parse --show-toplevel)"
image="${1:-sail-production:local}"
source_revision="$(git -C "${repo_root}" rev-parse HEAD)"
rust_profile="${RUST_PROFILE:-dev}"

docker build \
  --file "${repo_root}/deployments/production/Dockerfile" \
  --build-arg "RUST_PROFILE=${rust_profile}" \
  --build-arg "SOURCE_REVISION=${source_revision}" \
  --tag "${image}" \
  "${repo_root}"

docker image inspect \
  --format='image={{index .RepoTags 0}} id={{.Id}} revision={{index .Config.Labels "org.opencontainers.image.revision"}}' \
  "${image}"
