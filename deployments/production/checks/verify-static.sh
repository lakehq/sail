#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(git -C "${script_dir}" rev-parse --show-toplevel)"
overlay="${1:-${repo_root}/deployments/production/overlays/local-evaluation}"
require_image_digest="${REQUIRE_IMAGE_DIGEST:-0}"
rendered=""

cleanup() {
  rm -f "${rendered}"
}
trap cleanup EXIT

rendered="$(mktemp "${TMPDIR:-/tmp}/sail-production-rendered.XXXXXX.yaml")"
"${script_dir}/render.sh" "${overlay}" >"${rendered}"

verify_args=("${rendered}")
if [[ "${require_image_digest}" == "1" ]]; then
  verify_args=(--require-image-digest "${verify_args[@]}")
fi
node "${script_dir}/verify-rendered.mjs" "${verify_args[@]}"

if ! grep -Eq '^STOPSIGNAL SIGINT$' \
  "${repo_root}/deployments/production/Dockerfile"; then
  printf 'FAIL production image must define SIGINT as its stop signal\n' >&2
  exit 1
fi

printf 'PASS production image declares SIGINT as its container stop signal\n'
printf 'PASS production overlay renders and static checks pass\n'
