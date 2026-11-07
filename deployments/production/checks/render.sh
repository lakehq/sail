#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(git -C "${script_dir}" rev-parse --show-toplevel)"
overlay="${1:-${repo_root}/deployments/production/overlays/local-evaluation}"

kubectl kustomize "${overlay}"
