#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export SAIL_K8S_COMMAND="${SAIL_K8S_COMMAND:-scripts/kubernetes/test-artifacts.sh}"
exec "${ROOT}/scripts/kubernetes/artifact-tests/run.sh" "$@"
