#!/usr/bin/env bash

SAIL_K8S_CLEANUP_PIDS=()
SAIL_K8S_CLEANUP_NAMESPACES=()

sail_k8s_repo_root() {
  cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd
}

sail_k8s_require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

sail_k8s_require_commands() {
  local command
  for command in "$@"; do
    sail_k8s_require_command "${command}"
  done
}

sail_k8s_ensure_kind_cluster() {
  local cluster="$1"
  local config="$2"
  if ! kind get clusters | grep -Fxq "${cluster}"; then
    kind create cluster --name "${cluster}" --config "${config}"
  fi
}

sail_k8s_build_and_load_image() {
  local root="$1"
  local cluster="$2"
  local image="$3"
  local build_image="$4"

  if [[ "${build_image}" != "0" ]]; then
    docker build -t "${image}" -f "${root}/docker/dev/Dockerfile" "${root}"
  fi
  kind load docker-image "${image}" --name "${cluster}"
}

sail_k8s_render_template() {
  local template="$1"
  local output="$2"
  python3 - "${template}" "${output}" <<'PY'
import os
import sys
from pathlib import Path
from string import Template

template = Path(sys.argv[1])
output = Path(sys.argv[2])
output.write_text(Template(template.read_text()).substitute(os.environ))
PY
}

sail_k8s_register_process() {
  SAIL_K8S_CLEANUP_PIDS+=("$1")
}

sail_k8s_register_namespace() {
  SAIL_K8S_CLEANUP_NAMESPACES+=("$1")
}

sail_k8s_cleanup() {
  local status="$?"
  local pid
  local namespace

  trap - EXIT INT TERM
  if [[ "${#SAIL_K8S_CLEANUP_PIDS[@]}" -gt 0 ]]; then
    for pid in "${SAIL_K8S_CLEANUP_PIDS[@]}"; do
      sail_k8s_stop_process "${pid}"
    done
  fi
  if [[ "${SAIL_K8S_KEEP:-0}" != "1" ]]; then
    if [[ "${#SAIL_K8S_CLEANUP_NAMESPACES[@]}" -gt 0 ]]; then
      for namespace in "${SAIL_K8S_CLEANUP_NAMESPACES[@]}"; do
        if [[ -n "${SAIL_K8S_CONTEXT:-}" ]]; then
          kubectl --context "${SAIL_K8S_CONTEXT}" delete namespace "${namespace}" --ignore-not-found=true || true
        else
          kubectl delete namespace "${namespace}" --ignore-not-found=true || true
        fi
      done
    fi
  fi
  return "${status}"
}

sail_k8s_install_cleanup_trap() {
  trap sail_k8s_cleanup EXIT
  trap 'sail_k8s_cleanup; exit 130' INT
  trap 'sail_k8s_cleanup; exit 143' TERM
}

sail_k8s_wait_for_port_forward() {
  local pid="$1"
  local log_file="$2"
  for _ in $(seq 1 120); do
    if grep -q "Forwarding from" "${log_file}" 2>/dev/null; then
      return 0
    fi
    if ! kill -0 "${pid}" 2>/dev/null; then
      cat "${log_file}" >&2 || true
      return 1
    fi
    sleep 0.5
  done
  cat "${log_file}" >&2 || true
  return 1
}

sail_k8s_free_port() {
  python3 - <<'PY'
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

sail_k8s_stop_process() {
  local pid="$1"
  kill "${pid}" 2>/dev/null || true
  wait "${pid}" 2>/dev/null || true
}
