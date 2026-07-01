#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ROOT="$(cd "${K8S_DIR}/../.." && pwd)"
TARGET_DIR="${ROOT}/target/kubernetes/artifacts"
MANIFEST_DIR="${K8S_DIR}/manifests/artifact-tests"

source "${K8S_DIR}/lib.sh"

MODE="${1:-file-store}"
SAIL_K8S_COMMAND="${SAIL_K8S_COMMAND:-scripts/kubernetes/artifact-tests/run.sh}"
SAIL_K8S_CLUSTER="${SAIL_K8S_CLUSTER:-sail-artifacts}"
SAIL_K8S_CONTEXT="${SAIL_K8S_CONTEXT:-kind-${SAIL_K8S_CLUSTER}}"
SAIL_K8S_IMAGE="${SAIL_K8S_IMAGE:-sail:artifacts-$(git -C "${ROOT}" rev-parse --short HEAD)}"
SAIL_K8S_PYTHON="${SAIL_K8S_PYTHON:-3.14}"
SAIL_K8S_BUILD_IMAGE="${SAIL_K8S_BUILD_IMAGE:-1}"
SAIL_K8S_KEEP="${SAIL_K8S_KEEP:-0}"
SAIL_K8S_MINIO_IMAGE="${SAIL_K8S_MINIO_IMAGE:-minio/minio:RELEASE.2025-05-24T17-08-30Z}"
SAIL_K8S_MINIO_BUCKET="${SAIL_K8S_MINIO_BUCKET:-sail-artifact-store}"
SAIL_K8S_MINIO_USER="${SAIL_K8S_MINIO_USER:-admin}"
SAIL_K8S_MINIO_PASSWORD="${SAIL_K8S_MINIO_PASSWORD:-password}"

usage() {
  cat <<EOF
Usage: ${SAIL_K8S_COMMAND} [file-store|s3-object-store|all]

Aliases:
  file -> file-store
  s3   -> s3-object-store

Environment:
  SAIL_K8S_CLUSTER       kind cluster name, default: ${SAIL_K8S_CLUSTER}
  SAIL_K8S_IMAGE         Sail image tag, default: ${SAIL_K8S_IMAGE}
  SAIL_K8S_PYTHON        client Python version for uv, default: ${SAIL_K8S_PYTHON}
  SAIL_K8S_BUILD_IMAGE   set to 0 to skip Docker build, default: ${SAIL_K8S_BUILD_IMAGE}
  SAIL_K8S_KEEP          set to 1 to keep Kubernetes resources, default: ${SAIL_K8S_KEEP}

File-store matrix:
  SAIL_K8S_FILE_NAMESPACE         exact namespace, default: generated
  SAIL_K8S_FILE_NAMESPACE_PREFIX  default: sail-artifacts-file
  SAIL_K8S_FILE_PORT              default: free local port

S3 object-store matrix:
  SAIL_K8S_S3_NAMESPACE_PREFIX  default: sail-artifacts-s3
  SAIL_K8S_MINIO_IMAGE          default: ${SAIL_K8S_MINIO_IMAGE}
  SAIL_K8S_MINIO_BUCKET         default: ${SAIL_K8S_MINIO_BUCKET}
EOF
}

ensure_base() {
  sail_k8s_require_commands docker kind kubectl uv python3
  sail_k8s_ensure_kind_cluster "${SAIL_K8S_CLUSTER}" "${ROOT}/k8s/kind-config.yaml"
}

cleanup_namespace() {
  local namespace="$1"
  if [[ "${SAIL_K8S_KEEP}" != "1" ]]; then
    kubectl --context "${SAIL_K8S_CONTEXT}" delete namespace "${namespace}" --ignore-not-found=true
  fi
}

run_file_store() {
  local run_id
  local namespace
  local port
  local manifest
  local port_forward_log
  local port_forward_pid
  local status

  run_id="$(git -C "${ROOT}" rev-parse --short HEAD)-$(date +%s)"
  namespace="${SAIL_K8S_FILE_NAMESPACE:-${SAIL_K8S_FILE_NAMESPACE_PREFIX:-sail-artifacts-file}-${run_id}}"
  port="${SAIL_K8S_FILE_PORT:-$(sail_k8s_free_port)}"
  manifest="${TARGET_DIR}/file-store-${run_id}.yaml"
  port_forward_log="${TARGET_DIR}/file-store-port-forward-${run_id}.log"

  mkdir -p "${TARGET_DIR}"
  sail_k8s_register_namespace "${namespace}"
  export SAIL_K8S_NAMESPACE="${namespace}"
  export SAIL_K8S_IMAGE
  sail_k8s_render_template "${MANIFEST_DIR}/file-store.yaml" "${manifest}"

  kubectl --context "${SAIL_K8S_CONTEXT}" apply -f "${manifest}"
  kubectl --context "${SAIL_K8S_CONTEXT}" -n "${namespace}" rollout status deployment/sail-spark-server --timeout=180s
  kubectl --context "${SAIL_K8S_CONTEXT}" -n "${namespace}" exec deployment/sail-spark-server -- sh -lc \
    'rm -rf /tmp/sail/artifact-root/* /tmp/sail/artifact-store/* && mkdir -p /tmp/sail/artifact-root /tmp/sail/artifact-store'

  kubectl --context "${SAIL_K8S_CONTEXT}" -n "${namespace}" port-forward \
    service/sail-spark-server "${port}:50051" --address 127.0.0.1 >"${port_forward_log}" 2>&1 &
  port_forward_pid="$!"
  sail_k8s_register_process "${port_forward_pid}"
  sail_k8s_wait_for_port_forward "${port_forward_pid}" "${port_forward_log}"

  set +e
  env \
    SAIL_K8S_CONTEXT="${SAIL_K8S_CONTEXT}" \
    SAIL_K8S_NAMESPACE="${namespace}" \
    SAIL_K8S_ARTIFACT_RUN_ID="${run_id}" \
    SPARK_REMOTE="sc://localhost:${port}" \
    uv run --no-project --python "${SAIL_K8S_PYTHON}" --with "pyspark-client==4.1.1" \
      python "${SCRIPT_DIR}/matrix.py"
  status="$?"
  set -e

  sail_k8s_stop_process "${port_forward_pid}"
  cleanup_namespace "${namespace}"
  return "${status}"
}

run_s3_object_store() {
  local namespace="${SAIL_K8S_S3_NAMESPACE:-${SAIL_K8S_S3_NAMESPACE_PREFIX:-sail-artifacts-s3}-$(date +%s)}"
  local manifest
  local minio_port
  local sail_port
  local minio_port_forward_log
  local sail_port_forward_log
  local minio_port_forward_pid
  local sail_port_forward_pid
  local status

  manifest="${TARGET_DIR}/s3-object-store-${namespace}.yaml"
  minio_port="$(sail_k8s_free_port)"
  sail_port="$(sail_k8s_free_port)"
  minio_port_forward_log="${TARGET_DIR}/s3-object-store-minio-port-forward-${namespace}.log"
  sail_port_forward_log="${TARGET_DIR}/s3-object-store-sail-port-forward-${namespace}.log"

  if ! docker image inspect "${SAIL_K8S_MINIO_IMAGE}" >/dev/null 2>&1; then
    docker pull "${SAIL_K8S_MINIO_IMAGE}"
  fi
  kind load docker-image "${SAIL_K8S_MINIO_IMAGE}" --name "${SAIL_K8S_CLUSTER}"

  mkdir -p "${TARGET_DIR}"
  sail_k8s_register_namespace "${namespace}"
  export SAIL_K8S_NAMESPACE="${namespace}"
  export SAIL_K8S_IMAGE
  export SAIL_K8S_MINIO_IMAGE
  export SAIL_K8S_MINIO_BUCKET
  export SAIL_K8S_MINIO_USER
  export SAIL_K8S_MINIO_PASSWORD
  sail_k8s_render_template "${MANIFEST_DIR}/s3-object-store.yaml" "${manifest}"

  kubectl --context "${SAIL_K8S_CONTEXT}" apply -f "${manifest}"
  kubectl --context "${SAIL_K8S_CONTEXT}" -n "${namespace}" rollout status deployment/minio --timeout=180s

  kubectl --context "${SAIL_K8S_CONTEXT}" -n "${namespace}" port-forward \
    service/minio "${minio_port}:9000" --address 127.0.0.1 >"${minio_port_forward_log}" 2>&1 &
  minio_port_forward_pid="$!"
  sail_k8s_register_process "${minio_port_forward_pid}"
  sail_k8s_wait_for_port_forward "${minio_port_forward_pid}" "${minio_port_forward_log}"

  kubectl --context "${SAIL_K8S_CONTEXT}" -n "${namespace}" rollout status deployment/sail-spark-server --timeout=180s
  kubectl --context "${SAIL_K8S_CONTEXT}" -n "${namespace}" port-forward \
    service/sail-spark-server "${sail_port}:50051" --address 127.0.0.1 >"${sail_port_forward_log}" 2>&1 &
  sail_port_forward_pid="$!"
  sail_k8s_register_process "${sail_port_forward_pid}"
  sail_k8s_wait_for_port_forward "${sail_port_forward_pid}" "${sail_port_forward_log}"

  set +e
  env \
    SAIL_K8S_CONTEXT="${SAIL_K8S_CONTEXT}" \
    SAIL_K8S_NAMESPACE="${namespace}" \
    SAIL_K8S_MINIO_PORT="${minio_port}" \
    SAIL_K8S_MINIO_BUCKET="${SAIL_K8S_MINIO_BUCKET}" \
    SAIL_K8S_MINIO_USER="${SAIL_K8S_MINIO_USER}" \
    SAIL_K8S_MINIO_PASSWORD="${SAIL_K8S_MINIO_PASSWORD}" \
    SPARK_REMOTE="sc://localhost:${sail_port}" \
    uv run --no-project --python "${SAIL_K8S_PYTHON}" --with "pyspark-client==4.1.1" --with "boto3>=1.38,<2" \
      python "${SCRIPT_DIR}/s3_object_store.py"
  status="$?"
  set -e

  sail_k8s_stop_process "${sail_port_forward_pid}"
  sail_k8s_stop_process "${minio_port_forward_pid}"
  cleanup_namespace "${namespace}"
  return "${status}"
}

main() {
  sail_k8s_install_cleanup_trap
  case "${MODE}" in
    -h|--help|help)
      usage
      ;;
    file|file-store)
      ensure_base
      sail_k8s_build_and_load_image "${ROOT}" "${SAIL_K8S_CLUSTER}" "${SAIL_K8S_IMAGE}" "${SAIL_K8S_BUILD_IMAGE}"
      run_file_store
      ;;
    s3|s3-object-store)
      ensure_base
      sail_k8s_build_and_load_image "${ROOT}" "${SAIL_K8S_CLUSTER}" "${SAIL_K8S_IMAGE}" "${SAIL_K8S_BUILD_IMAGE}"
      run_s3_object_store
      ;;
    all)
      ensure_base
      sail_k8s_build_and_load_image "${ROOT}" "${SAIL_K8S_CLUSTER}" "${SAIL_K8S_IMAGE}" "${SAIL_K8S_BUILD_IMAGE}"
      run_file_store
      SAIL_K8S_BUILD_IMAGE=0 run_s3_object_store
      ;;
    *)
      usage >&2
      exit 2
      ;;
  esac
}

main "$@"
