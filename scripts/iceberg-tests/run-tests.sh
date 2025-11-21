#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

COMPOSE_FILE="docker/iceberg/compose.yml"

SERVER_PID=""

cleanup() {
  if [ -n "${SERVER_PID:-}" ] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi

  if command -v docker >/dev/null 2>&1; then
    docker compose -f "${COMPOSE_FILE}" down -v || true
  fi
}

trap cleanup EXIT

echo "Starting Iceberg REST + MinIO services..."
docker compose -f "${COMPOSE_FILE}" up -d

echo "Waiting for Iceberg REST catalog to become available..."
REST_READY=0
for _ in $(seq 1 60); do
  if curl -sS "http://localhost:8181/v1/config" >/dev/null 2>&1; then
    REST_READY=1
    break
  fi
  sleep 1
done

if [ "${REST_READY}" -ne 1 ]; then
  echo "Timed out waiting for Iceberg REST catalog on http://localhost:8181"
  exit 1
fi

export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-admin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-password}"
export AWS_ENDPOINT="${AWS_ENDPOINT:-http://localhost:9000}"
export AWS_ALLOW_HTTP="${AWS_ALLOW_HTTP:-true}"

export ICEBERG_REST_URI="${ICEBERG_REST_URI:-http://localhost:8181}"
export ICEBERG_S3_ENDPOINT="${ICEBERG_S3_ENDPOINT:-http://localhost:9000}"
export ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE:-s3://warehouse/}"

: "${SAIL_CATALOG__LIST:='[{name="iceberg_dev", type="iceberg-rest", uri="http://localhost:8181", warehouse="s3://warehouse/"}]'}"
: "${SAIL_CATALOG__DEFAULT_CATALOG:='"iceberg_dev"'}"
export SAIL_CATALOG__LIST
export SAIL_CATALOG__DEFAULT_CATALOG

echo "Starting Sail Spark server..."
hatch run scripts/spark-tests/run-server.sh &
SERVER_PID=$!

echo "Waiting for Sail Spark server to accept connections..."
READY=0
for _ in $(seq 1 60); do
  if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
    echo "Sail Spark server exited before becoming ready"
    exit 1
  fi
  if (echo > /dev/tcp/127.0.0.1/50051) >/dev/null 2>&1; then
    READY=1
    break
  fi
  sleep 1
done

if [ "${READY}" -ne 1 ]; then
  echo "Timed out waiting for Sail Spark server on port 50051"
  exit 1
fi

export TEST_PROFILE="${TEST_PROFILE:-integration}"
export SPARK_REMOTE="${SPARK_REMOTE:-sc://localhost:50051}"

echo "Running Iceberg integration tests..."
hatch run pytest python/pysail/tests/spark/iceberg/


