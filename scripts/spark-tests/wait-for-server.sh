#!/bin/bash

for _ in {1..20}; do
  if grpc_health_probe -addr localhost:50051 -connect-timeout 500ms -rpc-timeout 500ms; then
    exit 0
  fi
  sleep 3
done

echo "Error: The Spark Connect server is not ready."
exit 1
