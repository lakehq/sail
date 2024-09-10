#!/bin/bash

for _ in {1..24}; do
  if grpc_health_probe -addr localhost:50051 -connect-timeout 1000ms -rpc-timeout 1000ms; then
    exit 0
  fi
  sleep 1
done

echo "Error: The Spark Connect server is not ready."
exit 1
