#!/bin/bash

echo "Waiting for Flight SQL server to start..."

for i in {1..24}; do
  if grpc_health_probe -addr localhost:32010 -connect-timeout 1000ms -rpc-timeout 1000ms; then
    echo "Flight SQL server is ready!"
    exit 0
  fi
  echo "Attempt $i/24: Server not ready yet..."
  sleep 1
done

echo "Error: The Flight SQL server is not ready after 24 seconds."
exit 1
