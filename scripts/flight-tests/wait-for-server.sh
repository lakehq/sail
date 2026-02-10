#!/bin/bash

echo "Waiting for Flight SQL server to start..."

for i in {1..30}; do
  if grpc_health_probe -addr localhost:32010 -connect-timeout 1000ms -rpc-timeout 1000ms 2>/dev/null; then
    echo "Flight SQL server is ready!"
    exit 0
  fi
  echo "Attempt $i/30: Server not ready yet..."
  sleep 1
done

echo "Error: The Flight SQL server is not ready after 30 seconds."
exit 1
