#!/bin/bash

set -euo 'pipefail'

# Find and stop Sail server processes.
pids=$(pgrep -f "sail spark server" | tr '\n' ' ' || echo "")
if [ -n "$pids" ]; then
  echo "Found Sail server processes: $pids"
  # Send SIGINT for graceful shutdown.
  # shellcheck disable=SC2086
  kill -INT $pids 2>/dev/null || true
  # Wait up to 30 seconds for graceful shutdown.
  for _ in {1..30}; do
    if ! pgrep -f "sail spark server" > /dev/null; then
      echo "Sail servers stopped gracefully"
      break
    fi
    sleep 1
  done
  # Force kill if the server is still running.
  remaining_pids=$(pgrep -f "sail spark server" | tr '\n' ' ' || echo "")
  if [ -n "$remaining_pids" ]; then
    echo "Force killing remaining Sail server processes: $remaining_pids"
    # shellcheck disable=SC2086
    kill -KILL $remaining_pids 2>/dev/null || true
  fi
else
  echo "No Sail server processes found"
fi
