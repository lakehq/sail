#!/bin/bash

set -euo 'pipefail'

echo "Stopping Flight SQL server..."

# Find and kill sail-flight server processes
SAIL_PIDS=$(pgrep -f "sail-flight.*server" || echo "")
if [ -n "$SAIL_PIDS" ]; then
  echo "Found Flight SQL server processes: $SAIL_PIDS"
  # Send SIGINT for graceful shutdown
  kill -INT $SAIL_PIDS 2>/dev/null || true
  # Wait up to 30 seconds for graceful shutdown
  for i in {1..30}; do
    if ! pgrep -f "sail-flight.*server" > /dev/null; then
      echo "Flight SQL server stopped gracefully"
      break
    fi
    sleep 1
  done
  # Force kill if still running
  REMAINING_PIDS=$(pgrep -f "sail-flight.*server" || echo "")
  if [ -n "$REMAINING_PIDS" ]; then
    echo "Force killing remaining processes: $REMAINING_PIDS"
    kill -KILL $REMAINING_PIDS 2>/dev/null || true
  fi
else
  echo "No Flight SQL server processes found"
fi

sleep 5
