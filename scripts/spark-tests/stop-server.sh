#!/bin/bash

set -euo 'pipefail'

# Find and kill sail server processes
SAIL_PIDS=$(pgrep -f "sail spark server" || echo "")
if [ -n "$SAIL_PIDS" ]; then
  echo "Found sail server processes: $SAIL_PIDS"
  # Send SIGINT for graceful shutdown
  kill -INT $SAIL_PIDS 2>/dev/null || true
  # Wait up to 30 seconds for graceful shutdown
  for i in {1..30}; do
    if ! pgrep -f "sail spark server" > /dev/null; then
      echo "Sail server stopped gracefully"
      break
    fi
    sleep 1
  done
  # Force kill if still running
  REMAINING_PIDS=$(pgrep -f "sail spark server" || echo "")
  if [ -n "$REMAINING_PIDS" ]; then
    echo "Force killing remaining processes: $REMAINING_PIDS"
    kill -KILL $REMAINING_PIDS 2>/dev/null || true
  fi
else
  echo "No sail server processes found"
fi

sleep 10
