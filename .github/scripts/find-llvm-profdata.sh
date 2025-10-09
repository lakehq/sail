#!/bin/bash
set -euo pipefail

SYSROOT=$(rustc +stable --print sysroot)
HOST=$(rustc +stable -vV | awk '/^host:/ {print $2}')
CANDIDATE="$SYSROOT/lib/rustlib/$HOST/bin/llvm-profdata"
if [ ! -f "$CANDIDATE" ]; then
  CANDIDATE=$(find "$SYSROOT/lib/rustlib" -name llvm-profdata -print -quit || true)
fi
if [ -z "$CANDIDATE" ] || [ ! -f "$CANDIDATE" ]; then
  echo "llvm-profdata not found" >&2
  exit 1
fi
dirname "$CANDIDATE"

