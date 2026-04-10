#!/usr/bin/env bash

# This script reads the modification times, paths, and MD5 checksums of files from JSON Lines input,
# and restores the modification times of the files if they exist and have not changed.

set -euo pipefail

while IFS= read -r line || [[ -n "$line" ]]; do
  if [[ -z "$line" ]]; then
    continue
  fi

  path="$(jq -r '.path' <<<"$line")"
  mtime="$(jq -r '.mtime' <<<"$line")"
  md5="$(jq -r '.md5' <<<"$line")"

  if [[ ! -f "$path" ]]; then
    continue
  fi

  if [[ "$(md5sum -- "$path" | cut -d ' ' -f 1)" != "$md5" ]]; then
    continue
  fi

  touch -d "@${mtime}" -- "$path"
  printf '%s\n' "$path"
done
