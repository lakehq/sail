#!/usr/bin/env bash

# This script outputs the modification times of all tracked files in the working tree,
# along with their paths and MD5 checksums, in JSON Lines format.
# The output can be used to restore the modification times of the files later
# if the files have not changed.

set -euo pipefail

git ls-files -z | while IFS= read -r -d '' path; do
  if [[ ! -f "$path" ]]; then
    # Skip non-regular files
    continue
  fi

  md5="$(md5sum -- "$path" | cut -d ' ' -f 1)"
  mtime="$(stat -c %Y -- "$path")"

  jq -c -n \
    --arg path "$path" \
    --arg md5 "$md5" \
    --argjson mtime "$mtime" \
    '{path: $path, mtime: $mtime, md5: $md5}'
done
