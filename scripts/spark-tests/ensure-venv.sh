#!/usr/bin/env bash

# Remove a hatch/uv virtual environment whose Python interpreter symlink is
# broken, so the next `hatch run` recreates it cleanly instead of failing with:
#   error: Failed to inspect Python interpreter from active virtual environment
#   Caused by: Broken symlink at `.../bin/python3`, was the underlying Python
#              interpreter removed?
#
# This happens when the Nix interpreter the venv pointed to was garbage
# collected or the devshell Python changed.
#
# Usage: ensure-venv.sh <venv-dir>

set -euo 'pipefail'

venv_dir="${1:?usage: ensure-venv.sh <venv-dir>}"

for py in "${venv_dir}/bin/python3" "${venv_dir}/bin/python"; do
  if [ -L "${py}" ] && [ ! -e "${py}" ]; then
    echo "⛵ Broken virtual environment (dangling Python symlink): ${venv_dir}"
    echo "⛵ Removing it so it gets recreated..."
    rm -rf "${venv_dir}"
    break
  fi
done
