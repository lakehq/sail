#!/usr/bin/env bash

set -euo pipefail

# The Docker image already has nvm installed.
# But note that this script must be invoked in an interactive shell
# so that the nvm command (a shell function) is sourced properly.
nvm install 22
