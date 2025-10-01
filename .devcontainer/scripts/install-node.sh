#!/usr/bin/env bash

set -euo pipefail

# The Docker image already has nvm installed.
# But note that this script must be invoked in an interactive shell
# so that the `NVM_DIR` environment variable is set properly.
# And here we explicitly source the nvm script to ensure that
# the nvm command (a shell function) is available.
. "$NVM_DIR/nvm.sh"

nvm install 22

npm install -g pnpm@latest-10
