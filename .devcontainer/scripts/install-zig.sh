#!/usr/bin/env bash

set -euo pipefail

export ZIG_VERSION="0.15.1"
export ZIG_DOWNLOAD_URL="${ZIG_DOWNLOAD_URL:-https://ziglang.org/download}"

mkdir /opt/zig
curl -fsSL "${ZIG_DOWNLOAD_URL}/${ZIG_VERSION}/zig-$(uname -m)-linux-${ZIG_VERSION}.tar.xz" \
    | tar -xJf - -C /opt/zig --strip-components 1
chmod +x /opt/zig/zig
ln -s /opt/zig/zig /usr/local/bin/zig
