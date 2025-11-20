#!/usr/bin/env bash

set -euo pipefail

curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.deb.sh | bash

apt-get update
apt-get install -y --no-install-recommends \
    gcc \
    git \
    git-lfs \
    libc6-dev \
    libprotobuf-dev \
    libssl-dev \
    pkg-config \
    protobuf-compiler
rm -rf /var/lib/apt/lists/*

pip install --upgrade pip
pip install hatch maturin
