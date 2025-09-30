#!/usr/bin/env bash

set -euo pipefail

apt-get update
apt-get install -y --no-install-recommends
    gcc \
    git \
    libc6-dev \
    libprotobuf-dev \
    libssl-dev \
    pkg-config \
    protobuf-compiler
rm -rf /var/lib/apt/lists/*

pip install --upgrade pip
pip install hatch maturin
