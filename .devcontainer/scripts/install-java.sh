#!/usr/bin/env bash

set -euo pipefail

wget -O - https://apt.corretto.aws/corretto.key | gpg --dearmor -o /usr/share/keyrings/corretto-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/corretto-keyring.gpg] https://apt.corretto.aws stable main" | tee /etc/apt/sources.list.d/corretto.list

apt-get update
apt-get install -y java-17-amazon-corretto-jdk
rm -rf /var/lib/apt/lists/*
