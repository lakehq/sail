#!/bin/bash

set -euo 'pipefail'

BUILD_CONTEXT_DIR="tmp/docker-local-build-context"
rm -rf $BUILD_CONTEXT_DIR
mkdir -p $BUILD_CONTEXT_DIR

cp Cargo.toml $BUILD_CONTEXT_DIR/
cp Cargo.lock $BUILD_CONTEXT_DIR/
cp -r crates $BUILD_CONTEXT_DIR/

# Alternatively, you can use . as the build context if you're in the project root with the .dockerignore file.
docker build \
  -t sail:latest \
  -f docker/Dockerfile \
  --build-arg RUST_PROFILE=dev \
  $BUILD_CONTEXT_DIR

rm -rf $BUILD_CONTEXT_DIR
