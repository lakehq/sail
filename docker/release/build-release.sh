#!/bin/bash

set -euo 'pipefail'

if [ $# -lt 1 ]; then
    echo "Error: RELEASE_TAG argument is required"
    exit 1
fi

RELEASE_TAG=$1
DOCKERFILE_PATH=${2:-docker/Dockerfile} # Default to docker/Dockerfile if not provided

if [ $# -lt 2 ]; then
    echo "No Dockerfile path argument passed. Using the default path: '$DOCKERFILE_PATH'."
else
    echo "Using the specified Dockerfile path: '$DOCKERFILE_PATH'."
fi

if [ ! -f "$DOCKERFILE_PATH" ]; then
    echo "Error: The specified Dockerfile was not found at path: '$DOCKERFILE_PATH'"
    echo "Please ensure the path is correct and points to a valid Dockerfile."
    exit 1
fi

# Build args require a context, so we create an empty temporary directory to emulate an empty context.
BUILD_CONTEXT_DIR="tmp/docker-empty-build-context"
rm -rf $BUILD_CONTEXT_DIR
mkdir -p $BUILD_CONTEXT_DIR

docker build \
  -t sail:latest \
  -f "$DOCKERFILE_PATH" \
  --build-arg RUST_PROFILE=release \
  --build-arg RELEASE_TAG="$RELEASE_TAG" \
  $BUILD_CONTEXT_DIR

rm -rf $BUILD_CONTEXT_DIR
