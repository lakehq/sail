#!/bin/bash

set -euo 'pipefail'

docker build -t lakesail-framework -f docker/lakesail.Dockerfile .

# TODO: See if needed when running spark-connect server
# docker run -v "$(pwd):/app" lakesail-framework