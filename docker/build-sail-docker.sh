#!/bin/bash

set -euo 'pipefail'

docker build -t sail -f docker/Dockerfile .

# docker run -v "$(pwd):/app" sail
