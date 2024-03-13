#!/bin/bash

set -euo 'pipefail'

docker build -t lakesail-framework -f docker/lakesail.Dockerfile .

docker run -v "$(pwd):/app" lakesail-framework