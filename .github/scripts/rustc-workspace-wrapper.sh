#!/bin/bash
set -euo pipefail

exec "$1" "${@:2}" -C instrument-coverage --cfg coverage
