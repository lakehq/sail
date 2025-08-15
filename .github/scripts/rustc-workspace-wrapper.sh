#!/bin/bash

# [Credit]: <https://github.com/risingwavelabs/risingwave/blob/0cc1f7a54b0bfb09f884ec47baf0fd98fee25738/ci/scripts/rustc-workspace-wrapper.sh>
# Exits as soon as any line fails.
set -euo pipefail

# RUSTC_WORKSPACE_WRAPPER script that adds coverage-related rustflags
# for workspace members only when SAIL_BUILD_INSTRUMENT_COVERAGE is set.
# External dependencies won't get coverage flags because
# RUSTC_WORKSPACE_WRAPPER only applies to workspace members.
#
# Reference: https://github.com/rust-lang/cargo/issues/13040

if [[ "$1" == *rustc ]]; then
    # The first argument is the rustc executable path, respect it
    ACTUAL_RUSTC="$1"
    shift  # Remove the first argument (rustc path) from $@
else
    # Workaround for `sccache` does not work together with `RUSTC_WORKSPACE_WRAPPER`
    ACTUAL_RUSTC="rustc"
fi

# Only add coverage flags if SAIL_BUILD_INSTRUMENT_COVERAGE is set
if [[ "${SAIL_BUILD_INSTRUMENT_COVERAGE:-}" == "1" ]]; then
    exec "$ACTUAL_RUSTC" "$@" -C instrument-coverage --cfg coverage
else
    exec "$ACTUAL_RUSTC" "$@"
fi
