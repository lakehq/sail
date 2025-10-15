#!/bin/bash

set -euo pipefail

# Determine script directory and repository root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$REPO_ROOT"

echo "==> Generating Iceberg REST Catalog client..."

rm -rf crates/sail-catalog-iceberg/src/generated_rest_temp

openapi-generator generate \
  --generator-name rust \
  --config crates/sail-catalog-iceberg/openapi-generator-config.yaml \
  --input-spec crates/sail-catalog-iceberg/spec/iceberg-rest-catalog.yaml \
  --output crates/sail-catalog-iceberg/src/generated_rest_temp

rm -rf crates/sail-catalog-iceberg/src/generated_rest
cp -r crates/sail-catalog-iceberg/src/generated_rest_temp/src crates/sail-catalog-iceberg/src/generated_rest

rm -rf crates/sail-catalog-iceberg/src/generated_rest_temp

# Remove the generated lib.rs and create a proper mod.rs for the module
rm crates/sail-catalog-iceberg/src/generated_rest/lib.rs
cat > crates/sail-catalog-iceberg/src/generated_rest/mod.rs << 'EOF'
#![allow(unused_imports)]
#![allow(clippy::too_many_arguments)]

pub mod apis;
pub mod models;
EOF

echo "==> Running cargo fmt..."
cargo +nightly fmt