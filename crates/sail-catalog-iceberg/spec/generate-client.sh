#!/bin/bash

set -euo pipefail

# Determine script directory and repository root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

cd "$REPO_ROOT"

echo "==> Generating Iceberg REST Catalog client..."

rm -rf crates/sail-catalog-iceberg/src/generated_rest_temp

openapi-generator generate \
  --generator-name rust \
  --config crates/sail-catalog-iceberg/spec/openapi-generator-config.yaml \
  --input-spec crates/sail-catalog-iceberg/spec/iceberg-rest-catalog.yaml \
  --output crates/sail-catalog-iceberg/src/generated_rest_temp \
  --schema-mappings Type=crate::types::Type,StructType=crate::types::StructType,ListType=crate::types::ListType,MapType=crate::types::MapType,StructField=crate::types::NestedFieldRef

echo "==> Flattening generated code to src/..."

# Remove old generated directories and apis/ and models/ directly to src/
rm -rf crates/sail-catalog-iceberg/src/apis
rm -rf crates/sail-catalog-iceberg/src/models
cp -r crates/sail-catalog-iceberg/src/generated_rest_temp/src/apis crates/sail-catalog-iceberg/src/
cp -r crates/sail-catalog-iceberg/src/generated_rest_temp/src/models crates/sail-catalog-iceberg/src/

rm -rf crates/sail-catalog-iceberg/src/generated_rest_temp

echo "==> Running cargo fmt..."
cargo +nightly fmt