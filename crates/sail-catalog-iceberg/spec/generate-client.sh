# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
  --skip-validate-spec \
  --schema-mappings Type=sail_iceberg::spec::Type,StructType=sail_iceberg::spec::StructType,ListType=sail_iceberg::spec::ListType,MapType=sail_iceberg::spec::MapType,StructField=sail_iceberg::spec::NestedFieldRef

echo "==> Flattening generated code to src/..."

# Remove old generated directories and apis/ and models/ directly to src/
rm -rf crates/sail-catalog-iceberg/src/apis
rm -rf crates/sail-catalog-iceberg/src/models
cp -r crates/sail-catalog-iceberg/src/generated_rest_temp/src/apis crates/sail-catalog-iceberg/src/
cp -r crates/sail-catalog-iceberg/src/generated_rest_temp/src/models crates/sail-catalog-iceberg/src/

rm -rf crates/sail-catalog-iceberg/src/generated_rest_temp

echo "==> Running cargo fmt..."
cargo +nightly fmt