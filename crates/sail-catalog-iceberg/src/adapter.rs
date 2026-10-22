// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use sail_catalog::error::CatalogError;
use sail_catalog::provider::Namespace;
use sail_iceberg::spec::Schema;
use sail_iceberg::spec::types::Type;

use crate::r#gen;

impl From<Vec<String>> for r#gen::Namespace {
    fn from(value: Vec<String>) -> Self {
        Self(value)
    }
}

impl From<r#gen::Namespace> for Vec<String> {
    fn from(value: r#gen::Namespace) -> Self {
        value.0
    }
}

impl From<Namespace> for r#gen::Namespace {
    fn from(value: Namespace) -> Self {
        let value: Vec<String> = value.into();
        value.into()
    }
}

impl TryFrom<Schema> for r#gen::Schema {
    type Error = CatalogError;

    fn try_from(value: Schema) -> Result<Self, Self::Error> {
        serde_json::to_value(value)
            .and_then(serde_json::from_value)
            .map_err(|e| CatalogError::External(format!("failed to convert Iceberg schema: {e}")))
    }
}

impl TryFrom<r#gen::Type> for Type {
    type Error = CatalogError;

    fn try_from(value: r#gen::Type) -> Result<Self, Self::Error> {
        serde_json::to_value(value)
            .and_then(serde_json::from_value)
            .map_err(|e| {
                CatalogError::External(format!("failed to convert Iceberg type from REST: {e}"))
            })
    }
}
