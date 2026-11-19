// SPDX-License-Identifier: Apache-2.0

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
