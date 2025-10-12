use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// Default schema name mapping property key
pub const DEFAULT_SCHEMA_NAME_MAPPING: &str = "schema.name-mapping.default";

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(transparent)]
pub struct NameMapping(Vec<MappedField>);

impl NameMapping {
    /// Create a new `NameMapping` given mapped fields.
    pub fn new(fields: Vec<MappedField>) -> Self {
        Self(fields)
    }

    /// Returns mapped fields
    pub fn fields(&self) -> &[MappedField] {
        &self.0
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct MappedField {
    #[serde(skip_serializing_if = "Option::is_none")]
    field_id: Option<i32>,
    names: Vec<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    fields: Vec<Arc<MappedField>>,
}

impl MappedField {
    /// Create a new `MappedField`.
    pub fn new(field_id: Option<i32>, names: Vec<String>, fields: Vec<MappedField>) -> Self {
        Self {
            field_id,
            names,
            fields: fields.into_iter().map(Arc::new).collect(),
        }
    }

    /// Optional field id
    pub fn field_id(&self) -> Option<i32> {
        self.field_id
    }
    /// All names for this field
    pub fn names(&self) -> &[String] {
        &self.names
    }
    /// Child mapped fields
    pub fn fields(&self) -> &[Arc<MappedField>] {
        &self.fields
    }
}
