// SPDX-License-Identifier: Apache-2.0
// SPDX-FileComment: Credit: <https://github.com/apache/iceberg-rust/blob/dc349284a4204c1a56af47fb3177ace6f9e899a0/crates/iceberg/src/spec/name_mapping/mod.rs> Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership.

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

    pub fn field_names(&self) -> Result<std::collections::HashMap<i32, Vec<String>>, String> {
        fn visit<'a>(
            fields: impl Iterator<Item = &'a MappedField>,
            names: &mut std::collections::HashMap<i32, Vec<String>>,
        ) -> Result<(), String> {
            let mut siblings = std::collections::HashSet::new();
            for field in fields {
                for name in &field.names {
                    if !siblings.insert(name) {
                        return Err(format!("Ambiguous Iceberg name mapping for '{name}'"));
                    }
                }
                if let Some(id) = field.field_id
                    && (id <= 0 || names.insert(id, field.names.clone()).is_some())
                {
                    return Err(format!(
                        "Invalid or duplicate Iceberg name mapping field ID {id}"
                    ));
                }
                visit(field.fields.iter().map(Arc::as_ref), names)?;
            }
            Ok(())
        }
        let mut names = std::collections::HashMap::new();
        visit(self.0.iter(), &mut names)?;
        Ok(names)
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
