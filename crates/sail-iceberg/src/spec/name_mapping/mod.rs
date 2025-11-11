// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// [CREDIT]: https://raw.githubusercontent.com/apache/iceberg-rust/dc349284a4204c1a56af47fb3177ace6f9e899a0/crates/iceberg/src/spec/name_mapping/mod.rs

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
