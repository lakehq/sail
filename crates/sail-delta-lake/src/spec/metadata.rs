// https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/LICENSE
//
// Copyright 2023-2024 The Delta Kernel Rust Authors
// Portions Copyright 2025-2026 LakeSail, Inc.
// Ported and modified in 2026 by LakeSail, Inc.
//
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

use std::collections::HashMap;

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use serde::{Deserialize, Serialize};

use crate::spec::schema::StructType;
use crate::spec::{DeltaError as DeltaTableError, DeltaResult};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/actions/mod.rs#L185-L223>
pub struct Format {
    provider: String,
    options: HashMap<String, String>,
}

impl Format {
    pub(crate) fn default_parquet() -> Self {
        Self {
            provider: "parquet".to_string(),
            options: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
// [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/actions/mod.rs#L225-L372>
pub struct Metadata {
    id: String,
    name: Option<String>,
    description: Option<String>,
    format: Format,
    schema_string: String,
    partition_columns: Vec<String>,
    created_time: Option<i64>,
    configuration: HashMap<String, String>,
}

impl Metadata {
    // [Credit]: <https://github.com/delta-io/delta-kernel-rs/blob/f105333a003232d7284f1a8f06cca3b6d6b232a9/kernel/src/actions/mod.rs#L244-L285>
    pub fn try_new(
        name: Option<String>,
        description: Option<String>,
        schema: StructType,
        partition_columns: Vec<String>,
        created_time: i64,
        configuration: HashMap<String, String>,
    ) -> DeltaResult<Self> {
        Ok(Self {
            id: uuid::Uuid::new_v4().to_string(),
            name,
            description,
            format: Format::default_parquet(),
            schema_string: serde_json::to_string(&schema)?,
            partition_columns,
            created_time: Some(created_time),
            configuration,
        })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    pub fn created_time(&self) -> Option<i64> {
        self.created_time
    }

    pub fn configuration(&self) -> &HashMap<String, String> {
        &self.configuration
    }

    /// Parse the schema string into a `StructType` (Delta JSON format).
    pub fn parse_schema(&self) -> DeltaResult<StructType> {
        Ok(serde_json::from_str(&self.schema_string)?)
    }

    /// Parse the schema string and convert to an Arrow `Schema`.
    pub fn parse_schema_arrow(&self) -> DeltaResult<ArrowSchema> {
        let struct_type: StructType = serde_json::from_str(&self.schema_string)?;
        ArrowSchema::try_from(&struct_type)
            .map_err(|e| DeltaTableError::generic(format!("Failed to convert schema: {e}")))
    }

    pub fn partition_columns(&self) -> &Vec<String> {
        &self.partition_columns
    }

    pub fn with_table_id(self, table_id: String) -> Metadata {
        Metadata {
            id: table_id,
            ..self
        }
    }

    pub fn with_name(self, name: String) -> Metadata {
        Metadata {
            name: Some(name),
            ..self
        }
    }

    pub fn with_description(self, description: String) -> Metadata {
        Metadata {
            description: Some(description),
            ..self
        }
    }

    pub fn with_schema(self, schema: &StructType) -> DeltaResult<Metadata> {
        Ok(Metadata {
            schema_string: serde_json::to_string(schema)?,
            ..self
        })
    }

    pub fn add_config_key(self, key: String, value: String) -> Metadata {
        let mut configuration = self.configuration;
        configuration.insert(key, value);
        Metadata {
            configuration,
            ..self
        }
    }

    pub fn remove_config_key(self, key: &str) -> Metadata {
        let mut configuration = self.configuration;
        configuration.remove(key);
        Metadata {
            configuration,
            ..self
        }
    }
}
