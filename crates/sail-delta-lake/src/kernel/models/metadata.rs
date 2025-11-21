// https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
//
// Copyright (2020) QP Hou and a number of other contributors.
// Portions Copyright (2025) LakeSail, Inc.
// Modified in 2025 by LakeSail, Inc.
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

// [Credit]: <https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/crates/core/src/kernel/models/actions.rs>
use std::collections::HashMap;

use delta_kernel::actions::Metadata;
use serde_json::json;

use super::StructType;
use crate::kernel::DeltaResult;

/// Extension trait for working with `Metadata` until kernel exposes mutation APIs.
#[allow(dead_code)]
pub trait MetadataExt {
    fn with_table_id(self, table_id: String) -> DeltaResult<Metadata>;

    fn with_name(self, name: String) -> DeltaResult<Metadata>;

    fn with_description(self, description: String) -> DeltaResult<Metadata>;

    fn with_schema(self, schema: &StructType) -> DeltaResult<Metadata>;

    fn add_config_key(self, key: String, value: String) -> DeltaResult<Metadata>;

    fn remove_config_key(self, key: &str) -> DeltaResult<Metadata>;
}

impl MetadataExt for Metadata {
    fn with_table_id(self, table_id: String) -> DeltaResult<Metadata> {
        let schema_string = serde_json::to_string(&self.parse_schema()?)?;
        let value = json!({
            "id": table_id,
            "name": self.name(),
            "description": self.description(),
            "format": { "provider": "parquet", "options": {} },
            "schemaString": schema_string,
            "partitionColumns": self.partition_columns().clone(),
            "configuration": self.configuration().clone(),
            "createdTime": self.created_time(),
        });
        Ok(serde_json::from_value(value)?)
    }

    fn with_name(self, name: String) -> DeltaResult<Metadata> {
        let schema_string = serde_json::to_string(&self.parse_schema()?)?;
        let value = json!({
            "id": self.id(),
            "name": name,
            "description": self.description(),
            "format": { "provider": "parquet", "options": {} },
            "schemaString": schema_string,
            "partitionColumns": self.partition_columns().clone(),
            "configuration": self.configuration().clone(),
            "createdTime": self.created_time(),
        });
        Ok(serde_json::from_value(value)?)
    }

    fn with_description(self, description: String) -> DeltaResult<Metadata> {
        let schema_string = serde_json::to_string(&self.parse_schema()?)?;
        let value = json!({
            "id": self.id(),
            "name": self.name(),
            "description": description,
            "format": { "provider": "parquet", "options": {} },
            "schemaString": schema_string,
            "partitionColumns": self.partition_columns().clone(),
            "configuration": self.configuration().clone(),
            "createdTime": self.created_time(),
        });
        Ok(serde_json::from_value(value)?)
    }

    fn with_schema(self, schema: &StructType) -> DeltaResult<Metadata> {
        let value = json!({
            "id": self.id(),
            "name": self.name(),
            "description": self.description(),
            "format": { "provider": "parquet", "options": {} },
            "schemaString": serde_json::to_string(schema)?,
            "partitionColumns": self.partition_columns().clone(),
            "configuration": self.configuration().clone(),
            "createdTime": self.created_time(),
        });
        Ok(serde_json::from_value(value)?)
    }

    fn add_config_key(self, key: String, value: String) -> DeltaResult<Metadata> {
        let mut configuration: HashMap<String, String> = self.configuration().clone();
        configuration.insert(key, value);
        let schema_string = serde_json::to_string(&self.parse_schema()?)?;
        let value = json!({
            "id": self.id(),
            "name": self.name(),
            "description": self.description(),
            "format": { "provider": "parquet", "options": {} },
            "schemaString": schema_string,
            "partitionColumns": self.partition_columns().clone(),
            "configuration": configuration,
            "createdTime": self.created_time(),
        });
        Ok(serde_json::from_value(value)?)
    }

    fn remove_config_key(self, key: &str) -> DeltaResult<Metadata> {
        let mut configuration: HashMap<String, String> = self.configuration().clone();
        configuration.remove(key);
        let schema_string = serde_json::to_string(&self.parse_schema()?)?;
        let value = json!({
            "id": self.id(),
            "name": self.name(),
            "description": self.description(),
            "format": { "provider": "parquet", "options": {} },
            "schemaString": schema_string,
            "partitionColumns": self.partition_columns().clone(),
            "configuration": configuration,
            "createdTime": self.created_time(),
        });
        Ok(serde_json::from_value(value)?)
    }
}
