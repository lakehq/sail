#![allow(dead_code)]

use std::collections::HashMap;

use chrono::Utc;
use delta_kernel::actions::Metadata;
use serde_json::json;
use uuid::Uuid;

use super::StructType;
use crate::kernel::DeltaResult;

/// Temporary helper to create new metadata actions until delta-kernel exposes builders.
pub fn new_metadata(
    schema: &StructType,
    partition_columns: impl IntoIterator<Item = impl ToString>,
    configuration: impl IntoIterator<Item = (impl ToString, impl ToString)>,
) -> DeltaResult<Metadata> {
    let value = json!({
        "id": Uuid::new_v4().to_string(),
        "name": Option::<String>::None,
        "description": Option::<String>::None,
        "format": { "provider": "parquet", "options": {} },
        "schemaString": serde_json::to_string(schema)?,
        "partitionColumns": partition_columns.into_iter().map(|c| c.to_string()).collect::<Vec<_>>(),
        "configuration": configuration
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<HashMap<_, _>>(),
        "createdTime": Utc::now().timestamp_millis(),
    });
    Ok(serde_json::from_value(value)?)
}

/// Extension trait for working with `Metadata` until kernel exposes mutation APIs.
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
        let mut configuration = self.configuration().clone();
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
        let mut configuration = self.configuration().clone();
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
