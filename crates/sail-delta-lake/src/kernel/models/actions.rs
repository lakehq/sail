use std::collections::HashMap;

/// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/kernel/models/actions.rs>
pub use delta_kernel::actions::Metadata;
use deltalake::kernel::{DeltaResult, StructType};

pub fn new_metadata(
    schema: &StructType,
    partition_columns: impl IntoIterator<Item = impl ToString>,
    configuration: impl IntoIterator<Item = (impl ToString, impl ToString)>,
) -> DeltaResult<Metadata> {
    let value = serde_json::json!({
        "id": uuid::Uuid::new_v4().to_string(),
        "name": None::<String>,
        "description": None::<String>,
        "format": { "provider": "parquet", "options": {} },
        "schemaString": serde_json::to_string(schema)?,
        "partitionColumns": partition_columns.into_iter().map(|c| c.to_string()).collect::<Vec<_>>(),
        "configuration": configuration.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect::<HashMap<_, _>>(),
        "createdTime": chrono::Utc::now().timestamp_millis(),
    });
    Ok(serde_json::from_value(value)?)
}
