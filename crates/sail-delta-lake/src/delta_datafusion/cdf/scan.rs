use chrono::{TimeZone, Utc};
use datafusion::arrow::datatypes::DataType as ArrowDataType;
use datafusion::common::ScalarValue;
use datafusion::datasource::listing::PartitionedFile;
use deltalake::kernel::{Add, Error as DeltaKernelError, StructType};
use object_store::path::Path;
use object_store::ObjectMeta;
use serde_json::Value;

/// Replicates the logic from `delta-rs` to correctly convert a `serde_json::Value` from
/// a partition value into the appropriate `ScalarValue`.
pub(crate) fn to_correct_scalar_value(
    value: &Value,
    data_type: &ArrowDataType,
) -> datafusion::common::Result<ScalarValue> {
    match value {
        Value::Null => ScalarValue::try_from(data_type),
        Value::String(s) => ScalarValue::try_from_string(s.clone(), data_type),
        other => ScalarValue::try_from_string(other.to_string(), data_type),
    }
}

/// Creates a DataFusion `PartitionedFile` from a Delta `Add` action.
pub(crate) fn partitioned_file_from_action(
    action: &Add,
    partition_columns: &[String],
    schema: &StructType,
) -> Result<PartitionedFile, DeltaKernelError> {
    let partition_values = partition_columns
        .iter()
        .map(|part| {
            let value = action.partition_values.get(part).cloned().flatten();
            let field = schema.fields().find(|f| f.name() == part).ok_or_else(|| {
                DeltaKernelError::Generic(format!("Field {} not found in schema", part))
            })?;

            // Convert Delta field to Arrow data type
            let arrow_data_type: ArrowDataType = field.data_type().try_into().map_err(|e| {
                DeltaKernelError::Generic(format!("Failed to convert data type: {}", e))
            })?;

            match value {
                Some(value) => {
                    to_correct_scalar_value(&serde_json::Value::String(value), &arrow_data_type)
                        .map_err(|e| DeltaKernelError::Generic(e.to_string()))
                }
                None => ScalarValue::try_from(&arrow_data_type)
                    .map_err(|e| DeltaKernelError::Generic(e.to_string())),
            }
        })
        .collect::<Result<Vec<_>, DeltaKernelError>>()?;

    let last_modified = Utc
        .timestamp_millis_opt(action.modification_time)
        .single()
        .unwrap_or_else(Utc::now);

    Ok(PartitionedFile {
        object_meta: ObjectMeta {
            location: Path::parse(action.path.as_str())
                .map_err(|e| DeltaKernelError::Generic(e.to_string()))?,
            size: action.size as u64,
            e_tag: None,
            last_modified,
            version: None,
        },
        partition_values,
        range: None,
        extensions: None,
        statistics: None,
        metadata_size_hint: None,
    })
}
