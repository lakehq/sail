use std::collections::HashMap;

use chrono::TimeZone;
use datafusion::arrow::array::{Array, DictionaryArray, RecordBatch, StringArray};
use datafusion::arrow::compute::{cast_with_options, CastOptions};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema, UInt16Type};
use datafusion::common::scalar::ScalarValue;
use datafusion::common::Result;
use datafusion::datasource::listing::PartitionedFile;
use deltalake::errors::{DeltaResult, DeltaTableError};
use deltalake::kernel::Add;
use object_store::ObjectMeta;

/// Convert an Add action to a PartitionedFile for DataFusion scanning
pub fn partitioned_file_from_action(
    action: &Add,
    partition_columns: &[String],
    schema: &ArrowSchema,
) -> PartitionedFile {
    let partition_values = partition_columns
        .iter()
        .map(|part| {
            let partition_value = match action.partition_values.get(part) {
                Some(val) => val,
                None => return ScalarValue::Null,
            };

            let field = match schema.field_with_name(part) {
                Ok(field) => field,
                Err(_) => return ScalarValue::Null,
            };

            // Convert partition value to ScalarValue
            match partition_value {
                Some(value) => to_correct_scalar_value(
                    &serde_json::Value::String(value.to_string()),
                    field.data_type(),
                )
                .ok()
                .flatten()
                .unwrap_or(ScalarValue::Null),
                None => ScalarValue::try_new_null(field.data_type()).unwrap_or(ScalarValue::Null),
            }
        })
        .collect::<Vec<_>>();

    let ts_secs = action.modification_time / 1000;
    let ts_ns = (action.modification_time % 1000) * 1_000_000;
    #[allow(clippy::expect_used)]
    let last_modified = chrono::Utc.from_utc_datetime(
        &chrono::DateTime::from_timestamp(ts_secs, ts_ns as u32)
            .expect("Failed to create timestamp from seconds and nanoseconds")
            .naive_utc(),
    );
    PartitionedFile {
        #[allow(clippy::expect_used)]
        object_meta: ObjectMeta {
            last_modified,
            ..action
                .try_into()
                .expect("Failed to convert action to ObjectMeta")
        },
        partition_values,
        extensions: None,
        range: None,
        statistics: None,
        metadata_size_hint: None,
    }
}

fn parse_date(stat_val: &serde_json::Value, field_dt: &ArrowDataType) -> Result<ScalarValue> {
    let string = match stat_val {
        serde_json::Value::String(s) => s.to_owned(),
        _ => stat_val.to_string(),
    };

    let time_micro = ScalarValue::try_from_string(string, &ArrowDataType::Date32)?;
    let cast_arr = cast_with_options(
        &time_micro.to_array()?,
        field_dt,
        &CastOptions {
            safe: false,
            ..Default::default()
        },
    )?;
    ScalarValue::try_from_array(&cast_arr, 0)
}

fn parse_timestamp(stat_val: &serde_json::Value, field_dt: &ArrowDataType) -> Result<ScalarValue> {
    let string = match stat_val {
        serde_json::Value::String(s) => s.to_owned(),
        _ => stat_val.to_string(),
    };

    let time_micro = ScalarValue::try_from_string(
        string,
        &ArrowDataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None),
    )?;
    let cast_arr = cast_with_options(
        &time_micro.to_array()?,
        field_dt,
        &CastOptions {
            safe: false,
            ..Default::default()
        },
    )?;
    ScalarValue::try_from_array(&cast_arr, 0)
}

/// Convert a JSON value to the correct ScalarValue for the given Arrow data type
pub fn to_correct_scalar_value(
    stat_val: &serde_json::Value,
    field_dt: &ArrowDataType,
) -> Result<Option<ScalarValue>> {
    match stat_val {
        serde_json::Value::Array(_) => Ok(None),
        serde_json::Value::Object(_) => Ok(None),
        serde_json::Value::Null => {
            Ok(Some(ScalarValue::try_new_null(field_dt).map_err(|e| {
                datafusion_common::DataFusionError::External(Box::new(e))
            })?))
        }
        serde_json::Value::String(string_val) => match field_dt {
            ArrowDataType::Timestamp(_, _) => Ok(Some(parse_timestamp(stat_val, field_dt)?)),
            ArrowDataType::Date32 => Ok(Some(parse_date(stat_val, field_dt)?)),
            _ => Ok(Some(ScalarValue::try_from_string(
                string_val.to_owned(),
                field_dt,
            )?)),
        },
        other => match field_dt {
            ArrowDataType::Timestamp(_, _) => Ok(Some(parse_timestamp(stat_val, field_dt)?)),
            ArrowDataType::Date32 => Ok(Some(parse_date(stat_val, field_dt)?)),
            _ => Ok(Some(ScalarValue::try_from_string(
                other.to_string(),
                field_dt,
            )?)),
        },
    }
}

/// Extract file paths from a record batch containing a path column
pub fn get_path_column<'a>(
    batch: &'a RecordBatch,
    path_column: &str,
) -> DeltaResult<impl Iterator<Item = Option<&'a str>>> {
    let err = || DeltaTableError::Generic("Unable to obtain Delta-rs path column".to_string());
    let dict_array = batch
        .column_by_name(path_column)
        .ok_or_else(err)?
        .as_any()
        .downcast_ref::<DictionaryArray<UInt16Type>>()
        .ok_or_else(err)?;

    let values = dict_array
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(err)?;

    Ok(dict_array
        .keys()
        .iter()
        .map(move |key| key.and_then(|k| values.value(k as usize).into())))
}

/// Join record batches with Add actions based on file paths
pub fn join_batches_with_add_actions(
    batches: Vec<RecordBatch>,
    mut actions: HashMap<String, Add>,
    path_column: &str,
    dict_array: bool,
) -> DeltaResult<Vec<Add>> {
    let mut files = Vec::with_capacity(batches.iter().map(|batch| batch.num_rows()).sum());
    for batch in batches {
        let err = || DeltaTableError::Generic("Unable to obtain Delta-rs path column".to_string());

        let iter: Box<dyn Iterator<Item = Option<&str>>> = if dict_array {
            let array = get_path_column(&batch, path_column)?;
            Box::new(array)
        } else {
            let array = batch
                .column_by_name(path_column)
                .ok_or_else(err)?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(err)?;
            Box::new(array.iter())
        };

        for path in iter {
            let path = path.ok_or(DeltaTableError::Generic(format!(
                "{path_column} cannot be null"
            )))?;

            match actions.remove(path) {
                Some(action) => files.push(action),
                None => {
                    return Err(DeltaTableError::Generic(
                        "Unable to map __delta_rs_path to action.".to_owned(),
                    ))
                }
            }
        }
    }
    Ok(files)
}

/// Convert Add actions to Remove actions (used in commit operations)
pub fn adds_to_remove_actions(adds: Vec<Add>) -> Vec<deltalake::kernel::Remove> {
    adds.into_iter()
        .map(|add| deltalake::kernel::Remove {
            path: add.path,
            deletion_timestamp: Some(chrono::Utc::now().timestamp_millis()),
            data_change: true,
            extended_file_metadata: None,
            partition_values: Some(add.partition_values),
            size: Some(add.size),
            deletion_vector: add.deletion_vector,
            base_row_id: add.base_row_id,
            default_row_commit_version: add.default_row_commit_version,
            tags: add.tags,
        })
        .collect()
}
