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

use chrono::TimeZone;
use datafusion::arrow::array::{Array, DictionaryArray, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{Schema as ArrowSchema, UInt16Type};
use datafusion::common::scalar::ScalarValue;
use datafusion::datasource::listing::PartitionedFile;
use object_store::ObjectMeta;

use crate::kernel::arrow::scalar_converter::ScalarConverter;
use crate::kernel::models::{Add, Remove};
use crate::kernel::{DeltaResult, DeltaTableError};

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
                Some(value) => ScalarConverter::json_to_arrow_scalar_value(
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

    #[allow(clippy::expect_used)]
    let last_modified = chrono::Utc
        .timestamp_millis_opt(action.modification_time)
        .single()
        .expect("Failed to create timestamp from milliseconds");
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
pub fn adds_to_remove_actions(adds: Vec<Add>) -> Vec<Remove> {
    let deletion_timestamp = chrono::Utc::now().timestamp_millis();
    adds.into_iter()
        .map(|add| add.into_remove(deletion_timestamp))
        .collect()
}
