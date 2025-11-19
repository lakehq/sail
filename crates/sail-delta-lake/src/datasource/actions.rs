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

use chrono::TimeZone;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::common::scalar::ScalarValue;
use datafusion::datasource::listing::PartitionedFile;
use object_store::ObjectMeta;

use crate::conversion::ScalarConverter;
use crate::kernel::models::{Add, Remove};
use crate::kernel::{DeltaResult, DeltaTableError};

/// Convert an Add action to a PartitionedFile for DataFusion scanning
pub fn partitioned_file_from_action(
    action: &Add,
    partition_columns: &[String],
    schema: &ArrowSchema,
) -> DeltaResult<PartitionedFile> {
    let partition_values = partition_columns
        .iter()
        .map(|part| {
            let field = match schema.field_with_name(part) {
                Ok(field) => field,
                Err(_) => return ScalarValue::Null,
            };

            action
                .partition_values
                .get(part)
                .and_then(|value| value.as_ref())
                .map(|value| {
                    ScalarConverter::string_to_arrow_scalar_value(value, field.data_type())
                        .unwrap_or(ScalarValue::Null)
                })
                .unwrap_or_else(|| {
                    ScalarValue::try_new_null(field.data_type()).unwrap_or(ScalarValue::Null)
                })
        })
        .collect::<Vec<_>>();

    let last_modified = chrono::Utc
        .timestamp_millis_opt(action.modification_time)
        .single()
        .ok_or_else(|| {
            DeltaTableError::generic(format!(
                "Invalid modification time: {}",
                action.modification_time
            ))
        })?;

    let object_meta: ObjectMeta = action.try_into()?;

    Ok(PartitionedFile {
        object_meta: ObjectMeta {
            last_modified,
            ..object_meta
        },
        partition_values,
        extensions: None,
        range: None,
        statistics: None,
        metadata_size_hint: None,
    })
}

/// Convert Add actions to Remove actions (used in commit operations)
pub fn adds_to_remove_actions(adds: Vec<Add>) -> Vec<Remove> {
    let deletion_timestamp = chrono::Utc::now().timestamp_millis();
    adds.into_iter()
        .map(|add| add.into_remove(deletion_timestamp))
        .collect()
}
