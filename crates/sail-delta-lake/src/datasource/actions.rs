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

use chrono::TimeZone;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::common::scalar::ScalarValue;
use datafusion::datasource::listing::PartitionedFile;
use datafusion_common::extensions::Extensions;
use object_store::ObjectMeta;

/// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/delta_datafusion/mod.rs>
use crate::conversion::parse_optional_partition_value;
use crate::schema::PhysicalPartitionColumn;
use crate::spec::{Add, DeltaError as DeltaTableError, DeltaResult, Remove};

/// Convert an Add action to a PartitionedFile for DataFusion scanning
pub fn partitioned_file_from_action(
    action: &Add,
    partition_columns: &[PhysicalPartitionColumn],
    schema: &ArrowSchema,
) -> DeltaResult<PartitionedFile> {
    let partition_values = partition_columns
        .iter()
        .map(|column| {
            let field = match schema.field_with_name(&column.logical_name) {
                Ok(field) => field,
                Err(_) => return ScalarValue::Null,
            };

            action
                .partition_values
                .get(&column.physical_name)
                .and_then(|value| value.as_ref())
                .map(|value| parse_optional_partition_value(Some(value), field.data_type()))
                .unwrap_or_else(|| parse_optional_partition_value(None, field.data_type()))
                .unwrap_or(ScalarValue::Null)
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
        extensions: Extensions::default(),
        range: None,
        statistics: None,
        ordering: None,
        metadata_size_hint: None,
        table_reference: None,
    })
}

/// Convert Add actions to Remove actions (used in commit operations)
pub fn adds_to_remove_actions(adds: Vec<Add>) -> Vec<Remove> {
    let deletion_timestamp = chrono::Utc::now().timestamp_millis();
    adds.into_iter()
        .map(|add| add.into_remove(deletion_timestamp))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::ScalarValue;

    use super::partitioned_file_from_action;
    use crate::schema::PhysicalPartitionColumn;
    use crate::spec::Add;

    fn mapped_field(logical_name: &str, physical_name: &str) -> Field {
        Field::new(logical_name, DataType::Int32, true).with_metadata(HashMap::from([(
            "delta.columnMapping.physicalName".to_string(),
            physical_name.to_string(),
        )]))
    }

    #[test]
    fn mapped_partition_values_do_not_fallback_to_a_colliding_logical_name() {
        let schema = Schema::new(vec![
            mapped_field("source", "col-source"),
            mapped_field("col-source", "col-target"),
        ]);
        let partition_columns = vec![
            PhysicalPartitionColumn::new("source", "col-source"),
            PhysicalPartitionColumn::new("col-source", "col-target"),
        ];
        let action = Add {
            path: "part-00000.parquet".to_string(),
            partition_values: HashMap::from([("col-source".to_string(), Some("10".to_string()))]),
            size: 1,
            data_change: true,
            ..Default::default()
        };

        #[expect(clippy::expect_used)]
        let file = partitioned_file_from_action(&action, &partition_columns, &schema)
            .expect("partition values should parse");

        assert_eq!(
            file.partition_values,
            vec![ScalarValue::Int32(Some(10)), ScalarValue::Int32(None)]
        );
    }
}
