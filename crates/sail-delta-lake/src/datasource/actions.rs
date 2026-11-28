// SPDX-License-Identifier: Apache-2.0
// SPDX-FileComment: Credit: https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/delta_datafusion/mod.rs
// SPDX-FileAttributionText: License: https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
// SPDX-FileCopyrightText: Copyright (2020) QP Hou and a number of other contributors.
// SPDX-FileCopyrightText: Portions Copyright (2025) LakeSail, Inc.
// SPDX-FileContributor: Modified by LakeSail, Inc. in 2025

use chrono::TimeZone;
use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::common::scalar::ScalarValue;
use datafusion::datasource::listing::PartitionedFile;
use datafusion_common::extensions::Extensions;
use object_store::ObjectMeta;

use crate::conversion::parse_optional_partition_value;
use crate::schema::PhysicalPartitionColumn;
use crate::snapshot::DeltaSnapshot;
use crate::spec::{Add, CommitAction, DeltaError as DeltaTableError, DeltaResult, Remove};

/// Convert an Add action to a PartitionedFile for DataFusion scanning
pub fn partitioned_file_from_action(
    action: &Add,
    location: object_store::path::Path,
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

    Ok(PartitionedFile {
        object_meta: ObjectMeta {
            last_modified,
            location,
            size: action.size as u64,
            e_tag: None,
            version: None,
        },
        partition_values,
        extensions: Extensions::default(),
        range: None,
        statistics: None,
        ordering: None,
        metadata_size_hint: None,
        table_reference: None,
        arrow_schema: None,
    })
}

/// Convert Add actions to Remove actions (used in commit operations)
pub fn adds_to_remove_actions(adds: Vec<Add>) -> Vec<Remove> {
    let deletion_timestamp = chrono::Utc::now().timestamp_millis();
    adds.into_iter()
        .map(|add| add.into_remove(deletion_timestamp))
        .collect()
}

/// The typed values of the partition columns of the files of a table.
pub(crate) struct PartitionKeys {
    fields: Vec<(String, DataType)>,
}

impl PartitionKeys {
    /// Types the partition columns of the snapshot by the schema of the last metadata action of
    /// the commit, or else of the snapshot.
    pub(crate) fn try_new(snapshot: &DeltaSnapshot, actions: &[CommitAction]) -> DeltaResult<Self> {
        let schema = actions
            .iter()
            .rev()
            .find_map(|action| match action {
                CommitAction::Metadata(metadata) => Some(metadata.parse_schema_arrow()),
                _ => None,
            })
            .transpose()?
            .unwrap_or_else(|| snapshot.schema().clone());
        let fields = snapshot
            .physical_partition_columns()
            .into_iter()
            .map(|column| {
                let field = schema.field_with_name(&column.logical_name)?;
                Ok((column.physical_name, field.data_type().clone()))
            })
            .collect::<DeltaResult<Vec<_>>>()?;
        Ok(Self { fields })
    }

    pub(crate) fn key(&self, add: &Add) -> DeltaResult<Vec<ScalarValue>> {
        self.fields
            .iter()
            .map(|(name, data_type)| {
                parse_optional_partition_value(
                    add.partition_values
                        .get(name)
                        .and_then(|value| value.as_deref()),
                    data_type,
                )
                // ScalarValue compares float bits, while Spark equates signed zeros.
                .map(|value| match value {
                    ScalarValue::Float32(Some(0.0)) => ScalarValue::Float32(Some(0.0)),
                    ScalarValue::Float64(Some(0.0)) => ScalarValue::Float64(Some(0.0)),
                    value => value,
                })
            })
            .collect()
    }
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
        let file = partitioned_file_from_action(
            &action,
            object_store::path::Path::from(action.path.as_str()),
            &partition_columns,
            &schema,
        )
        .expect("partition values should parse");

        assert_eq!(
            file.partition_values,
            vec![ScalarValue::Int32(Some(10)), ScalarValue::Int32(None)]
        );
    }
}
