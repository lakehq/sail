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

// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/delta_datafusion/mod.rs>

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field, Fields, Schema as ArrowSchema, SchemaRef,
};
use sail_common_datafusion::datasource::{SAIL_METADATA_COLUMN_KEY, SAIL_METADATA_COLUMN_NAME_KEY};

use crate::kernel::snapshot::DeltaSnapshot;
use crate::spec::{DeltaError as DeltaTableError, DeltaResult};
use crate::table::features::RowTrackingToken;

/// Name of the synthetic struct column exposing row-tracking metadata to SQL.
pub const METADATA_COLUMN_NAME: &str = "_metadata";
pub const METADATA_ROW_ID_FIELD: &str = "row_id";
pub const METADATA_BASE_ROW_ID_FIELD: &str = "base_row_id";
pub const METADATA_DEFAULT_ROW_COMMIT_VERSION_FIELD: &str = "default_row_commit_version";
pub const METADATA_ROW_COMMIT_VERSION_FIELD: &str = "row_commit_version";

pub fn metadata_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new(METADATA_ROW_ID_FIELD, ArrowDataType::Int64, true),
        Field::new(METADATA_BASE_ROW_ID_FIELD, ArrowDataType::Int64, true),
        Field::new(
            METADATA_DEFAULT_ROW_COMMIT_VERSION_FIELD,
            ArrowDataType::Int64,
            true,
        ),
        Field::new(
            METADATA_ROW_COMMIT_VERSION_FIELD,
            ArrowDataType::Int64,
            true,
        ),
    ])
}

pub fn is_metadata_struct_field(field: &Field) -> bool {
    field
        .metadata()
        .get(SAIL_METADATA_COLUMN_KEY)
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
        && field
            .metadata()
            .get(SAIL_METADATA_COLUMN_NAME_KEY)
            .is_some_and(|value| value == METADATA_COLUMN_NAME)
}

pub fn metadata_struct_field_with_name(name: impl Into<String>) -> Field {
    Field::new(name, ArrowDataType::Struct(metadata_struct_fields()), true).with_metadata(
        HashMap::from([
            (SAIL_METADATA_COLUMN_KEY.to_string(), "true".to_string()),
            (
                SAIL_METADATA_COLUMN_NAME_KEY.to_string(),
                METADATA_COLUMN_NAME.to_string(),
            ),
        ]),
    )
}

pub fn metadata_struct_field() -> Field {
    metadata_struct_field_with_name(METADATA_COLUMN_NAME)
}

pub fn snapshot_exposes_row_tracking_metadata(snapshot: &DeltaSnapshot) -> bool {
    matches!(
        snapshot.get_row_tracking_state(),
        Ok(RowTrackingToken::Enabled(_))
    )
}

/// The logical schema for a Deltatable is different from the protocol level schema since partition
/// columns must appear at the end of the schema. This is to align with how partition are handled
/// at the physical level
pub fn df_logical_schema(
    snapshot: &DeltaSnapshot,
    file_column_name: &Option<String>,
    row_index_column_name: &Option<String>,
    commit_version_column_name: &Option<String>,
    commit_timestamp_column_name: &Option<String>,
    schema: Option<SchemaRef>,
) -> DeltaResult<SchemaRef> {
    let input_schema = match schema {
        Some(schema) => schema,
        None => snapshot.input_schema()?,
    };
    let table_partition_cols = &snapshot.metadata().partition_columns();

    let mut fields: Vec<Arc<Field>> = input_schema
        .fields()
        .iter()
        .filter(|field| !table_partition_cols.contains(field.name()))
        .cloned()
        .collect();

    for partition_col in table_partition_cols.iter() {
        fields.push(Arc::new(
            input_schema
                .field_with_name(partition_col)
                .map_err(|_| DeltaTableError::missing_column(partition_col))?
                .to_owned(),
        ));
    }

    if let Some(file_column_name) = file_column_name {
        fields.push(Arc::new(Field::new(
            file_column_name,
            ArrowDataType::Utf8,
            true,
        )));
    }

    if let Some(commit_version_column_name) = commit_version_column_name {
        fields.push(Arc::new(Field::new(
            commit_version_column_name,
            ArrowDataType::Int64,
            true,
        )));
    }
    if let Some(commit_timestamp_column_name) = commit_timestamp_column_name {
        fields.push(Arc::new(Field::new(
            commit_timestamp_column_name,
            ArrowDataType::Int64,
            true,
        )));
    }

    if let Some(row_index_column_name) = row_index_column_name {
        fields.push(Arc::new(Field::new(
            row_index_column_name,
            ArrowDataType::Int64,
            false,
        )));
    }

    if snapshot_exposes_row_tracking_metadata(snapshot) {
        let metadata_name = unique_metadata_column_name(&fields);
        fields.push(Arc::new(metadata_struct_field_with_name(metadata_name)));
    }

    Ok(Arc::new(ArrowSchema::new(fields)))
}

fn unique_metadata_column_name(fields: &[Arc<Field>]) -> String {
    let mut name = METADATA_COLUMN_NAME.to_string();
    let mut idx = 0;
    while fields.iter().any(|field| field.name() == &name) {
        idx += 1;
        name = format!("{METADATA_COLUMN_NAME}_{idx}");
    }
    name
}
