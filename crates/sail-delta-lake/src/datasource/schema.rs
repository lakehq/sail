// SPDX-License-Identifier: Apache-2.0
// SPDX-FileAttributionText: License: https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
// SPDX-FileComment: Credit: https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/delta_datafusion/mod.rs
// SPDX-FileCopyrightText: Copyright (2020) QP Hou and a number of other contributors.
// SPDX-FileCopyrightText: Portions Copyright (2025) LakeSail, Inc.
// SPDX-FileContributor: Modified by LakeSail, Inc. in 2025

use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field, Schema as ArrowSchema, SchemaRef,
};

use crate::snapshot::DeltaSnapshot;
use crate::spec::DeltaResult;

/// Build the logical schema for a Delta table.
///
/// The base table fields preserve the Delta metadata schema order. DataFusion's file scan still
/// uses an internal partition-last table schema; scan planning owns the conversion at that boundary.
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
        None => Arc::new(snapshot.schema().clone()),
    };

    let mut fields: Vec<Arc<Field>> = input_schema.fields().iter().cloned().collect();

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

    Ok(Arc::new(ArrowSchema::new(fields)))
}
