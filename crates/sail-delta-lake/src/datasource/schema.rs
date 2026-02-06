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

use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field, Schema as ArrowSchema, SchemaRef, SchemaRef as ArrowSchemaRef,
};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;

use crate::kernel::snapshot::{EagerSnapshot, LogDataHandler, Snapshot};
use crate::kernel::{DeltaResult, DeltaTableError};
use crate::schema::arrow_schema_from_struct_type;
use crate::table::DeltaTableState;

/// Convenience trait for calling common methods on snapshot hierarchies
pub trait DataFusionMixins {
    /// The physical datafusion schema of a table
    fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef>;

    /// Get the table schema as an [`ArrowSchemaRef`]
    fn input_schema(&self) -> DeltaResult<ArrowSchemaRef>;
}

impl DataFusionMixins for Snapshot {
    fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        arrow_schema_impl(self, true)
    }

    fn input_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        arrow_schema_impl(self, false)
    }
}

impl DataFusionMixins for EagerSnapshot {
    fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        arrow_schema_from_struct_type(self.schema(), self.metadata().partition_columns(), true)
    }

    fn input_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        arrow_schema_from_struct_type(self.schema(), self.metadata().partition_columns(), false)
    }
}

impl DataFusionMixins for DeltaTableState {
    fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        Ok(Arc::new(self.schema().try_into_arrow()?))
    }

    fn input_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        self.arrow_schema()
    }
}

impl DataFusionMixins for LogDataHandler<'_> {
    fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        unimplemented!("arrow_schema for LogDataHandler");
    }

    fn input_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        unimplemented!("input_schema for LogDataHandler");
    }
}

fn arrow_schema_impl(snapshot: &Snapshot, wrap_partitions: bool) -> DeltaResult<ArrowSchemaRef> {
    arrow_schema_from_struct_type(
        snapshot.schema(),
        snapshot.metadata().partition_columns(),
        wrap_partitions,
    )
}

/// The logical schema for a Deltatable is different from the protocol level schema since partition
/// columns must appear at the end of the schema. This is to align with how partition are handled
/// at the physical level
pub fn df_logical_schema(
    snapshot: &DeltaTableState,
    file_column_name: &Option<String>,
    commit_version_column_name: &Option<String>,
    commit_timestamp_column_name: &Option<String>,
    schema: Option<ArrowSchemaRef>,
) -> DeltaResult<SchemaRef> {
    let input_schema = match schema {
        Some(schema) => schema,
        None => snapshot.input_schema()?,
    };
    let table_partition_cols = &snapshot.metadata().partition_columns();

    let mut fields: Vec<Arc<Field>> = input_schema
        .fields()
        .iter()
        .filter(|f| !table_partition_cols.contains(f.name()))
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

    Ok(Arc::new(ArrowSchema::new(fields)))
}
