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

// [Credit]: <https://github.com/delta-io/delta-rs/blob/1f0b4d0965a85400c1effc6e9b4c7ebbb6795978/crates/core/src/table/state.rs>

//! The module for delta table state.

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use chrono::Utc;
use datafusion::arrow::array::{ArrayRef, StructArray};
use datafusion::arrow::datatypes::{Field, FieldRef, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use futures::TryStreamExt;

use crate::kernel::models::{ColumnMappingMode, ColumnMetadataKey, Remove};
use crate::kernel::snapshot::EagerSnapshot;
use crate::kernel::{DeltaResult, DeltaTableConfig, DeltaTableError, TablePropertiesExt};
use crate::storage::LogStore;

/// State snapshot currently held by the Delta Table instance.
#[derive(Debug, Clone)]
pub struct DeltaTableState {
    pub(crate) snapshot: EagerSnapshot,
}

impl DeltaTableState {
    /// Create a new DeltaTableState
    pub async fn try_new(
        log_store: &dyn LogStore,
        config: DeltaTableConfig,
        version: Option<i64>,
    ) -> DeltaResult<Self> {
        log_store.refresh().await?;
        // TODO: pass through predictae
        let snapshot = EagerSnapshot::try_new(log_store, config, version).await?;
        Ok(Self { snapshot })
    }

    /// Obtain the eagerly materialized snapshot.
    pub fn snapshot(&self) -> &EagerSnapshot {
        &self.snapshot
    }

    /// Full list of tombstones (remove actions) representing files removed from table state).
    pub async fn all_tombstones(
        &self,
        log_store: &dyn LogStore,
    ) -> DeltaResult<impl Iterator<Item = Remove>> {
        Ok(self
            .snapshot
            .snapshot()
            .tombstones(log_store)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter())
    }

    /// List of unexpired tombstones (remove actions) representing files removed from table state.
    /// The retention period is set by `deletedFileRetentionDuration` with default value of 1 week.
    pub async fn unexpired_tombstones(
        &self,
        log_store: &dyn LogStore,
    ) -> DeltaResult<impl Iterator<Item = Remove>> {
        let retention_timestamp = Utc::now().timestamp_millis()
            - self
                .table_properties()
                .deleted_file_retention_duration()
                .as_millis() as i64;
        let tombstones = self.all_tombstones(log_store).await?.collect::<Vec<_>>();
        Ok(tombstones
            .into_iter()
            .filter(move |t| t.deletion_timestamp.unwrap_or(0) > retention_timestamp))
    }

    /// Determine effective column mapping mode: when explicit mode is None but
    /// the schema carries column mapping annotations on any top-level field,
    /// treat it as Name.
    pub fn effective_column_mapping_mode(&self) -> ColumnMappingMode {
        let explicit = self
            .snapshot()
            .snapshot()
            .table_configuration()
            .column_mapping_mode();
        if matches!(explicit, ColumnMappingMode::None) {
            let kschema = self.snapshot().snapshot().schema().clone();
            let has_annotations = kschema.fields().any(|f| {
                f.metadata()
                    .contains_key(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref())
                    && f.metadata()
                        .contains_key(ColumnMetadataKey::ColumnMappingId.as_ref())
            });
            if has_annotations {
                return ColumnMappingMode::Name;
            }
        }
        explicit
    }

    /// Update the state of the table to the given version.
    pub async fn update(
        &mut self,
        log_store: &dyn LogStore,
        version: Option<i64>,
    ) -> Result<(), DeltaTableError> {
        log_store.refresh().await?;
        self.snapshot
            .update(log_store, version.map(|v| v as u64))
            .await?;
        Ok(())
    }

    /// Get an [arrow::record_batch::RecordBatch] containing add action data.
    ///
    /// # Arguments
    ///
    /// * `flatten` - whether to flatten the schema. Partition values columns are
    ///   given the prefix `partition.`, statistics (null_count, min, and max) are
    ///   given the prefix `null_count.`, `min.`, and `max.`, and tags the
    ///   prefix `tags.`. Nested field names are concatenated with `.`.
    ///
    /// # Data schema
    ///
    /// Each row represents a file that is a part of the selected tables state.
    ///
    /// * `path` (String): relative or absolute to a file.
    /// * `size_bytes` (Int64): size of file in bytes.
    /// * `modification_time` (Millisecond Timestamp): time the file was created.
    /// * `null_count.{col_name}` (Int64): number of null values for column in
    ///   this file.
    /// * `num_records.{col_name}` (Int64): number of records for column in
    ///   this file.
    /// * `min.{col_name}` (matches column type): minimum value of column in file
    ///   (if available).
    /// * `max.{col_name}` (matches column type): maximum value of column in file
    ///   (if available).
    /// * `partition.{partition column name}` (matches column type): value of
    ///   partition the file corresponds to.
    pub fn add_actions_table(&self, flatten: bool) -> Result<RecordBatch, DeltaTableError> {
        let actions = &self.snapshot.files;
        let mut fields: Vec<FieldRef> = Vec::new();
        let mut columns: Vec<ArrayRef> = Vec::new();

        push_renamed_column(actions, "path", "path", &mut fields, &mut columns)?;
        push_renamed_column(actions, "size", "size_bytes", &mut fields, &mut columns)?;
        push_renamed_column(
            actions,
            "modificationTime",
            "modification_time",
            &mut fields,
            &mut columns,
        )?;

        if let Some(stats) = struct_column(actions, "stats_parsed") {
            let (num_records, nullable) = required_struct_child(stats, "numRecords")?;
            fields.push(Arc::new(Field::new(
                "num_records",
                num_records.data_type().clone(),
                nullable,
            )));
            columns.push(num_records);

            if let Some((null_count, nullable)) = optional_struct_child(stats, "nullCount") {
                fields.push(Arc::new(Field::new(
                    "null_count",
                    null_count.data_type().clone(),
                    nullable,
                )));
                columns.push(null_count);
            }
            if let Some((min_values, nullable)) = optional_struct_child(stats, "minValues") {
                fields.push(Arc::new(Field::new(
                    "min",
                    min_values.data_type().clone(),
                    nullable,
                )));
                columns.push(min_values);
            }
            if let Some((max_values, nullable)) = optional_struct_child(stats, "maxValues") {
                fields.push(Arc::new(Field::new(
                    "max",
                    max_values.data_type().clone(),
                    nullable,
                )));
                columns.push(max_values);
            }
        }

        if !self.snapshot.metadata().partition_columns().is_empty() {
            push_renamed_column(
                actions,
                "partitionValues_parsed",
                "partition",
                &mut fields,
                &mut columns,
            )?;
        }

        let result = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns)?;
        if flatten {
            Ok(result.normalize(".", None)?)
        } else {
            Ok(result)
        }
    }
}

fn push_renamed_column(
    batch: &RecordBatch,
    input_name: &str,
    output_name: &str,
    fields: &mut Vec<FieldRef>,
    columns: &mut Vec<ArrayRef>,
) -> DeltaResult<()> {
    let schema = batch.schema();
    let index = schema.index_of(input_name).map_err(|_| {
        DeltaTableError::schema(format!("column {input_name} not found in add actions"))
    })?;
    let field = schema.field(index);
    fields.push(Arc::new(Field::new(
        output_name,
        field.data_type().clone(),
        field.is_nullable(),
    )));
    columns.push(batch.column(index).clone());
    Ok(())
}

fn struct_column<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a StructArray> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<StructArray>())
}

fn required_struct_child(
    array: &StructArray,
    name: &str,
) -> Result<(ArrayRef, bool), DeltaTableError> {
    optional_struct_child(array, name)
        .ok_or_else(|| DeltaTableError::schema(format!("{name} field not found in struct column")))
}

fn optional_struct_child(array: &StructArray, name: &str) -> Option<(ArrayRef, bool)> {
    let column = array.column_by_name(name)?.clone();
    let nullable = array
        .fields()
        .iter()
        .find(|f| f.name() == name)
        .map(|f| f.is_nullable())
        .unwrap_or(true);
    Some((column, nullable))
}

impl Deref for DeltaTableState {
    type Target = EagerSnapshot;

    fn deref(&self) -> &Self::Target {
        &self.snapshot
    }
}

impl DerefMut for DeltaTableState {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.snapshot
    }
}
