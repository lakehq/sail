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
use delta_kernel::engine::arrow_conversion::TryIntoKernel;
use delta_kernel::expressions::column_expr_ref;
use delta_kernel::schema::{ColumnMetadataKey, StructField};
use delta_kernel::table_features::ColumnMappingMode;
use delta_kernel::{EvaluationHandler, Expression};
use futures::TryStreamExt;

use crate::kernel::arrow::engine_ext::{ExpressionEvaluatorExt, SnapshotExt};
use crate::kernel::models::{DataType, Remove};
use crate::kernel::snapshot::EagerSnapshot;
use crate::kernel::{
    DeltaResult, DeltaTableConfig, DeltaTableError, TablePropertiesExt, ARROW_HANDLER,
};
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
    pub fn add_actions_table(
        &self,
        flatten: bool,
    ) -> Result<datafusion::arrow::record_batch::RecordBatch, DeltaTableError> {
        let mut expressions = vec![
            column_expr_ref!("path"),
            column_expr_ref!("size"),
            column_expr_ref!("modificationTime"),
        ];
        let mut fields = vec![
            StructField::not_null("path", DataType::STRING),
            StructField::not_null("size_bytes", DataType::LONG),
            StructField::not_null("modification_time", DataType::LONG),
        ];

        let stats_schema = self.snapshot.snapshot().inner.stats_schema()?;
        let num_records_field = stats_schema
            .field("numRecords")
            .ok_or_else(|| DeltaTableError::schema("numRecords field not found".to_string()))?
            .with_name("num_records");

        expressions.push(column_expr_ref!("stats_parsed.numRecords"));
        fields.push(num_records_field);

        if let Some(null_count_field) = stats_schema.field("nullCount") {
            let null_count_field = null_count_field.with_name("null_count");
            expressions.push(column_expr_ref!("stats_parsed.nullCount"));
            fields.push(null_count_field);
        }

        if let Some(min_values_field) = stats_schema.field("minValues") {
            let min_values_field = min_values_field.with_name("min");
            expressions.push(column_expr_ref!("stats_parsed.minValues"));
            fields.push(min_values_field);
        }

        if let Some(max_values_field) = stats_schema.field("maxValues") {
            let max_values_field = max_values_field.with_name("max");
            expressions.push(column_expr_ref!("stats_parsed.maxValues"));
            fields.push(max_values_field);
        }

        if let Some(partition_schema) = self.snapshot.snapshot().inner.partitions_schema()? {
            fields.push(StructField::nullable(
                "partition",
                DataType::try_struct_type(partition_schema.fields().cloned())?,
            ));
            expressions.push(column_expr_ref!("partitionValues_parsed"));
        }

        let expression = Expression::Struct(expressions);
        let table_schema = DataType::try_struct_type(fields)?;

        let input_schema = self.snapshot.files.schema();
        let input_schema = Arc::new(input_schema.as_ref().try_into_kernel()?);
        let actions = self.snapshot.files.clone();

        let evaluator = ARROW_HANDLER.new_expression_evaluator(
            input_schema,
            Arc::new(expression),
            table_schema,
        )?;
        let result = evaluator.evaluate_arrow(actions)?;

        if flatten {
            Ok(result.normalize(".", None)?)
        } else {
            Ok(result)
        }
    }
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
