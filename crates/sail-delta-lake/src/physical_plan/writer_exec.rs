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

// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/operations/write/execution.rs>
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::Utc;
use datafusion::arrow::array::{ArrayRef, PrimitiveArray, StringArray};
use datafusion::arrow::datatypes::{
    ArrowTimestampType, DataType, Field, Schema, SchemaRef, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_physical_expr::{Distribution, EquivalenceProperties};
use delta_kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use delta_kernel::schema::StructType;
use delta_kernel::table_features::ColumnMappingMode;
use futures::stream::{once, StreamExt};
use sail_common_datafusion::datasource::PhysicalSinkMode;
use serde_json::Value;
use url::Url;

use crate::conversion::DeltaTypeConverter;
use crate::kernel::models::{contains_timestampntz, Action, Metadata, Protocol};
// TODO: Follow upstream for `MetadataExt`.
use crate::kernel::{DeltaOperation, SaveMode};
use crate::operations::write::writer::{DeltaWriter, WriterConfig};
use crate::options::{ColumnMappingModeOption, TableDeltaOptions};
use crate::physical_plan::CommitInfo;
use crate::schema::{
    annotate_for_column_mapping, compute_max_column_id, evolve_schema, get_physical_schema,
    normalize_delta_schema,
};
use crate::storage::{get_object_store_from_context, StorageConfig};
use crate::table::open_table_with_object_store;

/// Schema handling mode for Delta Lake writes
#[derive(Debug, Clone, Copy, PartialEq)]
enum SchemaMode {
    /// Merge new schema with existing schema
    Merge,
    /// Overwrite existing schema with new schema
    Overwrite,
}

/// Physical execution node for Delta Lake writing operations
#[derive(Debug)]
pub struct DeltaWriterExec {
    input: Arc<dyn ExecutionPlan>,
    table_url: Url,
    options: TableDeltaOptions,
    partition_columns: Vec<String>,
    sink_mode: PhysicalSinkMode,
    table_exists: bool,
    sink_schema: SchemaRef,
    /// Optional override for commit operation metadata.
    operation_override: Option<DeltaOperation>,
    metrics: ExecutionPlanMetricsSet,
    cache: PlanProperties,
}

impl DeltaWriterExec {
    /// Build a map from physical field name to logical name for top-level columns
    fn build_physical_to_logical_map(
        logical_kernel: &StructType,
        column_mapping_mode: ColumnMappingMode,
    ) -> HashMap<String, String> {
        let mut map = HashMap::new();
        for kf in logical_kernel.fields() {
            map.insert(
                kf.physical_name(column_mapping_mode).to_string(),
                kf.name().clone(),
            );
        }
        map
    }
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        options: TableDeltaOptions,
        partition_columns: Vec<String>,
        sink_mode: PhysicalSinkMode,
        table_exists: bool,
        sink_schema: SchemaRef,
        operation_override: Option<DeltaOperation>,
    ) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new("data", DataType::Utf8, true)]));
        let cache = Self::compute_properties(schema);
        Self {
            input,
            table_url,
            options,
            partition_columns,
            sink_mode,
            table_exists,
            sink_schema,
            operation_override,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        }
    }

    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        )
    }

    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    pub fn options(&self) -> &TableDeltaOptions {
        &self.options
    }

    pub fn partition_columns(&self) -> &[String] {
        &self.partition_columns
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn sink_schema(&self) -> &SchemaRef {
        &self.sink_schema
    }

    pub fn sink_mode(&self) -> &PhysicalSinkMode {
        &self.sink_mode
    }

    pub fn table_exists(&self) -> bool {
        self.table_exists
    }

    pub fn operation_override(&self) -> Option<&DeltaOperation> {
        self.operation_override.as_ref()
    }
}

#[async_trait]
impl ExecutionPlan for DeltaWriterExec {
    fn name(&self) -> &'static str {
        "DeltaWriterExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("DeltaWriterExec requires exactly one child");
        }

        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.table_url.clone(),
            self.options.clone(),
            self.partition_columns.clone(),
            self.sink_mode.clone(),
            self.table_exists,
            self.sink_schema.clone(),
            self.operation_override.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("DeltaWriterExec can only be executed in a single partition");
        }

        let input_partitions = self.input.output_partitioning().partition_count();
        if input_partitions != 1 {
            return internal_err!(
                "DeltaWriterExec requires exactly one input partition, got {input_partitions}"
            );
        }

        let stream = self.input.execute(0, Arc::clone(&context))?;

        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);
        let output_bytes = MetricBuilder::new(&self.metrics).output_bytes(partition);
        let elapsed_compute = MetricBuilder::new(&self.metrics).elapsed_compute(partition);

        let table_url = self.table_url.clone();
        let options = self.options.clone();
        let partition_columns = self.partition_columns.clone();
        let sink_mode = self.sink_mode.clone();
        let table_exists = self.table_exists;
        let input_schema = normalize_delta_schema(&self.input.schema());
        let operation_override = self.operation_override.clone();
        // let sink_schema = self.sink_schema.clone();
        let session_timezone = context
            .session_config()
            .options()
            .execution
            .time_zone
            .clone();

        let schema = self.schema();
        let future = async move {
            let _elapsed_compute_timer = elapsed_compute.timer();
            let exec_start = Instant::now();
            let TableDeltaOptions {
                target_file_size,
                write_batch_size,
                ..
            } = &options;
            let timezone = session_timezone;

            let storage_config = StorageConfig;
            let object_store = get_object_store_from_context(&context, &table_url)?;

            // Calculate initial_actions and operation based on sink_mode
            let mut initial_actions: Vec<Action> = Vec::new();
            let mut operation: Option<DeltaOperation> = None;

            let table_result = open_table_with_object_store(
                table_url.clone(),
                object_store.clone(),
                storage_config.clone(),
            )
            .await;

            #[allow(clippy::unwrap_used)]
            let table = if table_exists {
                Some(table_result.unwrap())
            } else {
                None
            };

            match &sink_mode {
                PhysicalSinkMode::Append => {
                    operation = Some(DeltaOperation::Write {
                        mode: SaveMode::Append,
                        partition_by: if partition_columns.is_empty() {
                            None
                        } else {
                            Some(partition_columns.clone())
                        },
                        predicate: None,
                    });
                }
                PhysicalSinkMode::Overwrite => {
                    operation = Some(DeltaOperation::Write {
                        mode: SaveMode::Overwrite,
                        partition_by: if partition_columns.is_empty() {
                            None
                        } else {
                            Some(partition_columns.clone())
                        },
                        predicate: None,
                    });
                }
                PhysicalSinkMode::OverwriteIf { condition } => {
                    operation = Some(DeltaOperation::Write {
                        mode: SaveMode::Overwrite,
                        partition_by: if partition_columns.is_empty() {
                            None
                        } else {
                            Some(partition_columns.clone())
                        },
                        predicate: condition.source.clone(),
                    });
                }
                PhysicalSinkMode::ErrorIfExists => {
                    if table_exists {
                        return Err(DataFusionError::Plan(format!(
                            "Delta table already exists at path: {table_url}"
                        )));
                    }
                }
                PhysicalSinkMode::IgnoreIfExists => {
                    if table_exists {
                        // Table exists, ignore the write operation and return empty commit info
                        // Still update execution metrics so callers see a completed node.
                        output_rows.add(0);
                        output_bytes.add(0);
                        let commit_info = CommitInfo {
                            row_count: 0,
                            actions: Vec::new(),
                            initial_actions: Vec::new(),
                            operation: None,
                            operation_metrics: HashMap::new(),
                        };
                        let commit_info_json = serde_json::to_string(&commit_info)
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;

                        let data_array = Arc::new(StringArray::from(vec![commit_info_json]));
                        let batch = RecordBatch::try_new(schema, vec![data_array])?;
                        return Ok(batch);
                    }
                }
                PhysicalSinkMode::OverwritePartitions => {
                    return Err(DataFusionError::NotImplemented(
                        "OverwritePartitions mode not implemented".to_string(),
                    ));
                }
            }

            // Handle schema evolution if table exists
            let (final_schema, schema_actions) = if let Some(table) = &table {
                // Determine save mode from operation
                let save_mode = match &operation {
                    Some(DeltaOperation::Write { mode, .. }) => *mode,
                    Some(DeltaOperation::Create { mode, .. }) => *mode,
                    _ => SaveMode::Append,
                };

                // Get schema mode based on options
                let schema_mode = Self::get_schema_mode(&options, save_mode)?;

                Self::handle_schema_evolution(table, &input_schema, schema_mode).await?
            } else {
                (input_schema.clone(), Vec::new())
            };
            let final_schema = normalize_delta_schema(&final_schema);

            // Determine effective column mapping mode
            let effective_mode = if let Some(table) = &table {
                let mode = table
                    .snapshot()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .effective_column_mapping_mode();
                match mode {
                    delta_kernel::table_features::ColumnMappingMode::Name => {
                        ColumnMappingModeOption::Name
                    }
                    delta_kernel::table_features::ColumnMappingMode::Id => {
                        ColumnMappingModeOption::Id
                    }
                    _ => ColumnMappingModeOption::None,
                }
            } else {
                options.column_mapping_mode
            };

            // Determine the kernel column mapping mode once for downstream conversions
            let kernel_mode = match effective_mode {
                ColumnMappingModeOption::Name => ColumnMappingMode::Name,
                ColumnMappingModeOption::Id => ColumnMappingMode::Id,
                ColumnMappingModeOption::None => ColumnMappingMode::None,
            };

            // If creating a new table and column mapping or timestampNtz features are required,
            // prepare initial protocol+metadata
            let mut annotated_schema_opt: Option<StructType> = None;
            if !table_exists {
                // Build kernel schema for feature detection
                let kernel_schema: StructType = final_schema
                    .as_ref()
                    .try_into_kernel()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let has_timestamp_ntz = contains_timestampntz(kernel_schema.fields());

                if matches!(
                    effective_mode,
                    ColumnMappingModeOption::Name | ColumnMappingModeOption::Id
                ) {
                    let annotated_schema = annotate_for_column_mapping(&kernel_schema);
                    annotated_schema_opt = Some(annotated_schema.clone());

                    let mut reader_features = vec!["columnMapping"];
                    let mut writer_features = vec!["columnMapping"];
                    if has_timestamp_ntz {
                        reader_features.push("timestampNtz");
                        writer_features.push("timestampNtz");
                    }

                    #[allow(clippy::unwrap_used)]
                    let protocol: Protocol = serde_json::from_value(serde_json::json!({
                        "minReaderVersion": 3,
                        "minWriterVersion": 7,
                        "readerFeatures": reader_features,
                        "writerFeatures": writer_features
                    }))
                    .unwrap();

                    let mut configuration = HashMap::new();
                    let mode_str = match effective_mode {
                        ColumnMappingModeOption::Name => "name",
                        ColumnMappingModeOption::Id => "id",
                        ColumnMappingModeOption::None => "none",
                    };
                    configuration
                        .insert("delta.columnMapping.mode".to_string(), mode_str.to_string());
                    // Set maxColumnId for new tables
                    #[allow(clippy::unwrap_used)]
                    let max_id = compute_max_column_id(annotated_schema_opt.as_ref().unwrap());
                    configuration.insert(
                        "delta.columnMapping.maxColumnId".to_string(),
                        max_id.to_string(),
                    );

                    #[allow(clippy::unwrap_used)]
                    let metadata = Metadata::try_new(
                        None,
                        None,
                        annotated_schema_opt.as_ref().unwrap().clone(),
                        partition_columns.clone(),
                        Utc::now().timestamp_millis(),
                        configuration,
                    )
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    initial_actions.push(Action::Protocol(protocol.clone()));
                    initial_actions.push(Action::Metadata(metadata.clone()));
                    log::trace!(
                        "init_protocol: {:?}, init_metadata_has_mode: {:?}",
                        &protocol,
                        metadata.configuration().get("delta.columnMapping.mode")
                    );

                    operation = Some(DeltaOperation::Create {
                        mode: SaveMode::ErrorIfExists,
                        location: table_url.to_string(),
                        protocol,
                        metadata,
                    });
                } else if has_timestamp_ntz {
                    #[allow(clippy::unwrap_used)]
                    let protocol: Protocol = serde_json::from_value(serde_json::json!({
                        "minReaderVersion": 3,
                        "minWriterVersion": 7,
                        "readerFeatures": ["timestampNtz"],
                        "writerFeatures": ["timestampNtz"]
                    }))
                    .unwrap();

                    let metadata = Metadata::try_new(
                        None,
                        None,
                        kernel_schema,
                        partition_columns.clone(),
                        Utc::now().timestamp_millis(),
                        HashMap::new(),
                    )
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    initial_actions.push(Action::Protocol(protocol.clone()));
                    initial_actions.push(Action::Metadata(metadata.clone()));

                    operation = Some(DeltaOperation::Create {
                        mode: SaveMode::ErrorIfExists,
                        location: table_url.to_string(),
                        protocol,
                        metadata,
                    });
                }
            }

            // Build physical writer schema (use physical names and set parquet field ids)
            // Prefer schema from pending Metadata action (schema evolution) if present
            let (writer_schema, physical_partition_columns, logical_kernel_for_mapping) = if matches!(
                effective_mode,
                ColumnMappingModeOption::Name | ColumnMappingModeOption::Id
            ) {
                // Determine logical kernel schema (annotated for new tables; from snapshot for existing tables)
                #[allow(clippy::unwrap_used)]
                #[allow(clippy::expect_used)]
                let logical_kernel: StructType = if let Some(meta_action_schema) = schema_actions
                    .iter()
                    .find_map(|a| match a {
                        Action::Metadata(m) => Some(
                            m.parse_schema()
                                .map_err(|e| DataFusionError::External(Box::new(e))),
                        ),
                        _ => None,
                    })
                    .transpose()?
                {
                    meta_action_schema
                } else if table_exists {
                    table
                        .as_ref()
                        .unwrap()
                        .snapshot()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?
                        .snapshot()
                        .schema()
                        .clone()
                } else {
                    annotated_schema_opt
                        .clone()
                        .expect("annotated schema should exist for new table with column mapping")
                };

                // Build physical Arrow schema enriched with PARQUET:field_id
                let enriched_arrow = get_physical_schema(&logical_kernel, kernel_mode);
                let arc_schema = Arc::new(enriched_arrow);
                let writer_field_names: Vec<String> = arc_schema
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
                log::trace!(
                    "effective_mode: {:?}, writer_schema_fields: {:?}",
                    effective_mode,
                    &writer_field_names
                );

                // Resolve logical partition columns to their physical names so that the
                // writer can locate them in the batch when column mapping is enabled.
                let resolved_partitions = partition_columns
                    .iter()
                    .map(|logical_name| {
                        let field = logical_kernel.field(logical_name).ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "Partition column '{}' not found in logical schema",
                                logical_name
                            ))
                        })?;
                        Ok(field.physical_name(kernel_mode).to_string())
                    })
                    .collect::<Result<Vec<_>>>()?;

                (arc_schema, resolved_partitions, Some(logical_kernel))
            } else {
                (final_schema.clone(), partition_columns.clone(), None)
            };

            let writer_config = WriterConfig::new(
                writer_schema.clone(),
                partition_columns.clone(),
                physical_partition_columns.clone(),
                None,
                *target_file_size,
                *write_batch_size,
                32,
                None,
            );

            let writer_path = object_store::path::Path::from(table_url.path());
            let mut writer = DeltaWriter::new(object_store.clone(), writer_path, writer_config);

            // Compute physical-to-logical mapping once before the loop
            let phys_to_logical = logical_kernel_for_mapping.as_ref().map(|logical_kernel| {
                let map = Self::build_physical_to_logical_map(logical_kernel, kernel_mode);
                log::trace!("phys_to_logical: {:?}", &map);
                map
            });

            let mut total_rows = 0u64;
            let mut data = stream;
            let mut write_time_ms: u64 = 0;

            while let Some(batch_result) = data.next().await {
                let batch_start = Instant::now();
                let batch = batch_result?;
                let rows: u64 = u64::try_from(batch.num_rows()).unwrap_or_default();
                total_rows += rows;

                // Debug: input vs target schema field names for each batch
                let input_names: Vec<String> = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
                let target_names: Vec<String> = writer_schema
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
                log::trace!(
                    "input_batch_fields: {:?}, target_fields: {:?}",
                    &input_names,
                    &target_names
                );

                let validated_batch = Self::validate_and_adapt_batch(
                    batch,
                    &writer_schema,
                    phys_to_logical.as_ref(),
                    timezone.as_deref(),
                )?;

                writer
                    .write(&validated_batch)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                write_time_ms =
                    write_time_ms.saturating_add(batch_start.elapsed().as_millis() as u64);
            }

            let close_start = Instant::now();
            let add_actions = writer
                .close()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let close_time_ms = close_start.elapsed().as_millis() as u64;

            let num_added_files: u64 = add_actions.len() as u64;
            let num_added_bytes: u64 = add_actions
                .iter()
                .map(|a| u64::try_from(a.size).unwrap_or_default())
                .sum();

            // Combine add_actions and schema_actions into a single actions vector
            let mut actions: Vec<Action> = schema_actions;
            actions.extend(add_actions.into_iter().map(Action::Add));

            let operation = operation_override.or(operation);

            let mut operation_metrics: HashMap<String, Value> = HashMap::new();
            operation_metrics.insert("numOutputRows".to_string(), Value::from(total_rows));
            operation_metrics.insert("numFiles".to_string(), Value::from(num_added_files));
            operation_metrics.insert("numOutputFiles".to_string(), Value::from(num_added_files));
            operation_metrics.insert("numAddedFiles".to_string(), Value::from(num_added_files));
            operation_metrics.insert("numOutputBytes".to_string(), Value::from(num_added_bytes));
            operation_metrics.insert("numAddedBytes".to_string(), Value::from(num_added_bytes));
            operation_metrics.insert(
                "writeTimeMs".to_string(),
                Value::from(write_time_ms.saturating_add(close_time_ms)),
            );
            operation_metrics.insert(
                "executionTimeMs".to_string(),
                Value::from(exec_start.elapsed().as_millis() as u64),
            );

            let commit_info = CommitInfo {
                row_count: total_rows,
                actions,
                initial_actions,
                operation,
                operation_metrics,
            };

            output_rows.add(usize::try_from(total_rows).unwrap_or(usize::MAX));
            output_bytes.add(usize::try_from(num_added_bytes).unwrap_or(usize::MAX));

            let commit_info_json = serde_json::to_string(&commit_info)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let data_array = Arc::new(StringArray::from(vec![commit_info_json]));
            let batch = RecordBatch::try_new(schema, vec![data_array])?;
            Ok(batch)
        };

        let stream = once(future);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

impl DeltaWriterExec {
    /// Determine the schema mode based on options and save mode
    fn get_schema_mode(
        options: &TableDeltaOptions,
        save_mode: SaveMode,
    ) -> Result<Option<SchemaMode>> {
        match (options.merge_schema, options.overwrite_schema) {
            (true, true) => Err(DataFusionError::Plan(
                "Cannot specify both mergeSchema and overwriteSchema options".to_string(),
            )),
            (false, true) => {
                if save_mode != SaveMode::Overwrite {
                    Err(DataFusionError::Plan(
                        "overwriteSchema option can only be used with overwrite save mode"
                            .to_string(),
                    ))
                } else {
                    Ok(Some(SchemaMode::Overwrite))
                }
            }
            (true, false) => Ok(Some(SchemaMode::Merge)),
            (false, false) => Ok(None), // Default, should check
        }
    }

    /// Handle schema evolution based on the schema mode
    async fn handle_schema_evolution(
        table: &crate::table::DeltaTable,
        input_schema: &SchemaRef,
        schema_mode: Option<SchemaMode>,
    ) -> Result<(SchemaRef, Vec<Action>)> {
        let table_metadata = table.snapshot()?.metadata();
        let table_schema = table_metadata
            .parse_schema()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let table_arrow_schema = std::sync::Arc::new((&table_schema).try_into_arrow()?);

        match schema_mode {
            Some(SchemaMode::Merge) => {
                // Merge schemas
                let merged_schema = Self::merge_schemas(&table_arrow_schema, input_schema)?;
                if merged_schema.fields() != table_arrow_schema.fields() {
                    // Schema has changed, create metadata action
                    let candidate_kernel: StructType = merged_schema
                        .as_ref()
                        .try_into_kernel()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    let snapshot = table.snapshot()?;
                    let current_metadata = snapshot.metadata();
                    let current_kernel = snapshot.snapshot().schema().clone();
                    let kmode = snapshot.effective_column_mapping_mode();

                    // Delegate schema evolution to SchemaManager
                    let (_final_kernel, updated_metadata) =
                        evolve_schema(&current_kernel, &candidate_kernel, current_metadata, kmode)
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    // Use merged_schema (Arrow) for downstream planning; metadata carries final kernel schema
                    Ok((merged_schema, vec![Action::Metadata(updated_metadata)]))
                } else {
                    Ok((table_arrow_schema, Vec::new()))
                }
            }
            Some(SchemaMode::Overwrite) => {
                // Use input schema as-is
                let candidate_kernel: StructType = input_schema
                    .as_ref()
                    .try_into_kernel()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let snapshot = table.snapshot()?;
                let current_metadata = snapshot.metadata();
                let current_kernel = snapshot.snapshot().schema().clone();
                let kmode = snapshot.effective_column_mapping_mode();

                // Delegate schema overwrite to SchemaManager
                let (_final_kernel, updated_metadata) =
                    evolve_schema(&current_kernel, &candidate_kernel, current_metadata, kmode)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                Ok((
                    input_schema.clone(),
                    vec![Action::Metadata(updated_metadata)],
                ))
            }
            None => {
                // Validate schema compatibility
                Self::validate_schema_compatibility(&table_arrow_schema, input_schema)?;
                Ok((table_arrow_schema, Vec::new()))
            }
        }
    }

    /// Merge two Arrow schemas
    fn merge_schemas(table_schema: &Schema, input_schema: &Schema) -> Result<SchemaRef> {
        let mut field_map: HashMap<String, Field> = HashMap::new();
        let mut field_order: Vec<String> = Vec::new();

        // Add all fields from table schema first (preserve order)
        for field in table_schema.fields() {
            let field_name = field.name().clone();
            field_map.insert(field_name.clone(), field.as_ref().clone());
            field_order.push(field_name);
        }

        // Process fields from input schema
        for input_field in input_schema.fields() {
            let field_name = input_field.name().clone();

            if let Some(existing_field) = field_map.get(&field_name) {
                // Field exists in both schemas - check for type compatibility and promotion
                let promoted_field =
                    DeltaTypeConverter::promote_field_types(existing_field, input_field)?;
                field_map.insert(field_name, promoted_field);
            } else {
                // New field from input schema
                field_map.insert(field_name.clone(), input_field.as_ref().clone());
                field_order.push(field_name);
            }
        }

        // Build merged fields in the correct order
        #[allow(clippy::unwrap_used)]
        let merged_fields: Vec<Field> = field_order
            .into_iter()
            .map(|name| field_map.remove(&name).unwrap())
            .collect();

        Ok(std::sync::Arc::new(Schema::new(merged_fields)))
    }

    /// Validate schema compatibility
    fn validate_schema_compatibility(table_schema: &Schema, input_schema: &Schema) -> Result<()> {
        // Simple validation: check if all input fields exist in table schema with compatible types
        for input_field in input_schema.fields() {
            match table_schema.field_with_name(input_field.name()) {
                Ok(table_field) => {
                    if table_field.data_type() != input_field.data_type()
                        && DeltaTypeConverter::validate_cast_safety(
                            input_field.data_type(),
                            table_field.data_type(),
                            input_field.name(),
                        )
                        .is_err()
                    {
                        return Err(DataFusionError::Plan(format!(
                            "Schema mismatch for field '{}': table has type {:?}, input has type {:?}. Use mergeSchema=true to allow schema evolution.",
                            input_field.name(),
                            table_field.data_type(),
                            input_field.data_type()
                        )));
                    }
                }
                Err(_) => {
                    return Err(DataFusionError::Plan(format!(
                        "Field '{}' not found in table schema. Use mergeSchema=true to allow schema evolution.",
                        input_field.name()
                    )));
                }
            }
        }
        Ok(())
    }

    /// Validate and adapt a batch to match the final schema
    fn validate_and_adapt_batch(
        batch: RecordBatch,
        final_schema: &SchemaRef,
        phys_to_logical: Option<&HashMap<String, String>>,
        timezone: Option<&str>,
    ) -> Result<RecordBatch> {
        let batch_schema = batch.schema();

        // If schemas are identical, no adaptation needed
        if batch_schema.as_ref() == final_schema.as_ref() {
            return Ok(batch);
        }

        // Check if all required fields are present and types are compatible
        let mut adapted_columns = Vec::with_capacity(final_schema.fields().len());

        for final_field in final_schema.fields() {
            // For physical writer schema, final_field.name() is physical. Map to logical if provided
            let lookup_name: &str = if let Some(map) = phys_to_logical {
                map.get(final_field.name().as_str())
                    .map(|s| s.as_str())
                    .unwrap_or(final_field.name())
            } else {
                final_field.name()
            };

            match batch_schema.column_with_name(lookup_name) {
                Some((batch_index, batch_field)) => {
                    let batch_column = batch.column(batch_index);

                    if batch_field.data_type() == final_field.data_type() {
                        adapted_columns.push(batch_column.clone());
                    } else {
                        // Types don't match, validate cast safety and attempt casting
                        DeltaTypeConverter::validate_cast_safety(
                            batch_field.data_type(),
                            final_field.data_type(),
                            final_field.name(),
                        )?;

                        let casted_column =
                            match (batch_field.data_type(), final_field.data_type(), timezone) {
                                (
                                    DataType::Timestamp(unit_from, Some(_)),
                                    DataType::Timestamp(unit_to, Some(target_tz)),
                                    _,
                                ) if unit_from == unit_to => reinterpret_timestamp_timezone(
                                    batch_column,
                                    *unit_to,
                                    Some(target_tz.clone()),
                                )?,
                                (
                                    DataType::Timestamp(unit_from, None),
                                    DataType::Timestamp(_unit_to, Some(_)),
                                    Some(session_tz),
                                ) => {
                                    // Interpret NTZ as local time in session timezone before converting
                                    let session_type = DataType::Timestamp(
                                        *unit_from,
                                        Some(Arc::<str>::from(session_tz)),
                                    );
                                    let with_session_tz = datafusion::arrow::compute::cast(
                                        batch_column,
                                        &session_type,
                                    )
                                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                                    datafusion::arrow::compute::cast(
                                        &with_session_tz,
                                        final_field.data_type(),
                                    )
                                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?
                                }
                                (
                                    DataType::Timestamp(unit_from, Some(_)),
                                    DataType::Timestamp(_unit_to, None),
                                    Some(session_tz),
                                ) => {
                                    // Convert to session timezone before dropping timezone info
                                    let session_type = DataType::Timestamp(
                                        *unit_from,
                                        Some(Arc::<str>::from(session_tz)),
                                    );
                                    let session_aligned = datafusion::arrow::compute::cast(
                                        batch_column,
                                        &session_type,
                                    )
                                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                                    datafusion::arrow::compute::cast(
                                        &session_aligned,
                                        final_field.data_type(),
                                    )
                                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?
                                }
                                _ => datafusion::arrow::compute::cast(
                                    batch_column,
                                    final_field.data_type(),
                                )
                                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
                            };
                        adapted_columns.push(casted_column);
                    }
                }
                None => {
                    // Column missing from batch
                    if final_field.is_nullable() {
                        let null_array = datafusion::arrow::array::new_null_array(
                            final_field.data_type(),
                            batch.num_rows(),
                        );
                        adapted_columns.push(null_array);
                    } else {
                        return Err(DataFusionError::Plan(format!(
                            "Required non-nullable column '{}' is missing from input data",
                            final_field.name()
                        )));
                    }
                }
            }
        }

        // Create new RecordBatch with adapted columns
        RecordBatch::try_new(final_schema.clone(), adapted_columns)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

fn reinterpret_timestamp_timezone(
    array: &ArrayRef,
    unit: TimeUnit,
    target_tz: Option<Arc<str>>,
) -> Result<ArrayRef> {
    fn adjust<T: ArrowTimestampType>(
        array: &ArrayRef,
        target_tz: Option<Arc<str>>,
    ) -> Result<ArrayRef> {
        let ts_array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Failed to downcast timestamp array for timezone rewrite: {:?}",
                    array.data_type()
                ))
            })?;

        Ok(Arc::new(ts_array.clone().with_timezone_opt(target_tz)))
    }

    match unit {
        TimeUnit::Second => adjust::<TimestampSecondType>(array, target_tz),
        TimeUnit::Millisecond => adjust::<TimestampMillisecondType>(array, target_tz),
        TimeUnit::Microsecond => adjust::<TimestampMicrosecondType>(array, target_tz),
        TimeUnit::Nanosecond => adjust::<TimestampNanosecondType>(array, target_tz),
    }
}

impl DisplayAs for DeltaWriterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DeltaWriterExec(table_path={})", self.table_url)
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: delta")?;
                write!(f, "table_path={}", self.table_url)
            }
        }
    }
}
