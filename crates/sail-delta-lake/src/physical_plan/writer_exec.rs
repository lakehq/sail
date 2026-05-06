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
use datafusion::arrow::array::{ArrayRef, PrimitiveArray};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{
    ArrowTimestampType, DataType, SchemaRef, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, OrderingRequirements, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_physical_expr::{Distribution, EquivalenceProperties};
use futures::stream::{once, StreamExt};
use sail_common_datafusion::datasource::PhysicalSinkMode;
use url::Url;

use crate::conversion::DeltaTypeConverter;
use crate::kernel::transaction::OperationMetrics;
use crate::kernel::DeltaOperation;
use crate::operations::write::writer::{DeltaWriter, WriterConfig};
use crate::physical_plan::writer_options::DeltaWriterExecOptions;
use crate::physical_plan::{
    delta_action_schema, encode_actions, DeltaWriteContext, ExecCommitMeta,
};
use crate::spec::{Action, ColumnMappingMode, StructType};
use crate::storage::get_object_store_from_context;

/// Physical execution node for Delta Lake writing operations
#[derive(Debug)]
pub struct DeltaWriterExec {
    input: Arc<dyn ExecutionPlan>,
    table_url: Url,
    options: DeltaWriterExecOptions,
    metadata_configuration: HashMap<String, String>,
    partition_columns: Vec<String>,
    sink_mode: PhysicalSinkMode,
    table_exists: bool,
    sink_schema: SchemaRef,
    /// Optional override for commit operation metadata.
    operation_override: Option<DeltaOperation>,
    write_context: DeltaWriteContext,
    metrics: ExecutionPlanMetricsSet,
    cache: Arc<PlanProperties>,
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
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        options: DeltaWriterExecOptions,
        metadata_configuration: HashMap<String, String>,
        partition_columns: Vec<String>,
        sink_mode: PhysicalSinkMode,
        table_exists: bool,
        sink_schema: SchemaRef,
        operation_override: Option<DeltaOperation>,
        write_context: DeltaWriteContext,
    ) -> Result<Self> {
        let schema = delta_action_schema()?;
        let output_partitions = input.output_partitioning().partition_count().max(1);
        let cache = Self::compute_properties(schema, output_partitions);
        Ok(Self {
            input,
            table_url,
            options,
            metadata_configuration,
            partition_columns,
            sink_mode,
            table_exists,
            sink_schema,
            operation_override,
            write_context,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        })
    }

    fn compute_properties(schema: SchemaRef, output_partitions: usize) -> Arc<PlanProperties> {
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(output_partitions.max(1)),
            EmissionType::Final,
            Boundedness::Bounded,
        ))
    }

    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    pub fn options(&self) -> &DeltaWriterExecOptions {
        &self.options
    }

    pub fn metadata_configuration(&self) -> &HashMap<String, String> {
        &self.metadata_configuration
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

    pub fn write_context(&self) -> &DeltaWriteContext {
        &self.write_context
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

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        if self.partition_columns.is_empty() {
            // Upstream repartitioning controls file counts and small-file behavior.
            return vec![Distribution::UnspecifiedDistribution];
        }

        // For partitioned tables, require grouping by the partition key so that each task can
        // write its partitions correctly without opening many writers concurrently.
        //
        // TODO(optimizer): Reduce the cost of meeting this distribution requirement.
        let mut exprs: Vec<Arc<dyn datafusion_physical_expr::PhysicalExpr>> =
            Vec::with_capacity(self.partition_columns.len());
        for name in &self.partition_columns {
            let idx = match self.input.schema().index_of(name) {
                Ok(i) => i,
                Err(_) => return vec![Distribution::UnspecifiedDistribution],
            };
            exprs.push(Arc::new(
                datafusion_physical_expr::expressions::Column::new(name, idx),
            ));
        }

        vec![Distribution::HashPartitioned(exprs)]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        if self.partition_columns.is_empty() {
            return vec![None];
        }

        // TODO(optimizer): Reduce the cost of meeting this ordering requirement.
        // Goal: avoid unnecessary `SortExec` when we can prove the input is already ordered
        // (or sufficiently grouped) by the partition key.
        let mut sort_exprs: Vec<PhysicalSortExpr> =
            Vec::with_capacity(self.partition_columns.len());
        for name in &self.partition_columns {
            let idx = match self.input.schema().index_of(name) {
                Ok(i) => i,
                Err(_) => return vec![None],
            };
            sort_exprs.push(PhysicalSortExpr {
                expr: Arc::new(Column::new(name, idx)),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            });
        }

        let Some(ordering) = LexOrdering::new(sort_exprs) else {
            return vec![None];
        };
        vec![Some(OrderingRequirements::from(ordering))]
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
            self.metadata_configuration.clone(),
            self.partition_columns.clone(),
            self.sink_mode.clone(),
            self.table_exists,
            self.sink_schema.clone(),
            self.operation_override.clone(),
            self.write_context.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_partitions = self.input.output_partitioning().partition_count();
        if input_partitions == 0 {
            return internal_err!("DeltaWriterExec requires at least one input partition");
        }

        if partition >= input_partitions {
            return internal_err!(
                "DeltaWriterExec invalid partition {partition} (input partitions: {input_partitions})"
            );
        }
        let stream = self.input.execute(partition, Arc::clone(&context))?;
        self.execute_stream(stream, partition, context)
    }
}

impl DeltaWriterExec {
    fn execute_stream(
        &self,
        stream: SendableRecordBatchStream,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);
        let output_bytes = MetricBuilder::new(&self.metrics).output_bytes(partition);
        let elapsed_compute = MetricBuilder::new(&self.metrics).elapsed_compute(partition);

        let table_url = self.table_url.clone();
        let options = self.options.clone();
        let partition_columns = self.partition_columns.clone();
        let sink_mode = self.sink_mode.clone();
        let table_exists = self.table_exists;
        let write_context = self.write_context.clone();
        let session_timezone = context
            .session_config()
            .options()
            .execution
            .time_zone
            .clone();

        let future = async move {
            let _elapsed_compute_timer = elapsed_compute.timer();
            let exec_start = Instant::now();
            let DeltaWriterExecOptions {
                target_file_size,
                write_batch_size,
                ..
            } = &options;
            let timezone = session_timezone;

            let object_store = get_object_store_from_context(&context, &table_url)?;

            match &sink_mode {
                PhysicalSinkMode::Append
                | PhysicalSinkMode::Overwrite
                | PhysicalSinkMode::OverwriteIf { .. } => {}
                PhysicalSinkMode::ErrorIfExists => {
                    if table_exists {
                        return Err(DataFusionError::Plan(format!(
                            "Delta table already exists at path: {table_url}"
                        )));
                    }
                }
                PhysicalSinkMode::IgnoreIfExists => {
                    if table_exists {
                        // Table exists, ignore the write operation and return "no-op" output.
                        // Still update execution metrics so callers see a completed node.
                        output_rows.add(0);
                        output_bytes.add(0);
                        return encode_actions(vec![], Some(ExecCommitMeta::default()));
                    }
                }
                PhysicalSinkMode::OverwritePartitions => {
                    return Err(DataFusionError::NotImplemented(
                        "OverwritePartitions mode not implemented".to_string(),
                    ));
                }
            }

            let schema_actions = write_context.schema_actions.clone();
            let initial_actions = write_context.initial_actions.clone();
            let operation = write_context.operation.clone();
            let kernel_mode = write_context.effective_column_mapping_mode;
            let writer_schema = write_context.writer_schema()?;
            let physical_partition_columns = write_context.physical_partition_columns.clone();
            let logical_kernel_for_mapping = write_context.logical_kernel_for_mapping.clone();

            let writer_config = WriterConfig::new(
                writer_schema.clone(),
                partition_columns.clone(),
                physical_partition_columns.clone(),
                None,
                *target_file_size,
                write_batch_size.get(),
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

            // TODO: for MERGE, populate numSourceRows / numTargetRowsInserted /
            // numTargetRowsUpdated / numTargetRowsDeleted / numTargetRowsCopied by
            // counting rows per OPERATION_COLUMN value in the writer input stream
            // (copy=0, insert=1, update=2, delete=3). The column is currently stripped
            // before this point — plumb it through as side metrics without writing it
            // to disk.
            let operation_metrics = OperationMetrics {
                num_files: Some(num_added_files),
                num_output_rows: Some(total_rows),
                num_output_bytes: Some(num_added_bytes),
                execution_time_ms: Some(exec_start.elapsed().as_millis() as u64),
                num_added_files: Some(num_added_files),
                num_output_files: Some(num_added_files),
                num_added_bytes: Some(num_added_bytes),
                write_time_ms: Some(write_time_ms.saturating_add(close_time_ms)),
                ..Default::default()
            };

            output_rows.add(usize::try_from(total_rows).unwrap_or(usize::MAX));
            output_bytes.add(usize::try_from(num_added_bytes).unwrap_or(usize::MAX));

            // Build row-per-action output:
            // - protocol/metadata "initial actions" (when present)
            // - schema evolution actions (metadata)
            // - Add actions (one row per file)
            // - CommitMeta row (row_count + operation + metrics)
            let mut output_actions: Vec<Action> = Vec::new();

            if partition == 0 {
                for ia in &initial_actions {
                    match ia {
                        Action::Protocol(_) | Action::Metadata(_) => {
                            output_actions.push(ia.clone())
                        }
                        _ => {}
                    }
                }

                for sa in &actions {
                    match sa {
                        Action::Metadata(_) | Action::Protocol(_) => {
                            output_actions.push(sa.clone())
                        }
                        _ => {}
                    }
                }
            }

            for action in actions {
                if let Action::Add(add) = action {
                    output_actions.push(Action::Add(add));
                }
            }

            encode_actions(
                output_actions,
                Some(ExecCommitMeta {
                    row_count: total_rows,
                    operation,
                    operation_metrics,
                }),
            )
        };

        let stream = once(future);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

impl DeltaWriterExec {
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
