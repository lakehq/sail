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

use std::fmt::{Debug, Formatter};
use std::num::NonZeroUsize;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::config::{ExecutionOptions, TableParquetOptions};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    Distribution, EquivalenceProperties, LexOrdering, LexRequirement, PhysicalSortExpr,
};
use datafusion::physical_expr_common::sort_expr::{
    OrderingRequirements, format_physical_sort_requirement_list,
};
use datafusion::physical_plan::execution_plan::{EvaluationType, SchedulingType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, EmptyRecordBatchStream, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, PlanProperties, SendableRecordBatchStream, execute_input_stream,
};
use datafusion_common::{DataFusionError, Result, internal_err, not_impl_err, plan_datafusion_err};
use datafusion_datasource::file_sink_config::{FileOutputMode, FileSink, FileSinkConfig};
use datafusion_datasource_parquet::ParquetSink;
use futures::{StreamExt, stream};
use log::warn;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use parquet::file::metadata::SortingColumn;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::listing_write::{
    ListingWriteFile, ListingWriteTaskManifest, encode_listing_write_manifest,
    listing_write_manifest_schema,
};
use sail_common_datafusion::task_attempt::TaskAttemptContext;
use uuid::Uuid;

use crate::demux::{ParquetFileManifest, start_demuxer_task};

/// Settings captured in the physical plan that affect file layout and writer buffering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetWriteExecutionOptions {
    pub minimum_parallel_output_files: usize,
    pub soft_max_rows_per_output_file: usize,
    pub max_records_per_file: Option<NonZeroUsize>,
    pub max_buffered_batches_per_output_file: usize,
    pub objectstore_writer_buffer_size: usize,
}

impl From<&ExecutionOptions> for ParquetWriteExecutionOptions {
    fn from(options: &ExecutionOptions) -> Self {
        Self {
            minimum_parallel_output_files: options.minimum_parallel_output_files,
            soft_max_rows_per_output_file: options.soft_max_rows_per_output_file,
            max_records_per_file: None,
            max_buffered_batches_per_output_file: options.max_buffered_batches_per_output_file,
            objectstore_writer_buffer_size: options.objectstore_writer_buffer_size,
        }
    }
}

/// A listing Parquet writer that executes once for every input partition.
///
/// DataFusion's `DataSinkExec` requires `SinglePartition`. This plan keeps the
/// input partitioning so Sail can run independent demux/writer pipelines on
/// multiple workers.
#[derive(Clone)]
pub struct ParquetWriterExec {
    input: Arc<dyn ExecutionPlan>,
    sink_config: FileSinkConfig,
    parquet_options: TableParquetOptions,
    execution_options: ParquetWriteExecutionOptions,
    sort_order: Option<LexRequirement>,
    write_id: String,
    schema: SchemaRef,
    cache: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl ParquetWriterExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        sink_config: FileSinkConfig,
        parquet_options: TableParquetOptions,
        execution_options: ParquetWriteExecutionOptions,
        sort_order: Option<LexRequirement>,
    ) -> Result<Self> {
        Self::try_new_with_write_id(
            input,
            sink_config,
            parquet_options,
            execution_options,
            sort_order,
            Uuid::new_v4().to_string(),
        )
    }

    pub fn try_new_with_write_id(
        input: Arc<dyn ExecutionPlan>,
        mut sink_config: FileSinkConfig,
        parquet_options: TableParquetOptions,
        execution_options: ParquetWriteExecutionOptions,
        sort_order: Option<LexRequirement>,
        write_id: String,
    ) -> Result<Self> {
        if sink_config.table_paths.len() != 1 {
            return Err(plan_datafusion_err!(
                "Parquet writer requires exactly one output path, got {}",
                sink_config.table_paths.len()
            ));
        }
        if sink_config.insert_op != InsertOp::Append {
            return not_impl_err!("Parquet overwrite is not implemented");
        }
        if sink_config.file_extension.is_empty() {
            return Err(plan_datafusion_err!(
                "Parquet writer requires a file extension"
            ));
        }
        if write_id.is_empty() {
            return Err(plan_datafusion_err!("Parquet writer requires a write ID"));
        }
        // A listing-table write always targets a directory. Enforcing that
        // interpretation prevents multiple input partitions from racing on a
        // path that happens to end in `.parquet`.
        sink_config.file_output_mode = FileOutputMode::Directory;
        let schema = listing_write_manifest_schema();
        let partition_count = input.output_partitioning().partition_count().max(1);
        let cache = Arc::new(
            PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&schema)),
                Partitioning::UnknownPartitioning(partition_count),
                input.pipeline_behavior(),
                input.boundedness(),
            )
            .with_scheduling_type(SchedulingType::Cooperative)
            .with_evaluation_type(EvaluationType::Eager),
        );
        Ok(Self {
            input,
            sink_config,
            parquet_options,
            execution_options,
            sort_order,
            write_id,
            schema,
            cache,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn sink_config(&self) -> &FileSinkConfig {
        &self.sink_config
    }

    pub fn parquet_options(&self) -> &TableParquetOptions {
        &self.parquet_options
    }

    pub fn execution_options(&self) -> &ParquetWriteExecutionOptions {
        &self.execution_options
    }

    pub fn sort_order(&self) -> &Option<LexRequirement> {
        &self.sort_order
    }

    pub fn write_id(&self) -> &str {
        &self.write_id
    }

    pub fn staging_prefix(&self) -> Path {
        self.sink_config.table_paths[0]
            .prefix()
            .clone()
            .join("_temporary")
            .join("sail")
            .join(self.write_id.as_str())
    }

    fn attempt_staging_prefix(
        &self,
        context: &TaskContext,
        partition: usize,
    ) -> Result<(Path, TaskAttemptContext)> {
        let attempt = context
            .extension::<TaskAttemptContext>()
            .unwrap_or_else(|_| Arc::new(TaskAttemptContext::new(0, 0, partition, 0)));
        if attempt.partition() != partition {
            return Err(plan_datafusion_err!(
                "Parquet writer partition {partition} does not match task partition {}",
                attempt.partition()
            ));
        }
        Ok((
            self.staging_prefix().join(attempt.path_component()),
            *attempt,
        ))
    }

    fn writer_context(&self, context: &TaskContext) -> Arc<TaskContext> {
        let mut config = context.session_config().clone();
        let execution = &mut config.options_mut().execution;
        let partition_count = self.input.output_partitioning().partition_count().max(1);
        execution.minimum_parallel_output_files = self
            .execution_options
            .minimum_parallel_output_files
            .max(1)
            .div_ceil(partition_count)
            .max(1);
        execution.soft_max_rows_per_output_file =
            self.execution_options.soft_max_rows_per_output_file;
        execution.max_buffered_batches_per_output_file = self
            .execution_options
            .max_buffered_batches_per_output_file
            .max(1);
        execution.objectstore_writer_buffer_size =
            self.execution_options.objectstore_writer_buffer_size.max(1);
        Arc::new(TaskContext::new(
            context.task_id(),
            context.session_id(),
            config,
            context.scalar_functions().clone(),
            context.higher_order_functions().clone(),
            context.aggregate_functions().clone(),
            context.window_functions().clone(),
            context.runtime_env(),
        ))
    }
}

impl Debug for ParquetWriterExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetWriterExec")
            .field("sink_config", &self.sink_config)
            .field(
                "input_partitions",
                &self.input.output_partitioning().partition_count(),
            )
            .field("write_id", &self.write_id)
            .finish()
    }
}

impl DisplayAs for ParquetWriterExec {
    fn fmt_as(&self, format: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        let input_partitions = self.input.output_partitioning().partition_count();
        let partition_columns = self
            .sink_config
            .table_partition_cols
            .iter()
            .map(|(name, data_type)| format!("{name}:{data_type:?}"))
            .collect::<Vec<_>>()
            .join(", ");
        let sort_order = self.sort_order.as_deref().unwrap_or_default();
        let compression = self
            .parquet_options
            .global
            .compression
            .as_deref()
            .unwrap_or("default");

        match format {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "ParquetWriterExec: output={}, input_partitions={}, partition_by=[{}], \
                 keep_partition_by_columns={}, sort_order={}, file_extension={}, \
                 minimum_parallel_output_files={}, soft_max_rows_per_output_file={}, \
                 max_records_per_file={}, \
                 max_buffered_batches_per_output_file={}, object_store_writer_buffer_size={}, \
                 compression={}, max_row_group_size={}",
                self.sink_config.original_url,
                input_partitions,
                partition_columns,
                self.sink_config.keep_partition_by_columns,
                format_physical_sort_requirement_list(sort_order),
                self.sink_config.file_extension,
                self.execution_options.minimum_parallel_output_files,
                self.execution_options.soft_max_rows_per_output_file,
                self.execution_options
                    .max_records_per_file
                    .map_or_else(|| "unlimited".to_string(), |value| value.to_string()),
                self.execution_options.max_buffered_batches_per_output_file,
                self.execution_options.objectstore_writer_buffer_size,
                compression,
                self.parquet_options.global.max_row_group_size,
            ),
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: parquet")?;
                writeln!(f, "output={}", self.sink_config.original_url)?;
                writeln!(f, "input_partitions={input_partitions}")?;
                writeln!(f, "partition_by=[{partition_columns}]")?;
                writeln!(
                    f,
                    "sort_order={}",
                    format_physical_sort_requirement_list(sort_order)
                )?;
                writeln!(
                    f,
                    "minimum_parallel_output_files={}",
                    self.execution_options.minimum_parallel_output_files
                )?;
                writeln!(
                    f,
                    "soft_max_rows_per_output_file={}",
                    self.execution_options.soft_max_rows_per_output_file
                )?;
                writeln!(
                    f,
                    "max_records_per_file={}",
                    self.execution_options
                        .max_records_per_file
                        .map_or_else(|| "unlimited".to_string(), |value| value.to_string())
                )?;
                write!(f, "compression={compression}")
            }
        }
    }
}

impl ExecutionPlan for ParquetWriterExec {
    fn name(&self) -> &'static str {
        "ParquetWriterExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![true]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![self.sort_order.as_ref().cloned().map(Into::into)]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return internal_err!("ParquetWriterExec requires exactly one child");
        };
        Ok(Arc::new(Self::try_new_with_write_id(
            Arc::clone(input),
            self.sink_config.clone(),
            self.parquet_options.clone(),
            self.execution_options.clone(),
            self.sort_order.clone(),
            self.write_id.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_partition_count = self.input.output_partitioning().partition_count();
        let output_partition_count = input_partition_count.max(1);
        if partition >= output_partition_count {
            return internal_err!(
                "ParquetWriterExec invalid partition {partition} (output partitions: {output_partition_count})"
            );
        }
        let input = if input_partition_count == 0 {
            Box::pin(EmptyRecordBatchStream::new(Arc::clone(
                self.sink_config.output_schema(),
            ))) as SendableRecordBatchStream
        } else {
            execute_input_stream(
                Arc::clone(&self.input),
                Arc::clone(self.sink_config.output_schema()),
                partition,
                Arc::clone(&context),
            )?
        };
        let sorting_columns = self.sort_order.as_ref().and_then(|requirements| {
            let ordering: LexOrdering = requirements.clone().into();
            lex_ordering_to_sorting_columns(&ordering, &self.sink_config).ok()
        });
        let sink = Arc::new(
            ParquetSink::new(self.sink_config.clone(), self.parquet_options.clone())
                .with_sorting_columns(sorting_columns),
        );
        let writer_context = self.writer_context(&context);
        let (staging_prefix, task_attempt) =
            self.attempt_staging_prefix(&writer_context, partition)?;
        let object_store = writer_context
            .runtime_env()
            .object_store(&self.sink_config.object_store_url)?;
        let output_prefix = self.sink_config.table_paths[0].prefix().clone();
        let write_id = self.write_id.clone();
        let file_prefix = format!("part-{partition:05}-{}", self.write_id);
        let (demux_task, file_streams, mut file_manifests) = start_demuxer_task(
            &self.sink_config,
            input,
            &writer_context,
            staging_prefix.clone(),
            file_prefix,
            self.execution_options.max_records_per_file,
            partition == 0,
        )?;
        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);
        let output_bytes = MetricBuilder::new(&self.metrics).output_bytes(partition);
        let elapsed_compute = MetricBuilder::new(&self.metrics).elapsed_compute(partition);
        let schema = Arc::clone(&self.schema);
        let output = stream::once(async move {
            let _timer = elapsed_compute.timer();
            let mut cleanup =
                StagingCleanupGuard::new(Arc::clone(&object_store), staging_prefix.clone());
            let result = async {
                let rows = sink
                    .spawn_writer_tasks_and_join(
                        &writer_context,
                        demux_task,
                        file_streams,
                        Arc::clone(&object_store),
                    )
                    .await?;
                output_rows.add(usize::try_from(rows).map_err(|_| {
                    DataFusionError::Execution("Parquet row count is too large".to_string())
                })?);
                let written = sink
                    .written()
                    .into_iter()
                    .collect::<std::collections::HashMap<_, _>>();
                let mut paths = Vec::new();
                while let Some(path) = file_manifests.recv().await {
                    paths.push(path);
                }
                let (batch, bytes) = build_task_manifest(
                    &object_store,
                    &output_prefix,
                    &staging_prefix,
                    &write_id,
                    task_attempt,
                    rows,
                    written,
                    paths,
                )
                .await?;
                output_bytes.add(bytes);
                Ok(batch)
            }
            .await;
            match result {
                Ok(batch) => {
                    cleanup.disarm();
                    Ok(batch)
                }
                Err(error) => {
                    if let Err(cleanup_error) = cleanup.clean_up().await {
                        warn!(
                            "failed to clean Parquet task staging path {staging_prefix}: {cleanup_error}"
                        );
                    }
                    Err(error)
                }
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, output)))
    }
}

struct StagingCleanupGuard {
    object_store: Option<Arc<dyn object_store::ObjectStore>>,
    staging_prefix: Path,
}

impl StagingCleanupGuard {
    fn new(object_store: Arc<dyn object_store::ObjectStore>, staging_prefix: Path) -> Self {
        Self {
            object_store: Some(object_store),
            staging_prefix,
        }
    }

    fn disarm(&mut self) {
        self.object_store = None;
    }

    async fn clean_up(&mut self) -> Result<()> {
        if let Some(object_store) = self.object_store.take() {
            delete_staging_files(object_store.as_ref(), &self.staging_prefix).await?;
        }
        Ok(())
    }
}

impl Drop for StagingCleanupGuard {
    fn drop(&mut self) {
        let Some(object_store) = self.object_store.take() else {
            return;
        };
        let staging_prefix = self.staging_prefix.clone();
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            warn!(
                "cannot clean canceled Parquet task staging path {staging_prefix} without a runtime"
            );
            return;
        };
        std::mem::drop(runtime.spawn(async move {
            if let Err(error) = delete_staging_files(object_store.as_ref(), &staging_prefix).await {
                warn!(
                    "failed to clean canceled Parquet task staging path {staging_prefix}: {error}"
                );
            }
        }));
    }
}

async fn delete_staging_files(
    object_store: &dyn object_store::ObjectStore,
    staging_prefix: &Path,
) -> Result<()> {
    let mut objects = object_store.list(Some(staging_prefix));
    while let Some(object) = objects.next().await.transpose()? {
        match object_store.delete(&object.location).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(DataFusionError::ObjectStore(Box::new(error))),
        }
    }
    Ok(())
}

fn lex_ordering_to_sorting_columns(
    ordering: &LexOrdering,
    sink_config: &FileSinkConfig,
) -> Result<Vec<SortingColumn>> {
    ordering
        .iter()
        .map(|expression| sort_expr_to_sorting_column(expression, sink_config))
        .collect()
}

fn sort_expr_to_sorting_column(
    sort_expr: &PhysicalSortExpr,
    sink_config: &FileSinkConfig,
) -> Result<SortingColumn> {
    let column = sort_expr.expr.downcast_ref::<Column>().ok_or_else(|| {
        DataFusionError::Plan(format!(
            "Parquet sorting metadata only supports column expressions, got {}",
            sort_expr.expr
        ))
    })?;
    let input_index = column.index();
    let field = sink_config
        .output_schema
        .fields()
        .get(input_index)
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Parquet sort column index {input_index} is outside the sink schema"
            ))
        })?;
    let partition_names = sink_config
        .table_partition_cols
        .iter()
        .map(|(name, _)| name)
        .collect::<Vec<_>>();
    if !sink_config.keep_partition_by_columns && partition_names.contains(&field.name()) {
        return Err(plan_datafusion_err!(
            "partition column '{}' is not stored in the Parquet file",
            field.name()
        ));
    }
    let file_index = if sink_config.keep_partition_by_columns {
        input_index
    } else {
        sink_config
            .output_schema
            .fields()
            .iter()
            .take(input_index)
            .filter(|field| !partition_names.contains(&field.name()))
            .count()
    };
    Ok(SortingColumn {
        column_idx: i32::try_from(file_index).map_err(|_| {
            DataFusionError::Plan("Parquet sort column index is too large".to_string())
        })?,
        descending: sort_expr.options.descending,
        nulls_first: sort_expr.options.nulls_first,
    })
}

async fn build_task_manifest(
    object_store: &Arc<dyn object_store::ObjectStore>,
    output_prefix: &Path,
    staging_prefix: &Path,
    write_id: &str,
    task_attempt: TaskAttemptContext,
    rows: u64,
    written: std::collections::HashMap<Path, parquet::file::metadata::ParquetMetaData>,
    paths: Vec<ParquetFileManifest>,
) -> Result<(RecordBatch, usize)> {
    if written.len() != paths.len() {
        return internal_err!(
            "Parquet writer recorded {} files but wrote {} files",
            paths.len(),
            written.len()
        );
    }
    let mut files = Vec::with_capacity(paths.len());
    let mut total_size = 0_usize;
    for path in paths {
        let metadata = written.get(&path.staging_path).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "missing Parquet metadata for staged file {}",
                path.staging_path
            ))
        })?;
        if !path.staging_path.prefix_matches(staging_prefix) {
            return internal_err!(
                "staged Parquet file {} is outside attempt prefix {staging_prefix}",
                path.staging_path
            );
        }
        let relative = path.final_path.prefix_match(output_prefix).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "final Parquet file {} is outside output prefix {output_prefix}",
                path.final_path
            ))
        })?;
        let mut final_relative_path = Path::ROOT;
        final_relative_path.extend(relative);
        if final_relative_path.is_root() {
            return internal_err!("Parquet final path must name a file");
        }
        let object = object_store.head(&path.staging_path).await?;
        let size = usize::try_from(object.size).map_err(|_| {
            DataFusionError::Internal("Parquet object size is too large".to_string())
        })?;
        total_size = total_size
            .checked_add(size)
            .ok_or_else(|| DataFusionError::Internal("Parquet object size overflow".to_string()))?;
        files.push(ListingWriteFile {
            staging_path: path.staging_path.to_string(),
            final_relative_path: final_relative_path.to_string(),
            size: object.size,
            row_count: u64::try_from(metadata.file_metadata().num_rows()).map_err(|_| {
                DataFusionError::Internal("Parquet file row count must not be negative".to_string())
            })?,
            e_tag: object.e_tag,
            version: object.version,
        });
    }
    files.sort_by(|left, right| left.final_relative_path.cmp(&right.final_relative_path));
    let manifest = ListingWriteTaskManifest {
        write_id: write_id.to_string(),
        job_id: task_attempt.job_id(),
        stage: u64::try_from(task_attempt.stage())
            .map_err(|_| DataFusionError::Internal("task stage is too large".to_string()))?,
        partition: u64::try_from(task_attempt.partition())
            .map_err(|_| DataFusionError::Internal("task partition is too large".to_string()))?,
        attempt: u64::try_from(task_attempt.attempt())
            .map_err(|_| DataFusionError::Internal("task attempt is too large".to_string()))?,
        row_count: rows,
        files,
    };
    Ok((encode_listing_write_manifest(&manifest)?, total_size))
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::config::{ExecutionOptions, TableParquetOptions};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::physical_plan::{FileOutputMode, FileSinkConfig};
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{Distribution, LexRequirement, PhysicalSortRequirement};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::{
        ExecutionPlan, ExecutionPlanProperties, collect_partitioned, displayable,
    };
    use datafusion::prelude::SessionContext;
    use datafusion_common::{DataFusionError, Result};
    use datafusion_datasource::ListingTableUrl;
    use futures::{StreamExt, TryStreamExt};
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::metadata::SortingColumn;
    use sail_common_datafusion::listing_write::decode_listing_write_manifests;

    use super::{ParquetWriteExecutionOptions, ParquetWriterExec, StagingCleanupGuard};

    fn sink_config(
        output_schema: SchemaRef,
        table_partition_cols: Vec<(String, DataType)>,
    ) -> Result<FileSinkConfig> {
        Ok(FileSinkConfig {
            original_url: "memory:///output".to_string(),
            object_store_url: ObjectStoreUrl::parse("memory://")?,
            file_group: Default::default(),
            table_paths: vec![ListingTableUrl::parse("memory:///output")?],
            output_schema,
            table_partition_cols,
            insert_op: InsertOp::Append,
            keep_partition_by_columns: false,
            file_extension: "parquet".to_string(),
            file_output_mode: FileOutputMode::Automatic,
        })
    }

    #[tokio::test]
    async fn canceled_task_cleanup_guard_removes_attempt_files() -> Result<()> {
        let store = Arc::new(InMemory::new());
        let staging_prefix =
            Path::from("output/_temporary/sail/write-id/job-1-stage-2-part-0-attempt-3");
        let file = staging_prefix.clone().join("part.parquet");
        store.put(&file, b"staged".as_slice().into()).await?;

        let cleanup = StagingCleanupGuard::new(store.clone(), staging_prefix.clone());
        drop(cleanup);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if store.list(Some(&staging_prefix)).next().await.is_none() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| {
            DataFusionError::Execution("canceled task staging cleanup timed out".to_string())
        })?;
        Ok(())
    }

    fn output_row_count(partitions: &[Vec<RecordBatch>]) -> Result<u64> {
        partitions.iter().flatten().try_fold(0_u64, |total, batch| {
            decode_listing_write_manifests(batch)?
                .into_iter()
                .try_fold(total, |total, manifest| {
                    total.checked_add(manifest.row_count).ok_or_else(|| {
                        DataFusionError::Execution("Parquet output row count overflow".to_string())
                    })
                })
        })
    }

    async fn object_paths(store: &Arc<InMemory>) -> Result<Vec<String>> {
        let mut paths = store
            .list(Some(&Path::from("output")))
            .map_ok(|metadata| metadata.location.to_string())
            .try_collect::<Vec<_>>()
            .await
            .map_err(|error| DataFusionError::ObjectStore(Box::new(error)))?;
        paths.sort();
        Ok(paths)
    }

    fn staged_path(write_id: &str, partition: usize, final_relative_path: &str) -> String {
        format!(
            "output/_temporary/sail/{write_id}/job-0-stage-0-part-{partition}-attempt-0/{final_relative_path}"
        )
    }

    async fn parquet_row_count(store: &Arc<InMemory>, path: &str) -> Result<i64> {
        let bytes = store
            .get(&Path::from(path))
            .await
            .map_err(|error| DataFusionError::ObjectStore(Box::new(error)))?
            .bytes()
            .await
            .map_err(|error| DataFusionError::ObjectStore(Box::new(error)))?;
        Ok(ParquetRecordBatchReaderBuilder::try_new(bytes)?
            .metadata()
            .file_metadata()
            .num_rows())
    }

    #[test]
    fn preserves_input_partitioning_contract() -> Result<()> {
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(EmptyExec::new(Arc::clone(&input_schema)).with_partitions(4));
        let sink_config = sink_config(input_schema, vec![])?;
        let writer = ParquetWriterExec::try_new(
            input,
            sink_config,
            TableParquetOptions::default(),
            ParquetWriteExecutionOptions::from(&ExecutionOptions::default()),
            None,
        )?;

        assert_eq!(
            writer.properties().output_partitioning().partition_count(),
            4
        );
        assert!(matches!(
            writer.required_input_distribution().as_slice(),
            [Distribution::UnspecifiedDistribution]
        ));
        assert!(writer.benefits_from_input_partitioning()[0]);
        assert_eq!(
            writer.sink_config().file_output_mode,
            FileOutputMode::Directory
        );
        Ok(())
    }

    #[test]
    fn displays_write_configuration_without_write_id() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("bucket", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(EmptyExec::new(Arc::clone(&schema)).with_partitions(3));
        let mut sink_config = sink_config(
            Arc::clone(&schema),
            vec![("bucket".to_string(), DataType::Utf8)],
        )?;
        sink_config.keep_partition_by_columns = true;
        let mut parquet_options = TableParquetOptions::default();
        parquet_options.global.compression = Some("snappy".to_string());
        parquet_options.global.max_row_group_size = 123;
        let sort_order = LexRequirement::from([PhysicalSortRequirement::new(
            Arc::new(Column::new("value", 1)),
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
        )]);
        let writer = ParquetWriterExec::try_new_with_write_id(
            input,
            sink_config,
            parquet_options,
            ParquetWriteExecutionOptions {
                minimum_parallel_output_files: 7,
                soft_max_rows_per_output_file: 89,
                max_records_per_file: NonZeroUsize::new(97),
                max_buffered_batches_per_output_file: 11,
                objectstore_writer_buffer_size: 13,
            },
            Some(sort_order),
            "unstable-write-id".to_string(),
        )?;

        let display = displayable(&writer).one_line().to_string();
        assert_eq!(
            display.trim_end(),
            "ParquetWriterExec: output=memory:///output, input_partitions=3, \
             partition_by=[bucket:Utf8], keep_partition_by_columns=true, \
             sort_order=[value@1 DESC NULLS LAST], file_extension=parquet, \
             minimum_parallel_output_files=7, soft_max_rows_per_output_file=89, \
             max_records_per_file=97, \
             max_buffered_batches_per_output_file=11, object_store_writer_buffer_size=13, \
             compression=snappy, max_row_group_size=123"
        );
        Ok(())
    }

    #[tokio::test]
    async fn writes_each_input_partition_with_captured_parallelism_and_stable_paths() -> Result<()>
    {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let make_batch = |values: Vec<i64>| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(values)) as ArrayRef],
            )
        };
        let input = MemorySourceConfig::try_new_exec(
            &[
                vec![make_batch(vec![1, 2])?, make_batch(vec![3])?],
                vec![make_batch(vec![4])?, make_batch(vec![5, 6])?],
            ],
            Arc::clone(&schema),
            None,
        )?;
        let execution_options = ParquetWriteExecutionOptions {
            minimum_parallel_output_files: 4,
            soft_max_rows_per_output_file: usize::MAX,
            max_records_per_file: None,
            max_buffered_batches_per_output_file: 2,
            objectstore_writer_buffer_size: 64,
        };
        let writer: Arc<dyn ExecutionPlan> = Arc::new(ParquetWriterExec::try_new_with_write_id(
            input,
            sink_config(schema, vec![])?,
            TableParquetOptions::default(),
            execution_options,
            None,
            "retry-write".to_string(),
        )?);
        let context = SessionContext::new();
        let store = Arc::new(InMemory::new());
        let object_store_url = ObjectStoreUrl::parse("memory://")?;
        context
            .runtime_env()
            .register_object_store(object_store_url.as_ref(), store.clone());

        let first = collect_partitioned(Arc::clone(&writer), context.task_ctx()).await?;
        assert_eq!(first.len(), 2);
        assert_eq!(output_row_count(&first)?, 6);
        let expected_paths = vec![
            staged_path("retry-write", 0, "part-00000-retry-write-c000.parquet"),
            staged_path("retry-write", 0, "part-00000-retry-write-c001.parquet"),
            staged_path("retry-write", 1, "part-00001-retry-write-c000.parquet"),
            staged_path("retry-write", 1, "part-00001-retry-write-c001.parquet"),
        ];
        assert_eq!(object_paths(&store).await?, expected_paths);

        let second = collect_partitioned(writer, context.task_ctx()).await?;
        assert_eq!(output_row_count(&second)?, 6);
        assert_eq!(object_paths(&store).await?, expected_paths);
        Ok(())
    }

    #[tokio::test]
    async fn writes_one_schema_file_for_empty_input() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let input = MemorySourceConfig::try_new_exec(
            &[Vec::<RecordBatch>::new(), Vec::new()],
            Arc::clone(&schema),
            None,
        )?;
        let mut config = sink_config(Arc::clone(&schema), vec![])?;
        config.file_extension = "snappy.parquet".to_string();
        let writer: Arc<dyn ExecutionPlan> = Arc::new(ParquetWriterExec::try_new_with_write_id(
            input,
            config,
            TableParquetOptions::default(),
            ParquetWriteExecutionOptions {
                minimum_parallel_output_files: 4,
                soft_max_rows_per_output_file: usize::MAX,
                max_records_per_file: None,
                max_buffered_batches_per_output_file: 2,
                objectstore_writer_buffer_size: 64,
            },
            None,
            "empty-write".to_string(),
        )?);
        let context = SessionContext::new();
        let store = Arc::new(InMemory::new());
        let object_store_url = ObjectStoreUrl::parse("memory://")?;
        context
            .runtime_env()
            .register_object_store(object_store_url.as_ref(), store.clone());

        let output = collect_partitioned(writer, context.task_ctx()).await?;
        assert_eq!(output_row_count(&output)?, 0);
        let paths = object_paths(&store).await?;
        assert_eq!(
            paths,
            vec![staged_path(
                "empty-write",
                0,
                "part-00000-empty-write-c000.snappy.parquet"
            )]
        );
        assert_eq!(parquet_row_count(&store, &paths[0]).await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn writes_one_schema_file_when_input_has_no_partitions() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(EmptyExec::new(Arc::clone(&schema)).with_partitions(0));
        let writer: Arc<dyn ExecutionPlan> = Arc::new(ParquetWriterExec::try_new_with_write_id(
            input,
            sink_config(schema, vec![])?,
            TableParquetOptions::default(),
            ParquetWriteExecutionOptions {
                minimum_parallel_output_files: 1,
                soft_max_rows_per_output_file: usize::MAX,
                max_records_per_file: None,
                max_buffered_batches_per_output_file: 2,
                objectstore_writer_buffer_size: 64,
            },
            None,
            "zero-partitions".to_string(),
        )?);
        assert_eq!(writer.output_partitioning().partition_count(), 1);
        let context = SessionContext::new();
        let store = Arc::new(InMemory::new());
        let object_store_url = ObjectStoreUrl::parse("memory://")?;
        context
            .runtime_env()
            .register_object_store(object_store_url.as_ref(), store.clone());

        let output = collect_partitioned(writer, context.task_ctx()).await?;
        assert_eq!(output_row_count(&output)?, 0);
        assert_eq!(
            object_paths(&store).await?,
            vec![staged_path(
                "zero-partitions",
                0,
                "part-00000-zero-partitions-c000.parquet"
            )]
        );
        Ok(())
    }

    #[tokio::test]
    async fn partitioned_empty_input_writes_no_data_file() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("bucket", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let input = MemorySourceConfig::try_new_exec(
            &[Vec::<RecordBatch>::new(), Vec::new()],
            Arc::clone(&schema),
            None,
        )?;
        let writer: Arc<dyn ExecutionPlan> = Arc::new(ParquetWriterExec::try_new_with_write_id(
            input,
            sink_config(schema, vec![("bucket".to_string(), DataType::Utf8)])?,
            TableParquetOptions::default(),
            ParquetWriteExecutionOptions {
                minimum_parallel_output_files: 1,
                soft_max_rows_per_output_file: usize::MAX,
                max_records_per_file: None,
                max_buffered_batches_per_output_file: 2,
                objectstore_writer_buffer_size: 64,
            },
            None,
            "empty-partitioned".to_string(),
        )?);
        let context = SessionContext::new();
        let store = Arc::new(InMemory::new());
        let object_store_url = ObjectStoreUrl::parse("memory://")?;
        context
            .runtime_env()
            .register_object_store(object_store_url.as_ref(), store.clone());

        let output = collect_partitioned(writer, context.task_ctx()).await?;
        assert_eq!(output_row_count(&output)?, 0);
        assert!(object_paths(&store).await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn enforces_max_records_per_file_across_record_batches() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let make_batch = |values: Vec<i64>| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(values)) as ArrayRef],
            )
        };
        let input = MemorySourceConfig::try_new_exec(
            &[vec![make_batch(vec![1, 2, 3])?, make_batch(vec![4, 5])?]],
            Arc::clone(&schema),
            None,
        )?;
        let writer: Arc<dyn ExecutionPlan> = Arc::new(ParquetWriterExec::try_new_with_write_id(
            input,
            sink_config(schema, vec![])?,
            TableParquetOptions::default(),
            ParquetWriteExecutionOptions {
                minimum_parallel_output_files: 4,
                soft_max_rows_per_output_file: usize::MAX,
                max_records_per_file: NonZeroUsize::new(2),
                max_buffered_batches_per_output_file: 2,
                objectstore_writer_buffer_size: 64,
            },
            None,
            "limited-write".to_string(),
        )?);
        let context = SessionContext::new();
        let store = Arc::new(InMemory::new());
        let object_store_url = ObjectStoreUrl::parse("memory://")?;
        context
            .runtime_env()
            .register_object_store(object_store_url.as_ref(), store.clone());

        let output = collect_partitioned(writer, context.task_ctx()).await?;
        assert_eq!(output_row_count(&output)?, 5);
        let paths = object_paths(&store).await?;
        assert_eq!(
            paths,
            vec![
                staged_path("limited-write", 0, "part-00000-limited-write-c000.parquet"),
                staged_path("limited-write", 0, "part-00000-limited-write-c001.parquet"),
                staged_path("limited-write", 0, "part-00000-limited-write-c002.parquet"),
            ]
        );
        let mut row_counts = Vec::new();
        for path in &paths {
            row_counts.push(parquet_row_count(&store, path).await?);
        }
        assert_eq!(row_counts, vec![2, 2, 1]);
        Ok(())
    }

    #[tokio::test]
    async fn enforces_max_records_per_file_within_each_hive_partition() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("bucket", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![
                    "a", "a", "a", "a", "a", "b", "b", "b",
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8])) as ArrayRef,
            ],
        )?;
        let input = MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?;
        let writer: Arc<dyn ExecutionPlan> = Arc::new(ParquetWriterExec::try_new_with_write_id(
            input,
            sink_config(schema, vec![("bucket".to_string(), DataType::Utf8)])?,
            TableParquetOptions::default(),
            ParquetWriteExecutionOptions {
                minimum_parallel_output_files: 4,
                soft_max_rows_per_output_file: usize::MAX,
                max_records_per_file: NonZeroUsize::new(2),
                max_buffered_batches_per_output_file: 2,
                objectstore_writer_buffer_size: 64,
            },
            None,
            "limited-partitioned-write".to_string(),
        )?);
        let context = SessionContext::new();
        let store = Arc::new(InMemory::new());
        let object_store_url = ObjectStoreUrl::parse("memory://")?;
        context
            .runtime_env()
            .register_object_store(object_store_url.as_ref(), store.clone());

        let output = collect_partitioned(writer, context.task_ctx()).await?;
        assert_eq!(output_row_count(&output)?, 8);
        let paths = object_paths(&store).await?;
        assert_eq!(
            paths,
            vec![
                staged_path(
                    "limited-partitioned-write",
                    0,
                    "bucket=a/part-00000-limited-partitioned-write.c000.parquet",
                ),
                staged_path(
                    "limited-partitioned-write",
                    0,
                    "bucket=a/part-00000-limited-partitioned-write.c001.parquet",
                ),
                staged_path(
                    "limited-partitioned-write",
                    0,
                    "bucket=a/part-00000-limited-partitioned-write.c002.parquet",
                ),
                staged_path(
                    "limited-partitioned-write",
                    0,
                    "bucket=b/part-00000-limited-partitioned-write.c000.parquet",
                ),
                staged_path(
                    "limited-partitioned-write",
                    0,
                    "bucket=b/part-00000-limited-partitioned-write.c001.parquet",
                ),
            ]
        );
        let mut total_rows = 0;
        for path in &paths {
            let row_count = parquet_row_count(&store, path).await?;
            assert!(row_count <= 2);
            total_rows += row_count;
        }
        assert_eq!(total_rows, 8);
        Ok(())
    }

    #[tokio::test]
    async fn routes_hive_partitions_and_maps_sort_metadata_to_file_schema() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("bucket", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let make_batch = |buckets: Vec<&str>, values: Vec<i64>| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(buckets)) as ArrayRef,
                    Arc::new(Int64Array::from(values)) as ArrayRef,
                ],
            )
        };
        let input = MemorySourceConfig::try_new_exec(
            &[
                vec![make_batch(vec!["a", "b"], vec![1, 2])?],
                vec![make_batch(vec!["a", "b"], vec![3, 4])?],
            ],
            Arc::clone(&schema),
            None,
        )?;
        let sort_options = SortOptions {
            descending: true,
            nulls_first: false,
        };
        let sort_order = LexRequirement::from([PhysicalSortRequirement::new(
            Arc::new(Column::new("value", 1)),
            Some(sort_options),
        )]);
        let writer: Arc<dyn ExecutionPlan> = Arc::new(ParquetWriterExec::try_new_with_write_id(
            input,
            sink_config(schema, vec![("bucket".to_string(), DataType::Utf8)])?,
            TableParquetOptions::default(),
            ParquetWriteExecutionOptions {
                minimum_parallel_output_files: 1,
                soft_max_rows_per_output_file: usize::MAX,
                max_records_per_file: None,
                max_buffered_batches_per_output_file: 2,
                objectstore_writer_buffer_size: 64,
            },
            Some(sort_order),
            "partitioned-write".to_string(),
        )?);
        let context = SessionContext::new();
        let store = Arc::new(InMemory::new());
        let object_store_url = ObjectStoreUrl::parse("memory://")?;
        context
            .runtime_env()
            .register_object_store(object_store_url.as_ref(), store.clone());

        let output = collect_partitioned(writer, context.task_ctx()).await?;
        assert_eq!(output_row_count(&output)?, 4);
        let paths = object_paths(&store).await?;
        assert_eq!(
            paths,
            vec![
                staged_path(
                    "partitioned-write",
                    0,
                    "bucket=a/part-00000-partitioned-write.c000.parquet",
                ),
                staged_path(
                    "partitioned-write",
                    0,
                    "bucket=b/part-00000-partitioned-write.c000.parquet",
                ),
                staged_path(
                    "partitioned-write",
                    1,
                    "bucket=a/part-00001-partitioned-write.c000.parquet",
                ),
                staged_path(
                    "partitioned-write",
                    1,
                    "bucket=b/part-00001-partitioned-write.c000.parquet",
                ),
            ]
        );
        for path in paths {
            let bytes = store
                .get(&Path::from(path))
                .await
                .map_err(|error| DataFusionError::ObjectStore(Box::new(error)))?
                .bytes()
                .await
                .map_err(|error| DataFusionError::ObjectStore(Box::new(error)))?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
            assert_eq!(reader.schema().fields().len(), 1);
            assert_eq!(reader.schema().field(0).name(), "value");
            let sorting_columns = reader
                .metadata()
                .row_group(0)
                .sorting_columns()
                .ok_or_else(|| {
                    DataFusionError::Execution("expected Parquet sorting metadata".to_string())
                })?;
            assert_eq!(
                sorting_columns,
                &[SortingColumn {
                    column_idx: 0,
                    descending: true,
                    nulls_first: false,
                }]
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn writes_spark_compatible_hive_partition_paths() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("bucket", DataType::Utf8, true),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![
                    Some(""),
                    None,
                    Some("a/b"),
                    Some("a=b"),
                    Some("雪"),
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
            ],
        )?;
        let input = MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?;
        let writer: Arc<dyn ExecutionPlan> = Arc::new(ParquetWriterExec::try_new_with_write_id(
            input,
            sink_config(schema, vec![("bucket".to_string(), DataType::Utf8)])?,
            TableParquetOptions::default(),
            ParquetWriteExecutionOptions {
                minimum_parallel_output_files: 1,
                soft_max_rows_per_output_file: usize::MAX,
                max_records_per_file: None,
                max_buffered_batches_per_output_file: 2,
                objectstore_writer_buffer_size: 64,
            },
            None,
            "partitioned-write".to_string(),
        )?);
        let context = SessionContext::new();
        let store = Arc::new(InMemory::new());
        let object_store_url = ObjectStoreUrl::parse("memory://")?;
        context
            .runtime_env()
            .register_object_store(object_store_url.as_ref(), store.clone());

        let output = collect_partitioned(writer, context.task_ctx()).await?;
        assert_eq!(output_row_count(&output)?, 5);
        assert_eq!(
            object_paths(&store).await?,
            vec![
                staged_path(
                    "partitioned-write",
                    0,
                    "bucket=__HIVE_DEFAULT_PARTITION__/part-00000-partitioned-write.c000.parquet",
                ),
                staged_path(
                    "partitioned-write",
                    0,
                    "bucket=a%2Fb/part-00000-partitioned-write.c000.parquet",
                ),
                staged_path(
                    "partitioned-write",
                    0,
                    "bucket=a%3Db/part-00000-partitioned-write.c000.parquet",
                ),
                staged_path(
                    "partitioned-write",
                    0,
                    "bucket=雪/part-00000-partitioned-write.c000.parquet",
                ),
            ]
        );
        Ok(())
    }
}
