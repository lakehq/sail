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
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
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
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream, execute_input_stream,
};
use datafusion_common::{DataFusionError, Result, internal_err, not_impl_err, plan_datafusion_err};
use datafusion_datasource::file_sink_config::{FileOutputMode, FileSink, FileSinkConfig};
use datafusion_datasource_parquet::ParquetSink;
use futures::stream;
use parquet::file::metadata::SortingColumn;
use uuid::Uuid;

use crate::demux::start_demuxer_task;

/// DataFusion execution settings that affect the physical shape and buffering
/// of a Parquet write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetWriteExecutionOptions {
    pub minimum_parallel_output_files: usize,
    pub soft_max_rows_per_output_file: usize,
    pub max_buffered_batches_per_output_file: usize,
    pub objectstore_writer_buffer_size: usize,
}

impl From<&ExecutionOptions> for ParquetWriteExecutionOptions {
    fn from(options: &ExecutionOptions) -> Self {
        Self {
            minimum_parallel_output_files: options.minimum_parallel_output_files,
            soft_max_rows_per_output_file: options.soft_max_rows_per_output_file,
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
            Uuid::new_v4().simple().to_string(),
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
        let schema = count_schema();
        let partition_count = input.output_partitioning().partition_count();
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
        let partition_count = self.input.output_partitioning().partition_count();
        if partition >= partition_count {
            return internal_err!(
                "ParquetWriterExec invalid partition {partition} (input partitions: {partition_count})"
            );
        }
        let input = execute_input_stream(
            Arc::clone(&self.input),
            Arc::clone(self.sink_config.output_schema()),
            partition,
            Arc::clone(&context),
        )?;
        let sorting_columns = self.sort_order.as_ref().and_then(|requirements| {
            let ordering: LexOrdering = requirements.clone().into();
            lex_ordering_to_sorting_columns(&ordering, &self.sink_config).ok()
        });
        let sink = Arc::new(
            ParquetSink::new(self.sink_config.clone(), self.parquet_options.clone())
                .with_sorting_columns(sorting_columns),
        );
        let writer_context = self.writer_context(&context);
        let object_store = writer_context
            .runtime_env()
            .object_store(&self.sink_config.object_store_url)?;
        let write_id = format!("{}-{partition:05}", self.write_id);
        let (demux_task, file_streams) =
            start_demuxer_task(&self.sink_config, input, &writer_context, write_id)?;
        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);
        let output_bytes = MetricBuilder::new(&self.metrics).output_bytes(partition);
        let elapsed_compute = MetricBuilder::new(&self.metrics).elapsed_compute(partition);
        let schema = Arc::clone(&self.schema);
        let output = stream::once(async move {
            let _timer = elapsed_compute.timer();
            let rows = sink
                .spawn_writer_tasks_and_join(
                    &writer_context,
                    demux_task,
                    file_streams,
                    object_store,
                )
                .await?;
            output_rows.add(usize::try_from(rows).map_err(|_| {
                DataFusionError::Execution("Parquet row count is too large".to_string())
            })?);
            let bytes = sink
                .written()
                .values()
                .flat_map(|metadata| metadata.row_groups())
                .try_fold(0_usize, |total, row_group| {
                    let size = usize::try_from(row_group.compressed_size()).map_err(|_| {
                        DataFusionError::Execution(
                            "Parquet compressed byte count is too large".to_string(),
                        )
                    })?;
                    total.checked_add(size).ok_or_else(|| {
                        DataFusionError::Execution(
                            "Parquet compressed byte count overflow".to_string(),
                        )
                    })
                })?;
            output_bytes.add(bytes);
            count_batch(rows)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, output)))
    }
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

fn count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

fn count_batch(count: u64) -> Result<RecordBatch> {
    let values = Arc::new(UInt64Array::from(vec![count])) as ArrayRef;
    Ok(RecordBatch::try_from_iter_with_nullable(vec![(
        "count", values, false,
    )])?)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray, UInt64Array};
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
    use datafusion::physical_plan::{ExecutionPlan, collect_partitioned, displayable};
    use datafusion::prelude::SessionContext;
    use datafusion_common::{DataFusionError, Result};
    use datafusion_datasource::ListingTableUrl;
    use futures::TryStreamExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::metadata::SortingColumn;

    use super::{ParquetWriteExecutionOptions, ParquetWriterExec};

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

    fn output_row_count(partitions: &[Vec<RecordBatch>]) -> Result<u64> {
        partitions.iter().flatten().try_fold(0_u64, |total, batch| {
            let counts = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "Parquet writer returned an invalid count column".to_string(),
                    )
                })?;
            total.checked_add(counts.value(0)).ok_or_else(|| {
                DataFusionError::Execution("Parquet output row count overflow".to_string())
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
            "output/retry-write-00000_0.parquet".to_string(),
            "output/retry-write-00000_1.parquet".to_string(),
            "output/retry-write-00001_0.parquet".to_string(),
            "output/retry-write-00001_1.parquet".to_string(),
        ];
        assert_eq!(object_paths(&store).await?, expected_paths);

        let second = collect_partitioned(writer, context.task_ctx()).await?;
        assert_eq!(output_row_count(&second)?, 6);
        assert_eq!(object_paths(&store).await?, expected_paths);
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
                "output/bucket=a/partitioned-write-00000.parquet".to_string(),
                "output/bucket=a/partitioned-write-00001.parquet".to_string(),
                "output/bucket=b/partitioned-write-00000.parquet".to_string(),
                "output/bucket=b/partitioned-write-00001.parquet".to_string(),
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
}
