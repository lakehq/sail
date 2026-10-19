use std::fmt;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::{CastOptions, cast_with_options};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::util::display::array_value_to_string;
use datafusion::common::config::TableParquetOptions;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    EquivalenceProperties, LexOrdering, OrderingRequirements, Partitioning, PhysicalExpr,
    PhysicalSortExpr,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{Result, ScalarValue, internal_err, plan_datafusion_err, plan_err};
use datafusion_datasource::file_sink_config::FileSinkConfig;
use futures::{StreamExt, TryStreamExt};
use object_store::path::{Path, PathPart};
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::ArrowSchemaConverter;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::file::metadata::SortingColumn;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use sail_parquet::{ParquetObjectWriter, ParquetWriter};
use uuid::Uuid;

use crate::listing::partition::HIVE_DEFAULT_PARTITION;

/// Writes each execution partition independently. Hive partition columns are
/// sorted locally so a task needs only one open file, regardless of cardinality.
#[derive(Debug)]
pub struct ParquetWriterExec {
    input: Arc<dyn ExecutionPlan>,
    config: FileSinkConfig,
    options: TableParquetOptions,
    input_ordering: Option<LexOrdering>,
    max_rows_per_file: usize,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl ParquetWriterExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        config: FileSinkConfig,
        options: TableParquetOptions,
        input_ordering: Option<LexOrdering>,
        max_rows_per_file: usize,
    ) -> Result<Self> {
        if config.insert_op != InsertOp::Append || config.table_paths.len() != 1 {
            return plan_err!("ParquetWriterExec requires one append destination");
        }
        if config
            .file_output_mode
            .single_file_output(&config.table_paths[0])
        {
            return plan_err!("ParquetWriterExec requires a directory destination");
        }
        let input_schema = input.schema();
        // Remote expressions may lose field metadata. Preserve the sink's
        // metadata while requiring the same column structure and nullability.
        if config.output_schema.fields().len() != input_schema.fields().len()
            || config
                .output_schema
                .fields()
                .iter()
                .zip(input_schema.fields())
                .any(|(output, input)| {
                    output.name() != input.name()
                        || output.data_type() != input.data_type()
                        || output.is_nullable() != input.is_nullable()
                })
        {
            return plan_err!("Parquet writer schema does not match its input");
        }
        if max_rows_per_file == 0 {
            return plan_err!("Parquet file row limit must be positive");
        }
        if matches!(input.boundedness(), Boundedness::Unbounded { .. }) {
            return plan_err!("ParquetWriterExec requires bounded input");
        }
        let schema = &config.output_schema;
        for (name, _) in &config.table_partition_cols {
            let data_type = schema.field_with_name(name)?.data_type();
            if !supports_partition_type(data_type) {
                return plan_err!("Unsupported Parquet partition type for {name}: {data_type}");
            }
        }
        if schema.fields().iter().all(|field| {
            !config.keep_partition_by_columns
                && config
                    .table_partition_cols
                    .iter()
                    .any(|(name, _)| name == field.name())
        }) {
            return plan_err!("Parquet output requires at least one non-partition column");
        }
        let mut ordering = config
            .table_partition_cols
            .iter()
            .map(|(name, _)| {
                Ok(PhysicalSortExpr::new_default(Arc::new(Column::new(
                    name,
                    schema.index_of(name)?,
                ))))
            })
            .collect::<Result<Vec<_>>>()?;
        if let Some(input_ordering) = input_ordering {
            for expr in input_ordering {
                if !ordering.iter().any(|existing| existing.expr.eq(&expr.expr)) {
                    ordering.push(expr);
                }
            }
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::new(Schema::empty())),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            config,
            options,
            input_ordering: LexOrdering::new(ordering),
            max_rows_per_file,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
    pub fn config(&self) -> &FileSinkConfig {
        &self.config
    }
    pub fn options(&self) -> &TableParquetOptions {
        &self.options
    }
    pub fn input_ordering(&self) -> Option<&LexOrdering> {
        self.input_ordering.as_ref()
    }
    pub fn max_rows_per_file(&self) -> usize {
        self.max_rows_per_file
    }
}

impl DisplayAs for ParquetWriterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ParquetWriterExec: destination={}, partition_by={:?}, max_rows_per_file={}",
            self.config.original_url,
            self.config
                .table_partition_cols
                .iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>(),
            self.max_rows_per_file
        )
    }
}

impl ExecutionPlan for ParquetWriterExec {
    fn name(&self) -> &str {
        Self::static_name()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![self.input_ordering.clone().map(Into::into)]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        if let Some(ordering) = &self.input_ordering {
            for expr in ordering {
                if f(&expr.expr)? == TreeNodeRecursion::Stop {
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }

    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return internal_err!("ParquetWriterExec requires exactly one child");
        };
        Ok(Arc::new(Self::try_new(
            input.clone(),
            self.config.clone(),
            self.options.clone(),
            self.input_ordering.clone(),
            self.max_rows_per_file,
        )?))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context.clone())?;
        let store = context
            .runtime_env()
            .object_store(&self.config.object_store_url)?;
        let writer = PartitionWriter::try_new(self, partition, context, store)?;
        let output = futures::stream::once(async move {
            writer.write_all(input).await?;
            Ok::<_, datafusion_common::DataFusionError>(futures::stream::empty())
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            output,
        )))
    }
}

struct PartitionWriter {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    file_extension: String,
    write_id: String,
    partition: usize,
    partition_columns: Vec<(String, usize)>,
    projection: Vec<usize>,
    file_schema: SchemaRef,
    properties: WriterProperties,
    skip_arrow_metadata: bool,
    buffer_size: usize,
    batch_size: usize,
    max_rows: usize,
    active: Option<ParquetWriter<ParquetObjectWriter>>,
    current_key: Option<Vec<ScalarValue>>,
    current_rows: usize,
    files: WrittenFiles,
    reservation: MemoryReservation,
    rows_written: Count,
    bytes_written: Count,
    files_written: Count,
}

impl PartitionWriter {
    fn try_new(
        exec: &ParquetWriterExec,
        partition: usize,
        context: Arc<TaskContext>,
        store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let schema = &exec.config.output_schema;
        let partition_columns = exec
            .config
            .table_partition_cols
            .iter()
            .map(|(name, _)| Ok((name.clone(), schema.index_of(name)?)))
            .collect::<Result<Vec<_>>>()?;
        let projection = (0..schema.fields().len())
            .filter(|index| {
                exec.config.keep_partition_by_columns
                    || !partition_columns.iter().any(|(_, i)| index == i)
            })
            .collect::<Vec<_>>();
        let file_schema = Arc::new(schema.project(&projection)?);
        let mut options = exec.options.clone();
        if !options.global.skip_arrow_metadata {
            options.arrow_schema(&file_schema);
        }
        let mut properties = WriterPropertiesBuilder::try_from(&options)?;
        if let Some(ordering) = &exec.input_ordering {
            let descriptor = ArrowSchemaConverter::new().convert(&file_schema)?;
            let mut sorting_columns = Vec::new();
            for expr in ordering {
                let Some(column) = expr.expr.downcast_ref::<Column>() else {
                    break;
                };
                if !projection.contains(&column.index()) {
                    continue;
                }
                let Some(index) = descriptor
                    .columns()
                    .iter()
                    .position(|leaf| leaf.path().parts() == [column.name()])
                else {
                    break;
                };
                sorting_columns.push(SortingColumn {
                    column_idx: i32::try_from(index)
                        .map_err(|e| plan_datafusion_err!("invalid Parquet column index: {e}"))?,
                    descending: expr.options.descending,
                    nulls_first: expr.options.nulls_first,
                });
            }
            if !sorting_columns.is_empty() {
                properties = properties.set_sorting_columns(Some(sorting_columns));
            }
        }
        let execution = &context.session_config().options().execution;
        Ok(Self {
            store: store.clone(),
            prefix: exec.config.table_paths[0].prefix().clone(),
            file_extension: exec.config.file_extension.clone(),
            write_id: Uuid::new_v4().to_string(),
            partition,
            partition_columns,
            projection,
            file_schema,
            properties: properties.build(),
            skip_arrow_metadata: exec.options.global.skip_arrow_metadata,
            buffer_size: execution.objectstore_writer_buffer_size.max(1),
            batch_size: execution.batch_size.get(),
            max_rows: exec.max_rows_per_file,
            active: None,
            current_key: None,
            current_rows: 0,
            files: WrittenFiles {
                store,
                paths: Vec::new(),
                committed: false,
            },
            reservation: MemoryConsumer::new(format!("ParquetWriterExec[{partition}]"))
                .register(context.memory_pool()),
            rows_written: MetricBuilder::new(&exec.metrics).counter("rows_written", partition),
            bytes_written: MetricBuilder::new(&exec.metrics).counter("bytes_written", partition),
            files_written: MetricBuilder::new(&exec.metrics).counter("files_written", partition),
        })
    }

    async fn write_all(mut self, mut input: SendableRecordBatchStream) -> Result<()> {
        while let Some(batch) = input.next().await.transpose()? {
            if batch.num_rows() == 0 {
                continue;
            }
            let columns = self
                .partition_columns
                .iter()
                .map(|(_, index)| batch.column(*index).clone())
                .collect::<Vec<_>>();
            let ranges = if columns.is_empty() {
                std::iter::once(0..batch.num_rows()).collect()
            } else {
                arrow::compute::partition(&columns)?.ranges()
            };
            for range in ranges {
                let key = columns
                    .iter()
                    .map(|column| ScalarValue::try_from_array(column, range.start))
                    .collect::<Result<Vec<_>>>()?;
                if self.current_key.as_ref() != Some(&key) {
                    self.finish_file().await?;
                    self.current_key = Some(key);
                }
                let mut directory = self.prefix.clone();
                for (name, index) in &self.partition_columns {
                    let column = batch.column(*index);
                    let value = if column.is_null(range.start) {
                        String::new()
                    } else if partition_value_type(column.data_type()).is_binary() {
                        let options = CastOptions {
                            safe: false,
                            ..Default::default()
                        };
                        let binary = cast_with_options(
                            &column.slice(range.start, 1),
                            &DataType::Binary,
                            &options,
                        )?;
                        let value = cast_with_options(&binary, &DataType::Utf8, &options)?;
                        array_value_to_string(value.as_ref(), 0)?
                    } else {
                        array_value_to_string(column.as_ref(), range.start)?
                    };
                    let value = if value.is_empty() {
                        HIVE_DEFAULT_PARTITION
                    } else {
                        &value
                    };
                    let segment = format!("{}={}", escape_partition(name), escape_partition(value));
                    directory = directory.join(PathPart::parse(&segment).map_err(|e| {
                        plan_datafusion_err!("invalid Parquet partition path: {e}")
                    })?);
                }
                let projected = batch
                    .slice(range.start, range.len())
                    .project(&self.projection)?;
                let projected =
                    RecordBatch::try_new(self.file_schema.clone(), projected.columns().to_vec())?;
                let mut offset = 0;
                while offset < projected.num_rows() {
                    if self.active.is_none() {
                        self.start_file(&directory)?;
                    }
                    let len = (projected.num_rows() - offset)
                        .min(self.max_rows - self.current_rows)
                        .min(self.batch_size);
                    if let Some(writer) = &mut self.active {
                        writer.write(&projected.slice(offset, len)).await?;
                        if self
                            .reservation
                            .try_resize(writer.memory_size().saturating_add(self.buffer_size))
                            .is_err()
                        {
                            writer.flush().await?;
                            self.reservation.try_resize(
                                writer.memory_size().saturating_add(self.buffer_size),
                            )?;
                        }
                    }
                    self.current_rows += len;
                    offset += len;
                    if self.current_rows == self.max_rows {
                        self.finish_file().await?;
                    }
                }
            }
        }
        // Spark leaves a schema-bearing file from partition zero for empty
        // unpartitioned input. Other empty tasks and Hive partitions create none.
        if self.files.paths.is_empty() && self.partition == 0 && self.partition_columns.is_empty() {
            self.start_file(&self.prefix.clone())?;
        }
        self.finish_file().await?;
        self.files.committed = true;
        Ok(())
    }

    fn start_file(&mut self, directory: &Path) -> Result<()> {
        // Reserve the upload buffer separately from the Parquet encoder. A
        // row-group flush releases encoder memory but may leave buffered bytes.
        self.reservation.try_resize(self.buffer_size)?;
        let path = directory.clone().join(format!(
            "part-{:05}-{}-c{:03}.{}",
            self.partition,
            self.write_id,
            self.files.paths.len(),
            self.file_extension
        ));
        let output = ParquetObjectWriter::new(self.store.clone(), path.clone(), self.buffer_size);
        self.files.paths.push(path);
        self.active = Some(ParquetWriter::try_new(
            output,
            self.file_schema.clone(),
            ArrowWriterOptions::new()
                .with_properties(self.properties.clone())
                .with_skip_arrow_metadata(self.skip_arrow_metadata),
        )?);
        self.current_rows = 0;
        Ok(())
    }

    async fn finish_file(&mut self) -> Result<()> {
        if let Some(writer) = self.active.take() {
            let (_, metadata) = writer.finish().await?;
            self.rows_written.add(metadata.num_rows as usize);
            self.bytes_written.add(metadata.file_size as usize);
            self.files_written.add(1);
        }
        self.current_rows = 0;
        self.reservation.free();
        Ok(())
    }
}

fn partition_value_type(data_type: &DataType) -> &DataType {
    match data_type {
        DataType::Dictionary(_, value) => partition_value_type(value),
        value => value,
    }
}

fn supports_partition_type(data_type: &DataType) -> bool {
    matches!(
        partition_value_type(data_type),
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
    )
}

// Match Spark/Hive directory escaping, including literal '%' and '='.
const PARTITION_ESCAPE: &AsciiSet = &CONTROLS
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'\'')
    .add(b'*')
    .add(b'/')
    .add(b':')
    .add(b'=')
    .add(b'?')
    .add(b'\\')
    .add(b'{')
    .add(b'[')
    .add(b']')
    .add(b'^');

fn escape_partition(value: &str) -> String {
    utf8_percent_encode(value, PARTITION_ESCAPE).to_string()
}

/// Completed files belong to this task until it has consumed all of its input.
struct WrittenFiles {
    store: Arc<dyn ObjectStore>,
    paths: Vec<Path>,
    committed: bool,
}

impl Drop for WrittenFiles {
    fn drop(&mut self) {
        if !self.committed
            && !self.paths.is_empty()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            let store = self.store.clone();
            let paths = std::mem::take(&mut self.paths);
            runtime.spawn(async move {
                for path in paths {
                    if let Err(error) = store.delete(&path).await
                        && !matches!(error, object_store::Error::NotFound { .. })
                    {
                        log::warn!("Failed to remove abandoned Parquet file {path}: {error}");
                    }
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field};
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use datafusion_datasource::ListingTableUrl;
    use datafusion_datasource::file_sink_config::FileOutputMode;
    use datafusion_datasource::memory::MemorySourceConfig;
    use object_store::memory::InMemory;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;

    fn make_writer(
        batch: &RecordBatch,
        partitioned: bool,
    ) -> Result<(Arc<ParquetWriterExec>, Arc<TaskContext>, Arc<InMemory>)> {
        let ctx = SessionContext::new();
        let store = Arc::new(InMemory::new());
        let url = ListingTableUrl::parse("memory://test/output/")?;
        ctx.runtime_env()
            .register_object_store(url.object_store().as_ref(), store.clone());
        let config = FileSinkConfig {
            original_url: url.to_string(),
            object_store_url: ObjectStoreUrl::parse("memory://test")?,
            file_group: Default::default(),
            table_paths: vec![url],
            output_schema: batch.schema(),
            table_partition_cols: if partitioned {
                vec![("part".into(), DataType::Utf8)]
            } else {
                vec![]
            },
            insert_op: InsertOp::Append,
            keep_partition_by_columns: false,
            file_extension: "parquet".into(),
            file_output_mode: FileOutputMode::Directory,
        };
        let input = MemorySourceConfig::try_new_exec(
            &[vec![
                batch.slice(0, 5),
                batch.slice(5, batch.num_rows() - 5),
            ]],
            batch.schema(),
            None,
        )?;
        let writer = ParquetWriterExec::try_new(input, config, Default::default(), None, 3)?;
        Ok((Arc::new(writer), ctx.task_ctx(), store))
    }

    #[tokio::test]
    async fn rolls_files_inside_batches_and_across_partition_boundaries() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("part", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from_iter_values(0..11)),
                Arc::new(StringArray::from(vec![
                    "a", "a", "a", "a", "a", "a", "a", "b", "b", "b", "b",
                ])),
            ],
        )?;
        for partitioned in [false, true] {
            let (writer, context, store) = make_writer(&batch, partitioned)?;
            assert!(collect(writer, context).await?.is_empty());
            let objects = store.list(None).try_collect::<Vec<_>>().await?;
            let mut counts = Vec::new();
            let mut values = Vec::new();
            for object in objects {
                let bytes = store.get(&object.location).await?.bytes().await?;
                let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
                counts.push(builder.metadata().file_metadata().num_rows());
                assert_eq!(
                    builder.schema().fields().len(),
                    if partitioned { 1 } else { 2 }
                );
                let batches = builder
                    .build()?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                for batch in batches {
                    let ids = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| plan_datafusion_err!("expected Int64 output"))?;
                    values.extend(ids.values().iter().copied());
                }
            }
            counts.sort_unstable();
            values.sort_unstable();
            assert_eq!(
                counts,
                if partitioned {
                    vec![1, 1, 3, 3, 3]
                } else {
                    vec![2, 3, 3, 3]
                }
            );
            assert_eq!(values, (0..11).collect::<Vec<_>>());
        }
        Ok(())
    }

    #[tokio::test]
    async fn accounts_for_upload_buffer_after_row_group_flush()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let batch = RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(Int64Array::from_iter_values(0..9)) as arrow::array::ArrayRef,
        )])?;
        let (exec, context, store) = make_writer(&batch, false)?;
        let mut options = TableParquetOptions::default();
        options.global.max_row_group_size = 1;
        let exec = ParquetWriterExec::try_new(
            exec.input.clone(),
            exec.config.clone(),
            options,
            None,
            100,
        )?;
        let pool = context.memory_pool().clone();
        let writer = PartitionWriter::try_new(&exec, 0, context, store)?;
        let buffer_size = writer.buffer_size;
        let input_pool = pool.clone();
        let stream = RecordBatchStreamAdapter::new(
            batch.schema(),
            futures::stream::iter([Ok(batch)]).chain(futures::stream::once(async move {
                assert_eq!(input_pool.reserved(), buffer_size);
                Err(datafusion_common::exec_datafusion_err!(
                    "injected input failure"
                ))
            })),
        );
        assert!(writer.write_all(Box::pin(stream)).await.is_err());
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn preserves_sink_metadata_when_input_metadata_is_missing()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let batch = RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(Int64Array::from_iter_values(0..9)) as arrow::array::ArrayRef,
        )])?;
        let (exec, context, store) = make_writer(&batch, false)?;
        let mut config = exec.config.clone();
        let schema =
            Arc::new(Schema::new_with_metadata(
                vec![Field::new("id", DataType::Int64, false).with_metadata(
                    [(PARQUET_FIELD_ID_META_KEY.to_string(), "7".to_string())].into(),
                )],
                [("source".to_string(), "sink".to_string())].into(),
            ));
        config.output_schema = schema.clone();
        let plan =
            ParquetWriterExec::try_new(exec.input.clone(), config, Default::default(), None, 3)?;
        collect(Arc::new(plan), context).await?;
        let files = store.list(None).try_collect::<Vec<_>>().await?;
        assert_eq!(files.len(), 3);
        for file in files {
            let bytes = store.get(&file.location).await?.bytes().await?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
            assert_eq!(reader.schema(), &schema);
        }
        Ok(())
    }

    #[tokio::test]
    async fn input_error_removes_files_already_rolled()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let batch = RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(Int64Array::from_iter_values(0..9)) as arrow::array::ArrayRef,
        )])?;
        let (exec, context, store) = make_writer(&batch, false)?;
        let writer = PartitionWriter::try_new(&exec, 0, context, store.clone())?;
        let stream = RecordBatchStreamAdapter::new(
            batch.schema(),
            futures::stream::iter([
                Ok(batch),
                Err(datafusion_common::exec_datafusion_err!(
                    "injected input failure"
                )),
            ]),
        );
        assert!(writer.write_all(Box::pin(stream)).await.is_err());
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if store.list(None).try_collect::<Vec<_>>().await?.is_empty() {
                    break Ok::<_, object_store::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await??;
        Ok(())
    }
}
