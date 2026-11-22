// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::parquet::file::metadata::SortingColumn;
use datafusion::parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use datafusion::physical_expr_common::sort_expr::LexOrdering;
use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder, Time};
use datafusion_common::Result;
use datafusion_common::config::TableParquetOptions;
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::path::Path;
use uuid::Uuid;

use crate::formats::parquet::writer::file::ParquetFileWriter;
use crate::formats::parquet::writer::partition::{PartitionRouter, PartitionedWriter};
use crate::formats::parquet::writer::sorting::lex_ordering_to_sorting_columns;
use crate::listing::commit::{WrittenFile, manifest_batch};
use crate::listing::part_file::part_file_name;

/// The description of the work of one Parquet writer task.
pub struct ParquetWriteTask {
    pub store: Arc<dyn ObjectStore>,
    /// The root directory of the table.
    pub table_path: Path,
    pub write_id: Uuid,
    /// The index of the writer partition of the task.
    pub partition: usize,
    /// The number of writer partitions.
    pub partitions: usize,
    pub attempt: usize,
    /// The file name suffix such as `.zstd.parquet`.
    pub file_suffix: String,
    /// The columns whose values determine the directory of each row.
    /// These columns are not written to the files.
    pub partition_columns: Vec<String>,
    pub options: TableParquetOptions,
    /// The ordering of the input to record in the file metadata.
    pub sorting: Option<LexOrdering>,
    /// The capacity of the upload buffer of each file.
    pub buffer_size: usize,
    /// The number of rows after which the task continues with a new file.
    /// The limit is checked when a row group is completed,
    /// so a file can have more rows than this.
    pub max_rows_per_file: usize,
    /// The number of files that the task keeps open at the same time
    /// when the data has partition columns.
    pub max_open_files: usize,
}

/// The metrics of one Parquet writer task.
pub struct ParquetWriteMetrics {
    rows_written: Count,
    bytes_written: Count,
    files_written: Count,
    elapsed_compute: Time,
}

impl ParquetWriteMetrics {
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            rows_written: MetricBuilder::new(metrics).counter("rows_written", partition),
            bytes_written: MetricBuilder::new(metrics).counter("bytes_written", partition),
            files_written: MetricBuilder::new(metrics).counter("files_written", partition),
            elapsed_compute: MetricBuilder::new(metrics).elapsed_compute(partition),
        }
    }

    fn record(&self, file: &WrittenFile) {
        self.rows_written.add(file.num_rows as usize);
        self.bytes_written.add(file.size as usize);
        self.files_written.add(1);
    }
}

/// Creates the files of a writer task. All files have the same schema.
pub(super) struct FileFactory {
    store: Arc<dyn ObjectStore>,
    write_id: Uuid,
    partition: usize,
    attempt: usize,
    file_suffix: String,
    schema: SchemaRef,
    props: WriterProperties,
    skip_arrow_metadata: bool,
    buffer_size: usize,
    max_rows_per_file: u64,
}

impl FileFactory {
    fn try_new(
        task: ParquetWriteTask,
        input_schema: &SchemaRef,
        schema: SchemaRef,
    ) -> Result<Self> {
        let mut options = task.options;
        let skip_arrow_metadata = options.global.skip_arrow_metadata;
        if !skip_arrow_metadata {
            options.arrow_schema(&schema);
        }
        let mut builder = WriterPropertiesBuilder::try_from(&options)?;
        if let Some(sorting) = task
            .sorting
            .as_ref()
            .and_then(|ordering| sorting_columns(ordering, input_schema, &schema))
        {
            builder = builder.set_sorting_columns(Some(sorting));
        }
        Ok(Self {
            store: task.store,
            write_id: task.write_id,
            partition: task.partition,
            attempt: task.attempt,
            file_suffix: task.file_suffix,
            schema,
            props: builder.build(),
            skip_arrow_metadata,
            buffer_size: task.buffer_size,
            max_rows_per_file: task.max_rows_per_file as u64,
        })
    }

    fn create(&self, directory: &Path, counter: u32) -> Result<ParquetFileWriter> {
        let name = part_file_name(
            self.partition,
            &self.write_id,
            counter,
            self.attempt,
            &self.file_suffix,
        );
        ParquetFileWriter::try_new(
            Arc::clone(&self.store),
            directory.clone().join(name),
            Arc::clone(&self.schema),
            self.props.clone(),
            self.skip_arrow_metadata,
            self.buffer_size,
        )
    }
}

/// An ordering that cannot be expressed as Parquet sorting columns
/// (e.g. an ordering on an expression) is not recorded in the file.
fn sorting_columns(
    ordering: &LexOrdering,
    input_schema: &SchemaRef,
    schema: &SchemaRef,
) -> Option<Vec<SortingColumn>> {
    lex_ordering_to_sorting_columns(ordering, input_schema, schema)
        .ok()
        .filter(|columns| !columns.is_empty())
}

/// Writes the files of a writer task in one directory.
/// At most one file is open at a time.
pub(super) struct DirectoryWriter {
    directory: Path,
    writer: Option<Box<ParquetFileWriter>>,
    next_counter: u32,
}

impl DirectoryWriter {
    pub fn new(directory: Path) -> Self {
        Self {
            directory,
            writer: None,
            next_counter: 0,
        }
    }

    fn open(&mut self, factory: &FileFactory) -> Result<&mut ParquetFileWriter> {
        match &mut self.writer {
            Some(writer) => Ok(writer),
            writer @ None => {
                let file = factory.create(&self.directory, self.next_counter)?;
                self.next_counter += 1;
                Ok(writer.insert(Box::new(file)))
            }
        }
    }

    pub async fn write(
        &mut self,
        factory: &FileFactory,
        batch: &RecordBatch,
        written: &mut Vec<WrittenFile>,
    ) -> Result<()> {
        let writer = self.open(factory)?;
        let flushed = writer.write(batch).await?;
        if flushed && writer.rows() >= factory.max_rows_per_file {
            self.close(written).await?;
        }
        Ok(())
    }

    pub fn memory_size(&self) -> usize {
        self.writer.as_ref().map_or(0, |x| x.memory_size())
    }

    pub async fn close(&mut self, written: &mut Vec<WrittenFile>) -> Result<()> {
        if let Some(writer) = self.writer.take() {
            written.push(writer.close().await?);
        }
        Ok(())
    }
}

/// Returns whether the data type is or contains a union type,
/// which is the type that has no Parquet schema in the Arrow writer.
fn contains_union(data_type: &DataType) -> bool {
    match data_type {
        DataType::Union(_, _) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _)
        | DataType::RunEndEncoded(_, field) => contains_union(field.data_type()),
        DataType::Struct(fields) => fields.iter().any(|field| contains_union(field.data_type())),
        DataType::Dictionary(_, value) => contains_union(value),
        _ => false,
    }
}

/// The files that a writer task is writing.
enum TaskOutput {
    Directory(DirectoryWriter),
    Partitioned(Box<PartitionedWriter>),
}

impl TaskOutput {
    async fn write(
        &mut self,
        factory: &FileFactory,
        batch: &RecordBatch,
        written: &mut Vec<WrittenFile>,
    ) -> Result<()> {
        match self {
            Self::Directory(x) => x.write(factory, batch, written).await,
            Self::Partitioned(x) => x.write(factory, batch, written).await,
        }
    }

    fn memory_size(&self) -> usize {
        match self {
            Self::Directory(x) => x.memory_size(),
            Self::Partitioned(x) => x.memory_size(),
        }
    }

    async fn close(&mut self, written: &mut Vec<WrittenFile>) -> Result<()> {
        match self {
            Self::Directory(x) => x.close(written).await,
            Self::Partitioned(x) => x.close(written).await,
        }
    }
}

/// Writes one input partition and returns the manifest of the task.
pub async fn write_partition(
    task: ParquetWriteTask,
    mut input: SendableRecordBatchStream,
    reservation: MemoryReservation,
    metrics: ParquetWriteMetrics,
) -> Result<RecordBatch> {
    let partition = task.partition;
    let partitions = task.partitions;
    let attempt = task.attempt;
    let input_schema = input.schema();
    let (mut output, schema) = if task.partition_columns.is_empty() {
        let output = DirectoryWriter::new(task.table_path.clone());
        (TaskOutput::Directory(output), Arc::clone(&input_schema))
    } else {
        let router = PartitionRouter::try_new(&input_schema, &task.partition_columns)?;
        let schema = Arc::clone(router.file_schema());
        let output = PartitionedWriter::new(router, task.table_path.clone(), task.max_open_files);
        (TaskOutput::Partitioned(Box::new(output)), schema)
    };
    let factory = FileFactory::try_new(task, &input_schema, schema)?;
    let mut written = vec![];
    while let Some(batch) = input.next().await.transpose()? {
        if batch.num_rows() == 0 {
            continue;
        }
        let _timer = metrics.elapsed_compute.timer();
        output.write(&factory, &batch, &mut written).await?;
        reservation.try_resize(output.memory_size())?;
    }
    if let TaskOutput::Directory(output) = &mut output
        && partition == 0
        && written.is_empty()
        && !input_schema
            .fields()
            .iter()
            .any(|field| contains_union(field.data_type()))
    {
        // Without partition columns, the first partition always produces a file,
        // so that the schema of the data can be read from the table even when
        // the result is empty. This is skipped for a schema that the Parquet
        // writer cannot represent, so that an empty result can still be written.
        output.open(&factory)?;
    }
    {
        let _timer = metrics.elapsed_compute.timer();
        output.close(&mut written).await?;
    }
    reservation.free();
    for file in &written {
        metrics.record(file);
    }
    manifest_batch(partition, partitions, attempt, &written)
}
