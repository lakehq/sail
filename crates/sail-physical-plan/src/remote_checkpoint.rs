use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{Array, ArrayRef, StringArray, UInt8Array, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::config::TableParquetOptions;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, LexOrdering, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{DataFusionError, Result, Statistics, internal_datafusion_err};
use futures::{StreamExt, stream};
use log::warn;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::file::properties::WriterPropertiesBuilder;
use sail_cache::remote_checkpoint::{
    RemoteCheckpointDescriptor, RemoteCheckpointFile, RemoteCheckpointPartition,
    RemoteCheckpointRegistry,
};
use sail_common_datafusion::array::record_batch::record_batch_with_schema;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use tokio::io::AsyncWrite;
use uuid::Uuid;

pub const PARTITION_COLUMN: &str = "partition";
pub const LOCATION_COLUMN: &str = "location";
pub const SIZE_COLUMN: &str = "size";
pub const ROW_COUNT_COLUMN: &str = "row_count";
pub const ROW_MARKER_COLUMN: &str = "__sail_checkpoint_row_marker";

pub fn checkpoint_storage_schema(logical_schema: &SchemaRef) -> SchemaRef {
    if logical_schema.fields().is_empty() {
        return Arc::new(Schema::new_with_metadata(
            vec![Field::new(ROW_MARKER_COLUMN, DataType::UInt8, false)],
            logical_schema.metadata().clone(),
        ));
    }
    let fields = logical_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_name(format!("__sail_checkpoint_col_{index:05}")),
            )
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new_with_metadata(
        fields,
        logical_schema.metadata().clone(),
    ))
}

/// Pass-through scan boundary that reports properties captured at checkpoint materialization.
#[derive(Debug)]
pub struct RemoteCheckpointScanExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl RemoteCheckpointScanExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        output_partitioning: Partitioning,
        output_ordering: Option<LexOrdering>,
    ) -> Self {
        // Keep child equivalences; the descriptor only supplements partitioning and ordering.
        let mut equivalence_properties = input.equivalence_properties().clone();
        if let Some(ordering) = output_ordering {
            equivalence_properties.add_ordering(ordering);
        }
        let properties = Arc::new(PlanProperties::new(
            equivalence_properties,
            output_partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self { input, properties }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for RemoteCheckpointScanExec {
    fn fmt_as(&self, _display_type: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "RemoteCheckpointScanExec")
    }
}

impl ExecutionPlan for RemoteCheckpointScanExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return Err(internal_datafusion_err!(
                "RemoteCheckpointScanExec must have exactly one child"
            ));
        };
        if input.schema() != self.schema() {
            return Err(internal_datafusion_err!(
                "RemoteCheckpointScanExec child rewrite changed the checkpoint schema"
            ));
        }
        let input_partitioning = input.output_partitioning();
        let stored_partitioning = self.properties.output_partitioning();
        // Scan rewrites can lose a known distribution while retaining the physical partitions.
        let retains_stored_properties = matches!(
            input_partitioning,
            Partitioning::UnknownPartitioning(input_count)
                if *input_count == stored_partitioning.partition_count()
        );
        let output_partitioning = if retains_stored_properties {
            stored_partitioning.clone()
        } else {
            input_partitioning.clone()
        };
        let output_ordering = input.output_ordering().cloned().or_else(|| {
            retains_stored_properties
                .then(|| self.properties.output_ordering().cloned())
                .flatten()
        });
        Ok(Arc::new(Self::new(
            Arc::clone(input),
            output_partitioning,
            output_ordering,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.input.partition_statistics(partition)
    }
}

fn checkpoint_commit_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(PARTITION_COLUMN, DataType::UInt64, false),
        Field::new(LOCATION_COLUMN, DataType::Utf8, true),
        Field::new(SIZE_COLUMN, DataType::UInt64, false),
        Field::new(ROW_COUNT_COLUMN, DataType::UInt64, false),
    ]))
}

fn checkpoint_command_schema() -> SchemaRef {
    Arc::new(Schema::empty())
}

#[derive(Debug)]
pub struct RemoteCheckpointCommitExec {
    input: Arc<dyn ExecutionPlan>,
    relation_id: String,
    object_store_url: ObjectStoreUrl,
    prefix: Path,
    logical_schema: SchemaRef,
    storage_schema: SchemaRef,
    output_partitioning: Partitioning,
    output_ordering: Option<LexOrdering>,
    properties: Arc<PlanProperties>,
}

impl RemoteCheckpointCommitExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        relation_id: String,
        object_store_url: ObjectStoreUrl,
        prefix: Path,
        logical_schema: SchemaRef,
        storage_schema: SchemaRef,
        output_partitioning: Partitioning,
        output_ordering: Option<LexOrdering>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(checkpoint_command_schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            input,
            relation_id,
            object_store_url,
            prefix,
            logical_schema,
            storage_schema,
            output_partitioning,
            output_ordering,
            properties,
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn relation_id(&self) -> &str {
        &self.relation_id
    }

    pub fn object_store_url(&self) -> &ObjectStoreUrl {
        &self.object_store_url
    }

    pub fn prefix(&self) -> &Path {
        &self.prefix
    }

    pub fn logical_schema(&self) -> &SchemaRef {
        &self.logical_schema
    }

    pub fn storage_schema(&self) -> &SchemaRef {
        &self.storage_schema
    }

    pub fn checkpoint_partitioning(&self) -> &Partitioning {
        &self.output_partitioning
    }

    pub fn checkpoint_ordering(&self) -> Option<&LexOrdering> {
        self.output_ordering.as_ref()
    }
}

impl DisplayAs for RemoteCheckpointCommitExec {
    fn fmt_as(&self, _display_type: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "RemoteCheckpointCommitExec: relation_id={}",
            self.relation_id
        )
    }
}

impl ExecutionPlan for RemoteCheckpointCommitExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return Err(internal_datafusion_err!(
                "RemoteCheckpointCommitExec must have exactly one child"
            ));
        };
        Ok(Arc::new(Self::new(
            Arc::clone(input),
            self.relation_id.clone(),
            self.object_store_url.clone(),
            self.prefix.clone(),
            Arc::clone(&self.logical_schema),
            Arc::clone(&self.storage_schema),
            self.output_partitioning.clone(),
            self.output_ordering.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(internal_datafusion_err!(
                "RemoteCheckpointCommitExec can only execute partition 0"
            ));
        }
        let input_partitions = self.input.output_partitioning().partition_count();
        if input_partitions != 1 {
            return Err(internal_datafusion_err!(
                "RemoteCheckpointCommitExec requires one input partition, got {input_partitions}"
            ));
        }
        let input = self.input.execute(0, Arc::clone(&context))?;
        let relation_id = self.relation_id.clone();
        let object_store_url = self.object_store_url.clone();
        let prefix = self.prefix.clone();
        let logical_schema = Arc::clone(&self.logical_schema);
        let storage_schema = Arc::clone(&self.storage_schema);
        let output_partitioning = self.output_partitioning.clone();
        let output_ordering = self.output_ordering.clone();
        let partition_count = output_partitioning.partition_count();
        let output_schema = self.schema();
        let stream_schema = Arc::clone(&output_schema);
        let output = stream::once(async move {
            commit_checkpoint(
                input,
                context,
                relation_id,
                object_store_url,
                prefix,
                logical_schema,
                storage_schema,
                output_partitioning,
                output_ordering,
                partition_count,
                output_schema,
            )
            .await
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            output,
        )))
    }
}

#[derive(Debug)]
pub struct RemoteCheckpointWriteExec {
    input: Arc<dyn ExecutionPlan>,
    object_store_url: ObjectStoreUrl,
    prefix: Path,
    storage_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl RemoteCheckpointWriteExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        object_store_url: ObjectStoreUrl,
        prefix: Path,
        storage_schema: SchemaRef,
    ) -> Result<Self> {
        if matches!(input.boundedness(), Boundedness::Unbounded { .. }) {
            return Err(DataFusionError::NotImplemented(
                "checkpoint does not support unbounded input".to_string(),
            ));
        }
        let input_schema = input.schema();
        let schema_matches = if input_schema.fields().is_empty() {
            is_row_marker_schema(storage_schema.as_ref())
        } else {
            input_schema.fields().len() == storage_schema.fields().len()
                && input_schema
                    .fields()
                    .iter()
                    .zip(storage_schema.fields())
                    .all(|(input, storage)| input.data_type() == storage.data_type())
        };
        if !schema_matches {
            return Err(DataFusionError::Plan(
                "checkpoint storage schema does not match the input schema by position".to_string(),
            ));
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(checkpoint_commit_schema()),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            object_store_url,
            prefix,
            storage_schema,
            properties,
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn object_store_url(&self) -> &ObjectStoreUrl {
        &self.object_store_url
    }

    pub fn prefix(&self) -> &Path {
        &self.prefix
    }

    pub fn storage_schema(&self) -> &SchemaRef {
        &self.storage_schema
    }
}

impl DisplayAs for RemoteCheckpointWriteExec {
    fn fmt_as(&self, _display_type: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "RemoteCheckpointWriteExec: object_store_url={}, prefix={}",
            self.object_store_url, self.prefix
        )
    }
}

impl ExecutionPlan for RemoteCheckpointWriteExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return Err(internal_datafusion_err!(
                "RemoteCheckpointWriteExec must have exactly one child"
            ));
        };
        Ok(Arc::new(Self::try_new(
            Arc::clone(input),
            self.object_store_url.clone(),
            self.prefix.clone(),
            Arc::clone(&self.storage_schema),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, Arc::clone(&context))?;
        let store = context.runtime_env().object_store(&self.object_store_url)?;
        // Attempt-unique immutable objects make retries safe without rename or copy.
        let location = self
            .prefix
            .clone()
            .join("attempts")
            .join(format!("partition-{partition:020}"))
            .join(format!("{}.parquet", Uuid::new_v4()));
        let storage_schema = Arc::clone(&self.storage_schema);
        let output_schema = self.schema();
        let stream_schema = Arc::clone(&output_schema);
        let output = stream::once(write_checkpoint_partition(
            partition,
            input,
            context,
            store,
            location,
            storage_schema,
            output_schema,
        ));
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            output,
        )))
    }
}

struct CheckpointUpload {
    writer: Option<AsyncArrowWriter<CheckpointObjectWriter>>,
    reservation: MemoryReservation,
    object_store_buffer_size: usize,
    store: Arc<dyn ObjectStore>,
    location: Path,
    completed: bool,
}

struct CheckpointObjectWriter {
    writer: BufWriter,
    shutdown_started: bool,
}

impl AsyncWrite for CheckpointObjectWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.writer).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.writer).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.shutdown_started = true;
        Pin::new(&mut self.writer).poll_shutdown(context)
    }
}

impl CheckpointUpload {
    fn try_new(
        context: &TaskContext,
        store: Arc<dyn ObjectStore>,
        location: Path,
        schema: SchemaRef,
    ) -> Result<Self> {
        let object_store_buffer_size = context
            .session_config()
            .options()
            .execution
            .objectstore_writer_buffer_size;
        // Parquet trades encode/decode CPU for typically smaller object-store I/O and
        // row-group/page pruning metadata; Arrow IPC favors low-overhead interchange.
        let mut parquet_options = TableParquetOptions {
            global: context.session_config().options().execution.parquet.clone(),
            ..Default::default()
        };
        if !parquet_options.global.skip_arrow_metadata {
            parquet_options.arrow_schema(&schema);
        }
        let properties = WriterPropertiesBuilder::try_from(&parquet_options)?.build();
        let options = ArrowWriterOptions::new()
            .with_properties(properties)
            .with_skip_arrow_metadata(parquet_options.global.skip_arrow_metadata);
        let buffer = CheckpointObjectWriter {
            writer: BufWriter::with_capacity(
                Arc::clone(&store),
                location.clone(),
                object_store_buffer_size,
            ),
            shutdown_started: false,
        };
        let writer = AsyncArrowWriter::try_new_with_options(buffer, schema, options)?;
        // Charge both object-store buffering and Parquet's live encoding state to the task pool.
        let reservation = MemoryConsumer::new(format!("RemoteCheckpoint[{location}]"))
            .register(context.memory_pool());
        reservation.try_resize(object_store_buffer_size)?;
        Ok(Self {
            writer: Some(writer),
            reservation,
            object_store_buffer_size,
            store,
            location,
            completed: false,
        })
    }

    async fn write(&mut self, batch: &RecordBatch, schema: &SchemaRef) -> Result<()> {
        let batch = checkpoint_record_batch(batch, schema)?;
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| internal_datafusion_err!("checkpoint writer is not available"))?;
        writer.write(&batch).await?;
        self.reservation
            .try_resize(self.object_store_buffer_size + writer.memory_size())?;
        Ok(())
    }

    async fn finish(&mut self) -> Result<u64> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| internal_datafusion_err!("checkpoint writer is not available"))?;
        writer.finish().await?;
        let size = u64::try_from(writer.bytes_written())
            .map_err(|_| internal_datafusion_err!("checkpoint object size is too large"))?;
        self.completed = true;
        self.writer.take();
        Ok(size)
    }

    async fn abort(&mut self) {
        let Some(writer) = self.writer.take() else {
            self.completed = true;
            return;
        };
        let mut output = writer.into_inner();
        if output.shutdown_started {
            if let Err(error) = self.store.delete(&self.location).await
                && !matches!(error, object_store::Error::NotFound { .. })
            {
                warn!("failed to delete incomplete checkpoint object: {error}");
            }
        } else {
            if let Err(error) = output.writer.abort().await {
                warn!("failed to abort checkpoint multipart upload: {error}");
            }
        }
        self.completed = true;
    }
}

impl Drop for CheckpointUpload {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let Some(writer) = self.writer.take() else {
            return;
        };
        let store = Arc::clone(&self.store);
        let location = self.location.clone();
        let mut output = writer.into_inner();
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        handle.spawn(async move {
            if output.shutdown_started {
                let _ = store.delete(&location).await;
            } else {
                let _ = output.writer.abort().await;
            }
        });
    }
}

fn is_row_marker_schema(schema: &Schema) -> bool {
    schema.fields().len() == 1
        && schema.field(0).name() == ROW_MARKER_COLUMN
        && schema.field(0).data_type() == &DataType::UInt8
        && !schema.field(0).is_nullable()
}

fn checkpoint_record_batch(batch: &RecordBatch, storage_schema: &SchemaRef) -> Result<RecordBatch> {
    if batch.num_columns() == 0 && is_row_marker_schema(storage_schema.as_ref()) {
        return Ok(RecordBatch::try_new(
            Arc::clone(storage_schema),
            vec![Arc::new(UInt8Array::from(vec![0; batch.num_rows()]))],
        )?);
    }
    record_batch_with_schema(batch.clone(), storage_schema)
}

async fn write_checkpoint_partition(
    partition: usize,
    mut input: SendableRecordBatchStream,
    context: Arc<TaskContext>,
    store: Arc<dyn ObjectStore>,
    location: Path,
    storage_schema: SchemaRef,
    output_schema: SchemaRef,
) -> Result<RecordBatch> {
    let mut upload: Option<CheckpointUpload> = None;
    let mut row_count = 0_u64;
    while let Some(batch) = input.next().await {
        let batch = match batch {
            Ok(batch) => batch,
            Err(error) => {
                if let Some(upload) = &mut upload {
                    upload.abort().await;
                }
                return Err(error);
            }
        };
        row_count = row_count
            .checked_add(u64::try_from(batch.num_rows()).map_err(|_| {
                internal_datafusion_err!("checkpoint partition row count is too large")
            })?)
            .ok_or_else(|| internal_datafusion_err!("checkpoint partition row count overflow"))?;
        if batch.num_rows() == 0 {
            continue;
        }
        if upload.is_none() {
            upload = Some(CheckpointUpload::try_new(
                context.as_ref(),
                Arc::clone(&store),
                location.clone(),
                Arc::clone(&storage_schema),
            )?);
        }
        let Some(current_upload) = upload.as_mut() else {
            return Err(internal_datafusion_err!(
                "checkpoint upload was not initialized"
            ));
        };
        if let Err(error) = current_upload.write(&batch, &storage_schema).await {
            current_upload.abort().await;
            return Err(error);
        }
    }

    // An empty partition stays in the descriptor but does not allocate an object.
    let (location, size) = if let Some(upload) = &mut upload {
        match upload.finish().await {
            Ok(size) => (Some(location.to_string()), size),
            Err(error) => {
                upload.abort().await;
                return Err(error);
            }
        }
    } else {
        (None, 0)
    };
    let partition = u64::try_from(partition)
        .map_err(|_| internal_datafusion_err!("checkpoint partition index is too large"))?;
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(vec![partition])),
        Arc::new(StringArray::from(vec![location])),
        Arc::new(UInt64Array::from(vec![size])),
        Arc::new(UInt64Array::from(vec![row_count])),
    ];
    Ok(RecordBatch::try_new(output_schema, columns)?)
}

#[expect(clippy::too_many_arguments)]
async fn commit_checkpoint(
    input: SendableRecordBatchStream,
    context: Arc<TaskContext>,
    relation_id: String,
    object_store_url: ObjectStoreUrl,
    prefix: Path,
    logical_schema: SchemaRef,
    storage_schema: SchemaRef,
    output_partitioning: Partitioning,
    output_ordering: Option<LexOrdering>,
    partition_count: usize,
    output_schema: SchemaRef,
) -> Result<RecordBatch> {
    let registry = context.extension::<RemoteCheckpointRegistry>()?;
    let commit = async {
        let partitions = collect_checkpoint_partitions(input, partition_count, &prefix).await?;
        registry.insert(RemoteCheckpointDescriptor {
            relation_id,
            object_store_url: object_store_url.clone(),
            prefix: prefix.clone(),
            logical_schema,
            storage_schema,
            output_partitioning,
            output_ordering,
            partitions,
        })?;
        Ok(RecordBatch::new_empty(output_schema))
    }
    .await;
    match commit {
        Ok(batch) => Ok(batch),
        Err(error) => {
            match registry
                .cleanup_relation(context.runtime_env().as_ref(), &object_store_url, &prefix)
                .await
            {
                Ok(()) => Err(error),
                Err(cleanup_error) => Err(internal_datafusion_err!(
                    "checkpoint failed: {error}; additionally failed to clean {prefix}: {cleanup_error}"
                )),
            }
        }
    }
}

async fn collect_checkpoint_partitions(
    mut stream: SendableRecordBatchStream,
    partition_count: usize,
    relation_prefix: &Path,
) -> Result<Vec<RemoteCheckpointPartition>> {
    let mut partitions = vec![None; partition_count];
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        if batch.num_columns() != 4 {
            return Err(internal_datafusion_err!(
                "checkpoint returned {} metadata columns instead of 4",
                batch.num_columns()
            ));
        }
        let partition_values = checkpoint_u64_column(&batch, 0, PARTITION_COLUMN)?;
        let locations = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                internal_datafusion_err!("invalid checkpoint {LOCATION_COLUMN} column")
            })?;
        let sizes = checkpoint_u64_column(&batch, 2, SIZE_COLUMN)?;
        let row_counts = checkpoint_u64_column(&batch, 3, ROW_COUNT_COLUMN)?;
        for row in 0..batch.num_rows() {
            if partition_values.is_null(row) || sizes.is_null(row) || row_counts.is_null(row) {
                return Err(internal_datafusion_err!(
                    "checkpoint returned null required metadata"
                ));
            }
            let partition = usize::try_from(partition_values.value(row))
                .map_err(|_| internal_datafusion_err!("checkpoint partition index is too large"))?;
            let size = sizes.value(row);
            let row_count = row_counts.value(row);
            let file = if locations.is_null(row) {
                if size != 0 || row_count != 0 {
                    return Err(internal_datafusion_err!(
                        "empty checkpoint partition {partition} has nonzero metadata"
                    ));
                }
                None
            } else {
                if size == 0 || row_count == 0 {
                    return Err(internal_datafusion_err!(
                        "checkpoint partition {partition} has an empty file"
                    ));
                }
                let location = Path::parse(locations.value(row)).map_err(|error| {
                    internal_datafusion_err!("invalid checkpoint object location: {error}")
                })?;
                validate_attempt_location(relation_prefix, partition, &location)?;
                Some(RemoteCheckpointFile {
                    location,
                    size,
                    row_count,
                })
            };
            let Some(slot) = partitions.get_mut(partition) else {
                return Err(internal_datafusion_err!(
                    "checkpoint returned invalid partition {partition}"
                ));
            };
            if slot
                .replace(RemoteCheckpointPartition { partition, file })
                .is_some()
            {
                return Err(internal_datafusion_err!(
                    "checkpoint returned duplicate partition {partition}"
                ));
            }
        }
    }
    partitions
        .into_iter()
        .enumerate()
        .map(|(partition, value)| {
            value.ok_or_else(|| {
                internal_datafusion_err!("checkpoint did not return partition {partition}")
            })
        })
        .collect()
}

fn checkpoint_u64_column<'a>(
    batch: &'a RecordBatch,
    index: usize,
    name: &str,
) -> Result<&'a UInt64Array> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| internal_datafusion_err!("invalid checkpoint {name} column"))
}

fn validate_attempt_location(
    relation_prefix: &Path,
    partition: usize,
    location: &Path,
) -> Result<()> {
    let partition_prefix = relation_prefix
        .clone()
        .join("attempts")
        .join(format!("partition-{partition:020}"));
    if !location.prefix_matches(&partition_prefix)
        || location.parts_count() != partition_prefix.parts_count() + 1
        || location
            .filename()
            .is_none_or(|filename| !filename.ends_with(".parquet"))
    {
        return Err(internal_datafusion_err!(
            "checkpoint returned unexpected object {location} for partition {partition}"
        ));
    }
    Ok(())
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use std::fs;

    use datafusion::arrow::array::{Array, Int32Array};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;

    #[test]
    fn storage_schema_uses_positional_names_and_preserves_metadata() {
        let logical = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("value", DataType::Int64, false).with_metadata(
                    [("field-key".to_string(), "field-value".to_string())]
                        .into_iter()
                        .collect(),
                ),
                Field::new("value", DataType::Utf8, true),
            ],
            [("schema-key".to_string(), "schema-value".to_string())]
                .into_iter()
                .collect(),
        ));

        let storage = checkpoint_storage_schema(&logical);

        assert_eq!(storage.field(0).name(), "__sail_checkpoint_col_00000");
        assert_eq!(storage.field(1).name(), "__sail_checkpoint_col_00001");
        assert_eq!(storage.field(0).metadata(), logical.field(0).metadata());
        assert_eq!(storage.metadata(), logical.metadata());

        let empty = checkpoint_storage_schema(&Arc::new(Schema::empty()));
        assert_eq!(empty.fields().len(), 1);
        assert_eq!(empty.field(0).name(), ROW_MARKER_COLUMN);
        assert_eq!(empty.field(0).data_type(), &DataType::UInt8);
    }

    #[test]
    fn scan_rewrite_retains_checkpoint_properties_for_the_same_partition_count() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
        let key =
            Arc::new(Column::new("key", 0)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>;
        let ordering =
            LexOrdering::new([datafusion::physical_expr::PhysicalSortExpr::new_default(
                Arc::clone(&key),
            )]);
        let scan = Arc::new(RemoteCheckpointScanExec::new(
            Arc::new(EmptyExec::new(Arc::clone(&schema))),
            Partitioning::Hash(vec![key], 1),
            ordering,
        ));

        let rewritten = scan.with_new_children(vec![Arc::new(EmptyExec::new(schema))])?;

        assert!(matches!(
            rewritten.output_partitioning(),
            Partitioning::Hash(_, 1)
        ));
        assert!(rewritten.output_ordering().is_some());
        Ok(())
    }

    #[test]
    fn scan_rewrite_adopts_ordering_added_to_its_child() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
        let key =
            Arc::new(Column::new("key", 0)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>;
        let ordering =
            LexOrdering::new([datafusion::physical_expr::PhysicalSortExpr::new_default(
                key,
            )])
            .expect("non-empty ordering");
        let scan = Arc::new(RemoteCheckpointScanExec::new(
            Arc::new(EmptyExec::new(Arc::clone(&schema))),
            Partitioning::UnknownPartitioning(1),
            None,
        ));
        let sorted: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(
            ordering.clone(),
            Arc::new(EmptyExec::new(schema)),
        ));

        let rewritten = scan.with_new_children(vec![sorted])?;

        assert_eq!(rewritten.output_ordering(), Some(&ordering));
        Ok(())
    }

    #[tokio::test]
    async fn writes_a_partition_directly_to_object_storage() -> Result<()> {
        let logical_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let storage_schema = Arc::new(Schema::new(vec![Field::new(
            "__sail_checkpoint_col_0",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&logical_schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;
        let input =
            MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&logical_schema), None)?;
        let object_store_url = ObjectStoreUrl::parse("memory://")?;
        let checkpoint = RemoteCheckpointWriteExec::try_new(
            input,
            object_store_url.clone(),
            Path::from("checkpoint"),
            storage_schema,
        )?;
        let context = SessionContext::new();
        context
            .runtime_env()
            .register_object_store(object_store_url.as_ref(), Arc::new(InMemory::new()));

        let mut output = checkpoint.execute(0, context.task_ctx())?;
        let commit = output.next().await.expect("commit batch")?;
        let locations = commit
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("location column");
        assert!(!locations.is_null(0));
        assert_eq!(
            commit
                .column(3)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("row count column")
                .value(0),
            3
        );
        let path = Path::parse(locations.value(0))
            .map_err(|error| internal_datafusion_err!("invalid path: {error}"))?;
        let store = context.runtime_env().object_store(&object_store_url)?;
        let bytes = store
            .get(&path)
            .await
            .map_err(|error| DataFusionError::ObjectStore(Box::new(error)))?
            .bytes()
            .await
            .map_err(|error| DataFusionError::ObjectStore(Box::new(error)))?;
        let rows = ParquetRecordBatchReaderBuilder::try_new(bytes)?
            .build()?
            .map(|batch| batch.map(|batch| batch.num_rows()))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        assert_eq!(rows, vec![3]);
        Ok(())
    }

    #[tokio::test]
    async fn empty_partition_does_not_create_an_object() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let input = MemorySourceConfig::try_new_exec(&[vec![]], Arc::clone(&schema), None)?;
        let object_store_url = ObjectStoreUrl::parse("memory://")?;
        let checkpoint = RemoteCheckpointWriteExec::try_new(
            input,
            object_store_url.clone(),
            Path::from("checkpoint"),
            schema,
        )?;
        let context = SessionContext::new();
        context
            .runtime_env()
            .register_object_store(object_store_url.as_ref(), Arc::new(InMemory::new()));

        let mut output = checkpoint.execute(0, context.task_ctx())?;
        let commit = output.next().await.expect("commit batch")?;
        let locations = commit
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("location column");
        assert!(locations.is_null(0));
        let store = context.runtime_env().object_store(&object_store_url)?;
        assert!(
            store
                .list(Some(&Path::from("checkpoint")))
                .next()
                .await
                .is_none()
        );
        Ok(())
    }

    #[tokio::test]
    async fn commit_publishes_checkpoint_and_registry_cleans_it() -> Result<()> {
        let logical_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let storage_schema = checkpoint_storage_schema(&logical_schema);
        let batch = RecordBatch::try_new(
            Arc::clone(&logical_schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;
        let input = MemorySourceConfig::try_new_exec(
            &[vec![batch], vec![]],
            Arc::clone(&logical_schema),
            None,
        )?;
        let registry = Arc::new(RemoteCheckpointRegistry::new(Some(
            "memory:///checkpoint".to_string(),
        )));
        let config = SessionConfig::new().with_extension(Arc::clone(&registry));
        let context = SessionContext::new_with_config(config);
        let object_store_url = ObjectStoreUrl::parse("memory://")?;
        context
            .runtime_env()
            .register_object_store(object_store_url.as_ref(), Arc::new(InMemory::new()));
        let (object_store_url, prefix) =
            registry.resolve_relation(context.runtime_env().as_ref(), "relation")?;
        let writer: Arc<dyn ExecutionPlan> = Arc::new(RemoteCheckpointWriteExec::try_new(
            input,
            object_store_url.clone(),
            prefix.clone(),
            Arc::clone(&storage_schema),
        )?);
        let commit = RemoteCheckpointCommitExec::new(
            Arc::new(CoalescePartitionsExec::new(writer)),
            "relation".to_string(),
            object_store_url.clone(),
            prefix.clone(),
            Arc::clone(&logical_schema),
            storage_schema,
            Partitioning::UnknownPartitioning(2),
            None,
        );

        let mut output = commit.execute(0, context.task_ctx())?;
        output.next().await.expect("checkpoint commit batch")?;
        let descriptor = registry
            .get("relation")?
            .expect("checkpoint descriptor must be published");
        assert_eq!(descriptor.partitions.len(), 2);
        assert!(descriptor.partitions[0].file.is_some());
        assert!(descriptor.partitions[1].file.is_none());
        let store = context.runtime_env().object_store(&object_store_url)?;
        assert!(store.list(Some(&prefix)).next().await.is_some());

        registry
            .cleanup_session(context.runtime_env().as_ref())
            .await?;
        assert!(registry.get("relation")?.is_none());
        assert!(store.list(Some(&prefix)).next().await.is_none());
        assert!(
            registry
                .resolve_relation(context.runtime_env().as_ref(), "another")
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn finalize_error_does_not_abort_a_shutdown_writer() -> Result<()> {
        let root =
            std::env::temp_dir().join(format!("sail-checkpoint-finalize-error-{}", Uuid::new_v4()));
        fs::create_dir(&root).map_err(|error| DataFusionError::External(Box::new(error)))?;
        fs::write(root.join("not-a-directory"), b"blocker")
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let store: Arc<dyn ObjectStore> = Arc::new(
            LocalFileSystem::new_with_prefix(&root)
                .map_err(|error| DataFusionError::ObjectStore(Box::new(error)))?,
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )?;
        let context = SessionContext::new();
        let mut upload = CheckpointUpload::try_new(
            context.task_ctx().as_ref(),
            store,
            Path::from("not-a-directory/checkpoint.parquet"),
            Arc::clone(&schema),
        )?;
        upload.write(&batch, &schema).await?;
        assert!(upload.finish().await.is_err());

        upload.abort().await;
        fs::remove_file(root.join("not-a-directory"))
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        fs::remove_dir(root).map_err(|error| DataFusionError::External(Box::new(error)))?;
        Ok(())
    }
}
