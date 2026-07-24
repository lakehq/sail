use std::any::Any;
use std::collections::BTreeMap;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{Array, ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::config::{ConfigOptions, TableParquetOptions};
use datafusion::datasource::source::{DataSource, OpenArgs};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::{
    Distribution, EquivalenceProperties, LexOrdering, Partitioning, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, SchedulingType};
use datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SortOrderPushdownResult,
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
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWrite;
use uuid::Uuid;

pub const METADATA_COLUMN: &str = "metadata";

/// Exposes the partitioning recorded by a checkpoint instead of the layout-derived partitioning
/// reported by its underlying file or memory source.
///
/// Source rewrites remain wrapped so optimizer pushdowns cannot discard this property.
#[derive(Debug)]
pub struct CheckpointDataSource {
    source: Arc<dyn DataSource>,
    output_partitioning: Partitioning,
}

impl CheckpointDataSource {
    pub fn new(source: Arc<dyn DataSource>, output_partitioning: Partitioning) -> Self {
        Self {
            source,
            output_partitioning,
        }
    }

    pub fn source(&self) -> &Arc<dyn DataSource> {
        &self.source
    }

    pub fn checkpoint_partitioning(&self) -> &Partitioning {
        &self.output_partitioning
    }

    fn map_source(&self, source: Arc<dyn DataSource>) -> Arc<dyn DataSource> {
        Arc::new(Self::new(source, self.output_partitioning.clone()))
    }
}

impl DataSource for CheckpointDataSource {
    fn open(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.source.open(partition, context)
    }

    fn fmt_as(&self, display_type: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        self.source.fmt_as(display_type, f)
    }

    fn output_partitioning(&self) -> Partitioning {
        self.output_partitioning.clone()
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        self.source.eq_properties()
    }

    fn scheduling_type(&self) -> SchedulingType {
        self.source.scheduling_type()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.source.partition_statistics(partition)
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn DataSource>> {
        self.source
            .with_fetch(limit)
            .map(|source| self.map_source(source))
    }

    fn fetch(&self) -> Option<usize> {
        self.source.fetch()
    }

    fn metrics(&self) -> ExecutionPlanMetricsSet {
        self.source.metrics()
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        Ok(None)
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn DataSource>>> {
        let result = self.source.try_pushdown_filters(filters, config)?;
        Ok(FilterPushdownPropagation {
            filters: result.filters,
            updated_node: result.updated_node.map(|source| self.map_source(source)),
        })
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn DataSource>>> {
        Ok(self
            .source
            .try_pushdown_sort(order)?
            .map(|source| self.map_source(source)))
    }

    fn with_preserve_order(&self, preserve_order: bool) -> Option<Arc<dyn DataSource>> {
        self.source
            .with_preserve_order(preserve_order)
            .map(|source| self.map_source(source))
    }

    fn with_new_state(&self, state: Arc<dyn Any + Send + Sync>) -> Option<Arc<dyn DataSource>> {
        self.source
            .with_new_state(state)
            .map(|source| self.map_source(source))
    }

    fn create_sibling_state(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        self.source.create_sibling_state()
    }

    fn open_with_args(&self, args: OpenArgs) -> Result<SendableRecordBatchStream> {
        self.source.open_with_args(args)
    }
}

pub fn checkpoint_storage_schema(logical_schema: &SchemaRef) -> SchemaRef {
    let fields = logical_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| Arc::new(field.as_ref().clone().with_name(format!("_c{index}"))))
        .collect::<Vec<_>>();
    Arc::new(Schema::new_with_metadata(
        fields,
        logical_schema.metadata().clone(),
    ))
}

fn checkpoint_metadata_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        METADATA_COLUMN,
        DataType::Utf8,
        false,
    )]))
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointPartitionMetadata {
    partition: u64,
    location: Option<String>,
    size: u64,
    row_count: u64,
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
        let schema_matches = input_schema.fields().len() == storage_schema.fields().len()
            && input_schema
                .fields()
                .iter()
                .zip(storage_schema.fields())
                .all(|(input, storage)| input.data_type() == storage.data_type());
        if !schema_matches {
            return Err(DataFusionError::Plan(
                "checkpoint storage schema does not match the input schema by position".to_string(),
            ));
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(checkpoint_metadata_schema()),
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
        let location = self
            .prefix
            .clone()
            .join(format!("part-{partition:05}-{}.parquet", Uuid::new_v4()));
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
        let batch = record_batch_with_schema(batch.clone(), schema)?;
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
        if batch.num_rows() == 0 || storage_schema.fields().is_empty() {
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
    let metadata = serde_json::to_string(&CheckpointPartitionMetadata {
        partition,
        location,
        size,
        row_count,
    })
    .map_err(|error| internal_datafusion_err!("failed to encode checkpoint metadata: {error}"))?;
    let columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![metadata]))];
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
    output_schema: SchemaRef,
) -> Result<RecordBatch> {
    let registry = context.extension::<RemoteCheckpointRegistry>()?;
    let commit = async {
        let partitions = collect_checkpoint_partitions(input, &prefix, &storage_schema).await?;
        let output_partitioning =
            checkpoint_partitioning_with_count(output_partitioning, partitions.len());
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
    relation_prefix: &Path,
    storage_schema: &Schema,
) -> Result<Vec<RemoteCheckpointPartition>> {
    let mut partitions = BTreeMap::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        if batch.num_columns() != 1 {
            return Err(internal_datafusion_err!(
                "checkpoint returned {} metadata columns instead of 1",
                batch.num_columns()
            ));
        }
        let metadata_values = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                internal_datafusion_err!("invalid checkpoint {METADATA_COLUMN} column")
            })?;
        for row in 0..batch.num_rows() {
            if metadata_values.is_null(row) {
                return Err(internal_datafusion_err!(
                    "checkpoint returned null metadata"
                ));
            }
            let CheckpointPartitionMetadata {
                partition,
                location,
                size,
                row_count,
            } = serde_json::from_str(metadata_values.value(row)).map_err(|error| {
                internal_datafusion_err!("invalid checkpoint metadata JSON: {error}")
            })?;
            let partition = usize::try_from(partition)
                .map_err(|_| internal_datafusion_err!("checkpoint partition index is too large"))?;
            let file = if let Some(location) = location {
                if storage_schema.fields().is_empty() {
                    return Err(internal_datafusion_err!(
                        "zero-column checkpoint partition {partition} returned a file"
                    ));
                }
                if size == 0 || row_count == 0 {
                    return Err(internal_datafusion_err!(
                        "checkpoint partition {partition} has an empty file"
                    ));
                }
                let location = Path::parse(location).map_err(|error| {
                    internal_datafusion_err!("invalid checkpoint object location: {error}")
                })?;
                validate_partition_location(relation_prefix, partition, &location)?;
                Some(RemoteCheckpointFile { location, size })
            } else {
                if size != 0 {
                    return Err(internal_datafusion_err!(
                        "checkpoint partition {partition} has a size without a file"
                    ));
                }
                if row_count != 0 && !storage_schema.fields().is_empty() {
                    return Err(internal_datafusion_err!(
                        "non-empty checkpoint partition {partition} is missing a file"
                    ));
                }
                None
            };
            if partitions
                .insert(
                    partition,
                    RemoteCheckpointPartition {
                        partition,
                        row_count,
                        file,
                    },
                )
                .is_some()
            {
                return Err(internal_datafusion_err!(
                    "checkpoint returned duplicate partition {partition}"
                ));
            }
        }
    }
    if partitions.is_empty() {
        return Err(internal_datafusion_err!(
            "checkpoint did not return partition 0"
        ));
    }
    partitions
        .into_iter()
        .enumerate()
        .map(|(expected, (partition, value))| {
            if partition != expected {
                return Err(internal_datafusion_err!(
                    "checkpoint did not return partition {expected}"
                ));
            }
            Ok(value)
        })
        .collect()
}

fn checkpoint_partitioning_with_count(
    partitioning: Partitioning,
    partition_count: usize,
) -> Partitioning {
    match partitioning {
        Partitioning::RoundRobinBatch(_) => Partitioning::RoundRobinBatch(partition_count),
        Partitioning::Hash(expressions, expected) if expected == partition_count => {
            Partitioning::Hash(expressions, partition_count)
        }
        Partitioning::Hash(_, _) => Partitioning::UnknownPartitioning(partition_count),
        Partitioning::UnknownPartitioning(_) => Partitioning::UnknownPartitioning(partition_count),
    }
}

fn validate_partition_location(
    relation_prefix: &Path,
    partition: usize,
    location: &Path,
) -> Result<()> {
    let Some(filename) = location.filename() else {
        return Err(internal_datafusion_err!(
            "checkpoint returned an invalid object for partition {partition}: {location}"
        ));
    };
    let prefix = format!("part-{partition:05}-");
    let file_id = filename
        .strip_prefix(&prefix)
        .and_then(|value| value.strip_suffix(".parquet"));
    let valid_file_id = file_id.is_some_and(|value| Uuid::parse_str(value).is_ok());
    if !location.prefix_matches(relation_prefix)
        || location.parts_count() != relation_prefix.parts_count() + 1
        || !valid_file_id
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

    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::record_batch::RecordBatchOptions;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;

    fn partition_metadata(batch: &RecordBatch) -> CheckpointPartitionMetadata {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("metadata column");
        serde_json::from_str(values.value(0)).expect("valid checkpoint metadata")
    }

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

        assert_eq!(storage.field(0).name(), "_c0");
        assert_eq!(storage.field(1).name(), "_c1");
        assert_eq!(storage.field(0).metadata(), logical.field(0).metadata());
        assert_eq!(storage.metadata(), logical.metadata());

        let empty = checkpoint_storage_schema(&Arc::new(Schema::empty()));
        assert!(empty.fields().is_empty());
    }

    #[tokio::test]
    async fn writes_a_partition_directly_to_object_storage() -> Result<()> {
        let logical_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let storage_schema = Arc::new(Schema::new(vec![Field::new("_c0", DataType::Int32, false)]));
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
        let metadata = partition_metadata(&commit);
        assert_eq!(metadata.partition, 0);
        assert_eq!(metadata.row_count, 3);
        let path = Path::parse(metadata.location.expect("checkpoint file"))
            .map_err(|error| internal_datafusion_err!("invalid path: {error}"))?;
        assert!(
            path.filename()
                .is_some_and(|name| name.starts_with("part-00000-"))
        );
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
        let metadata = partition_metadata(&commit);
        assert!(metadata.location.is_none());
        assert_eq!(metadata.row_count, 0);
        assert_eq!(metadata.size, 0);
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
    async fn zero_column_partition_records_rows_without_an_object() -> Result<()> {
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::try_new_with_options(
            Arc::clone(&schema),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(3)),
        )?;
        let input = MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?;
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
            registry.resolve_relation(context.runtime_env().as_ref(), "zero-columns")?;
        let writer: Arc<dyn ExecutionPlan> = Arc::new(RemoteCheckpointWriteExec::try_new(
            input,
            object_store_url.clone(),
            prefix.clone(),
            Arc::clone(&schema),
        )?);
        let checkpoint = RemoteCheckpointCommitExec::new(
            Arc::new(CoalescePartitionsExec::new(writer)),
            "zero-columns".to_string(),
            object_store_url.clone(),
            prefix.clone(),
            Arc::clone(&schema),
            schema,
            Partitioning::UnknownPartitioning(1),
            None,
        );

        let mut output = checkpoint.execute(0, context.task_ctx())?;
        output.next().await.expect("checkpoint commit batch")?;
        let descriptor = registry
            .get("zero-columns")?
            .expect("checkpoint descriptor must be published");
        assert_eq!(descriptor.partitions[0].row_count, 3);
        assert!(descriptor.partitions[0].file.is_none());
        let store = context.runtime_env().object_store(&object_store_url)?;
        assert!(store.list(Some(&prefix)).next().await.is_none());
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
            Partitioning::UnknownPartitioning(1),
            None,
        );

        let mut output = commit.execute(0, context.task_ctx())?;
        output.next().await.expect("checkpoint commit batch")?;
        let descriptor = registry
            .get("relation")?
            .expect("checkpoint descriptor must be published");
        assert!(matches!(
            descriptor.output_partitioning,
            Partitioning::UnknownPartitioning(2)
        ));
        assert_eq!(descriptor.partitions.len(), 2);
        assert_eq!(descriptor.partitions[0].row_count, 3);
        assert_eq!(descriptor.partitions[1].row_count, 0);
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
