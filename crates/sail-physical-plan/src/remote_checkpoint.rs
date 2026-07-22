use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{ArrayRef, StringArray, UInt8Array, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::config::TableParquetOptions;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, Partitioning};
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
use sail_common_datafusion::array::record_batch::record_batch_with_schema;
use tokio::io::AsyncWrite;
use uuid::Uuid;

pub const PARTITION_COLUMN: &str = "partition";
pub const LOCATION_COLUMN: &str = "location";
pub const SIZE_COLUMN: &str = "size";
pub const ROW_COUNT_COLUMN: &str = "row_count";
pub const ROW_MARKER_COLUMN: &str = "__sail_checkpoint_row_marker";

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

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use std::fs;

    use datafusion::arrow::array::{Array, Int32Array};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::prelude::SessionContext;
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;

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
