//! Immutable, spill-backed partitions with independent, unbuffered readers.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayData, BinaryViewArray, StringViewArray, make_array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::utils::memory::get_record_batch_memory_size;
use datafusion::common::{Result, exec_err};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::spill_file::SpillFile;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
use datafusion::physical_plan::spill::SpillManager;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{StreamExt, stream};

// DataFusion exposes the incremental writer's methods but not its type.
type Append = Box<dyn FnMut(Option<&RecordBatch>) -> Result<Option<Arc<dyn SpillFile>>> + Send>;

pub struct ReplayWriter {
    manager: SpillManager,
    append: Append,
    reservation: MemoryReservation,
    reader_capacity: usize,
}

impl ReplayWriter {
    pub fn try_new(context: &TaskContext, schema: SchemaRef) -> Result<Self> {
        let manager = SpillManager::new(
            context.runtime_env(),
            SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            schema,
        )
        .with_compression_type(
            context
                .session_config()
                .options()
                .execution
                .spill_compression,
        );
        let mut file = manager.create_in_progress_file("materialized partition replay")?;
        Ok(Self {
            manager,
            append: Box::new(move |batch| match batch {
                Some(batch) => {
                    file.append_batch(batch)?;
                    Ok(None)
                }
                None => file.finish(),
            }),
            reservation: MemoryConsumer::new("ReplayWriter").register(context.memory_pool()),
            reader_capacity: 0,
        })
    }

    pub fn append(&mut self, batch: &RecordBatch) -> Result<()> {
        // Never retain payload batches. Reserve the producer's current batch plus
        // IPC/GC working space for only the slice being appended.
        let bytes = get_record_batch_memory_size(batch);
        self.reservation.try_resize(bytes)?;
        // Small replay batches bound each independent reader's working set, even
        // when the producer uses large batches. The file remains a single spool.
        let rows = batch.num_rows();
        let rows_per_batch = ((256 * 1024usize).saturating_mul(rows) / bytes.max(1)).max(1);
        let result = (0..rows).step_by(rows_per_batch).try_for_each(|offset| {
            let slice = batch.slice(offset, rows_per_batch.min(rows - offset));
            let compact_bytes = slice.columns().iter().try_fold(0usize, |total, array| {
                let data = array.to_data();
                Ok::<_, datafusion::common::DataFusionError>(
                    total + data.get_slice_memory_size()? + view_value_bytes(&data),
                )
            })?;
            let reserved = compact_bytes
                .checked_mul(2)
                .and_then(|scratch| bytes.checked_add(scratch))
                .ok_or_else(|| {
                    datafusion::common::exec_datafusion_err!("replay batch size overflow")
                })?;
            self.reservation.try_resize(reserved)?;
            // Include DataFusion's 128KiB file read buffer as well as the
            // decoded batch and decompression scratch space.
            let capacity = compact_bytes
                .checked_mul(2)
                .and_then(|bytes| bytes.checked_add(128 * 1024))
                .ok_or_else(|| {
                    datafusion::common::exec_datafusion_err!("replay reader size overflow")
                })?;
            self.reader_capacity = self.reader_capacity.max(capacity);
            (self.append)(Some(&slice)).map(|_| ())
        });
        self.reservation.free();
        result
    }

    pub fn finish(mut self) -> Result<ReplayPartition> {
        let file = (self.append)(None)?;
        Ok(ReplayPartition {
            manager: self.manager,
            file,
            reader_capacity: self.reader_capacity,
        })
    }
}

// Arrow's sliced memory size excludes view data buffers. Count the referenced
// values that SpillManager's recursive GC copies, including nested view arrays.
fn view_value_bytes(data: &ArrayData) -> usize {
    let array = make_array(data.clone());
    let own = if let Some(array) = array.as_any().downcast_ref::<StringViewArray>() {
        array.iter().flatten().map(str::len).sum()
    } else if let Some(array) = array.as_any().downcast_ref::<BinaryViewArray>() {
        array.iter().flatten().map(<[u8]>::len).sum()
    } else {
        0
    };
    own + data
        .child_data()
        .iter()
        .map(view_value_bytes)
        .sum::<usize>()
}

pub struct ReplayPartition {
    manager: SpillManager,
    // None is an explicitly committed empty partition, not a missing output.
    file: Option<Arc<dyn SpillFile>>,
    reader_capacity: usize,
}

impl std::fmt::Debug for ReplayPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplayPartition")
            .field("empty", &self.file.is_none())
            .finish()
    }
}

impl ReplayPartition {
    pub async fn materialize(
        mut input: SendableRecordBatchStream,
        context: &TaskContext,
    ) -> Result<Self> {
        let mut writer = ReplayWriter::try_new(context, input.schema())?;
        while let Some(batch) = input.next().await {
            writer.append(&batch?)?;
        }
        writer.finish()
    }

    pub fn open(&self, context: &TaskContext) -> Result<SendableRecordBatchStream> {
        let schema = self.manager.schema().clone();
        let Some(file) = &self.file else {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                stream::empty(),
            )));
        };
        let reservation = MemoryConsumer::new("ReplayReader").register(context.memory_pool());
        // Reserve before the downstream operator fills its pool. Growing this
        // reservation while reading could prevent a join/sort from seeing the
        // next batch that would otherwise trigger its own spilling.
        reservation.try_resize(self.reader_capacity)?;
        let input = self
            .manager
            .read_spill_as_stream_unbuffered(file.clone(), None)?;
        let stream = input.map(move |batch| {
            let batch = batch?;
            let bytes = get_record_batch_memory_size(&batch);
            if bytes > reservation.size() {
                return exec_err!(
                    "decoded replay batch ({bytes} bytes) exceeds its reserved working set ({} bytes)",
                    reservation.size()
                );
            }
            Ok(batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// A terminal initialization failure is cached as well, so a changing source is never rerun.
pub fn replay_error(
    error: Arc<datafusion::common::DataFusionError>,
) -> datafusion::common::DataFusionError {
    datafusion::common::DataFusionError::Shared(error)
}

pub fn check_bounded(input: &dyn datafusion::physical_plan::ExecutionPlan) -> Result<()> {
    if input.properties().boundedness.is_unbounded() {
        return exec_err!("materialized partition replay does not support unbounded input");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use futures::TryStreamExt;

    use super::*;

    #[tokio::test]
    async fn committed_partition_has_independent_readers() -> Result<()> {
        let context = TaskContext::default();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )?;
        let mut writer = ReplayWriter::try_new(&context, schema)?;
        writer.append(&batch)?;
        writer.append(&batch)?;
        let replay = writer.finish()?;
        let first = replay.open(&context)?;
        let second = replay.open(&context)?;
        drop(replay);
        let (first, second) = futures::try_join!(
            first.try_collect::<Vec<_>>(),
            second.try_collect::<Vec<_>>()
        )?;
        assert_eq!(first, vec![batch.clone(), batch]);
        assert_eq!(first, second);
        assert_eq!(context.memory_pool().reserved(), 0);
        Ok(())
    }

    #[test]
    fn aborted_and_failed_writes_release_disk_and_memory() -> Result<()> {
        let context = TaskContext::default();
        let runtime = context.runtime_env();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1; 8192]))],
        )?;
        let mut writer = ReplayWriter::try_new(&context, schema.clone())?;
        writer.append(&batch)?;
        assert!(runtime.disk_manager.used_disk_space() > 0);
        drop(writer);
        assert_eq!(runtime.disk_manager.used_disk_space(), 0);
        runtime.disk_manager.set_max_temp_directory_size(1)?;
        let mut writer = ReplayWriter::try_new(&context, schema)?;
        assert!(writer.append(&batch).is_err());
        drop(writer);
        assert_eq!(runtime.disk_manager.used_disk_space(), 0);
        assert_eq!(context.memory_pool().reserved(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn canceled_reader_releases_its_reservation() -> Result<()> {
        let context = TaskContext::default();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1; 8192]))],
        )?;
        let mut writer = ReplayWriter::try_new(&context, schema)?;
        writer.append(&batch)?;
        let replay = writer.finish()?;
        let mut reader = replay.open(&context)?;
        assert!(reader.try_next().await?.is_some());
        assert!(context.memory_pool().reserved() > 0);
        drop(reader);
        assert_eq!(context.memory_pool().reserved(), 0);
        drop(replay);
        assert_eq!(context.runtime_env().disk_manager.used_disk_space(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn empty_partition_is_committed() -> Result<()> {
        let context = TaskContext::default();
        let schema = Arc::new(Schema::empty());
        let replay = ReplayWriter::try_new(&context, schema.clone())?.finish()?;
        for _ in 0..2 {
            let stream = replay.open(&context)?;
            assert_eq!(stream.schema(), schema);
            assert!(stream.try_collect::<Vec<_>>().await?.is_empty());
        }
        Ok(())
    }
}
