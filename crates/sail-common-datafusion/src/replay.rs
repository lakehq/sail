//! Query-scoped, partition-preserving materialization with independent readers.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, SpillFile, TaskContext};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
use datafusion::physical_plan::spill::SpillManager;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

// Bound each partition even when the session uses an unbounded memory pool.
const MEMORY_LIMIT: usize = 8 * 1024 * 1024;

pub struct ReplayBuffer {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    reservation: MemoryReservation,
    manager: SpillManager,
    files: Vec<Arc<dyn SpillFile>>,
}

impl ReplayBuffer {
    pub fn new(
        context: &TaskContext,
        schema: SchemaRef,
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Self {
        Self {
            schema: schema.clone(),
            batches: vec![],
            reservation: MemoryConsumer::new("SharedOutput")
                .with_can_spill(true)
                .register(context.memory_pool()),
            manager: SpillManager::new(
                context.runtime_env().clone(),
                SpillMetrics::new(metrics, partition),
                schema,
            ),
            files: vec![],
        }
    }

    pub fn append(&mut self, batch: RecordBatch) -> Result<()> {
        let size = batch.get_array_memory_size();
        if self.reservation.size().saturating_add(size) > MEMORY_LIMIT {
            self.flush()?;
        }
        if size > MEMORY_LIMIT || self.reservation.try_grow(size).is_err() {
            self.flush()?;
            if let Some(file) = self
                .manager
                .spill_record_batch_and_finish(&[batch], "shared output")?
            {
                self.files.push(file);
            }
        } else {
            self.batches.push(batch);
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if let Some(file) = self
            .manager
            .spill_record_batch_and_finish(&self.batches, "shared output")?
        {
            self.files.push(file);
        }
        self.batches.clear();
        self.reservation.free();
        Ok(())
    }

    pub fn finish(mut self) -> Result<ReplayOutput> {
        if !self.files.is_empty() {
            self.flush()?;
        }
        Ok(ReplayOutput {
            schema: self.schema,
            batches: self.batches,
            _reservation: self.reservation,
            manager: self.manager,
            files: self.files,
        })
    }
}

/// Immutable output. Readers hold this object until their streams are dropped,
/// keeping both the memory reservation and spill files alive.
pub struct ReplayOutput {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    _reservation: MemoryReservation,
    manager: SpillManager,
    files: Vec<Arc<dyn SpillFile>>,
}

impl ReplayOutput {
    pub fn stream(self: &Arc<Self>) -> Result<SendableRecordBatchStream> {
        let output = self.clone();
        let stream: SendableRecordBatchStream = if self.files.is_empty() {
            Box::pin(RecordBatchStreamAdapter::new(
                self.schema.clone(),
                futures::stream::iter(self.batches.clone().into_iter().map(Ok)),
            ))
        } else {
            use futures::TryStreamExt;
            let output = self.clone();
            let files = self.files.clone();
            Box::pin(RecordBatchStreamAdapter::new(
                self.schema.clone(),
                futures::stream::iter(
                    files
                        .into_iter()
                        .map(move |file| output.manager.read_spill_as_stream(file, None)),
                )
                .try_flatten(),
            ))
        };
        // Keep the output alive even after the owner of the result registry exits.
        use futures::StreamExt;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream.map(move |batch| {
                let _keep_alive = &output;
                batch
            }),
        )))
    }
}
