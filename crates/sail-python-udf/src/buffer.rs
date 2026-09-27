use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use arrow_pyarrow::{FromPyArrow, ToPyArrow};
use datafusion::arrow::array::RecordBatch;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
use datafusion::physical_plan::spill::SpillManager;
use datafusion::physical_plan::spill::spill_pool::{SpillPoolSink, spsc_channel};
use datafusion_common::{Result, internal_datafusion_err};
use futures::StreamExt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::{Bound, Py, PyAny, PyResult, Python, pyclass, pymethods};
use tokio::runtime::Handle;

struct SpilledBatches {
    writer: SpillPoolSink,
    reader: SendableRecordBatchStream,
    remaining: usize,
}

/// FIFO storage for rows consumed by Python before their output is available.
struct PassthroughBuffer {
    batches: VecDeque<RecordBatch>,
    reservation: MemoryReservation,
    spilled: Option<SpilledBatches>,
    context: Arc<TaskContext>,
    capacity: usize,
}

impl PassthroughBuffer {
    fn new(context: Arc<TaskContext>, capacity: usize) -> Self {
        Self {
            batches: VecDeque::new(),
            reservation: MemoryConsumer::new("ScalarIteratorPassthrough")
                .with_can_spill(true)
                .register(context.memory_pool()),
            spilled: None,
            context,
            capacity,
        }
    }

    fn push(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let size = batch.get_array_memory_size();
        if self.spilled.is_none()
            && size <= self.capacity.saturating_sub(self.reservation.size())
            && self.reservation.try_grow(size).is_ok()
        {
            self.batches.push_back(batch);
            return Ok(());
        }
        if self.spilled.is_none() {
            let options = &self.context.session_config().options().execution;
            let manager = SpillManager::new(
                self.context.runtime_env(),
                SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
                batch.schema(),
            )
            .with_compression_type(options.spill_compression);
            let (writer, reader) =
                spsc_channel(options.max_spill_file_size_bytes.get(), Arc::new(manager));
            self.spilled = Some(SpilledBatches {
                writer,
                reader,
                remaining: 0,
            });
        }
        if let Some(spilled) = &mut self.spilled {
            while let Some(buffered) = self.batches.front() {
                spilled.writer.push_batch(buffered)?;
                spilled.remaining += 1;
                self.reservation.shrink(buffered.get_array_memory_size());
                self.batches.pop_front();
            }
            spilled.writer.push_batch(&batch)?;
            spilled.remaining += 1;
        }
        Ok(())
    }

    fn pop(&mut self, handle: &Handle) -> Result<Option<RecordBatch>> {
        if let Some(batch) = self.batches.pop_front() {
            self.reservation.shrink(batch.get_array_memory_size());
            return Ok(Some(batch));
        }
        let Some(spilled) = &mut self.spilled else {
            return Ok(None);
        };
        let batch = handle
            .block_on(spilled.reader.next())
            .ok_or_else(|| internal_datafusion_err!("missing scalar iterator spill batch"))??;
        spilled.remaining -= 1;
        if spilled.remaining == 0 {
            self.spilled = None;
        }
        Ok(Some(batch))
    }

    fn clear(&mut self) {
        self.batches.clear();
        self.reservation.free();
        self.spilled = None;
    }
}

#[pyclass]
pub(crate) struct PyPassthroughBuffer {
    state: Mutex<PassthroughBuffer>,
    handle: Handle,
}

impl PyPassthroughBuffer {
    pub(crate) fn new(context: Arc<TaskContext>) -> Self {
        // Bound each partition even when the session uses an unbounded memory pool.
        const CAPACITY: usize = 8 * 1024 * 1024;
        Self {
            state: Mutex::new(PassthroughBuffer::new(context, CAPACITY)),
            handle: Handle::current(),
        }
    }
}

#[pymethods]
impl PyPassthroughBuffer {
    fn append(&self, batch: &Bound<'_, PyAny>) -> PyResult<()> {
        let value = RecordBatch::from_pyarrow_bound(batch)?;
        batch.py().detach(|| {
            self.state
                .lock()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
                .push(value)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    fn popleft(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let batch = py.detach(|| {
            self.state
                .lock()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
                .pop(&self.handle)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })?;
        batch
            .map(|batch| batch.to_pyarrow(py).map(Bound::unbind))
            .transpose()
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            self.state
                .lock()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
                .clear();
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;

    use super::*;

    fn batch(value: i64) -> Result<RecordBatch> {
        Ok(RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int64Array::from(vec![value, value + 1])) as _,
            ),
            (
                "payload",
                Arc::new(StringArray::from(vec![Some("数据"), None])) as _,
            ),
        ])?)
    }

    #[test]
    fn prefetched_batches_spill_in_order_and_release_resources() -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;
        let batches = (0..20).map(batch).collect::<Result<Vec<_>>>()?;
        let budget = batches[0].get_array_memory_size() * 2;
        let environment = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(budget)))
            .build_arc()?;
        let context = Arc::new(TaskContext::default().with_runtime(Arc::clone(&environment)));
        // The shared pool must enforce its budget even below the per-partition cap.
        let mut buffer = PassthroughBuffer::new(context, usize::MAX);
        for batch in &batches {
            buffer.push(batch.clone())?;
            assert!(environment.memory_pool.reserved() <= budget);
        }
        assert!(buffer.spilled.is_some());
        assert_eq!(environment.memory_pool.reserved(), 0);
        assert!(
            environment
                .disk_manager
                .spilling_progress()
                .active_files_count
                > 0
        );
        for batch in &batches {
            assert_eq!(buffer.pop(runtime.handle())?, Some(batch.clone()));
        }
        assert!(buffer.pop(runtime.handle())?.is_none());
        assert_eq!(environment.memory_pool.reserved(), 0);
        assert_eq!(
            environment
                .disk_manager
                .spilling_progress()
                .active_files_count,
            0
        );
        // Resume in memory after the spill queue has drained.
        buffer.push(batches[0].clone())?;
        assert!(buffer.spilled.is_none());
        assert_eq!(buffer.pop(runtime.handle())?, Some(batches[0].clone()));
        Ok(())
    }

    #[test]
    fn dropping_prefetched_input_releases_spill_files() -> Result<()> {
        let environment = RuntimeEnvBuilder::new().build_arc()?;
        let context = Arc::new(TaskContext::default().with_runtime(Arc::clone(&environment)));
        let mut buffer = PassthroughBuffer::new(context, 0);
        buffer.push(batch(0)?)?;
        buffer.push(batch(2)?)?;
        assert_eq!(buffer.reservation.size(), 0);
        assert!(
            environment
                .disk_manager
                .spilling_progress()
                .active_files_count
                > 0
        );
        drop(buffer);
        assert_eq!(environment.memory_pool.reserved(), 0);
        assert_eq!(
            environment
                .disk_manager
                .spilling_progress()
                .active_files_count,
            0
        );
        Ok(())
    }

    #[test]
    fn disabled_spilling_reports_exhaustion_without_retaining_input() -> Result<()> {
        let environment = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(0)))
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
            )
            .build_arc()?;
        let context = Arc::new(TaskContext::default().with_runtime(Arc::clone(&environment)));
        let mut buffer = PassthroughBuffer::new(context, usize::MAX);
        assert!(buffer.push(batch(0)?).is_err());
        buffer.clear();
        assert_eq!(environment.memory_pool.reserved(), 0);
        assert_eq!(
            environment
                .disk_manager
                .spilling_progress()
                .active_files_count,
            0
        );
        Ok(())
    }
}
