// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use datafusion::arrow::array::{Array, ArrayData, ArrayRef};
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::runtime::SpawnedTask;
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::parquet::arrow::arrow_writer::{
    ArrowColumnChunk, ArrowColumnWriter, compute_leaves,
};
use datafusion::parquet::errors::{ParquetError, Result};
use datafusion::physical_plan::metrics::Time;
use tokio::sync::{mpsc, oneshot};

use crate::formats::parquet::writer::resources::Admission;

/// The columns of a part of a batch for one lane.
pub(super) struct LaneInput {
    /// The values of the columns of the lane, in the order of the columns.
    pub columns: Vec<ArrayRef>,
    /// Receives the estimated size of the encoded data of the lane
    /// once the columns have been encoded.
    pub encoded: Option<oneshot::Sender<usize>>,
    /// Counts the columns as waiting input until they have been encoded.
    pub admission: Admission,
}

/// The columns that a lane has completed.
pub(super) struct LaneOutput {
    /// The chunks of the leaf columns of the lane, in order.
    pub chunks: Vec<ArrowColumnChunk>,
    /// The memory of the chunks.
    pub reservation: MemoryReservation,
}

/// Tells the task of a lane to stop when the lane is dropped.
struct LaneStop(Arc<AtomicBool>);

impl Drop for LaneStop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

/// Marks the lane as ended when the task of the lane ends in any way.
struct LaneEnd(Arc<AtomicBool>);

impl Drop for LaneEnd {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

/// A group of top-level columns of one row group that is encoded by one task.
///
/// The task receives the parts of the batches of the row group in order through
/// a queue, so each column writer sees the same data in the same order as it
/// would without lanes. The task reserves the memory of its column writers
/// itself, since the writer task does not wait for the lane. The task is
/// canceled when the lane is dropped, and it does not encode more input then.
pub(super) struct Lane {
    /// The indices of the columns in the schema, in increasing order.
    columns: Vec<usize>,
    /// The queue of the task. It is closed when the row group has all its rows.
    queue: Option<mpsc::UnboundedSender<LaneInput>>,
    task: SpawnedTask<Result<LaneOutput>>,
    /// Whether the task has ended.
    ended: Arc<AtomicBool>,
    _stop: LaneStop,
}

impl Lane {
    /// Starts the task of a lane. The writers are the writers of
    /// the leaf columns of the given columns, in order.
    pub fn spawn(
        columns: Vec<usize>,
        fields: Vec<FieldRef>,
        writers: Vec<ArrowColumnWriter>,
        reservation: MemoryReservation,
        elapsed_compute: Time,
    ) -> Self {
        let (queue, inputs) = mpsc::unbounded_channel();
        let ended = Arc::new(AtomicBool::new(false));
        let stopped = Arc::new(AtomicBool::new(false));
        let task = SpawnedTask::spawn(encode_lane(
            fields,
            writers,
            inputs,
            reservation,
            Arc::clone(&stopped),
            LaneEnd(Arc::clone(&ended)),
            elapsed_compute,
        ));
        Self {
            columns,
            queue: Some(queue),
            task,
            ended,
            _stop: LaneStop(stopped),
        }
    }

    /// The indices of the columns of the lane in the schema, in increasing order.
    pub fn columns(&self) -> &[usize] {
        &self.columns
    }

    /// Adds input to the queue of the lane.
    /// If the task of the lane has ended, this returns the error of the task.
    pub async fn send(&mut self, input: LaneInput) -> Result<()> {
        let sent = match &self.queue {
            Some(queue) => queue.send(input).is_ok(),
            None => false,
        };
        if sent {
            Ok(())
        } else {
            Err(self.failure().await)
        }
    }

    /// Closes the queue, so that the task completes the columns
    /// after it has encoded the input in the queue.
    pub fn seal(&mut self) {
        self.queue = None;
    }

    /// Whether the task of the lane has ended, so that [`Self::join`] does not wait.
    pub fn has_ended(&self) -> bool {
        self.ended.load(Ordering::Acquire)
    }

    /// Waits for the task and returns the columns of the lane.
    pub async fn join(self) -> Result<LaneOutput> {
        self.task
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))?
    }

    /// Returns the error of a task that has ended before its queue was closed.
    pub async fn failure(&mut self) -> ParquetError {
        match (&mut self.task).await {
            Ok(Ok(_)) => ParquetError::General(
                "the task of a column encoder has ended before its input".to_string(),
            ),
            Ok(Err(e)) => e,
            Err(e) => ParquetError::External(Box::new(e)),
        }
    }
}

async fn encode_lane(
    fields: Vec<FieldRef>,
    mut writers: Vec<ArrowColumnWriter>,
    mut inputs: mpsc::UnboundedReceiver<LaneInput>,
    reservation: MemoryReservation,
    stopped: Arc<AtomicBool>,
    _end: LaneEnd,
    elapsed_compute: Time,
) -> Result<LaneOutput> {
    while let Some(input) = inputs.recv().await {
        // A task that is canceled keeps running until it has to wait, which can
        // be after many inputs when the queue is long. The lane has been dropped
        // in that case, and the remaining input is not encoded.
        if stopped.load(Ordering::Acquire) {
            return Err(ParquetError::General(
                "the column encoder has been dropped".to_string(),
            ));
        }
        let _timer = elapsed_compute.timer();
        let mut lane_writers = writers.iter_mut();
        for (field, column) in fields.iter().zip(&input.columns) {
            for leaf in compute_leaves(field.as_ref(), column)? {
                lane_writers
                    .next()
                    .ok_or_else(|| {
                        ParquetError::General("missing column writer for leaf column".to_string())
                    })?
                    .write(&leaf)?;
            }
        }
        reserve(&reservation, writers.iter().map(|x| x.memory_size()).sum())?;
        if let Some(encoded) = input.encoded {
            let estimated = writers.iter().map(|x| x.get_estimated_total_bytes()).sum();
            // The writer of the file may have been dropped.
            let _ = encoded.send(estimated);
        }
        // The columns are no longer waiting input from here on.
        drop(input.columns);
        drop(input.admission);
    }
    let _timer = elapsed_compute.timer();
    let chunks = writers
        .into_iter()
        .map(|writer| writer.close())
        .collect::<Result<Vec<_>>>()?;
    let size = chunks
        .iter()
        .map(|chunk| chunk.close().bytes_written as usize)
        .sum();
    reserve(&reservation, size)?;
    Ok(LaneOutput {
        chunks,
        reservation,
    })
}

fn reserve(reservation: &MemoryReservation, size: usize) -> Result<()> {
    reservation
        .try_resize(size)
        .map_err(|e| ParquetError::External(Box::new(e)))
}

/// Divides the columns into lanes of similar weight.
/// Returns the indices of the columns of each lane in increasing order.
pub(super) fn assign_lanes(weights: &[usize], lanes: usize) -> Vec<Vec<usize>> {
    let mut assigned: Vec<(usize, Vec<usize>)> = (0..lanes).map(|_| (0, vec![])).collect();
    let mut columns: Vec<(usize, usize)> = weights.iter().copied().enumerate().collect();
    columns.sort_by_key(|&(_, column_weight)| std::cmp::Reverse(column_weight));
    for (column, column_weight) in columns {
        if let Some((weight, lane)) = assigned
            .iter_mut()
            .min_by_key(|(weight, lane)| (*weight, lane.len()))
        {
            *weight += column_weight;
            lane.push(column);
        }
    }
    assigned
        .into_iter()
        .map(|(_, mut lane)| {
            lane.sort_unstable();
            lane
        })
        .collect()
}

/// The size of the values that the array refers to.
///
/// This is not the size of the buffers of the array, since an array can be
/// a slice of buffers that are much larger and that are shared with other arrays.
pub(super) fn logical_size(array: &dyn Array) -> usize {
    let data = array.to_data();
    match data.get_slice_memory_size() {
        Ok(size) => size + view_data_size(&data),
        Err(_) => array.get_array_memory_size(),
    }
}

/// The size of the values of view arrays that are stored outside the views,
/// which the size of a slice of the array data does not include.
fn view_data_size(data: &ArrayData) -> usize {
    let own = match data.data_type() {
        DataType::Utf8View | DataType::BinaryView => data
            .buffer::<u128>(0)
            .iter()
            .take(data.len())
            .map(|view| *view as u32 as usize)
            .filter(|length| *length > 12)
            .sum(),
        _ => 0,
    };
    let children: usize = data.child_data().iter().map(view_data_size).sum();
    own + children
}
