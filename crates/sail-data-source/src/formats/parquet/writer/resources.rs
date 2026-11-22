// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::parquet::errors::{ParquetError, Result};
use datafusion::physical_plan::metrics::Time;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// The size of the input that may wait to be encoded, for each unit of the degree.
const READ_AHEAD_BYTES_PER_DEGREE: usize = 16 * 1024 * 1024;

/// The number of row groups that are encoded at the same time for each
/// set of lanes that the degree has room for, in addition to the row group
/// that receives rows.
const WINDOW_MULTIPLIER: usize = 1;

/// The number of bytes that one permit of the read-ahead budget stands for.
const BYTES_PER_PERMIT: usize = 1024;

/// How the files of a writer task are encoded.
#[derive(Debug, Clone)]
pub struct EncodeSettings {
    /// The number of threads that the task may keep busy with encoding.
    /// The files are encoded by the writer task itself when this is one.
    pub degree: usize,
    /// The number of lanes of a row group.
    /// This is derived from the degree when it is not set.
    pub lanes: Option<usize>,
    /// The number of row groups of a file that are encoded at the same time.
    /// This is derived from the degree when it is not set.
    pub window: Option<usize>,
    /// The size of the input that may wait to be encoded.
    /// This is derived from the degree when it is not set.
    pub read_ahead_bytes: Option<usize>,
}

impl EncodeSettings {
    pub fn new(degree: usize) -> Self {
        Self {
            degree,
            lanes: None,
            window: None,
            read_ahead_bytes: None,
        }
    }
}

/// How a row group is encoded.
pub(super) struct RowGroupPlan {
    /// The number of tasks that encode the columns of the row group.
    /// The writer task encodes the columns itself when this is zero.
    pub lanes: usize,
    /// The number of row groups of the file that are encoded at the same time,
    /// including this row group.
    pub window: usize,
}

/// What a writer task may spend on encoding. This is shared by the files of the task.
pub struct EncodeResources {
    settings: EncodeSettings,
    /// The size of the input that may wait to be encoded, in permits.
    read_ahead: Arc<Semaphore>,
    read_ahead_permits: usize,
    /// The memory of the input that waits to be encoded.
    reservation: Arc<MemoryReservation>,
    elapsed_compute: Time,
}

impl EncodeResources {
    /// Creates the resources of a writer task.
    /// The reservation is used for the input that waits to be encoded.
    /// The lanes reserve their memory with the same memory consumer.
    pub fn new(
        settings: EncodeSettings,
        reservation: MemoryReservation,
        elapsed_compute: Time,
    ) -> Self {
        let bytes = settings
            .read_ahead_bytes
            .unwrap_or_else(|| settings.degree.saturating_mul(READ_AHEAD_BYTES_PER_DEGREE));
        let permits = bytes.div_ceil(BYTES_PER_PERMIT).clamp(1, u32::MAX as usize);
        Self {
            settings,
            read_ahead: Arc::new(Semaphore::new(permits)),
            read_ahead_permits: permits,
            reservation: Arc::new(reservation),
            elapsed_compute,
        }
    }

    /// Decides how a row group that is about to receive rows is encoded.
    pub(super) fn plan_row_group(&self, columns: usize) -> RowGroupPlan {
        let degree = self.settings.degree;
        if degree <= 1 || columns == 0 {
            return RowGroupPlan {
                lanes: 0,
                window: 1,
            };
        }
        let lanes = self.settings.lanes.unwrap_or(degree).clamp(1, columns);
        let window = self
            .settings
            .window
            .unwrap_or(WINDOW_MULTIPLIER * degree.div_ceil(lanes) + 1)
            .max(1);
        RowGroupPlan { lanes, window }
    }

    /// Waits until input of the given size may be added to the input
    /// that waits to be encoded. The input is counted until the returned
    /// value is dropped.
    ///
    /// Input that is larger than the whole budget is admitted when no other
    /// input is waiting, so that the write always makes progress.
    pub(super) async fn admit(&self, bytes: usize) -> Result<Admission> {
        let permits = bytes
            .div_ceil(BYTES_PER_PERMIT)
            .min(self.read_ahead_permits);
        let permit = Arc::clone(&self.read_ahead)
            .acquire_many_owned(permits as u32)
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))?;
        self.reservation.grow(bytes);
        Ok(Admission {
            bytes,
            reservation: Arc::clone(&self.reservation),
            _permit: permit,
        })
    }

    /// Creates a reservation for the memory of a lane.
    pub(super) fn new_reservation(&self) -> MemoryReservation {
        self.reservation.new_empty()
    }

    /// The size of the input that waits to be encoded.
    #[cfg(test)]
    pub(super) fn waiting_input_size(&self) -> usize {
        self.reservation.size()
    }

    /// The time that the task has spent on encoding.
    pub(super) fn elapsed_compute(&self) -> &Time {
        &self.elapsed_compute
    }
}

/// Input that waits to be encoded and is counted in the read-ahead budget
/// and in the memory reservation of the writer task.
pub(super) struct Admission {
    bytes: usize,
    reservation: Arc<MemoryReservation>,
    _permit: OwnedSemaphorePermit,
}

impl Drop for Admission {
    fn drop(&mut self) {
        self.reservation.shrink(self.bytes);
    }
}
