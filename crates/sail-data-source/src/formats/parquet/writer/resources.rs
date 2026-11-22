// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};

use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::parquet::errors::{ParquetError, Result};
use datafusion::physical_plan::metrics::Time;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// The size of the input that may wait to be encoded, for each unit of the degree.
const READ_AHEAD_BYTES_PER_DEGREE: usize = 16 * 1024 * 1024;

/// The number of row groups that are encoded at the same time for each
/// set of lanes that the degree has room for, in addition to the row group
/// that receives rows.
///
/// A file with few columns has few lanes for each row group, so its row groups
/// must overlap for the encoding to use the degree. The value comes from
/// measurement: writes of tables with few columns were faster with 3 than
/// with 1 or 2, and writes of tables with many columns did not change.
const WINDOW_MULTIPLIER: usize = 3;

/// The number of bytes that one permit of the read-ahead budget stands for.
const BYTES_PER_PERMIT: usize = 1024;

/// The number of writer tasks in this process that are reading their input.
/// The writer tasks share the threads of the runtime, whichever query
/// the tasks belong to.
static ACTIVE_WRITERS: LazyLock<Arc<AtomicUsize>> = LazyLock::new(|| Arc::new(AtomicUsize::new(0)));

/// A writer task that is counted as active until this is dropped.
pub(super) struct ActiveWriter(Arc<AtomicUsize>);

impl ActiveWriter {
    pub fn new(active_writers: &Arc<AtomicUsize>) -> Self {
        active_writers.fetch_add(1, Ordering::Relaxed);
        Self(Arc::clone(active_writers))
    }
}

impl Drop for ActiveWriter {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// How the files of a writer task are encoded.
#[derive(Debug, Clone)]
pub struct EncodeSettings {
    /// The largest number of threads that the task may keep busy with encoding,
    /// or zero when there is no limit. The files are encoded by the writer task
    /// itself when this is one.
    ///
    /// The threads are divided among the active writer tasks each time a row
    /// group starts to receive rows, so a task uses more threads after other
    /// tasks have ended.
    pub max_degree: usize,
    /// The number of threads that the active writer tasks share.
    /// This is the number of worker threads of the runtime when it is not set.
    pub threads: Option<usize>,
    /// The number of active writer tasks that share the threads.
    pub active_writers: Arc<AtomicUsize>,
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
    /// The task shares the threads of the runtime with the other active
    /// writer tasks in this process, and it keeps at most the given number
    /// of threads busy with encoding. Zero means that there is no limit.
    pub fn new(max_degree: usize) -> Self {
        Self {
            max_degree,
            threads: None,
            active_writers: Arc::clone(&ACTIVE_WRITERS),
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
    /// The number of threads that the active writer tasks share.
    threads: usize,
    /// The number of files of the task that are being encoded.
    open_files: AtomicUsize,
    /// The size of the input that may wait to be encoded, in permits.
    /// Only the writer task takes permits and changes the number of permits.
    read_ahead: Arc<Semaphore>,
    read_ahead_permits: AtomicUsize,
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
        let threads = settings.threads.unwrap_or_else(|| {
            tokio::runtime::Handle::try_current().map_or(1, |x| x.metrics().num_workers())
        });
        let permits = read_ahead_permits(
            settings
                .read_ahead_bytes
                .unwrap_or(READ_AHEAD_BYTES_PER_DEGREE),
        );
        Self {
            settings,
            threads,
            open_files: AtomicUsize::new(0),
            read_ahead: Arc::new(Semaphore::new(permits)),
            read_ahead_permits: AtomicUsize::new(permits),
            reservation: Arc::new(reservation),
            elapsed_compute,
        }
    }

    /// Counts a file of the task as being encoded until the returned value is dropped.
    pub(super) fn open_file(self: &Arc<Self>) -> OpenFile {
        self.open_files.fetch_add(1, Ordering::Relaxed);
        OpenFile(Arc::clone(self))
    }

    /// The number of threads that the task may keep busy with encoding at this time.
    fn task_degree(&self) -> usize {
        let active_writers = self.settings.active_writers.load(Ordering::Relaxed);
        let degree = self.threads / active_writers.max(1);
        match self.settings.max_degree {
            0 => degree,
            max_degree => degree.min(max_degree),
        }
    }

    /// Changes the read-ahead budget to the budget for the degree of the task.
    /// Permits that are in use are not taken away, so the budget can stay
    /// higher until the next change.
    fn set_read_ahead(&self, degree: usize) {
        if self.settings.read_ahead_bytes.is_some() {
            return;
        }
        let target = read_ahead_permits(degree.max(1).saturating_mul(READ_AHEAD_BYTES_PER_DEGREE));
        let current = self.read_ahead_permits.load(Ordering::Relaxed);
        if target > current {
            self.read_ahead.add_permits(target - current);
            self.read_ahead_permits.store(target, Ordering::Relaxed);
        } else if target < current {
            let removed = self.read_ahead.forget_permits(current - target);
            self.read_ahead_permits
                .store(current - removed, Ordering::Relaxed);
        }
    }

    /// Decides how a row group that is about to receive rows is encoded.
    ///
    /// The degree of the task is divided among the files of the task that are
    /// being encoded. The files that are already open keep their lanes until
    /// their next row group, so more threads than the degree can be busy for
    /// a short time after a file is opened or another writer task has started.
    pub(super) fn plan_row_group(&self, columns: usize) -> RowGroupPlan {
        let degree = self.task_degree();
        self.set_read_ahead(degree);
        let degree = degree / self.open_files.load(Ordering::Relaxed).max(1);
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
            .min(self.read_ahead_permits.load(Ordering::Relaxed));
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

    /// The size of the read-ahead budget.
    #[cfg(test)]
    pub(super) fn read_ahead_size(&self) -> usize {
        self.read_ahead_permits.load(Ordering::Relaxed) * BYTES_PER_PERMIT
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

/// The number of permits for a read-ahead budget of the given size.
fn read_ahead_permits(bytes: usize) -> usize {
    bytes.div_ceil(BYTES_PER_PERMIT).clamp(1, u32::MAX as usize)
}

/// A file of a writer task that is counted as being encoded until this is dropped.
pub(super) struct OpenFile(Arc<EncodeResources>);

impl Drop for OpenFile {
    fn drop(&mut self) {
        self.0.open_files.fetch_sub(1, Ordering::Relaxed);
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
