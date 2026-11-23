// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::parquet::errors::{ParquetError, Result};
use datafusion::physical_plan::metrics::Time;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

/// The largest size of the input that the jobs of a row group receive at a time.
///
/// A job encodes the values of one column for a whole slab before its thread
/// encodes another column, which is faster than encoding the columns batch
/// by batch, and more so the more values of a column a slab has. Measured with
/// 48 files that were written at the same time on 48 threads with Zstandard:
/// a table with 16 columns took the same time with slabs of 16 to 128 MiB,
/// and a table with 121 columns took 8.26 s with 16 MiB, 7.6 s with 32 MiB,
/// 7.05 s with 64 MiB and 6.85 s with 128 MiB. This is the smallest of these
/// sizes that is within 3% of the best time for both tables.
const SLAB_BYTES: usize = 64 * 1024 * 1024;

/// The size of the input that the job of a column receives at a time, which
/// gives the size of a slab for a file with fewer than 64 columns.
///
/// A file keeps the rows of a slab before it starts their jobs, so a large
/// slab is memory for every file that receives rows, and the 121 columns
/// above have half a megabyte each in a slab. Measured with 600 million rows
/// of 4 columns that were written to 4 and to 100 partition directories:
/// 4.91 s with 10.7 GiB and 5.91 s with 11.6 GiB with slabs of 64 MiB, and
/// 4.77 s with 3.5 GiB and 5.82 s with 12.3 GiB with this size for each column,
/// which became 5.97 s with 7.1 GiB with the limit on the jobs that wait.
/// 60 million rows of 16 columns took 1.31 s and 1.30 s in one file,
/// and 1.98 s and 2.01 s in 48 files.
const COLUMN_SLAB_BYTES: usize = 1024 * 1024;

/// The number of slabs in the encode budget for each thread of the runtime:
/// one slab that the thread is encoding and one slab that is waiting for it.
/// Measured in the same way: with one and two slabs for each thread,
/// the table with 16 columns took 6.28 s and 6.29 s, and the table
/// with 121 columns took 7.22 s and 6.67 s.
const SLABS_PER_THREAD: usize = 2;

/// The number of jobs that encode at the same time for each thread of the runtime.
///
/// A file has a job for each of its columns, so the files of a process can
/// have many more jobs than the runtime has threads, and a task of the runtime
/// then runs once in the time that all these jobs take for a time slice.
/// Measured with 48 files of 121 columns that were written at the same time
/// on 48 threads, as the time of the write and the delay of a timer of 1 ms
/// on the same runtime at the 99th percentile: 7.05 s and 592 ms without
/// a limit, and 6.69 s and 32 ms with one job for each thread. A job passes
/// its turn to the next job of its column. Without that, the write took
/// 7.74 s with one job and 7.79 s with two jobs for each thread.
const JOBS_PER_THREAD: usize = 1;

/// The number of jobs that may wait for their turn to encode for each job
/// that may encode at the same time. The files start no more jobs while
/// this number of jobs is waiting, so that a job that ends is followed by
/// a job that waits, and the rows and the column writers of more jobs than
/// that are not kept in memory.
///
/// Measured as the time of a write and the peak memory of the process with
/// 1, 4 and 16 waiting jobs and without a limit: 60 million rows of 16
/// columns in one file 1.27 s with 7.4 GiB, 1.28 s with 8.9 GiB, 1.30 s with
/// 11.7 GiB, and 1.32 s with 11.9 GiB, the same rows in 48 files 2.14 s,
/// 2.13 s, 2.00 s and 2.01 s with 8.6 to 8.7 GiB, and 600 million rows of
/// 4 columns in 100 partition directories 5.97 s with 7.1 GiB, 5.96 s with
/// 7.2 GiB, 5.82 s with 12.1 GiB, and 5.85 s with 12.1 GiB.
const QUEUE_PER_JOB: usize = 1;

/// The memory that the Parquet files of this process may use for their input
/// and for their row groups that are encoded and not in a file.
///
/// The budget counts the rows that a file has received and not encoded,
/// and the encoded data of a row group from the time when the row group
/// has all its rows until the row group is in the file. A file that cannot
/// take from the budget encodes its rows without keeping them, so no file
/// waits for another file.
#[derive(Debug)]
pub struct EncodeBudget {
    capacity: usize,
    /// The largest size of the input that the jobs of a row group receive at a time.
    slab_bytes: usize,
    /// The size of the input that the job of a column receives at a time.
    column_slab_bytes: usize,
    used: AtomicUsize,
    /// The jobs of all files that may encode at the same time.
    jobs: Arc<Semaphore>,
    /// The number of the jobs of all files that wait for their turn to encode.
    waiting: AtomicUsize,
    /// The number of waiting jobs at which the files start no more jobs.
    queue: usize,
    /// Wakes a file that waits to start jobs when a job has had its turn.
    turn: Notify,
}

/// Counts a job as waiting for its turn to encode until this is dropped.
struct Waiting<'a>(&'a EncodeBudget);

impl<'a> Waiting<'a> {
    fn new(budget: &'a EncodeBudget) -> Self {
        budget.waiting.fetch_add(1, Ordering::AcqRel);
        Self(budget)
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.waiting.fetch_sub(1, Ordering::AcqRel);
        self.0.turn.notify_one();
    }
}

static PROCESS_BUDGET: OnceLock<Arc<EncodeBudget>> = OnceLock::new();

/// The number of worker threads of the runtime of the caller.
fn runtime_threads() -> usize {
    tokio::runtime::Handle::try_current()
        .map(|x| x.metrics().num_workers())
        .or_else(|_| std::thread::available_parallelism().map(|x| x.get()))
        .unwrap_or(1)
        .max(1)
}

impl EncodeBudget {
    /// Creates a budget of the given size for files whose jobs run
    /// on the runtime of the caller.
    pub fn new(capacity: usize) -> Arc<Self> {
        Self::with_slabs(capacity, SLAB_BYTES, COLUMN_SLAB_BYTES)
    }

    /// Creates a budget for files whose jobs receive input of the given size
    /// at a time, whatever the number of the columns of a file.
    #[cfg(test)]
    pub(super) fn with_slab_bytes(capacity: usize, slab_bytes: usize) -> Arc<Self> {
        Self::with_slabs(capacity, slab_bytes, slab_bytes)
    }

    fn with_slabs(capacity: usize, slab_bytes: usize, column_slab_bytes: usize) -> Arc<Self> {
        let jobs = runtime_threads()
            .saturating_mul(JOBS_PER_THREAD)
            .min(Semaphore::MAX_PERMITS);
        Arc::new(Self {
            capacity,
            slab_bytes,
            column_slab_bytes,
            used: AtomicUsize::new(0),
            jobs: Arc::new(Semaphore::new(jobs)),
            waiting: AtomicUsize::new(0),
            queue: jobs.saturating_mul(QUEUE_PER_JOB),
            turn: Notify::new(),
        })
    }

    /// The budget that all files of this process share. Its size follows from
    /// the number of worker threads of the runtime that calls this first.
    pub fn of_process() -> Arc<Self> {
        Arc::clone(PROCESS_BUDGET.get_or_init(|| {
            Self::new(
                runtime_threads()
                    .saturating_mul(SLABS_PER_THREAD)
                    .saturating_mul(SLAB_BYTES),
            )
        }))
    }

    /// The size of the budget.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The size of the input that the jobs of a row group of a file
    /// with the given number of columns receive at a time.
    fn slab_bytes(&self, columns: usize) -> usize {
        self.slab_bytes
            .min(columns.max(1).saturating_mul(self.column_slab_bytes))
    }

    /// The part of the budget that is in use. This can be more than the size
    /// of the budget, since encoded row groups are always counted.
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    /// Takes the bytes from the budget if the budget has them.
    fn try_take(&self, bytes: usize) -> bool {
        self.used
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(bytes).filter(|x| *x <= self.capacity)
            })
            .is_ok()
    }

    fn take(&self, bytes: usize) {
        self.used.fetch_add(bytes, Ordering::Relaxed);
    }

    fn give(&self, bytes: usize) {
        self.used.fetch_sub(bytes, Ordering::Relaxed);
    }
}

/// What the files of a writer task share for encoding.
pub struct EncodeResources {
    budget: Arc<EncodeBudget>,
    /// The memory that the files of the task have taken from the budget.
    reservation: Arc<MemoryReservation>,
    elapsed_compute: Time,
}

impl EncodeResources {
    /// Creates the resources of a writer task. The reservation shows
    /// the memory pool what the files of the task have taken from the budget.
    pub fn new(
        budget: Arc<EncodeBudget>,
        reservation: MemoryReservation,
        elapsed_compute: Time,
    ) -> Self {
        Self {
            budget,
            reservation: Arc::new(reservation),
            elapsed_compute,
        }
    }

    /// Takes the bytes from the budget and from the memory pool,
    /// or returns nothing if either does not have them.
    pub(super) fn try_charge(&self, bytes: usize) -> Option<Charge> {
        if !self.budget.try_take(bytes) {
            return None;
        }
        if self.reservation.try_grow(bytes).is_err() {
            self.budget.give(bytes);
            return None;
        }
        Some(self.charged(bytes))
    }

    /// Takes the bytes from the budget and from the memory pool
    /// whether they have them or not.
    pub(super) fn charge(&self, bytes: usize) -> Charge {
        self.budget.take(bytes);
        self.reservation.grow(bytes);
        self.charged(bytes)
    }

    fn charged(&self, bytes: usize) -> Charge {
        Charge {
            bytes,
            budget: Some(Arc::clone(&self.budget)),
            reservation: Arc::clone(&self.reservation),
        }
    }

    /// Returns a charge for memory that is taken from the memory pool only,
    /// whether the memory pool has it or not.
    pub(super) fn charge_outside_budget(&self) -> Charge {
        Charge {
            bytes: 0,
            budget: None,
            reservation: Arc::clone(&self.reservation),
        }
    }

    /// The size of the input that the jobs of a row group of a file
    /// with the given number of columns receive at a time.
    pub(super) fn slab_bytes(&self, columns: usize) -> usize {
        self.budget.slab_bytes(columns)
    }

    /// Waits while the jobs of the process that wait for their turn to encode
    /// are as many as the queue allows. A file calls this before it starts
    /// jobs: more jobs would not be encoded sooner, and they would keep their
    /// rows and the column writers of their row group in memory while they
    /// wait. The jobs that are waited for need nothing but a thread, so they
    /// have their turn whatever the file that waits does.
    pub(super) async fn admit(&self) {
        loop {
            let turn = self.budget.turn.notified();
            if self.budget.waiting.load(Ordering::Acquire) < self.budget.queue {
                return;
            }
            turn.await;
        }
    }

    /// Waits until a job may encode. The job may encode until the returned
    /// value is dropped. A job must not wait for anything but its turn
    /// on a thread while it has the value.
    pub(super) async fn job(&self) -> Result<OwnedSemaphorePermit> {
        let _waiting = Waiting::new(&self.budget);
        Arc::clone(&self.budget.jobs)
            .acquire_owned()
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))
    }

    /// The memory that the files of the task have taken from the budget.
    #[cfg(test)]
    pub(super) fn charged_size(&self) -> usize {
        self.reservation.size()
    }

    /// The time that the task has spent on encoding.
    pub(super) fn elapsed_compute(&self) -> &Time {
        &self.elapsed_compute
    }
}

/// Memory that is taken from the memory pool, and from the budget
/// unless the memory is counted outside the budget, until this is dropped.
pub(super) struct Charge {
    bytes: usize,
    budget: Option<Arc<EncodeBudget>>,
    reservation: Arc<MemoryReservation>,
}

impl Charge {
    /// Moves the memory of the other charge of the same kind to this charge.
    pub fn merge(&mut self, mut other: Charge) {
        self.bytes += std::mem::take(&mut other.bytes);
    }

    /// Moves at most the given number of bytes to a new charge.
    pub fn split(&mut self, bytes: usize) -> Charge {
        let bytes = bytes.min(self.bytes);
        self.bytes -= bytes;
        Charge {
            bytes,
            budget: self.budget.clone(),
            reservation: Arc::clone(&self.reservation),
        }
    }

    /// Changes the size of the memory, whether the memory is available or not.
    pub fn resize(&mut self, bytes: usize) {
        if bytes > self.bytes {
            if let Some(budget) = &self.budget {
                budget.take(bytes - self.bytes);
            }
            self.reservation.grow(bytes - self.bytes);
        } else if bytes < self.bytes {
            if let Some(budget) = &self.budget {
                budget.give(self.bytes - bytes);
            }
            self.reservation.shrink(self.bytes - bytes);
        }
        self.bytes = bytes;
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.resize(0);
    }
}
