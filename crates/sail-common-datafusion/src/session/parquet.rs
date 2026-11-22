// SPDX-License-Identifier: Apache-2.0

use std::num::NonZeroUsize;

use crate::extension::SessionExtension;

/// The values of the Parquet write options that a write uses
/// when the write does not set the options.
#[derive(Debug)]
pub struct ParquetWriteConfig {
    encoding_parallelism: usize,
    max_open_files_per_task: NonZeroUsize,
    max_records_per_file: i64,
    max_file_size: u64,
}

impl ParquetWriteConfig {
    pub fn new(encoding_parallelism: usize, max_open_files_per_task: NonZeroUsize) -> Self {
        Self {
            encoding_parallelism,
            max_open_files_per_task,
            max_records_per_file: 0,
            max_file_size: 0,
        }
    }

    /// Sets the limits on the rows and the size in bytes of each file.
    pub fn with_file_limits(mut self, max_records_per_file: i64, max_file_size: u64) -> Self {
        self.max_records_per_file = max_records_per_file;
        self.max_file_size = max_file_size;
        self
    }

    /// The maximum number of rows in each file.
    /// There is no limit when the value is zero or negative.
    pub fn max_records_per_file(&self) -> i64 {
        self.max_records_per_file
    }

    /// The target maximum size in bytes of each file,
    /// or `0` when there is no limit.
    pub fn max_file_size(&self) -> u64 {
        self.max_file_size
    }

    /// The maximum number of threads that one write task uses to encode files,
    /// or `0` when the number is chosen automatically.
    pub fn encoding_parallelism(&self) -> usize {
        self.encoding_parallelism
    }

    /// The maximum number of files that one write task keeps open
    /// when the data is written with partition columns.
    pub fn max_open_files_per_task(&self) -> NonZeroUsize {
        self.max_open_files_per_task
    }
}

impl SessionExtension for ParquetWriteConfig {
    fn name() -> &'static str {
        "ParquetWriteConfig"
    }
}
