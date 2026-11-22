// SPDX-License-Identifier: Apache-2.0

use std::num::NonZeroUsize;

use crate::extension::SessionExtension;

/// The values of the Parquet write options that a write uses
/// when the write does not set the options.
#[derive(Debug)]
pub struct ParquetWriteConfig {
    encoding_parallelism: usize,
    max_open_files_per_task: NonZeroUsize,
}

impl ParquetWriteConfig {
    pub fn new(encoding_parallelism: usize, max_open_files_per_task: NonZeroUsize) -> Self {
        Self {
            encoding_parallelism,
            max_open_files_per_task,
        }
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
