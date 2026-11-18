// SPDX-License-Identifier: Apache-2.0

pub mod arrow_parquet;
pub mod base_writer;
pub mod config;
pub mod file_writer;
pub mod metrics;
pub mod partition;
pub mod table_writer;
pub mod variant_shredding;

use crate::spec::DataFile;

pub trait IcebergWriter<T> {
    fn add(&mut self, data: T) -> Result<(), String>;
    fn close(self) -> Result<Vec<DataFile>, String>;
}

pub struct WriteOutcome {
    pub data_file: DataFile,
}
