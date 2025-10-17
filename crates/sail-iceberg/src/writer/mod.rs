pub mod base_writer;
pub mod file_writer;

use crate::spec::DataFile;

pub trait IcebergWriter<T> {
    fn add(&mut self, data: T) -> Result<(), String>;
    fn close(self) -> Result<Vec<DataFile>, String>;
}

pub struct WriteOutcome {
    pub data_file: DataFile,
}
