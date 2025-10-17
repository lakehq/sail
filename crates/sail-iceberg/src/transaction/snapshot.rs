use crate::spec::DataFile;

use super::{ActionCommit, Transaction};

pub trait SnapshotProduceOperation: Send + Sync {
    fn operation(&self) -> &'static str;
}

pub struct SnapshotProducer<'a> {
    pub tx: &'a Transaction,
    pub added_data_files: Vec<DataFile>,
}

impl<'a> SnapshotProducer<'a> {
    pub fn new(tx: &'a Transaction, added_data_files: Vec<DataFile>) -> Self {
        Self {
            tx,
            added_data_files,
        }
    }

    pub fn validate_added_data_files(&self, _files: &[DataFile]) -> Result<(), String> {
        Ok(())
    }

    pub async fn commit(self, _op: impl SnapshotProduceOperation) -> Result<ActionCommit, String> {
        Err("snapshot commit is not implemented yet".to_string())
    }
}
