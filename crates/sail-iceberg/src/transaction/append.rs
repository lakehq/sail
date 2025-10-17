use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::spec::DataFile;

use super::{ActionCommit, SnapshotProduceOperation, SnapshotProducer, Transaction, TransactionAction};

pub struct FastAppendAction {
    check_duplicate: bool,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
}

impl FastAppendAction {
    pub fn new() -> Self {
        Self {
            check_duplicate: false,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::new(),
            added_data_files: Vec::new(),
        }
    }

    pub fn add_file(&mut self, file: DataFile) {
        self.added_data_files.push(file);
    }
}

#[async_trait]
impl TransactionAction for FastAppendAction {
    async fn commit(self: Arc<Self>, tx: &Transaction) -> Result<ActionCommit, String> {
        let snapshot_producer = SnapshotProducer::new(tx, self.added_data_files.clone());
        snapshot_producer.validate_added_data_files(&self.added_data_files)?;

        if self.check_duplicate {
            // TODO: validate duplicate files later
        }

        struct FastAppendOperation;
        impl SnapshotProduceOperation for FastAppendOperation {
            fn operation(&self) -> &'static str {
                "append"
            }
        }

        snapshot_producer.commit(FastAppendOperation).await
    }
}


