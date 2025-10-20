use std::sync::Arc;

use async_trait::async_trait;

use super::{
    ActionCommit, SnapshotProduceOperation, SnapshotProducer, Transaction, TransactionAction,
};

pub struct OverwriteAction;

impl Default for OverwriteAction {
    fn default() -> Self {
        Self::new()
    }
}

impl OverwriteAction {
    pub fn new() -> Self {
        Self
    }
}

struct OverwriteOperation;
impl SnapshotProduceOperation for OverwriteOperation {
    fn operation(&self) -> &'static str {
        "overwrite"
    }
}

#[async_trait]
impl TransactionAction for OverwriteAction {
    async fn commit(self: Arc<Self>, tx: &Transaction) -> Result<ActionCommit, String> {
        // In this simplified overwrite action, we rely on SnapshotProducer to generate
        // a new snapshot that references only the newly added data files.
        // The caller must have configured `added_data_files`, `store`, and `manifest_metadata`.
        let snapshot_producer = SnapshotProducer::new(tx, vec![], None, None);
        snapshot_producer.commit(OverwriteOperation).await
    }
}
