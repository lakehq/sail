pub mod action;
pub mod append;
pub mod helpers;
pub mod snapshot;

pub use action::*;
pub use append::*;
pub use snapshot::*;

use crate::spec::Snapshot;

pub struct Transaction {
    table_uri: String,
    snapshot: Snapshot,
    actions: Vec<std::sync::Arc<dyn action::TransactionAction>>,
}

impl Transaction {
    pub fn new(table_uri: String, snapshot: Snapshot) -> Self {
        Self {
            table_uri,
            snapshot,
            actions: Vec::new(),
        }
    }

    pub fn table_uri(&self) -> &str {
        &self.table_uri
    }

    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    pub async fn commit(self, _summary_op: &str) -> Result<Snapshot, String> {
        Err("commit is not implemented yet".to_string())
    }
}
