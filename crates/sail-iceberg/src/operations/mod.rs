// SPDX-License-Identifier: Apache-2.0

pub mod action;
pub mod append;
pub mod bootstrap;
pub mod helpers;
pub(crate) mod metadata_commit;
pub mod overwrite;
#[cfg(test)]
mod publication_tests;
pub mod replacement;
#[cfg(test)]
mod replacement_test_support;
pub mod snapshot;
pub mod write;

pub use action::*;
pub use append::*;
pub use bootstrap::*;
pub use overwrite::*;
pub use snapshot::*;

use crate::spec::{FormatVersion, Snapshot};

pub struct Transaction {
    table_uri: String,
    snapshot: Snapshot,
    last_sequence_number: i64,
    actions: Vec<std::sync::Arc<dyn action::TransactionAction>>,
}

impl Transaction {
    pub fn new(table_uri: String, snapshot: Snapshot, last_sequence_number: i64) -> Self {
        Self {
            table_uri,
            snapshot,
            last_sequence_number,
            actions: Vec::new(),
        }
    }

    pub fn table_uri(&self) -> &str {
        &self.table_uri
    }

    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    pub fn next_sequence_number(&self, format_version: FormatVersion) -> Result<i64, String> {
        if format_version == FormatVersion::V1 {
            return Ok(0);
        }
        self.last_sequence_number.checked_add(1).ok_or_else(|| {
            "cannot allocate Iceberg snapshot sequence number: table sequence is exhausted"
                .to_string()
        })
    }

    pub async fn commit(self, _summary_op: &str) -> Result<Snapshot, String> {
        Err("commit is not implemented yet".to_string())
    }

    pub fn overwrite(&self) -> OverwriteAction {
        OverwriteAction::new()
    }
}
