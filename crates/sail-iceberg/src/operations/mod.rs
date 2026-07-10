// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod action;
pub mod append;
pub mod bootstrap;
pub mod helpers;
pub mod overwrite;
pub mod snapshot;
pub mod write;

pub use action::*;
pub use append::*;
pub use bootstrap::*;
pub use overwrite::*;
pub use snapshot::*;

use crate::spec::Snapshot;

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

    pub fn next_sequence_number(&self) -> Result<i64, String> {
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

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::spec::{Operation, SnapshotBuilder};

    #[test]
    fn transaction_allocates_sequence_after_table_sequence() {
        let parent_snapshot = SnapshotBuilder::new()
            .with_snapshot_id(7)
            .with_sequence_number(3)
            .with_manifest_list("metadata/snap-7.avro".to_string())
            .with_summary(crate::spec::snapshots::Summary::new(Operation::Append))
            .build()
            .expect("parent snapshot");
        let transaction = Transaction::new("file:///tmp/table".to_string(), parent_snapshot, 11);

        assert_eq!(transaction.next_sequence_number(), Ok(12));
    }

    #[test]
    fn transaction_rejects_sequence_number_overflow() {
        let parent_snapshot = SnapshotBuilder::new()
            .with_snapshot_id(7)
            .with_sequence_number(3)
            .with_manifest_list("metadata/snap-7.avro".to_string())
            .with_summary(crate::spec::snapshots::Summary::new(Operation::Append))
            .build()
            .expect("parent snapshot");
        let transaction =
            Transaction::new("file:///tmp/table".to_string(), parent_snapshot, i64::MAX);

        assert!(transaction.next_sequence_number().is_err());
    }
}
