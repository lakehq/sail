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
        // TODO: Implement full overwrite semantics (predicate/partition replaces, conflict checks,
        // delete manifests) instead of relying solely on SnapshotProducer for added data files.
        let snapshot_producer = SnapshotProducer::new(tx, vec![], None, None);
        snapshot_producer.commit(OverwriteOperation).await
    }
}
