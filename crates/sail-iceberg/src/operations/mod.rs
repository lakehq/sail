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

    pub fn overwrite(&self) -> OverwriteAction {
        OverwriteAction::new()
    }
}
