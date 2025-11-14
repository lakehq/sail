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

use super::Transaction;
use crate::spec::{TableRequirement, TableUpdate};

pub struct ActionCommit {
    updates: Vec<TableUpdate>,
    requirements: Vec<TableRequirement>,
}

impl ActionCommit {
    pub fn new(updates: Vec<TableUpdate>, requirements: Vec<TableRequirement>) -> Self {
        Self {
            updates,
            requirements,
        }
    }

    pub fn updates(&self) -> &[TableUpdate] {
        &self.updates
    }

    pub fn into_updates(self) -> Vec<TableUpdate> {
        self.updates
    }

    pub fn requirements(&self) -> &[TableRequirement] {
        &self.requirements
    }
}

#[async_trait]
pub trait TransactionAction: Send + Sync {
    async fn commit(self: Arc<Self>, _tx: &Transaction) -> Result<ActionCommit, String>;
}

pub trait ApplyTransactionAction {
    fn apply(self, tx: &mut Transaction);
}

impl<T: TransactionAction + 'static> ApplyTransactionAction for T {
    fn apply(self, tx: &mut Transaction) {
        tx.actions.push(Arc::new(self));
    }
}
