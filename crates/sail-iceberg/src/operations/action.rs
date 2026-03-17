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
use sail_catalog::provider::{IcebergTableCommitPayload, TableCommitPayload};

use super::Transaction;
use crate::spec::{TableRequirement, TableUpdate};

#[derive(Debug, Clone)]
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

impl TryFrom<IcebergTableCommitPayload> for ActionCommit {
    type Error = String;

    fn try_from(value: IcebergTableCommitPayload) -> Result<Self, Self::Error> {
        let requirements = value
            .requirements
            .into_iter()
            .map(|requirement| {
                serde_json::from_value(requirement).map_err(|error| error.to_string())
            })
            .collect::<Result<Vec<TableRequirement>, _>>()?;
        let updates = value
            .updates
            .into_iter()
            .map(|update| serde_json::from_value(update).map_err(|error| error.to_string()))
            .collect::<Result<Vec<TableUpdate>, _>>()?;
        Ok(Self::new(updates, requirements))
    }
}

impl From<ActionCommit> for IcebergTableCommitPayload {
    #[expect(clippy::expect_used)]
    fn from(value: ActionCommit) -> Self {
        Self {
            requirements: value
                .requirements
                .into_iter()
                .map(|requirement| {
                    serde_json::to_value(requirement).expect("serialize iceberg requirement")
                })
                .collect(),
            updates: value
                .updates
                .into_iter()
                .map(|update| serde_json::to_value(update).expect("serialize iceberg update"))
                .collect(),
        }
    }
}

impl From<ActionCommit> for TableCommitPayload {
    fn from(value: ActionCommit) -> Self {
        TableCommitPayload::Iceberg(value.into())
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
