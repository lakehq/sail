use std::sync::Arc;

use async_trait::async_trait;

use crate::spec::{TableRequirement, TableUpdate};

use super::Transaction;

pub struct ActionCommit {
    updates: Vec<TableUpdate>,
    requirements: Vec<TableRequirement>,
}

impl ActionCommit {
    pub fn new(updates: Vec<TableUpdate>, requirements: Vec<TableRequirement>) -> Self {
        Self { updates, requirements }
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


