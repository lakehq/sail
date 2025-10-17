use std::sync::Arc;

use super::{ActionCommit, FastAppendAction, Transaction, TransactionAction};

impl Transaction {
    pub fn fast_append(&self) -> FastAppendAction {
        FastAppendAction::new()
    }
}
