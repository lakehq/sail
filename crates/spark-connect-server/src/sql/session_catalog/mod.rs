use datafusion::execution::context::SessionState;
use datafusion::prelude::SessionContext;
use datafusion_common::exec_datafusion_err;
use datafusion_common::Result;
use serde::{Deserialize, Serialize};

pub(crate) mod catalog;
pub(crate) mod column;
pub(crate) mod database;
pub(crate) mod function;
pub(crate) mod table;
pub(crate) mod view;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EmptyMetadata {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SingleValueMetadata<T> {
    pub(crate) value: T,
}

trait SessionContextExt {
    fn read_state<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&SessionState) -> Result<T>;

    fn write_state<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut SessionState) -> Result<T>;
}

impl SessionContextExt for SessionContext {
    fn read_state<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&SessionState) -> Result<T>,
    {
        let state_ref = self
            .state_weak_ref()
            .upgrade()
            .ok_or_else(|| exec_datafusion_err!("failed to read session context state"))?;
        let state = state_ref.read();
        f(&state)
    }

    fn write_state<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut SessionState) -> Result<T>,
    {
        let state_ref = self
            .state_weak_ref()
            .upgrade()
            .ok_or_else(|| exec_datafusion_err!("failed to write session context state"))?;
        let mut state = state_ref.write();
        f(&mut state)
    }
}

pub(crate) struct SessionCatalogContext<'a> {
    ctx: &'a SessionContext,
}

impl SessionCatalogContext<'_> {
    pub(crate) fn new(ctx: &SessionContext) -> SessionCatalogContext {
        SessionCatalogContext { ctx }
    }
}
