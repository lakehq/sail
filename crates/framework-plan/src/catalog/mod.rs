use std::sync::Arc;

use datafusion::execution::context::SessionState;
use datafusion::prelude::SessionContext;
use datafusion_common::{exec_datafusion_err, Result, SchemaReference, TableReference};
use serde::{Deserialize, Serialize};

use crate::config::PlanConfig;

#[allow(clippy::module_inception)]
pub(crate) mod catalog;
pub(crate) mod column;
pub(crate) mod database;
pub(crate) mod function;
pub(crate) mod table;
pub(crate) mod utils;
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

pub(crate) struct CatalogManager<'a> {
    ctx: &'a SessionContext,
    config: Arc<PlanConfig>,
}

impl<'a> CatalogManager<'a> {
    pub(crate) fn new(ctx: &'a SessionContext, config: Arc<PlanConfig>) -> Self {
        CatalogManager { ctx, config }
    }

    pub(crate) fn resolve_catalog_reference(&self, reference: Option<String>) -> Result<Arc<str>> {
        match reference {
            Some(catalog) => Ok(catalog.into()),
            None => Ok(self.default_catalog()?.into()),
        }
    }

    pub(crate) fn resolve_database_reference(
        &self,
        reference: Option<SchemaReference>,
    ) -> Result<(Arc<str>, Arc<str>)> {
        match reference {
            Some(SchemaReference::Bare { schema }) => Ok((self.default_catalog()?.into(), schema)),
            Some(SchemaReference::Full { catalog, schema }) => Ok((catalog, schema)),
            None => Ok((
                self.default_catalog()?.into(),
                self.default_database()?.into(),
            )),
        }
    }

    pub(crate) fn resolve_table_reference(
        &self,
        reference: TableReference,
    ) -> Result<(Arc<str>, Arc<str>, Arc<str>)> {
        match reference {
            TableReference::Bare { table } => Ok((
                self.default_catalog()?.into(),
                self.default_database()?.into(),
                table,
            )),
            TableReference::Partial { schema, table } => {
                Ok((self.default_catalog()?.into(), schema, table))
            }
            TableReference::Full {
                catalog,
                schema,
                table,
            } => Ok((catalog, schema, table)),
        }
    }
}
