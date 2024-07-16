use std::sync::Arc;

use datafusion::prelude::SessionContext;
use datafusion_common::{Result, SchemaReference, TableReference};
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
