use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::prelude::SessionContext;
use datafusion_common::{Result, SchemaReference, TableReference};
use serde::{Deserialize, Serialize};

#[allow(clippy::module_inception)]
pub mod catalog;
pub mod column;
pub mod database;
pub mod function;
pub mod table;
pub mod utils;
pub mod view;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmptyMetadata {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SingleValueMetadata<T> {
    pub value: T,
}

pub trait CatalogManagerConfig: Send + Sync {
    // TODO: This is an intermediate solution.
    //   We may have make column/table/database/catalog metadata generic
    //   so that we can customize the output for various `SHOW` SQL statements.
    fn data_type_to_simple_string(&self, data_type: &DataType) -> Result<String>;

    fn global_temporary_database(&self) -> &str;
}

pub struct CatalogManager<'a> {
    ctx: &'a SessionContext,
    config: Arc<dyn CatalogManagerConfig>,
}

impl<'a> CatalogManager<'a> {
    pub fn new(ctx: &'a SessionContext, config: Arc<dyn CatalogManagerConfig>) -> Self {
        CatalogManager { ctx, config }
    }

    pub fn resolve_catalog_reference(&self, reference: Option<String>) -> Result<Arc<str>> {
        match reference {
            Some(catalog) => Ok(catalog.into()),
            None => Ok(self.default_catalog()?.into()),
        }
    }

    pub fn resolve_database_reference(
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

    pub fn resolve_table_reference(
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

    pub fn is_global_temporary_view_database(&self, database: &Option<SchemaReference>) -> bool {
        database.as_ref().is_some_and(|x| match x {
            SchemaReference::Bare { schema } => {
                schema.as_ref() == self.config.global_temporary_database()
            }
            SchemaReference::Full { .. } => false,
        })
    }
}
