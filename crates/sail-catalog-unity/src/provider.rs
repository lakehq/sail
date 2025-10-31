// CHECK HERE
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(clippy::todo)]

use std::collections::HashMap;
use std::sync::Arc;

use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::provider::{
    CatalogProvider, CatalogTableConstraint, CatalogTableSort, CreateDatabaseOptions,
    CreateTableColumnOptions, CreateTableOptions, CreateViewColumnOptions, CreateViewOptions,
    DatabaseStatus, DropDatabaseOptions, DropTableOptions, DropViewOptions, Namespace,
    TableColumnStatus, TableKind, TableStatus,
};
use tokio::sync::OnceCell;

/// Provider for Unity Catalog
pub struct UnityCatalogProvider {
    name: String,
}

impl UnityCatalogProvider {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[async_trait::async_trait]
impl CatalogProvider for UnityCatalogProvider {
    fn get_name(&self) -> &str {
        &self.name
    }

    async fn create_database(
        &self,
        database: &Namespace,
        options: CreateDatabaseOptions,
    ) -> CatalogResult<DatabaseStatus> {
        todo!()
    }

    async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus> {
        todo!()
    }

    async fn list_databases(
        &self,
        prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<DatabaseStatus>> {
        todo!()
    }

    async fn drop_database(
        &self,
        database: &Namespace,
        options: DropDatabaseOptions,
    ) -> CatalogResult<()> {
        todo!()
    }

    async fn create_table(
        &self,
        database: &Namespace,
        table: &str,
        options: CreateTableOptions,
    ) -> CatalogResult<TableStatus> {
        todo!()
    }

    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus> {
        todo!()
    }

    async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        todo!()
    }

    async fn drop_table(
        &self,
        database: &Namespace,
        table: &str,
        options: DropTableOptions,
    ) -> CatalogResult<()> {
        todo!()
    }

    async fn create_view(
        &self,
        database: &Namespace,
        view: &str,
        options: CreateViewOptions,
    ) -> CatalogResult<TableStatus> {
        todo!()
    }

    async fn get_view(&self, database: &Namespace, view: &str) -> CatalogResult<TableStatus> {
        todo!()
    }

    async fn list_views(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        todo!()
    }

    async fn drop_view(
        &self,
        database: &Namespace,
        view: &str,
        options: DropViewOptions,
    ) -> CatalogResult<()> {
        todo!()
    }
}
