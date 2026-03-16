mod namespace;
mod options;
mod runtime;
mod transaction;

use std::sync::Arc;

pub use namespace::*;
pub use options::*;
pub use runtime::*;
use sail_common::spec::AlterTableOperation;
use sail_common_datafusion::catalog::{DatabaseStatus, TableStatus};
pub use transaction::*;

use crate::error::{CatalogError, CatalogResult};

/// A trait that defines the interface for a catalog.
/// A catalog contains *databases*, where each database has a multi-level name
/// that represents a *namespace*.
/// A database contains *objects* such as *tables* and *views*.
#[async_trait::async_trait]
pub trait CatalogProvider: Send + Sync {
    /// The name of the catalog in the session.
    /// Note that the same catalog can be registered under different names
    /// in different sessions.
    fn get_name(&self) -> &str;

    /// Creates a new database in the catalog.
    async fn create_database(
        &self,
        database: &Namespace,
        options: CreateDatabaseOptions,
    ) -> CatalogResult<DatabaseStatus>;

    /// Gets the status of a database in the catalog.
    async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus>;

    /// Lists all databases in the catalog.
    /// If `prefix` is provided, only databases whose namespace starts with the prefix
    /// are returned.
    async fn list_databases(
        &self,
        prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<DatabaseStatus>>;

    /// Drops a database in the catalog.
    async fn drop_database(
        &self,
        database: &Namespace,
        options: DropDatabaseOptions,
    ) -> CatalogResult<()>;

    /// Creates a table in the catalog.
    async fn create_table(
        &self,
        database: &Namespace,
        table: &str,
        options: CreateTableOptions,
    ) -> CatalogResult<TableStatus>;

    /// Gets the status of a table in the catalog.
    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus>;

    /// Lists all tables in a database in the catalog.
    async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>>;

    /// Drops a table in the catalog.
    async fn drop_table(
        &self,
        database: &Namespace,
        table: &str,
        options: DropTableOptions,
    ) -> CatalogResult<()>;

    /// Creates a view in the catalog.
    async fn create_view(
        &self,
        database: &Namespace,
        view: &str,
        options: CreateViewOptions,
    ) -> CatalogResult<TableStatus>;

    /// Gets the status of a view in the catalog.
    async fn get_view(&self, database: &Namespace, view: &str) -> CatalogResult<TableStatus>;

    /// Lists all views in a database in the catalog.
    async fn list_views(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>>;

    /// Drops a view in the catalog.
    async fn drop_view(
        &self,
        database: &Namespace,
        view: &str,
        options: DropViewOptions,
    ) -> CatalogResult<()>;

    /// Opens a catalog-managed table commit.
    async fn begin_table_commit(
        &self,
        database: &Namespace,
        table: &str,
    ) -> CatalogResult<Arc<dyn TableCommitter>> {
        let _ = (database, table);
        Err(CatalogError::NotSupported(format!(
            "table commit is not supported for catalog '{}'",
            self.get_name()
        )))
    }

    /// Applies a catalog-managed ALTER TABLE operation and returns the updated table status.
    async fn alter_table(
        &self,
        database: &Namespace,
        table: &str,
        operation: AlterTableOperation,
    ) -> CatalogResult<TableStatus> {
        let _ = (database, table, operation);
        Err(CatalogError::NotSupported(format!(
            "ALTER TABLE is not supported for catalog '{}'",
            self.get_name()
        )))
    }
}
