mod cache;
mod namespace;
mod options;
mod runtime;

pub use cache::*;
pub use namespace::*;
pub use options::*;
pub use runtime::*;
use sail_common_datafusion::catalog::{DatabaseStatus, TableStatus};

use crate::error::CatalogResult;

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

    /// Whether Spark session-catalog semantics should synthesize a default
    /// database location when `CREATE DATABASE` omits `LOCATION`.
    ///
    /// This models planner behavior rather than a raw backend API requirement:
    /// some catalogs accept an unset location but Spark still manufactures one
    /// before delegating the operation.
    fn uses_spark_default_database_location(&self) -> bool {
        false
    }

    /// Whether Spark session-catalog semantics should synthesize a default
    /// table location when table creation omits an explicit storage location.
    ///
    /// Native catalogs own absent table-location semantics and should use the
    /// default `false`.
    fn uses_spark_default_table_location(&self) -> bool {
        false
    }

    /// Whether Sail should validate table identifiers before deriving a default
    /// table location from them.
    ///
    /// Catalogs that own their own namespace/table location semantics should
    /// return `false`.
    fn requires_identifier_validation_for_default_table_location(&self) -> bool {
        false
    }

    /// Whether Sail should apply Spark session-catalog qualification rules to
    /// explicit table locations before passing them to the catalog.
    ///
    /// V2/native catalogs own relative location semantics, so they should use
    /// the default `false` and receive relative paths unchanged.
    fn uses_spark_table_location_qualification(&self) -> bool {
        false
    }

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

    /// Alters a table in the catalog.
    async fn alter_table(
        &self,
        database: &Namespace,
        table: &str,
        options: AlterTableOptions,
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
}
