mod namespace;
mod options;
mod status;

pub use namespace::*;
pub use options::*;
pub use status::*;

use crate::error::CatalogResult;

#[async_trait::async_trait]
pub trait CatalogProvider: Send + Sync {
    fn get_name(&self) -> &str;

    async fn create_database(
        &self,
        database: &Namespace,
        options: CreateDatabaseOptions,
    ) -> CatalogResult<DatabaseStatus>;

    async fn drop_database(
        &self,
        database: &Namespace,
        options: DropDatabaseOptions,
    ) -> CatalogResult<()>;

    async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus>;

    async fn list_databases(
        &self,
        prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<DatabaseStatus>>;

    async fn create_table(
        &self,
        database: &Namespace,
        table: &str,
        options: CreateTableOptions,
    ) -> CatalogResult<TableStatus>;

    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus>;

    async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>>;

    async fn drop_table(
        &self,
        database: &Namespace,
        table: &str,
        options: DropTableOptions,
    ) -> CatalogResult<()>;

    async fn create_view(
        &self,
        database: &Namespace,
        view: &str,
        options: CreateViewOptions,
    ) -> CatalogResult<TableStatus>;

    async fn get_view(&self, database: &Namespace, view: &str) -> CatalogResult<TableStatus>;

    async fn list_views(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>>;

    async fn drop_view(
        &self,
        database: &Namespace,
        view: &str,
        options: DropViewOptions,
    ) -> CatalogResult<()>;
}
