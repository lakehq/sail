use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::provider::{
    CatalogProvider, CreateDatabaseOptions, CreateTableOptions, CreateViewOptions,
    DropDatabaseOptions, DropTableOptions, DropViewOptions, Namespace,
};
use sail_common_datafusion::catalog::{DatabaseStatus, TableStatus};

/// An AWS Glue Data Catalog provider.
pub struct GlueCatalogProvider {
    name: String,
}

impl GlueCatalogProvider {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[async_trait::async_trait]
impl CatalogProvider for GlueCatalogProvider {
    fn get_name(&self) -> &str {
        &self.name
    }

    async fn create_database(
        &self,
        _database: &Namespace,
        _options: CreateDatabaseOptions,
    ) -> CatalogResult<DatabaseStatus> {
        Err(CatalogError::NotSupported(
            "AWS Glue catalog is not yet implemented".to_string(),
        ))
    }

    async fn get_database(&self, _database: &Namespace) -> CatalogResult<DatabaseStatus> {
        Err(CatalogError::NotSupported(
            "AWS Glue catalog is not yet implemented".to_string(),
        ))
    }

    async fn list_databases(
        &self,
        _prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<DatabaseStatus>> {
        Err(CatalogError::NotSupported(
            "AWS Glue catalog is not yet implemented".to_string(),
        ))
    }

    async fn drop_database(
        &self,
        _database: &Namespace,
        _options: DropDatabaseOptions,
    ) -> CatalogResult<()> {
        Err(CatalogError::NotSupported(
            "AWS Glue catalog is not yet implemented".to_string(),
        ))
    }

    async fn create_table(
        &self,
        _database: &Namespace,
        _table: &str,
        _options: CreateTableOptions,
    ) -> CatalogResult<TableStatus> {
        Err(CatalogError::NotSupported(
            "AWS Glue catalog is not yet implemented".to_string(),
        ))
    }

    async fn get_table(&self, _database: &Namespace, _table: &str) -> CatalogResult<TableStatus> {
        Err(CatalogError::NotSupported(
            "AWS Glue catalog is not yet implemented".to_string(),
        ))
    }

    async fn list_tables(&self, _database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        Err(CatalogError::NotSupported(
            "AWS Glue catalog is not yet implemented".to_string(),
        ))
    }

    async fn drop_table(
        &self,
        _database: &Namespace,
        _table: &str,
        _options: DropTableOptions,
    ) -> CatalogResult<()> {
        Err(CatalogError::NotSupported(
            "AWS Glue catalog is not yet implemented".to_string(),
        ))
    }

    async fn create_view(
        &self,
        _database: &Namespace,
        _view: &str,
        _options: CreateViewOptions,
    ) -> CatalogResult<TableStatus> {
        Err(CatalogError::NotSupported(
            "AWS Glue catalog is not yet implemented".to_string(),
        ))
    }

    async fn get_view(&self, _database: &Namespace, _view: &str) -> CatalogResult<TableStatus> {
        Err(CatalogError::NotSupported(
            "AWS Glue catalog is not yet implemented".to_string(),
        ))
    }

    async fn list_views(&self, _database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        Err(CatalogError::NotSupported(
            "AWS Glue catalog is not yet implemented".to_string(),
        ))
    }

    async fn drop_view(
        &self,
        _database: &Namespace,
        _view: &str,
        _options: DropViewOptions,
    ) -> CatalogResult<()> {
        Err(CatalogError::NotSupported(
            "AWS Glue catalog is not yet implemented".to_string(),
        ))
    }
}
