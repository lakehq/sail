use std::sync::Arc;

use sail_common_datafusion::catalog::{DatabaseStatus, TableStatus};
use tokio::runtime::Handle;

use super::{
    CatalogProvider, CreateDatabaseOptions, CreateTableOptions, CreateViewOptions,
    DropDatabaseOptions, DropTableOptions, DropViewOptions, Namespace,
};
use crate::error::{CatalogError, CatalogResult};

pub struct RuntimeAwareCatalogProvider<P: CatalogProvider> {
    inner: Arc<P>,
    handle: Handle,
}

impl<P: CatalogProvider> RuntimeAwareCatalogProvider<P> {
    pub fn try_new<F>(initializer: F, handle: Handle) -> CatalogResult<Self>
    where
        F: FnOnce() -> CatalogResult<P>,
    {
        let _guard = handle.enter();
        let inner = Arc::new(initializer()?);
        Ok(Self { inner, handle })
    }
}

#[async_trait::async_trait]
impl<P: CatalogProvider + 'static> CatalogProvider for RuntimeAwareCatalogProvider<P> {
    fn get_name(&self) -> &str {
        self.inner.get_name()
    }

    async fn create_database(
        &self,
        database: &Namespace,
        options: CreateDatabaseOptions,
    ) -> CatalogResult<DatabaseStatus> {
        let inner = self.inner.clone();
        let database = database.clone();
        self.handle
            .spawn(async move { inner.create_database(&database, options).await })
            .await
            .map_err(|e| {
                CatalogError::External(format!("Failed to execute create_database: {e}"))
            })?
    }

    async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus> {
        let inner = self.inner.clone();
        let database = database.clone();
        self.handle
            .spawn(async move { inner.get_database(&database).await })
            .await
            .map_err(|e| CatalogError::External(format!("Failed to execute get_database: {e}")))?
    }

    async fn list_databases(
        &self,
        prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<DatabaseStatus>> {
        let inner = self.inner.clone();
        let prefix = prefix.cloned();
        self.handle
            .spawn(async move { inner.list_databases(prefix.as_ref()).await })
            .await
            .map_err(|e| CatalogError::External(format!("Failed to execute list_databases: {e}")))?
    }

    async fn drop_database(
        &self,
        database: &Namespace,
        options: DropDatabaseOptions,
    ) -> CatalogResult<()> {
        let inner = self.inner.clone();
        let database = database.clone();
        self.handle
            .spawn(async move { inner.drop_database(&database, options).await })
            .await
            .map_err(|e| CatalogError::External(format!("Failed to execute drop_database: {e}")))?
    }

    async fn create_table(
        &self,
        database: &Namespace,
        table: &str,
        options: CreateTableOptions,
    ) -> CatalogResult<TableStatus> {
        let inner = self.inner.clone();
        let database = database.clone();
        let table = table.to_string();
        self.handle
            .spawn(async move { inner.create_table(&database, &table, options).await })
            .await
            .map_err(|e| CatalogError::External(format!("Failed to execute create_table: {e}")))?
    }

    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus> {
        let inner = self.inner.clone();
        let database = database.clone();
        let table = table.to_string();
        self.handle
            .spawn(async move { inner.get_table(&database, &table).await })
            .await
            .map_err(|e| CatalogError::External(format!("Failed to execute get_table: {e}")))?
    }

    async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let inner = self.inner.clone();
        let database = database.clone();
        self.handle
            .spawn(async move { inner.list_tables(&database).await })
            .await
            .map_err(|e| CatalogError::External(format!("Failed to execute list_tables: {e}")))?
    }

    async fn drop_table(
        &self,
        database: &Namespace,
        table: &str,
        options: DropTableOptions,
    ) -> CatalogResult<()> {
        let inner = self.inner.clone();
        let database = database.clone();
        let table = table.to_string();
        self.handle
            .spawn(async move { inner.drop_table(&database, &table, options).await })
            .await
            .map_err(|e| CatalogError::External(format!("Failed to execute drop_table: {e}")))?
    }

    async fn create_view(
        &self,
        database: &Namespace,
        view: &str,
        options: CreateViewOptions,
    ) -> CatalogResult<TableStatus> {
        let inner = self.inner.clone();
        let database = database.clone();
        let view = view.to_string();
        self.handle
            .spawn(async move { inner.create_view(&database, &view, options).await })
            .await
            .map_err(|e| CatalogError::External(format!("Failed to execute create_view: {e}")))?
    }

    async fn get_view(&self, database: &Namespace, view: &str) -> CatalogResult<TableStatus> {
        let inner = self.inner.clone();
        let database = database.clone();
        let view = view.to_string();
        self.handle
            .spawn(async move { inner.get_view(&database, &view).await })
            .await
            .map_err(|e| CatalogError::External(format!("Failed to execute get_view: {e}")))?
    }

    async fn list_views(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let inner = self.inner.clone();
        let database = database.clone();
        self.handle
            .spawn(async move { inner.list_views(&database).await })
            .await
            .map_err(|e| CatalogError::External(format!("Failed to execute list_views: {e}")))?
    }

    async fn drop_view(
        &self,
        database: &Namespace,
        view: &str,
        options: DropViewOptions,
    ) -> CatalogResult<()> {
        let inner = self.inner.clone();
        let database = database.clone();
        let view = view.to_string();
        self.handle
            .spawn(async move { inner.drop_view(&database, &view, options).await })
            .await
            .map_err(|e| CatalogError::External(format!("Failed to execute drop_view: {e}")))?
    }
}
