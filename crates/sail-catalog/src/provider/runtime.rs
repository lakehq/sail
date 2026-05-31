use std::sync::Arc;

use sail_common_datafusion::catalog::{DatabaseStatus, TableStatus};
use tokio::runtime::Handle;

use super::{
    AlterTableOptions, CatalogProvider, CreateDatabaseOptions, CreateTableOptions,
    CreateViewOptions, DropDatabaseOptions, DropTableOptions, DropViewOptions, Namespace,
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

    fn uses_spark_default_database_location(&self) -> bool {
        self.inner.uses_spark_default_database_location()
    }

    fn uses_spark_default_table_location(&self) -> bool {
        self.inner.uses_spark_default_table_location()
    }

    fn requires_identifier_validation_for_default_table_location(&self) -> bool {
        self.inner
            .requires_identifier_validation_for_default_table_location()
    }

    fn uses_spark_table_location_qualification(&self) -> bool {
        self.inner.uses_spark_table_location_qualification()
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

    async fn alter_table(
        &self,
        database: &Namespace,
        table: &str,
        options: AlterTableOptions,
    ) -> CatalogResult<()> {
        let inner = self.inner.clone();
        let database = database.clone();
        let table = table.to_string();
        self.handle
            .spawn(async move { inner.alter_table(&database, &table, options).await })
            .await
            .map_err(|e| CatalogError::External(format!("Failed to execute alter_table: {e}")))?
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sail_common_datafusion::catalog::{DatabaseStatus, TableStatus};
    use tokio::runtime::Handle;

    use super::*;

    struct MockProvider {
        uses_spark_default_database_location: bool,
        uses_spark_default_table_location: bool,
        requires_identifier_validation_for_default_table_location: bool,
        uses_spark_table_location_qualification: bool,
    }

    #[async_trait::async_trait]
    impl CatalogProvider for MockProvider {
        fn get_name(&self) -> &str {
            "mock"
        }

        fn uses_spark_default_database_location(&self) -> bool {
            self.uses_spark_default_database_location
        }

        fn uses_spark_default_table_location(&self) -> bool {
            self.uses_spark_default_table_location
        }

        fn requires_identifier_validation_for_default_table_location(&self) -> bool {
            self.requires_identifier_validation_for_default_table_location
        }

        fn uses_spark_table_location_qualification(&self) -> bool {
            self.uses_spark_table_location_qualification
        }

        async fn create_database(
            &self,
            _database: &Namespace,
            _options: CreateDatabaseOptions,
        ) -> CatalogResult<DatabaseStatus> {
            unreachable!()
        }

        async fn get_database(&self, _database: &Namespace) -> CatalogResult<DatabaseStatus> {
            unreachable!()
        }

        async fn list_databases(
            &self,
            _prefix: Option<&Namespace>,
        ) -> CatalogResult<Vec<DatabaseStatus>> {
            unreachable!()
        }

        async fn drop_database(
            &self,
            _database: &Namespace,
            _options: DropDatabaseOptions,
        ) -> CatalogResult<()> {
            unreachable!()
        }

        async fn create_table(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: CreateTableOptions,
        ) -> CatalogResult<TableStatus> {
            unreachable!()
        }

        async fn get_table(
            &self,
            _database: &Namespace,
            _table: &str,
        ) -> CatalogResult<TableStatus> {
            unreachable!()
        }

        async fn list_tables(&self, _database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
            unreachable!()
        }

        async fn drop_table(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: DropTableOptions,
        ) -> CatalogResult<()> {
            unreachable!()
        }

        async fn alter_table(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: AlterTableOptions,
        ) -> CatalogResult<()> {
            unreachable!()
        }

        async fn create_view(
            &self,
            _database: &Namespace,
            _view: &str,
            _options: CreateViewOptions,
        ) -> CatalogResult<TableStatus> {
            unreachable!()
        }

        async fn get_view(&self, _database: &Namespace, _view: &str) -> CatalogResult<TableStatus> {
            unreachable!()
        }

        async fn list_views(&self, _database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
            unreachable!()
        }

        async fn drop_view(
            &self,
            _database: &Namespace,
            _view: &str,
            _options: DropViewOptions,
        ) -> CatalogResult<()> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn test_runtime_aware_provider_preserves_spark_default_database_location_capability() {
        let provider = RuntimeAwareCatalogProvider {
            inner: Arc::new(MockProvider {
                uses_spark_default_database_location: true,
                uses_spark_default_table_location: true,
                requires_identifier_validation_for_default_table_location: true,
                uses_spark_table_location_qualification: true,
            }),
            handle: Handle::current(),
        };

        assert!(provider.uses_spark_default_database_location());
        assert!(provider.uses_spark_default_table_location());
        assert!(provider.requires_identifier_validation_for_default_table_location());
        assert!(provider.uses_spark_table_location_qualification());
    }
}
