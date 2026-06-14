use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use moka::future::Cache;
use moka::policy::EvictionPolicy;
use sail_common::config::CatalogCacheConfig;
use sail_common_datafusion::catalog::{DatabaseStatus, TableStatus};

use crate::error::{CatalogError, CatalogResult};
use crate::provider::{
    AlterTableOptions, CatalogLocationPolicy, CatalogProvider, CommitTableOptions,
    CreateDatabaseOptions, CreateTableOptions, CreateViewOptions, DropDatabaseOptions,
    DropTableOptions, DropViewOptions, GetTableCommitsOptions, GetTableCommitsResponse, Namespace,
};

#[derive(Clone)]
pub struct CatalogCacheBundle {
    pub database_cache: Option<Cache<Option<Namespace>, Vec<DatabaseStatus>>>,
    pub table_cache: Option<Cache<Namespace, Vec<TableStatus>>>,
    pub view_cache: Option<Cache<Namespace, Vec<TableStatus>>>,
}

pub struct CatalogCacheManager {
    caches: RwLock<HashMap<String, Arc<CatalogCacheBundle>>>,
}

impl Default for CatalogCacheManager {
    fn default() -> Self {
        Self::new()
    }
}

impl CatalogCacheManager {
    pub fn new() -> Self {
        Self {
            caches: RwLock::new(HashMap::new()),
        }
    }

    pub fn get_cache(&self, name: &str) -> CatalogResult<Option<Arc<CatalogCacheBundle>>> {
        let caches = self
            .caches
            .read()
            .map_err(|e| CatalogError::Internal(e.to_string()))?;
        Ok(caches.get(name).cloned())
    }

    pub fn set_cache(&self, name: String, bundle: Arc<CatalogCacheBundle>) -> CatalogResult<()> {
        let mut caches = self
            .caches
            .write()
            .map_err(|e| CatalogError::Internal(e.to_string()))?;
        caches.insert(name, bundle);
        Ok(())
    }
}

pub struct CachingCatalogProvider<P: CatalogProvider + ?Sized> {
    inner: Arc<P>,
    database_cache: Option<Cache<Option<Namespace>, Vec<DatabaseStatus>>>,
    table_cache: Option<Cache<Namespace, Vec<TableStatus>>>,
    view_cache: Option<Cache<Namespace, Vec<TableStatus>>>,
}

impl<P: CatalogProvider + ?Sized> CachingCatalogProvider<P> {
    pub fn new(
        inner: Arc<P>,
        config: CatalogCacheConfig,
        global_bundle: Option<Arc<CatalogCacheBundle>>,
    ) -> Self {
        let database_cache = match config.database_cache_type {
            sail_common::config::CacheType::None => None,
            sail_common::config::CacheType::Global => global_bundle
                .as_ref()
                .and_then(|b| b.database_cache.clone()),
            sail_common::config::CacheType::Session => {
                let mut builder = Cache::builder().eviction_policy(EvictionPolicy::lru());
                if let Some(size) = config.database_cache_size.filter(|&s| s > 0) {
                    builder = builder.max_capacity(size as u64);
                }
                if let Some(ttl) = config.database_cache_ttl_secs.filter(|&t| t > 0) {
                    builder = builder.time_to_live(Duration::from_secs(ttl));
                }
                Some(builder.build())
            }
        };

        let table_cache = match config.table_cache_type {
            sail_common::config::CacheType::None => None,
            sail_common::config::CacheType::Global => {
                global_bundle.as_ref().and_then(|b| b.table_cache.clone())
            }
            sail_common::config::CacheType::Session => {
                let mut builder = Cache::builder().eviction_policy(EvictionPolicy::lru());
                if let Some(size) = config.table_cache_size.filter(|&s| s > 0) {
                    builder = builder.max_capacity(size as u64);
                }
                if let Some(ttl) = config.table_cache_ttl_secs.filter(|&t| t > 0) {
                    builder = builder.time_to_live(Duration::from_secs(ttl));
                }
                Some(builder.build())
            }
        };

        let view_cache = match config.view_cache_type {
            sail_common::config::CacheType::None => None,
            sail_common::config::CacheType::Global => {
                global_bundle.as_ref().and_then(|b| b.view_cache.clone())
            }
            sail_common::config::CacheType::Session => {
                let mut builder = Cache::builder().eviction_policy(EvictionPolicy::lru());
                if let Some(size) = config.view_cache_size.filter(|&s| s > 0) {
                    builder = builder.max_capacity(size as u64);
                }
                if let Some(ttl) = config.view_cache_ttl_secs.filter(|&t| t > 0) {
                    builder = builder.time_to_live(Duration::from_secs(ttl));
                }
                Some(builder.build())
            }
        };

        Self {
            inner,
            database_cache,
            table_cache,
            view_cache,
        }
    }

    pub fn get_cache_bundle(&self) -> Arc<CatalogCacheBundle> {
        Arc::new(CatalogCacheBundle {
            database_cache: self.database_cache.clone(),
            table_cache: self.table_cache.clone(),
            view_cache: self.view_cache.clone(),
        })
    }
}

impl CatalogCacheBundle {
    pub fn new(config: &CatalogCacheConfig) -> Self {
        let database_cache = if matches!(
            config.database_cache_type,
            sail_common::config::CacheType::Global
        ) {
            let mut builder = Cache::builder().eviction_policy(EvictionPolicy::lru());
            if let Some(size) = config.database_cache_size.filter(|&s| s > 0) {
                builder = builder.max_capacity(size as u64);
            }
            if let Some(ttl) = config.database_cache_ttl_secs.filter(|&t| t > 0) {
                builder = builder.time_to_live(Duration::from_secs(ttl));
            }
            Some(builder.build())
        } else {
            None
        };

        let table_cache = if matches!(
            config.table_cache_type,
            sail_common::config::CacheType::Global
        ) {
            let mut builder = Cache::builder().eviction_policy(EvictionPolicy::lru());
            if let Some(size) = config.table_cache_size.filter(|&s| s > 0) {
                builder = builder.max_capacity(size as u64);
            }
            if let Some(ttl) = config.table_cache_ttl_secs.filter(|&t| t > 0) {
                builder = builder.time_to_live(Duration::from_secs(ttl));
            }
            Some(builder.build())
        } else {
            None
        };

        let view_cache = if matches!(
            config.view_cache_type,
            sail_common::config::CacheType::Global
        ) {
            let mut builder = Cache::builder().eviction_policy(EvictionPolicy::lru());
            if let Some(size) = config.view_cache_size.filter(|&s| s > 0) {
                builder = builder.max_capacity(size as u64);
            }
            if let Some(ttl) = config.view_cache_ttl_secs.filter(|&t| t > 0) {
                builder = builder.time_to_live(Duration::from_secs(ttl));
            }
            Some(builder.build())
        } else {
            None
        };

        Self {
            database_cache,
            table_cache,
            view_cache,
        }
    }
}

#[async_trait::async_trait]
impl<P: CatalogProvider + ?Sized + 'static> CatalogProvider for CachingCatalogProvider<P> {
    fn get_name(&self) -> &str {
        self.inner.get_name()
    }

    fn location_policy(&self) -> CatalogLocationPolicy {
        self.inner.location_policy()
    }

    async fn create_database(
        &self,
        database: &Namespace,
        options: CreateDatabaseOptions,
    ) -> CatalogResult<DatabaseStatus> {
        let status = self.inner.create_database(database, options).await?;
        if let Some(c) = self.database_cache.as_ref() {
            let c: &Cache<Option<Namespace>, Vec<DatabaseStatus>> = c;
            c.invalidate_all();
        }
        Ok(status)
    }

    async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus> {
        self.inner.get_database(database).await
    }

    async fn list_databases(
        &self,
        prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<DatabaseStatus>> {
        if let Some(c) = self.database_cache.as_ref() {
            let c: &Cache<Option<Namespace>, Vec<DatabaseStatus>> = c;
            let key = prefix.cloned();
            log::debug!("CachingCatalogProvider::list_databases(prefix={:?})", key);
            if let Some(v) = c.get(&key).await {
                return Ok(v);
            }
            let v = self.inner.list_databases(prefix).await?;
            c.insert(key, v.clone()).await;
            Ok(v)
        } else {
            self.inner.list_databases(prefix).await
        }
    }

    async fn drop_database(
        &self,
        database: &Namespace,
        options: DropDatabaseOptions,
    ) -> CatalogResult<()> {
        self.inner.drop_database(database, options).await?;
        if let Some(c) = self.database_cache.as_ref() {
            let c: &Cache<Option<Namespace>, Vec<DatabaseStatus>> = c;
            c.invalidate_all();
        }
        if let Some(c) = self.table_cache.as_ref() {
            let c: &Cache<Namespace, Vec<TableStatus>> = c;
            c.invalidate(database).await;
        }
        if let Some(c) = self.view_cache.as_ref() {
            let c: &Cache<Namespace, Vec<TableStatus>> = c;
            c.invalidate(database).await;
        }
        Ok(())
    }

    async fn create_table(
        &self,
        database: &Namespace,
        table: &str,
        options: CreateTableOptions,
    ) -> CatalogResult<TableStatus> {
        let status = self.inner.create_table(database, table, options).await?;
        if let Some(c) = self.table_cache.as_ref() {
            let c: &Cache<Namespace, Vec<TableStatus>> = c;
            c.invalidate(database).await;
        }
        Ok(status)
    }

    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus> {
        self.inner.get_table(database, table).await
    }

    async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        if let Some(c) = self.table_cache.as_ref() {
            let c: &Cache<Namespace, Vec<TableStatus>> = c;
            let key = database.clone();
            if let Some(v) = c.get(&key).await {
                return Ok(v);
            }
            let v = self.inner.list_tables(database).await?;
            c.insert(key, v.clone()).await;
            Ok(v)
        } else {
            self.inner.list_tables(database).await
        }
    }

    async fn drop_table(
        &self,
        database: &Namespace,
        table: &str,
        options: DropTableOptions,
    ) -> CatalogResult<()> {
        self.inner.drop_table(database, table, options).await?;
        if let Some(c) = self.table_cache.as_ref() {
            let c: &Cache<Namespace, Vec<TableStatus>> = c;
            c.invalidate(database).await;
        }
        Ok(())
    }

    async fn alter_table(
        &self,
        database: &Namespace,
        table: &str,
        options: AlterTableOptions,
    ) -> CatalogResult<()> {
        self.inner.alter_table(database, table, options).await?;
        if let Some(c) = self.table_cache.as_ref() {
            let c: &Cache<Namespace, Vec<TableStatus>> = c;
            c.invalidate(database).await;
        }
        Ok(())
    }

    async fn commit_table(
        &self,
        database: &Namespace,
        table: &str,
        options: CommitTableOptions,
    ) -> CatalogResult<TableStatus> {
        let status = self.inner.commit_table(database, table, options).await?;
        if let Some(c) = self.table_cache.as_ref() {
            let c: &Cache<Namespace, Vec<TableStatus>> = c;
            c.invalidate(database).await;
        }
        Ok(status)
    }

    async fn get_table_commits(
        &self,
        database: &Namespace,
        table: &str,
        options: GetTableCommitsOptions,
    ) -> CatalogResult<GetTableCommitsResponse> {
        self.inner.get_table_commits(database, table, options).await
    }

    async fn create_view(
        &self,
        database: &Namespace,
        view: &str,
        options: CreateViewOptions,
    ) -> CatalogResult<TableStatus> {
        let status = self.inner.create_view(database, view, options).await?;
        if let Some(c) = self.view_cache.as_ref() {
            let c: &Cache<Namespace, Vec<TableStatus>> = c;
            c.invalidate(database).await;
        }
        Ok(status)
    }

    async fn get_view(&self, database: &Namespace, view: &str) -> CatalogResult<TableStatus> {
        self.inner.get_view(database, view).await
    }

    async fn list_views(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        if let Some(c) = self.view_cache.as_ref() {
            let c: &Cache<Namespace, Vec<TableStatus>> = c;
            let key = database.clone();
            if let Some(v) = c.get(&key).await {
                return Ok(v);
            }
            let v = self.inner.list_views(database).await?;
            c.insert(key, v.clone()).await;
            Ok(v)
        } else {
            self.inner.list_views(database).await
        }
    }

    async fn drop_view(
        &self,
        database: &Namespace,
        view: &str,
        options: DropViewOptions,
    ) -> CatalogResult<()> {
        self.inner.drop_view(database, view, options).await?;
        if let Some(c) = self.view_cache.as_ref() {
            let c: &Cache<Namespace, Vec<TableStatus>> = c;
            c.invalidate(database).await;
        }
        Ok(())
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use sail_common_datafusion::catalog::{DatabaseStatus, TableKind, TableStatus};

    use super::*;
    use crate::provider::{CreateDatabaseOptions, CreateTableOptions, Namespace};

    struct MockProvider {
        db_calls: AtomicUsize,
        table_calls: AtomicUsize,
        view_calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl CatalogProvider for MockProvider {
        fn get_name(&self) -> &str {
            "mock"
        }

        fn location_policy(&self) -> CatalogLocationPolicy {
            CatalogLocationPolicy::SPARK_SESSION
        }

        async fn create_database(
            &self,
            database: &Namespace,
            _options: CreateDatabaseOptions,
        ) -> CatalogResult<DatabaseStatus> {
            Ok(DatabaseStatus {
                catalog: "cat".to_string(),
                database: database.clone().into(),
                comment: None,
                location: None,
                properties: vec![],
            })
        }

        async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus> {
            Ok(DatabaseStatus {
                catalog: "cat".to_string(),
                database: database.clone().into(),
                comment: None,
                location: None,
                properties: vec![],
            })
        }

        async fn list_databases(
            &self,
            _prefix: Option<&Namespace>,
        ) -> CatalogResult<Vec<DatabaseStatus>> {
            self.db_calls.fetch_add(1, Ordering::SeqCst);
            Ok(vec![DatabaseStatus {
                catalog: "cat".to_string(),
                database: vec!["db1".to_string()],
                comment: None,
                location: None,
                properties: vec![],
            }])
        }

        async fn drop_database(
            &self,
            _database: &Namespace,
            _options: DropDatabaseOptions,
        ) -> CatalogResult<()> {
            Ok(())
        }

        async fn create_table(
            &self,
            database: &Namespace,
            table: &str,
            _options: CreateTableOptions,
        ) -> CatalogResult<TableStatus> {
            Ok(TableStatus {
                catalog: Some("cat".to_string()),
                database: database.clone().into(),
                name: table.to_string(),
                kind: TableKind::Table {
                    columns: vec![],
                    comment: None,
                    constraints: vec![],
                    location: None,
                    format: "parquet".to_string(),
                    partition_by: vec![],
                    sort_by: vec![],
                    bucket_by: None,
                    properties: vec![],
                    is_external: false,
                },
            })
        }

        async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus> {
            Ok(TableStatus {
                catalog: Some("cat".to_string()),
                database: database.clone().into(),
                name: table.to_string(),
                kind: TableKind::Table {
                    columns: vec![],
                    comment: None,
                    constraints: vec![],
                    location: None,
                    format: "parquet".to_string(),
                    partition_by: vec![],
                    sort_by: vec![],
                    bucket_by: None,
                    properties: vec![],
                    is_external: false,
                },
            })
        }

        async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
            self.table_calls.fetch_add(1, Ordering::SeqCst);
            Ok(vec![TableStatus {
                catalog: Some("cat".to_string()),
                database: database.clone().into(),
                name: "t1".to_string(),
                kind: TableKind::Table {
                    columns: vec![],
                    comment: None,
                    constraints: vec![],
                    location: None,
                    format: "parquet".to_string(),
                    partition_by: vec![],
                    sort_by: vec![],
                    bucket_by: None,
                    properties: vec![],
                    is_external: false,
                },
            }])
        }

        async fn drop_table(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: DropTableOptions,
        ) -> CatalogResult<()> {
            Ok(())
        }

        async fn alter_table(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: AlterTableOptions,
        ) -> CatalogResult<()> {
            Ok(())
        }

        async fn create_view(
            &self,
            database: &Namespace,
            view: &str,
            _options: CreateViewOptions,
        ) -> CatalogResult<TableStatus> {
            Ok(TableStatus {
                catalog: Some("cat".to_string()),
                database: database.clone().into(),
                name: view.to_string(),
                kind: TableKind::View {
                    definition: "SELECT 1".to_string(),
                    columns: vec![],
                    comment: None,
                    properties: vec![],
                },
            })
        }

        async fn get_view(&self, database: &Namespace, view: &str) -> CatalogResult<TableStatus> {
            Ok(TableStatus {
                catalog: Some("cat".to_string()),
                database: database.clone().into(),
                name: view.to_string(),
                kind: TableKind::View {
                    definition: "SELECT 1".to_string(),
                    columns: vec![],
                    comment: None,
                    properties: vec![],
                },
            })
        }

        async fn list_views(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
            self.view_calls.fetch_add(1, Ordering::SeqCst);
            Ok(vec![TableStatus {
                catalog: Some("cat".to_string()),
                database: database.clone().into(),
                name: "v1".to_string(),
                kind: TableKind::View {
                    definition: "SELECT 1".to_string(),
                    columns: vec![],
                    comment: None,
                    properties: vec![],
                },
            }])
        }

        async fn drop_view(
            &self,
            _database: &Namespace,
            _view: &str,
            _options: DropViewOptions,
        ) -> CatalogResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_caching_behavior() {
        let mock = std::sync::Arc::new(MockProvider {
            db_calls: AtomicUsize::new(0),
            table_calls: AtomicUsize::new(0),
            view_calls: AtomicUsize::new(0),
        });
        let config = CatalogCacheConfig {
            database_cache_type: sail_common::config::CacheType::Session,
            database_cache_size: Some(10),
            database_cache_ttl_secs: Some(60),
            table_cache_type: sail_common::config::CacheType::Session,
            table_cache_size: Some(10),
            table_cache_ttl_secs: Some(60),
            view_cache_type: sail_common::config::CacheType::Session,
            view_cache_size: Some(10),
            view_cache_ttl_secs: Some(60),
        };
        let provider = CachingCatalogProvider::new(mock.clone(), config, None);

        // First call - should hit mock
        let dbs = provider.list_databases(None).await.unwrap();
        assert_eq!(dbs.len(), 1);
        assert_eq!(mock.db_calls.load(Ordering::SeqCst), 1);

        // Second call - should hit cache
        let dbs = provider.list_databases(None).await.unwrap();
        assert_eq!(dbs.len(), 1);
        assert_eq!(mock.db_calls.load(Ordering::SeqCst), 1);

        // First table call - should hit mock
        let ns = Namespace::try_from(vec!["db1"]).unwrap();
        let tables = provider.list_tables(&ns).await.unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(mock.table_calls.load(Ordering::SeqCst), 1);

        // Second table call - should hit cache
        let tables = provider.list_tables(&ns).await.unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(mock.table_calls.load(Ordering::SeqCst), 1);

        // First view call - should hit mock
        let views = provider.list_views(&ns).await.unwrap();
        assert_eq!(views.len(), 1);
        assert_eq!(mock.view_calls.load(Ordering::SeqCst), 1);

        // Second view call - should hit cache
        let views = provider.list_views(&ns).await.unwrap();
        assert_eq!(views.len(), 1);
        assert_eq!(mock.view_calls.load(Ordering::SeqCst), 1);

        // Invalidate cache via create_table
        let options = CreateTableOptions {
            columns: vec![],
            comment: None,
            constraints: vec![],
            location: None,
            format: "parquet".to_string(),
            partition_by: vec![],
            sort_by: vec![],
            bucket_by: None,
            if_not_exists: false,
            replace: false,
            properties: vec![],
            is_external: false,
            is_write_precondition: false,
        };
        provider.create_table(&ns, "t2", options).await.unwrap();

        // Third table call - should hit mock again
        let tables = provider.list_tables(&ns).await.unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(mock.table_calls.load(Ordering::SeqCst), 2);

        // Invalidate view cache via create_view
        let view_options = CreateViewOptions {
            columns: vec![],
            definition: "SELECT 1".to_string(),
            if_not_exists: false,
            replace: false,
            comment: None,
            properties: vec![],
        };
        provider.create_view(&ns, "v2", view_options).await.unwrap();

        // Third view call - should hit mock again
        let views = provider.list_views(&ns).await.unwrap();
        assert_eq!(views.len(), 1);
        assert_eq!(mock.view_calls.load(Ordering::SeqCst), 2);

        // Test get methods (direct pass-through)
        provider.get_database(&ns).await.unwrap();
        // Call get_table and get_view as well to exercise the wrapper pass-through paths.
        let _ = provider.get_table(&ns, "t1").await;
        let _ = provider.get_view(&ns, "v1").await;

        // Test alter table invalidation
        provider
            .alter_table(
                &ns,
                "t1",
                AlterTableOptions::SetTableProperties { properties: vec![] },
            )
            .await
            .unwrap();
        let _ = provider.list_tables(&ns).await.unwrap();
        assert_eq!(mock.table_calls.load(Ordering::SeqCst), 3);

        // Test drop table invalidation
        provider
            .drop_table(
                &ns,
                "t1",
                DropTableOptions {
                    if_exists: false,
                    purge: false,
                },
            )
            .await
            .unwrap();
        let _ = provider.list_tables(&ns).await.unwrap();
        assert_eq!(mock.table_calls.load(Ordering::SeqCst), 4);

        // Test drop view invalidation
        provider
            .drop_view(&ns, "v1", DropViewOptions { if_exists: false })
            .await
            .unwrap();
        let _ = provider.list_views(&ns).await.unwrap();
        assert_eq!(mock.view_calls.load(Ordering::SeqCst), 3);

        // Test drop database invalidation (invalidates all)
        provider
            .drop_database(
                &ns,
                DropDatabaseOptions {
                    if_exists: false,
                    cascade: false,
                },
            )
            .await
            .unwrap();
        let _ = provider.list_databases(None).await.unwrap();
        assert_eq!(mock.db_calls.load(Ordering::SeqCst), 2);
        let _ = provider.list_tables(&ns).await.unwrap();
        assert_eq!(mock.table_calls.load(Ordering::SeqCst), 5);
        let _ = provider.list_views(&ns).await.unwrap();
        assert_eq!(mock.view_calls.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn test_catalog_cache_manager() {
        let manager = CatalogCacheManager::new();
        let name = "test_catalog".to_string();

        // Initially no cache
        assert!(manager.get_cache(&name).unwrap().is_none());

        let config = CatalogCacheConfig {
            database_cache_type: sail_common::config::CacheType::Session,
            table_cache_type: sail_common::config::CacheType::Session,
            ..Default::default()
        };
        // Use a dummy provider to get a bundle
        let mock = Arc::new(MockProvider {
            db_calls: AtomicUsize::new(0),
            table_calls: AtomicUsize::new(0),
            view_calls: AtomicUsize::new(0),
        });
        let provider = CachingCatalogProvider::new(mock, config, None);
        let bundle = provider.get_cache_bundle();

        manager.set_cache(name.clone(), bundle).unwrap();

        // Now cache should exist
        let retrieved = manager.get_cache(&name).unwrap();
        assert!(retrieved.is_some());
        let bundle = retrieved.unwrap();
        assert!(bundle.database_cache.is_some());
        assert!(bundle.table_cache.is_some());
    }
    #[tokio::test]
    async fn test_cache_config_normalization() {
        let mock = std::sync::Arc::new(MockProvider {
            db_calls: AtomicUsize::new(0),
            table_calls: AtomicUsize::new(0),
            view_calls: AtomicUsize::new(0),
        });
        // Config with 0 values - should be treated as unbounded
        let config = CatalogCacheConfig {
            database_cache_type: sail_common::config::CacheType::Session,
            database_cache_size: Some(0),
            database_cache_ttl_secs: Some(0),
            table_cache_type: sail_common::config::CacheType::Session,
            table_cache_size: Some(0),
            table_cache_ttl_secs: Some(0),
            ..Default::default()
        };
        let provider = CachingCatalogProvider::new(mock.clone(), config, None);

        // Verification: if it was treated as max_capacity(0), it wouldn't cache anything.
        // We can't easily inspect the internal Moka cache settings, but we can verify behavior.

        // First call
        provider.list_databases(None).await.unwrap();
        assert_eq!(mock.db_calls.load(Ordering::SeqCst), 1);

        // Second call - should hit cache (meaning max_capacity(0) was NOT applied)
        provider.list_databases(None).await.unwrap();
        assert_eq!(mock.db_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_caching_provider_preserves_spark_default_database_location_capability() {
        let mock = Arc::new(MockProvider {
            db_calls: AtomicUsize::new(0),
            table_calls: AtomicUsize::new(0),
            view_calls: AtomicUsize::new(0),
        });
        let provider = CachingCatalogProvider::new(
            mock,
            CatalogCacheConfig {
                database_cache_type: sail_common::config::CacheType::Session,
                ..Default::default()
            },
            None,
        );

        assert_eq!(
            provider.location_policy(),
            CatalogLocationPolicy::SPARK_SESSION
        );
    }
}
