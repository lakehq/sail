use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use moka::future::Cache;
use sail_common::config::CatalogCacheConfig;
use sail_common_datafusion::catalog::{DatabaseStatus, TableStatus};

use crate::error::CatalogResult;
use crate::provider::{
    AlterTableOptions, CatalogProvider, CreateDatabaseOptions, CreateTableOptions,
    CreateViewOptions, DropDatabaseOptions, DropTableOptions, DropViewOptions, Namespace,
};

pub struct CachedCatalogProvider {
    inner: Arc<dyn CatalogProvider>,
    db_list_cache: Option<Cache<Option<Namespace>, Vec<DatabaseStatus>>>,
    table_list_cache: Option<Cache<Namespace, Vec<TableStatus>>>,
    view_list_cache: Option<Cache<Namespace, Vec<TableStatus>>>,
}

impl CachedCatalogProvider {
    pub fn new(inner: Arc<dyn CatalogProvider>, config: &CatalogCacheConfig) -> Self {
        let db_list_cache = if config.database_cache_enabled {
            let mut builder = Cache::builder();
            if let Some(size) = config.database_cache_size {
                builder = builder.max_capacity(size as u64);
            }
            if let Some(ttl) = config.database_cache_ttl_secs {
                builder = builder.time_to_live(Duration::from_secs(ttl));
            }
            Some(builder.build())
        } else {
            None
        };

        let table_list_cache = if config.table_cache_enabled {
            let mut builder = Cache::builder();
            if let Some(size) = config.table_cache_size {
                builder = builder.max_capacity(size as u64);
            }
            if let Some(ttl) = config.table_cache_ttl_secs {
                builder = builder.time_to_live(Duration::from_secs(ttl));
            }
            Some(builder.build())
        } else {
            None
        };

        let view_list_cache = if config.table_cache_enabled {
            let mut builder = Cache::builder();
            if let Some(size) = config.table_cache_size {
                builder = builder.max_capacity(size as u64);
            }
            if let Some(ttl) = config.table_cache_ttl_secs {
                builder = builder.time_to_live(Duration::from_secs(ttl));
            }
            Some(builder.build())
        } else {
            None
        };

        Self {
            inner,
            db_list_cache,
            table_list_cache,
            view_list_cache,
        }
    }
}

#[async_trait]
impl CatalogProvider for CachedCatalogProvider {
    fn get_name(&self) -> &str {
        self.inner.get_name()
    }

    async fn create_database(
        &self,
        database: &Namespace,
        options: CreateDatabaseOptions,
    ) -> CatalogResult<DatabaseStatus> {
        let res = self.inner.create_database(database, options).await?;
        if let Some(cache) = &self.db_list_cache {
            cache.invalidate_all();
        }
        Ok(res)
    }

    async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus> {
        self.inner.get_database(database).await
    }

    async fn list_databases(
        &self,
        prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<DatabaseStatus>> {
        if let Some(cache) = &self.db_list_cache {
            let key = prefix.cloned();
            if let Some(cached) = cache.get(&key).await {
                return Ok(cached);
            }
            let res = self.inner.list_databases(prefix).await?;
            cache.insert(key, res.clone()).await;
            Ok(res)
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
        if let Some(cache) = &self.db_list_cache {
            cache.invalidate_all();
        }
        if let Some(cache) = &self.table_list_cache {
            cache.invalidate(database).await;
        }
        if let Some(cache) = &self.view_list_cache {
            cache.invalidate(database).await;
        }
        Ok(())
    }

    async fn create_table(
        &self,
        database: &Namespace,
        table: &str,
        options: CreateTableOptions,
    ) -> CatalogResult<TableStatus> {
        let res = self.inner.create_table(database, table, options).await?;
        if let Some(cache) = &self.table_list_cache {
            cache.invalidate(database).await;
        }
        Ok(res)
    }

    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus> {
        self.inner.get_table(database, table).await
    }

    async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        if let Some(cache) = &self.table_list_cache {
            if let Some(cached) = cache.get(database).await {
                return Ok(cached);
            }
            let res = self.inner.list_tables(database).await?;
            cache.insert(database.clone(), res.clone()).await;
            Ok(res)
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
        if let Some(cache) = &self.table_list_cache {
            cache.invalidate(database).await;
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
        if let Some(cache) = &self.table_list_cache {
            cache.invalidate(database).await;
        }
        Ok(())
    }

    async fn create_view(
        &self,
        database: &Namespace,
        view: &str,
        options: CreateViewOptions,
    ) -> CatalogResult<TableStatus> {
        let res = self.inner.create_view(database, view, options).await?;
        if let Some(cache) = &self.view_list_cache {
            cache.invalidate(database).await;
        }
        Ok(res)
    }

    async fn get_view(&self, database: &Namespace, view: &str) -> CatalogResult<TableStatus> {
        self.inner.get_view(database, view).await
    }

    async fn list_views(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        if let Some(cache) = &self.view_list_cache {
            if let Some(cached) = cache.get(database).await {
                return Ok(cached);
            }
            let res = self.inner.list_views(database).await?;
            cache.insert(database.clone(), res.clone()).await;
            Ok(res)
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
        if let Some(cache) = &self.view_list_cache {
            cache.invalidate(database).await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use sail_common_datafusion::catalog::TableKind;

    use super::*;
    use crate::provider::{DatabaseStatus, Namespace, TableStatus};

    struct MockCatalogProvider {
        call_count: Arc<Mutex<usize>>,
    }

    #[async_trait]
    impl CatalogProvider for MockCatalogProvider {
        fn get_name(&self) -> &str {
            "mock"
        }
        async fn create_database(
            &self,
            _ns: &Namespace,
            _opt: CreateDatabaseOptions,
        ) -> CatalogResult<DatabaseStatus> {
            Ok(DatabaseStatus {
                catalog: "mock".to_string(),
                database: vec![],
                comment: None,
                location: None,
                properties: vec![],
            })
        }
        async fn get_database(&self, _ns: &Namespace) -> CatalogResult<DatabaseStatus> {
            Ok(DatabaseStatus {
                catalog: "mock".to_string(),
                database: vec![],
                comment: None,
                location: None,
                properties: vec![],
            })
        }
        async fn list_databases(
            &self,
            _prefix: Option<&Namespace>,
        ) -> CatalogResult<Vec<DatabaseStatus>> {
            let mut count = self.call_count.lock().unwrap();
            *count += 1;
            Ok(vec![])
        }
        async fn drop_database(
            &self,
            _ns: &Namespace,
            _opt: DropDatabaseOptions,
        ) -> CatalogResult<()> {
            Ok(())
        }
        async fn create_table(
            &self,
            _ns: &Namespace,
            _t: &str,
            _opt: CreateTableOptions,
        ) -> CatalogResult<TableStatus> {
            Ok(TableStatus {
                catalog: Some("mock".to_string()),
                database: vec![],
                name: "t".to_string(),
                kind: TableKind::Table {
                    columns: vec![],
                    comment: None,
                    constraints: vec![],
                    location: None,
                    format: "".to_string(),
                    partition_by: vec![],
                    sort_by: vec![],
                    bucket_by: None,
                    properties: vec![],
                },
            })
        }
        async fn get_table(&self, _ns: &Namespace, _t: &str) -> CatalogResult<TableStatus> {
            Ok(TableStatus {
                catalog: Some("mock".to_string()),
                database: vec![],
                name: "t".to_string(),
                kind: TableKind::Table {
                    columns: vec![],
                    comment: None,
                    constraints: vec![],
                    location: None,
                    format: "".to_string(),
                    partition_by: vec![],
                    sort_by: vec![],
                    bucket_by: None,
                    properties: vec![],
                },
            })
        }
        async fn list_tables(&self, _ns: &Namespace) -> CatalogResult<Vec<TableStatus>> {
            let mut count = self.call_count.lock().unwrap();
            *count += 1;
            Ok(vec![])
        }
        async fn drop_table(
            &self,
            _ns: &Namespace,
            _t: &str,
            _opt: DropTableOptions,
        ) -> CatalogResult<()> {
            Ok(())
        }
        async fn alter_table(
            &self,
            _ns: &Namespace,
            _t: &str,
            _opt: AlterTableOptions,
        ) -> CatalogResult<()> {
            Ok(())
        }
        async fn create_view(
            &self,
            _ns: &Namespace,
            _v: &str,
            _opt: CreateViewOptions,
        ) -> CatalogResult<TableStatus> {
            Ok(TableStatus {
                catalog: Some("mock".to_string()),
                database: vec![],
                name: "v".to_string(),
                kind: TableKind::View {
                    definition: "".to_string(),
                    columns: vec![],
                    comment: None,
                    properties: vec![],
                },
            })
        }
        async fn get_view(&self, _ns: &Namespace, _v: &str) -> CatalogResult<TableStatus> {
            Ok(TableStatus {
                catalog: Some("mock".to_string()),
                database: vec![],
                name: "v".to_string(),
                kind: TableKind::View {
                    definition: "".to_string(),
                    columns: vec![],
                    comment: None,
                    properties: vec![],
                },
            })
        }
        async fn list_views(&self, _ns: &Namespace) -> CatalogResult<Vec<TableStatus>> {
            let mut count = self.call_count.lock().unwrap();
            *count += 1;
            Ok(vec![])
        }
        async fn drop_view(
            &self,
            _ns: &Namespace,
            _v: &str,
            _opt: DropViewOptions,
        ) -> CatalogResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_cache_hits_and_misses() {
        let call_count = Arc::new(Mutex::new(0));
        let inner = Arc::new(MockCatalogProvider {
            call_count: call_count.clone(),
        });
        let config = CatalogCacheConfig {
            database_cache_enabled: true,
            table_cache_enabled: true,
            ..Default::default()
        };
        let cached = CachedCatalogProvider::new(inner, &config);
        let ns = Namespace {
            head: "db1".to_string().into(),
            tail: vec![],
        };

        // Database list cache
        cached.list_databases(None).await.unwrap();
        assert_eq!(*call_count.lock().unwrap(), 1);
        cached.list_databases(None).await.unwrap();
        assert_eq!(*call_count.lock().unwrap(), 1); // Hit

        // Table list cache
        cached.list_tables(&ns).await.unwrap();
        assert_eq!(*call_count.lock().unwrap(), 2);
        cached.list_tables(&ns).await.unwrap();
        assert_eq!(*call_count.lock().unwrap(), 2); // Hit

        // Invalidation via create_table
        cached
            .create_table(
                &ns,
                "t1",
                CreateTableOptions {
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
                },
            )
            .await
            .unwrap();
        cached.list_tables(&ns).await.unwrap();
        assert_eq!(*call_count.lock().unwrap(), 3); // Miss after invalidation
    }
}
