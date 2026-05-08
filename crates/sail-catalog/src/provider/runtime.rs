use std::sync::Arc;

use sail_common_datafusion::catalog::{DatabaseStatus, TableStatistics, TableStatus};
use tokio::runtime::Handle;

use super::{
    AlterTableOptions, CatalogProvider, CreateDatabaseOptions, CreatePartitionsOptions,
    CreateTableOptions, CreateViewOptions, DropDatabaseOptions, DropPartitionsOptions,
    DropTableOptions, DropViewOptions, GetPartitionsOptions, Namespace, PartitionSpec,
    PartitionStatus,
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

    async fn alter_table_stats(
        &self,
        database: &Namespace,
        table: &str,
        stats: Option<TableStatistics>,
    ) -> CatalogResult<()> {
        let inner = self.inner.clone();
        let database = database.clone();
        let table = table.to_string();
        self.handle
            .spawn(async move { inner.alter_table_stats(&database, &table, stats).await })
            .await
            .map_err(|e| {
                CatalogError::External(format!("Failed to execute alter_table_stats: {e}"))
            })?
    }

    async fn get_partitions(
        &self,
        database: &Namespace,
        table: &str,
        options: GetPartitionsOptions,
    ) -> CatalogResult<Vec<PartitionStatus>> {
        let inner = self.inner.clone();
        let database = database.clone();
        let table = table.to_string();
        self.handle
            .spawn(async move { inner.get_partitions(&database, &table, options).await })
            .await
            .map_err(|e| CatalogError::External(format!("Failed to execute get_partitions: {e}")))?
    }

    async fn create_partitions(
        &self,
        database: &Namespace,
        table: &str,
        partitions: Vec<PartitionStatus>,
        options: CreatePartitionsOptions,
    ) -> CatalogResult<()> {
        let inner = self.inner.clone();
        let database = database.clone();
        let table = table.to_string();
        self.handle
            .spawn(async move {
                inner
                    .create_partitions(&database, &table, partitions, options)
                    .await
            })
            .await
            .map_err(|e| {
                CatalogError::External(format!("Failed to execute create_partitions: {e}"))
            })?
    }

    async fn drop_partitions(
        &self,
        database: &Namespace,
        table: &str,
        specs: Vec<PartitionSpec>,
        options: DropPartitionsOptions,
    ) -> CatalogResult<()> {
        let inner = self.inner.clone();
        let database = database.clone();
        let table = table.to_string();
        self.handle
            .spawn(async move {
                inner
                    .drop_partitions(&database, &table, specs, options)
                    .await
            })
            .await
            .map_err(|e| {
                CatalogError::External(format!("Failed to execute drop_partitions: {e}"))
            })?
    }

    async fn alter_partitions(
        &self,
        database: &Namespace,
        table: &str,
        partitions: Vec<PartitionStatus>,
    ) -> CatalogResult<()> {
        let inner = self.inner.clone();
        let database = database.clone();
        let table = table.to_string();
        self.handle
            .spawn(async move { inner.alter_partitions(&database, &table, partitions).await })
            .await
            .map_err(|e| {
                CatalogError::External(format!("Failed to execute alter_partitions: {e}"))
            })?
    }

    async fn rename_partitions(
        &self,
        database: &Namespace,
        table: &str,
        old_specs: Vec<PartitionSpec>,
        new_specs: Vec<PartitionSpec>,
    ) -> CatalogResult<()> {
        let inner = self.inner.clone();
        let database = database.clone();
        let table = table.to_string();
        self.handle
            .spawn(async move {
                inner
                    .rename_partitions(&database, &table, old_specs, new_specs)
                    .await
            })
            .await
            .map_err(|e| {
                CatalogError::External(format!("Failed to execute rename_partitions: {e}"))
            })?
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
    #![expect(clippy::unwrap_used)]

    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    use sail_common_datafusion::catalog::{DatabaseStatus, TableStatistics, TableStatus};

    use super::RuntimeAwareCatalogProvider;
    use crate::error::{CatalogError, CatalogResult};
    use crate::provider::{
        AlterTableOptions, CatalogProvider, CreateDatabaseOptions, CreatePartitionsOptions,
        CreateTableOptions, CreateViewOptions, DropDatabaseOptions, DropPartitionsOptions,
        DropTableOptions, DropViewOptions, GetPartitionsOptions, Namespace, PartitionSpec,
        PartitionStatus,
    };

    struct StatsProvider {
        alter_stats_called: Arc<AtomicBool>,
        partition_mutation_calls: Arc<AtomicUsize>,
    }

    struct DefaultOnlyProvider;

    #[async_trait::async_trait]
    impl CatalogProvider for StatsProvider {
        fn get_name(&self) -> &str {
            "stats"
        }

        async fn create_database(
            &self,
            _database: &Namespace,
            _options: CreateDatabaseOptions,
        ) -> CatalogResult<DatabaseStatus> {
            unimplemented!()
        }

        async fn get_database(&self, _database: &Namespace) -> CatalogResult<DatabaseStatus> {
            unimplemented!()
        }

        async fn list_databases(
            &self,
            _prefix: Option<&Namespace>,
        ) -> CatalogResult<Vec<DatabaseStatus>> {
            unimplemented!()
        }

        async fn drop_database(
            &self,
            _database: &Namespace,
            _options: DropDatabaseOptions,
        ) -> CatalogResult<()> {
            unimplemented!()
        }

        async fn create_table(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: CreateTableOptions,
        ) -> CatalogResult<TableStatus> {
            unimplemented!()
        }

        async fn get_table(
            &self,
            _database: &Namespace,
            _table: &str,
        ) -> CatalogResult<TableStatus> {
            unimplemented!()
        }

        async fn list_tables(&self, _database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
            unimplemented!()
        }

        async fn drop_table(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: DropTableOptions,
        ) -> CatalogResult<()> {
            unimplemented!()
        }

        async fn alter_table(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: AlterTableOptions,
        ) -> CatalogResult<()> {
            unimplemented!()
        }

        async fn create_view(
            &self,
            _database: &Namespace,
            _view: &str,
            _options: CreateViewOptions,
        ) -> CatalogResult<TableStatus> {
            unimplemented!()
        }

        async fn get_view(&self, _database: &Namespace, _view: &str) -> CatalogResult<TableStatus> {
            unimplemented!()
        }

        async fn list_views(&self, _database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
            unimplemented!()
        }

        async fn drop_view(
            &self,
            _database: &Namespace,
            _view: &str,
            _options: DropViewOptions,
        ) -> CatalogResult<()> {
            unimplemented!()
        }

        async fn alter_table_stats(
            &self,
            _database: &Namespace,
            _table: &str,
            _stats: Option<TableStatistics>,
        ) -> CatalogResult<()> {
            self.alter_stats_called.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn get_partitions(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: GetPartitionsOptions,
        ) -> CatalogResult<Vec<PartitionStatus>> {
            Ok(vec![PartitionStatus {
                spec: vec![("dt".to_string(), "2026-04-26".to_string())],
                location: None,
                parameters: Default::default(),
                create_time: None,
                last_access_time: None,
                statistics: None,
            }])
        }

        async fn create_partitions(
            &self,
            _database: &Namespace,
            _table: &str,
            _partitions: Vec<PartitionStatus>,
            _options: CreatePartitionsOptions,
        ) -> CatalogResult<()> {
            self.partition_mutation_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn drop_partitions(
            &self,
            _database: &Namespace,
            _table: &str,
            _specs: Vec<PartitionSpec>,
            _options: DropPartitionsOptions,
        ) -> CatalogResult<()> {
            self.partition_mutation_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn alter_partitions(
            &self,
            _database: &Namespace,
            _table: &str,
            _partitions: Vec<PartitionStatus>,
        ) -> CatalogResult<()> {
            self.partition_mutation_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn rename_partitions(
            &self,
            _database: &Namespace,
            _table: &str,
            _old_specs: Vec<PartitionSpec>,
            _new_specs: Vec<PartitionSpec>,
        ) -> CatalogResult<()> {
            self.partition_mutation_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl CatalogProvider for DefaultOnlyProvider {
        fn get_name(&self) -> &str {
            "default-only"
        }

        async fn create_database(
            &self,
            _database: &Namespace,
            _options: CreateDatabaseOptions,
        ) -> CatalogResult<DatabaseStatus> {
            unimplemented!()
        }

        async fn get_database(&self, _database: &Namespace) -> CatalogResult<DatabaseStatus> {
            unimplemented!()
        }

        async fn list_databases(
            &self,
            _prefix: Option<&Namespace>,
        ) -> CatalogResult<Vec<DatabaseStatus>> {
            unimplemented!()
        }

        async fn drop_database(
            &self,
            _database: &Namespace,
            _options: DropDatabaseOptions,
        ) -> CatalogResult<()> {
            unimplemented!()
        }

        async fn create_table(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: CreateTableOptions,
        ) -> CatalogResult<TableStatus> {
            unimplemented!()
        }

        async fn get_table(
            &self,
            _database: &Namespace,
            _table: &str,
        ) -> CatalogResult<TableStatus> {
            unimplemented!()
        }

        async fn list_tables(&self, _database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
            unimplemented!()
        }

        async fn drop_table(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: DropTableOptions,
        ) -> CatalogResult<()> {
            unimplemented!()
        }

        async fn alter_table(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: AlterTableOptions,
        ) -> CatalogResult<()> {
            unimplemented!()
        }

        async fn create_view(
            &self,
            _database: &Namespace,
            _view: &str,
            _options: CreateViewOptions,
        ) -> CatalogResult<TableStatus> {
            unimplemented!()
        }

        async fn get_view(&self, _database: &Namespace, _view: &str) -> CatalogResult<TableStatus> {
            unimplemented!()
        }

        async fn list_views(&self, _database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
            unimplemented!()
        }

        async fn drop_view(
            &self,
            _database: &Namespace,
            _view: &str,
            _options: DropViewOptions,
        ) -> CatalogResult<()> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn runtime_provider_forwards_alter_table_stats_to_inner_provider() {
        let alter_stats_called = Arc::new(AtomicBool::new(false));
        let provider = RuntimeAwareCatalogProvider::try_new(
            {
                let alter_stats_called = alter_stats_called.clone();
                move || {
                    Ok(StatsProvider {
                        alter_stats_called,
                        partition_mutation_calls: Arc::new(AtomicUsize::new(0)),
                    })
                }
            },
            tokio::runtime::Handle::current(),
        )
        .unwrap();

        provider
            .alter_table_stats(&Namespace::try_from(vec!["db"]).unwrap(), "tbl", None)
            .await
            .unwrap();

        assert!(alter_stats_called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn runtime_provider_forwards_get_partitions_to_inner_provider() {
        let provider = RuntimeAwareCatalogProvider::try_new(
            || {
                Ok(StatsProvider {
                    alter_stats_called: Arc::new(AtomicBool::new(false)),
                    partition_mutation_calls: Arc::new(AtomicUsize::new(0)),
                })
            },
            tokio::runtime::Handle::current(),
        )
        .unwrap();

        let partitions = provider
            .get_partitions(
                &Namespace::try_from(vec!["db"]).unwrap(),
                "tbl",
                GetPartitionsOptions::default(),
            )
            .await
            .unwrap();

        assert_eq!(
            partitions[0].spec,
            vec![("dt".to_string(), "2026-04-26".to_string())]
        );
    }

    #[tokio::test]
    async fn runtime_provider_forwards_partition_mutations_to_inner_provider() {
        let partition_mutation_calls = Arc::new(AtomicUsize::new(0));
        let provider = RuntimeAwareCatalogProvider::try_new(
            {
                let partition_mutation_calls = partition_mutation_calls.clone();
                move || {
                    Ok(StatsProvider {
                        alter_stats_called: Arc::new(AtomicBool::new(false)),
                        partition_mutation_calls,
                    })
                }
            },
            tokio::runtime::Handle::current(),
        )
        .unwrap();
        let database = Namespace::try_from(vec!["db"]).unwrap();
        let partition = PartitionStatus {
            spec: vec![("dt".to_string(), "2026-04-26".to_string())],
            location: None,
            parameters: Default::default(),
            create_time: None,
            last_access_time: None,
            statistics: None,
        };
        let spec = partition.spec.clone();

        provider
            .create_partitions(
                &database,
                "tbl",
                vec![partition.clone()],
                CreatePartitionsOptions {
                    ignore_if_exists: true,
                },
            )
            .await
            .unwrap();
        provider
            .drop_partitions(
                &database,
                "tbl",
                vec![spec.clone()],
                DropPartitionsOptions {
                    ignore_if_not_exists: true,
                    purge: true,
                    retain_data: false,
                },
            )
            .await
            .unwrap();
        provider
            .alter_partitions(&database, "tbl", vec![partition.clone()])
            .await
            .unwrap();
        provider
            .rename_partitions(&database, "tbl", vec![spec.clone()], vec![spec])
            .await
            .unwrap();

        assert_eq!(partition_mutation_calls.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn default_partition_and_stats_methods_return_not_supported() {
        let provider = DefaultOnlyProvider;
        let database = Namespace::try_from(vec!["db"]).unwrap();
        let partition = PartitionStatus {
            spec: vec![("dt".to_string(), "2026-04-26".to_string())],
            location: None,
            parameters: Default::default(),
            create_time: None,
            last_access_time: None,
            statistics: None,
        };
        let spec = partition.spec.clone();

        let errors = vec![
            provider
                .alter_table_stats(&database, "tbl", None)
                .await
                .unwrap_err(),
            provider
                .get_partitions(&database, "tbl", GetPartitionsOptions::default())
                .await
                .unwrap_err(),
            provider
                .create_partitions(
                    &database,
                    "tbl",
                    vec![partition.clone()],
                    CreatePartitionsOptions {
                        ignore_if_exists: true,
                    },
                )
                .await
                .unwrap_err(),
            provider
                .drop_partitions(
                    &database,
                    "tbl",
                    vec![spec.clone()],
                    DropPartitionsOptions {
                        ignore_if_not_exists: true,
                        purge: false,
                        retain_data: true,
                    },
                )
                .await
                .unwrap_err(),
            provider
                .alter_partitions(&database, "tbl", vec![partition])
                .await
                .unwrap_err(),
            provider
                .rename_partitions(&database, "tbl", vec![spec.clone()], vec![spec])
                .await
                .unwrap_err(),
        ];

        assert!(errors
            .into_iter()
            .all(|error| matches!(error, CatalogError::NotSupported(_))));
    }
}
