use std::sync::Arc;

use datafusion::execution::TaskContext;
use datafusion_common::{DataFusionError, Result};
use moka::future::Cache as FutureCache;
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use url::Url;

use crate::kernel::DeltaSnapshotConfig;
use crate::storage::StorageConfig;
use crate::table::{
    create_logstore_with_object_store, load_catalog_managed_commits_for_snapshot, DeltaSnapshot,
    DeltaTable,
};

const DEFAULT_MAX_ENTRIES: u64 = 1024;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct TableCacheKey {
    pub(crate) table_url: String,
    pub(crate) version: i64,
    pub(crate) catalog_table: Vec<String>,
}

pub(crate) struct CachedTable {
    pub(crate) snapshot: Arc<DeltaSnapshot>,
    pub(crate) log_store: crate::storage::LogStoreRef,
}

pub struct DeltaTableCache {
    cache: FutureCache<TableCacheKey, Arc<CachedTable>>,
}

impl DeltaTableCache {
    pub fn new(max_entries: u64) -> Self {
        let cache = FutureCache::builder().max_capacity(max_entries).build();
        Self { cache }
    }

    pub(crate) async fn get(
        &self,
        context: &TaskContext,
        table_url: &Url,
        version: i64,
        lakehouse_table: Option<&LakehouseExecutionContext>,
    ) -> Result<Arc<CachedTable>> {
        let key = TableCacheKey {
            table_url: table_url.to_string(),
            version,
            catalog_table: lakehouse_table
                .map(LakehouseExecutionContext::catalog_table)
                .unwrap_or_default()
                .to_vec(),
        };
        let table_url = table_url.clone();
        let lakehouse_table = lakehouse_table.cloned();
        self.cache
            .try_get_with(key, async move {
                load_table_uncached(context, &table_url, version, lakehouse_table.as_ref()).await
            })
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}

impl Default for DeltaTableCache {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_ENTRIES)
    }
}

impl sail_common_datafusion::extension::SessionExtension for DeltaTableCache {
    fn name() -> &'static str {
        "delta_table_cache"
    }
}

pub(crate) async fn load_table_uncached(
    context: &TaskContext,
    table_url: &Url,
    version: i64,
    lakehouse_table: Option<&LakehouseExecutionContext>,
) -> Result<Arc<CachedTable>> {
    let object_store = context
        .runtime_env()
        .object_store_registry
        .get_store(table_url)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let log_store =
        create_logstore_with_object_store(object_store, table_url.clone(), StorageConfig)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let mut table_config = DeltaSnapshotConfig {
        require_files: false,
        ..Default::default()
    };
    if let Some(lakehouse_table) = lakehouse_table {
        table_config.catalog_managed_commits = load_catalog_managed_commits_for_snapshot(
            context,
            lakehouse_table,
            table_url,
            log_store.clone(),
            Some(version),
        )
        .await?;
    }
    let mut table = DeltaTable::new(log_store.clone(), table_config);
    table
        .load_version(version)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let snapshot_state = table
        .snapshot()
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .clone();
    Ok(Arc::new(CachedTable {
        snapshot: snapshot_state,
        log_store: table.log_store(),
    }))
}
