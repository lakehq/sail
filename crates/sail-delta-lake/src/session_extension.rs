use std::sync::Arc;

use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::TaskContext;
use datafusion_common::{DataFusionError, Result};
use moka::future::Cache as FutureCache;
use url::Url;

use crate::kernel::DeltaTableConfig;
use crate::storage::StorageConfig;
use crate::table::open_table_with_object_store_and_table_config_at_version;

const DEFAULT_MAX_ENTRIES: u64 = 1024;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct TableCacheKey {
    pub(crate) table_url: String,
    pub(crate) version: i64,
}

pub(crate) struct CachedTable {
    pub(crate) snapshot: crate::table::DeltaTableState,
    pub(crate) log_store: crate::storage::LogStoreRef,
}

pub struct DeltaTableCache {
    cache: FutureCache<TableCacheKey, Arc<CachedTable>>,
}

impl DeltaTableCache {
    pub fn new(max_entries: u64) -> Self {
        // NOTE: We intentionally scope this cache to the SessionConfig (via extension).
        // This avoids leaking state across sessions / RuntimeEnvs (per-user credentials, etc).
        let cache = FutureCache::builder().max_capacity(max_entries).build();
        Self { cache }
    }

    pub(crate) async fn get(
        &self,
        context: &TaskContext,
        table_url: &Url,
        version: i64,
    ) -> Result<Arc<CachedTable>> {
        let key = TableCacheKey {
            table_url: table_url.to_string(),
            version,
        };
        let runtime_env = context.runtime_env();
        let table_url = table_url.clone();
        self.cache
            .try_get_with(key, async move {
                load_table_uncached(runtime_env, &table_url, version).await
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
    runtime_env: Arc<RuntimeEnv>,
    table_url: &Url,
    version: i64,
) -> Result<Arc<CachedTable>> {
    let object_store = runtime_env
        .object_store_registry
        .get_store(table_url)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let table_config = DeltaTableConfig {
        // Avoid eagerly materializing file-level metadata on the driver; the scan exec
        // will consume Add actions from upstream to find the actual files to read.
        require_files: false,
        ..Default::default()
    };
    let table = open_table_with_object_store_and_table_config_at_version(
        table_url.clone(),
        object_store,
        StorageConfig,
        table_config,
        version,
    )
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
