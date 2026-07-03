use std::sync::Arc;

use datafusion::execution::TaskContext;
use datafusion_common::{DataFusionError, Result};
use moka::future::Cache as FutureCache;
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use url::Url;

use crate::delta_log::StorageConfig;
use crate::snapshot::DeltaSnapshotConfig;
use crate::table::{
    catalog_managed_commit_context, create_logstore_with_object_store,
    load_catalog_managed_commits_for_snapshot, DeltaSnapshot, DeltaTable,
};

const DEFAULT_MAX_ENTRIES: u64 = 1024;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct TableCacheKey {
    pub(crate) table_url: String,
    pub(crate) version: i64,
    pub(crate) lakehouse_table: Option<LakehouseExecutionContext>,
}

impl TableCacheKey {
    fn new(
        table_url: &Url,
        version: i64,
        lakehouse_table: Option<&LakehouseExecutionContext>,
    ) -> Self {
        Self {
            table_url: table_url.to_string(),
            version,
            lakehouse_table: lakehouse_table.cloned(),
        }
    }
}

pub(crate) struct CachedTable {
    pub(crate) snapshot: Arc<DeltaSnapshot>,
    pub(crate) log_store: crate::delta_log::LogStoreRef,
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
        let lakehouse_table = catalog_managed_commit_context(lakehouse_table);
        let key = TableCacheKey::new(table_url, version, lakehouse_table);
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
    if let Some(lakehouse_table) = catalog_managed_commit_context(lakehouse_table) {
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

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use sail_common_datafusion::catalog::{
        CapabilityFingerprint, CatalogProviderId, CatalogTableIdentity, CommitAuthority,
        LakehouseAuthority, LakehouseExecutionContext, LakehouseFormat, LakehouseOperation,
        MetadataPointerAuthority, ScanAuthority, TableAccessSessionRef, TableLifecycle,
    };

    use super::*;

    fn lakehouse_context(session_fingerprint: &str) -> LakehouseExecutionContext {
        let mut context = LakehouseExecutionContext::catalog_table_context(
            CatalogProviderId("unity".to_string()),
            vec![
                "unity".to_string(),
                "schema".to_string(),
                "table".to_string(),
            ],
            CatalogTableIdentity {
                table_id: Some("table-id".to_string()),
                table_uri: Some("file:///tmp/table".to_string()),
            },
            LakehouseOperation::Read,
            LakehouseFormat::Delta,
            LakehouseAuthority::CatalogAuthoritative {
                lifecycle: TableLifecycle::Managed,
                pointer: MetadataPointerAuthority::DeltaRatifiedCommits,
                commit: CommitAuthority::DeltaRatifiedCommit,
            },
            ScanAuthority::ClientTableFormat,
        );
        context.access_session = Some(TableAccessSessionRef {
            fingerprint: session_fingerprint.to_string(),
        });
        context.capability_fingerprint =
            CapabilityFingerprint(format!("unity:schema.table:{session_fingerprint}"));
        context
    }

    #[test]
    fn table_cache_key_is_stable_without_lakehouse_context() {
        let url = Url::parse("file:///tmp/table").unwrap();

        assert_eq!(
            TableCacheKey::new(&url, 7, None),
            TableCacheKey::new(&url, 7, None)
        );
    }

    #[test]
    fn table_cache_key_separates_access_sessions() {
        let url = Url::parse("file:///tmp/table").unwrap();
        let session_a = lakehouse_context("session-a");
        let session_b = lakehouse_context("session-b");

        assert_ne!(
            TableCacheKey::new(&url, 7, Some(&session_a)),
            TableCacheKey::new(&url, 7, Some(&session_b))
        );
    }
}
