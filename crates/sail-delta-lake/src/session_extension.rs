// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::execution::TaskContext;
use datafusion_common::{DataFusionError, Result};
use moka::future::Cache as FutureCache;
use url::Url;

use crate::delta_log::StorageConfig;
use crate::snapshot::{CatalogManagedCommitSet, DeltaSnapshotConfig};
use crate::table::{DeltaSnapshot, DeltaTable, create_logstore_with_object_store};

const DEFAULT_MAX_ENTRIES: u64 = 1024;

/// The key of a table that is loaded at a version. A version of a catalog-managed table is
/// loaded with catalog commits, which are identified by their versions and their files.
/// What a catalog tells about the latest version of the table is not part of the key, since
/// it changes with every commit to the table while the version stays the same.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct TableCacheKey {
    pub(crate) table_url: String,
    pub(crate) version: i64,
    pub(crate) catalog_managed_commits: Option<Vec<(i64, String)>>,
}

impl TableCacheKey {
    fn new(
        table_url: &Url,
        version: i64,
        catalog_managed_commits: Option<&CatalogManagedCommitSet>,
    ) -> Self {
        Self {
            table_url: table_url.to_string(),
            version,
            catalog_managed_commits: catalog_managed_commits.map(|commits| {
                commits
                    .commits
                    .iter()
                    .map(|commit| (commit.version, commit.file_name.clone()))
                    .collect()
            }),
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

    /// Returns the table at the version, which is loaded with the catalog commits for a
    /// catalog-managed table. The catalog is not asked.
    pub(crate) async fn get(
        &self,
        context: &TaskContext,
        table_url: &Url,
        version: i64,
        catalog_managed_commits: Option<&CatalogManagedCommitSet>,
    ) -> Result<Arc<CachedTable>> {
        let key = TableCacheKey::new(table_url, version, catalog_managed_commits);
        let table_url = table_url.clone();
        let catalog_managed_commits = catalog_managed_commits.cloned();
        self.cache
            .try_get_with(key, async move {
                load_table_uncached(context, &table_url, version, catalog_managed_commits).await
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
    catalog_managed_commits: Option<CatalogManagedCommitSet>,
) -> Result<Arc<CachedTable>> {
    let object_store = context
        .runtime_env()
        .object_store_registry
        .get_store(table_url)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let log_store =
        create_logstore_with_object_store(object_store, table_url.clone(), StorageConfig)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let table_config = DeltaSnapshotConfig {
        require_files: false,
        catalog_managed_commits,
        ..Default::default()
    };
    let mut table = DeltaTable::new(log_store, table_config);
    table
        .load_version(version)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    cached_table(table)
}

fn cached_table(table: DeltaTable) -> Result<Arc<CachedTable>> {
    let snapshot = table
        .snapshot()
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .clone();
    Ok(Arc::new(CachedTable {
        snapshot,
        log_store: table.log_store(),
    }))
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::snapshot::CatalogManagedCommitFile;

    fn commits(latest_table_version: i64, files: &[(i64, &str)]) -> CatalogManagedCommitSet {
        CatalogManagedCommitSet {
            latest_table_version,
            commits: files
                .iter()
                .map(|(version, file_name)| CatalogManagedCommitFile {
                    version: *version,
                    file_name: file_name.to_string(),
                    file_size: 0,
                    file_modification_timestamp: 0,
                })
                .collect(),
        }
    }

    #[test]
    fn table_cache_key_is_stable_without_catalog_commits() {
        let url = Url::parse("file:///tmp/table").unwrap();

        assert_eq!(
            TableCacheKey::new(&url, 7, None),
            TableCacheKey::new(&url, 7, None)
        );
        assert_ne!(
            TableCacheKey::new(&url, 7, None),
            TableCacheKey::new(&url, 8, None)
        );
    }

    #[test]
    fn table_cache_key_does_not_change_with_the_latest_version_of_the_table() {
        let url = Url::parse("file:///tmp/table").unwrap();
        let files = [(6, "6.a.json"), (7, "7.b.json")];

        assert_eq!(
            TableCacheKey::new(&url, 7, Some(&commits(7, &files))),
            TableCacheKey::new(&url, 7, Some(&commits(9, &files)))
        );
    }

    #[test]
    fn table_cache_key_separates_catalog_commits() {
        let url = Url::parse("file:///tmp/table").unwrap();

        assert_ne!(
            TableCacheKey::new(&url, 7, Some(&commits(7, &[(7, "7.a.json")]))),
            TableCacheKey::new(&url, 7, Some(&commits(7, &[(7, "7.b.json")])))
        );
        // A table that is not catalog-managed is not loaded as a catalog-managed table
        // whose commits are all published.
        assert_ne!(
            TableCacheKey::new(&url, 7, Some(&commits(7, &[]))),
            TableCacheKey::new(&url, 7, None)
        );
    }
}
