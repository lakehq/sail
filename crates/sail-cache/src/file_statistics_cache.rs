use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use datafusion::common::{Result, TableReference};
use datafusion::execution::cache::cache_manager::{
    CachedFileMetadata, FileStatisticsCache, FileStatisticsCacheEntry,
};
use datafusion::execution::cache::{CacheAccessor, TableScopedPath};
use log::debug;
use moka::sync::Cache;
use object_store::path::Path;

pub struct MokaFileStatisticsCache {
    statistics: Cache<TableScopedPath, CachedFileMetadata>,
    cache_limit: Arc<AtomicUsize>,
}

impl MokaFileStatisticsCache {
    const NAME: &'static str = "MokaFileStatisticsCache";
    const DEFAULT_CACHE_LIMIT: usize = usize::MAX;

    pub fn new(ttl: Option<u64>, max_entries: Option<u64>) -> Self {
        let mut builder = Cache::builder();

        if let Some(ttl) = ttl {
            let ttl = Duration::from_secs(ttl);
            debug!("Setting TTL for {} to {ttl:?}", Self::NAME);
            builder = builder.time_to_live(ttl);
        }
        if let Some(max_entries) = max_entries {
            debug!(
                "Setting maximum number of entries for {} to {max_entries}",
                Self::NAME
            );
            builder = builder.max_capacity(max_entries);
        }

        Self {
            statistics: builder.build(),
            cache_limit: Arc::new(AtomicUsize::new(Self::DEFAULT_CACHE_LIMIT)),
        }
    }
}

impl CacheAccessor<TableScopedPath, CachedFileMetadata> for MokaFileStatisticsCache {
    fn get(&self, k: &TableScopedPath) -> Option<CachedFileMetadata> {
        self.statistics.get(k)
    }

    fn put(&self, key: &TableScopedPath, value: CachedFileMetadata) -> Option<CachedFileMetadata> {
        // Honor the runtime `cache_limit`; moka's `entry_count` is eventually
        // consistent so the bound is `cache_limit + O(concurrent writers)`.
        // Updates to existing keys are always allowed.
        let cache_limit = self.cache_limit.load(Ordering::Relaxed);
        if self.statistics.contains_key(key)
            || (self.statistics.entry_count() as usize) < cache_limit
        {
            self.statistics.insert(key.clone(), value);
        }
        None
    }

    fn remove(&self, k: &TableScopedPath) -> Option<CachedFileMetadata> {
        self.statistics.remove(k)
    }

    fn contains_key(&self, k: &TableScopedPath) -> bool {
        self.statistics.contains_key(k)
    }

    fn len(&self) -> usize {
        self.statistics.entry_count() as usize
    }

    fn clear(&self) {
        self.statistics.invalidate_all();
    }

    fn name(&self) -> String {
        Self::NAME.to_string()
    }
}

impl FileStatisticsCache for MokaFileStatisticsCache {
    fn cache_limit(&self) -> usize {
        self.cache_limit.load(Ordering::Relaxed)
    }

    fn update_cache_limit(&self, limit: usize) {
        self.cache_limit.store(limit, Ordering::Relaxed);
    }

    fn list_entries(&self) -> HashMap<TableScopedPath, FileStatisticsCacheEntry> {
        self.statistics
            .iter()
            .map(|(path, cached)| {
                (
                    path.as_ref().clone(),
                    FileStatisticsCacheEntry {
                        object_meta: cached.meta.clone(),
                        num_rows: cached.statistics.num_rows,
                        num_columns: cached.statistics.column_statistics.len(),
                        table_size_bytes: cached.statistics.total_byte_size,
                        statistics_size_bytes: 0,
                        has_ordering: cached.ordering.is_some(),
                    },
                )
            })
            .collect()
    }

    fn drop_table_entries(&self, table_ref: &Option<TableReference>) -> Result<()> {
        let keys_to_remove: Vec<_> = self
            .statistics
            .iter()
            .filter(|(k, _)| &k.table == table_ref)
            .map(|(k, _)| k.as_ref().clone())
            .collect();
        for key in keys_to_remove {
            self.statistics.remove(&key);
        }
        Ok(())
    }
}

/// Helper to create a `TableScopedPath` from a bare `Path` (no table scope).
pub fn scoped_path(path: Path) -> TableScopedPath {
    TableScopedPath { table: None, path }
}

#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::DateTime;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::common::Statistics;
    use object_store::path::Path;
    use object_store::ObjectMeta;

    use super::*;

    #[test]
    fn test_file_statistics_cache() {
        let meta = ObjectMeta {
            location: Path::from("test"),
            last_modified: DateTime::parse_from_rfc3339("2022-09-27T22:36:00+02:00")
                .unwrap()
                .into(),
            size: 1024,
            e_tag: None,
            version: None,
        };
        let cache = MokaFileStatisticsCache::new(None, None);
        let key = scoped_path(meta.location.clone());
        assert!(cache.get(&key).is_none());

        let stats = Arc::new(Statistics::new_unknown(&Schema::new(vec![Field::new(
            "test_column",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        )])));
        let cached = CachedFileMetadata::new(meta.clone(), Arc::clone(&stats), None);
        cache.put(&key, cached);
        let cached = cache.get(&key);
        assert!(cached.is_some());
        assert!(cached.unwrap().is_valid_for(&meta));

        // file size changed
        let mut meta2 = meta.clone();
        meta2.size = 2048;
        let key2 = scoped_path(meta2.location.clone());
        assert!(!cache
            .get(&key2)
            .map(|c| c.is_valid_for(&meta2))
            .unwrap_or(false));

        // different file
        let mut meta2 = meta;
        meta2.location = Path::from("test2");
        let key3 = scoped_path(meta2.location.clone());
        assert!(cache.get(&key3).is_none());
    }

    #[test]
    fn put_respects_runtime_cache_limit() {
        let cache = MokaFileStatisticsCache::new(None, None);
        cache.update_cache_limit(1);

        let make = |name: &str| {
            let meta = ObjectMeta {
                location: Path::from(name),
                last_modified: DateTime::parse_from_rfc3339("2022-09-27T22:36:00+02:00")
                    .unwrap()
                    .into(),
                size: 1024,
                e_tag: None,
                version: None,
            };
            let stats = Arc::new(Statistics::new_unknown(&Schema::new(vec![Field::new(
                "c",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            )])));
            let cached = CachedFileMetadata::new(meta.clone(), stats, None);
            (scoped_path(meta.location.clone()), cached)
        };

        let (k1, v1) = make("a");
        let (k2, v2) = make("b");
        cache.put(&k1, v1);
        // `entry_count` is eventually consistent in moka; settle before asserting.
        cache.statistics.run_pending_tasks();
        cache.put(&k2, v2.clone());
        cache.statistics.run_pending_tasks();

        assert!(cache.get(&k1).is_some(), "first entry must be retained");
        assert!(
            cache.get(&k2).is_none(),
            "second entry must be rejected when at limit"
        );

        // Updating an existing key is always allowed, even at limit.
        cache.put(&k1, v2);
        cache.statistics.run_pending_tasks();
        assert!(cache.get(&k1).is_some());
    }
}
