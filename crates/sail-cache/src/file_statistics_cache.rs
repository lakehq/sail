use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use datafusion::common::heap_size::{DFHeapSize, DFHeapSizeCtx};
use datafusion::common::{Result, TableReference};
use datafusion::execution::cache::cache_manager::{
    CachedFileMetadata, FileStatisticsCache, FileStatisticsCacheEntry,
};
use datafusion::execution::cache::{CacheAccessor, TableScopedPath};
use log::debug;
use moka::sync::Cache;

pub struct MokaFileStatisticsCache {
    cache_limit: AtomicUsize,
    statistics: Cache<TableScopedPath, CachedFileMetadata>,
}

impl MokaFileStatisticsCache {
    const NAME: &'static str = "MokaFileStatisticsCache";

    pub fn new(ttl: Option<u64>, max_entries: Option<u64>) -> Self {
        let mut builder = Cache::builder();
        builder = builder.weigher(|k: &TableScopedPath, v: &CachedFileMetadata| {
            let mut ctx = DFHeapSizeCtx::default();
            let bytes = k.heap_size(&mut ctx) + v.heap_size(&mut ctx);
            u32::try_from(bytes).unwrap_or(u32::MAX)
        });

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
            cache_limit: AtomicUsize::new(
                max_entries
                    .and_then(|x| usize::try_from(x).ok())
                    .unwrap_or(usize::MAX),
            ),
            statistics: builder.build(),
        }
    }
}

impl CacheAccessor<TableScopedPath, CachedFileMetadata> for MokaFileStatisticsCache {
    fn get(&self, k: &TableScopedPath) -> Option<CachedFileMetadata> {
        self.statistics.get(k)
    }

    fn put(&self, key: &TableScopedPath, value: CachedFileMetadata) -> Option<CachedFileMetadata> {
        self.statistics.insert(key.clone(), value);
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
        if limit == 0 {
            self.clear();
        }
    }

    fn list_entries(&self) -> HashMap<TableScopedPath, FileStatisticsCacheEntry> {
        let mut ctx = DFHeapSizeCtx::default();
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
                        statistics_size_bytes: cached.statistics.heap_size(&mut ctx),
                        has_ordering: cached.ordering.is_some(),
                    },
                )
            })
            .collect()
    }

    fn drop_table_entries(&self, table_ref: &Option<TableReference>) -> Result<()> {
        let keys_to_remove = self
            .statistics
            .iter()
            .filter_map(|(key, _)| {
                if key.table == *table_ref {
                    Some(key.as_ref().clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        for key in keys_to_remove {
            self.statistics.invalidate(&key);
        }
        Ok(())
    }
}

#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::DateTime;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::common::Statistics;
    use datafusion::execution::cache::TableScopedPath;
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
        let scoped = TableScopedPath {
            table: None,
            path: meta.location.clone(),
        };
        assert!(cache.get(&scoped).is_none());

        let stats = Arc::new(Statistics::new_unknown(&Schema::new(vec![Field::new(
            "test_column",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        )])));
        let cached = CachedFileMetadata::new(meta.clone(), Arc::clone(&stats), None);
        cache.put(&scoped, cached);
        let cached = cache.get(&scoped);
        assert!(cached.is_some());
        assert!(cached.unwrap().is_valid_for(&meta));

        // file size changed
        let mut meta2 = meta.clone();
        meta2.size = 2048;
        let scoped2 = TableScopedPath {
            table: None,
            path: meta2.location.clone(),
        };
        assert!(!cache
            .get(&scoped2)
            .map(|c| c.is_valid_for(&meta2))
            .unwrap_or(false));

        // different file
        let mut meta2 = meta;
        meta2.location = Path::from("test2");
        let scoped2 = TableScopedPath {
            table: None,
            path: meta2.location.clone(),
        };
        assert!(cache.get(&scoped2).is_none());
    }
}
