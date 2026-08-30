use std::time::Duration;

use datafusion::common::{HashMap, Result, TableReference};
use datafusion::execution::cache::cache_manager::CachedFileMetadata;
use datafusion::execution::cache::{
    Cache as DataFusionCache, CacheEntryInfo, CacheValue, TableScopedPath,
};
use log::debug;
use moka::sync::Cache;

pub struct MokaFileStatisticsCache {
    statistics: Cache<TableScopedPath, CachedFileMetadata>,
    ttl: Option<Duration>,
    max_entries: Option<u64>,
}

impl MokaFileStatisticsCache {
    const NAME: &'static str = "MokaFileStatisticsCache";

    pub fn new(ttl: Option<u64>, max_entries: Option<u64>) -> Self {
        let mut builder = Cache::builder();

        let ttl = ttl.map(Duration::from_secs);
        if let Some(ttl) = ttl {
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
            ttl,
            max_entries,
        }
    }
}

impl DataFusionCache<TableScopedPath, CachedFileMetadata> for MokaFileStatisticsCache {
    fn get(&self, key: &TableScopedPath) -> Option<CachedFileMetadata> {
        self.statistics.get(key)
    }

    fn put(&self, key: &TableScopedPath, value: CachedFileMetadata) -> Option<CachedFileMetadata> {
        let previous = self.statistics.get(key);
        self.statistics.insert(key.clone(), value);
        previous
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

    fn cache_limit(&self) -> usize {
        self.max_entries
            .map(|limit| limit as usize)
            .unwrap_or(usize::MAX)
    }

    fn update_cache_limit(&self, _limit: usize) {
        // TODO: support dynamic update of cache limit
    }

    fn cache_ttl(&self) -> Option<Duration> {
        self.ttl
    }

    fn update_cache_ttl(&self, _ttl: Option<Duration>) {
        // TODO: support dynamic update of cache ttl
    }

    fn list_entries(&self) -> HashMap<TableScopedPath, CacheEntryInfo<CachedFileMetadata>> {
        self.statistics
            .iter()
            .map(|(path, cached)| {
                (
                    path.as_ref().clone(),
                    CacheEntryInfo {
                        size_bytes: cached.size(),
                        value: cached,
                        hits: 0,
                        expires: None,
                    },
                )
            })
            .collect()
    }

    fn drop_table_entries(&self, table_ref: &TableReference) -> Result<()> {
        let keys_to_remove: Vec<_> = self
            .statistics
            .iter()
            .filter(|(key, _)| key.table.as_ref() == Some(table_ref))
            .map(|(key, _)| key.as_ref().clone())
            .collect();
        for key in keys_to_remove {
            self.statistics.remove(&key);
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
    use datafusion::execution::cache::SchemaFingerprint;
    use object_store::ObjectMeta;
    use object_store::path::Path;

    use super::*;

    pub fn scoped_path(path: Path) -> TableScopedPath {
        TableScopedPath { table: None, path }
    }

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

        let schema = Schema::new(vec![Field::new(
            "test_column",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        )]);
        let schema_fingerprint = Arc::new(SchemaFingerprint::from_schema(&schema));
        let stats = Arc::new(Statistics::new_unknown(&schema));
        let cached = CachedFileMetadata::new(
            meta.clone(),
            Arc::clone(&schema_fingerprint),
            Arc::clone(&stats),
            None,
        );
        cache.put(&key, cached);
        let cached = cache.get(&key);
        assert!(cached.is_some());
        assert!(cached.unwrap().is_valid_for(&meta, &schema_fingerprint));

        // file size changed
        let mut meta2 = meta.clone();
        meta2.size = 2048;
        let key2 = scoped_path(meta2.location.clone());
        assert!(
            !cache
                .get(&key2)
                .map(|c| c.is_valid_for(&meta2, &schema_fingerprint))
                .unwrap_or(false)
        );

        // different file
        let mut meta2 = meta;
        meta2.location = Path::from("test2");
        let key3 = scoped_path(meta2.location.clone());
        assert!(cache.get(&key3).is_none());
    }
}
