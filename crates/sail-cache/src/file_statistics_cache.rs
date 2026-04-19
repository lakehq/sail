use std::collections::HashMap;
use std::time::Duration;

use datafusion::execution::cache::cache_manager::{
    CachedFileMetadata, FileStatisticsCache, FileStatisticsCacheEntry,
};
use datafusion::execution::cache::CacheAccessor;
use log::debug;
use moka::sync::Cache;
use object_store::path::Path;

pub struct MokaFileStatisticsCache {
    statistics: Cache<Path, CachedFileMetadata>,
}

impl MokaFileStatisticsCache {
    const NAME: &'static str = "MokaFileStatisticsCache";

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
        }
    }
}

impl CacheAccessor<Path, CachedFileMetadata> for MokaFileStatisticsCache {
    fn get(&self, k: &Path) -> Option<CachedFileMetadata> {
        self.statistics.get(k)
    }

    fn put(&self, key: &Path, value: CachedFileMetadata) -> Option<CachedFileMetadata> {
        self.statistics.insert(key.clone(), value);
        None
    }

    fn remove(&self, k: &Path) -> Option<CachedFileMetadata> {
        self.statistics.remove(k)
    }

    fn contains_key(&self, k: &Path) -> bool {
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
    fn list_entries(&self) -> HashMap<Path, FileStatisticsCacheEntry> {
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
                        statistics_size_bytes: 0, // TODO: set to the real size in the future
                        has_ordering: cached.ordering.is_some(),
                    },
                )
            })
            .collect()
    }
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
        assert!(cache.get(&meta.location).is_none());

        let stats = Arc::new(Statistics::new_unknown(&Schema::new(vec![Field::new(
            "test_column",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        )])));
        let cached = CachedFileMetadata::new(meta.clone(), Arc::clone(&stats), None);
        cache.put(&meta.location, cached);
        let cached = cache.get(&meta.location);
        assert!(cached.is_some());
        assert!(cached.unwrap().is_valid_for(&meta));

        // file size changed
        let mut meta2 = meta.clone();
        meta2.size = 2048;
        assert!(!cache
            .get(&meta2.location)
            .map(|c| c.is_valid_for(&meta2))
            .unwrap_or(false));

        // different file
        let mut meta2 = meta;
        meta2.location = Path::from("test2");
        assert!(cache.get(&meta2.location).is_none());
    }
}
