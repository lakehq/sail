use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use datafusion::common::Statistics;
use datafusion::execution::cache::cache_manager::{FileStatisticsCache, FileStatisticsCacheEntry};
use datafusion::execution::cache::CacheAccessor;
use log::{debug, error};
use moka::sync::Cache;
use object_store::path::Path;
use object_store::ObjectMeta;

pub struct MokaFileStatisticsCache {
    statistics: Cache<Path, (ObjectMeta, Arc<Statistics>)>,
}

impl MokaFileStatisticsCache {
    const NAME: &'static str = "MokaFileStatisticsCache";

    pub fn new(ttl: Option<u64>, max_entries: Option<u64>) -> Self {
        let mut builder = Cache::builder();

        if let Some(ttl) = ttl {
            debug!("Setting TTL for {} to {ttl} second(s)", Self::NAME);
            builder = builder.time_to_live(Duration::from_secs(ttl));
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

impl CacheAccessor<Path, Arc<Statistics>> for MokaFileStatisticsCache {
    type Extra = ObjectMeta;

    fn get(&self, k: &Path) -> Option<Arc<Statistics>> {
        self.statistics
            .get(k)
            .map(|(_saved_meta, statistics)| statistics)
    }

    fn get_with_extra(&self, k: &Path, e: &Self::Extra) -> Option<Arc<Statistics>> {
        self.statistics.get(k).and_then(|(saved_meta, statistics)| {
            if saved_meta.size == e.size && saved_meta.last_modified == e.last_modified {
                Some(Arc::clone(&statistics))
            } else {
                None
            }
        })
    }

    fn put(&self, _key: &Path, _value: Arc<Statistics>) -> Option<Arc<Statistics>> {
        error!("Put cache in {} without Extra is not supported", Self::NAME);
        None
    }

    fn put_with_extra(
        &self,
        key: &Path,
        value: Arc<Statistics>,
        e: &Self::Extra,
    ) -> Option<Arc<Statistics>> {
        self.statistics.insert(key.clone(), (e.clone(), value));
        None
    }

    fn remove(&self, k: &Path) -> Option<Arc<Statistics>> {
        self.statistics.remove(k).map(|(_, statistics)| statistics)
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
            .map(|(path, (object_meta, stats))| {
                (
                    path.as_ref().clone(),
                    FileStatisticsCacheEntry {
                        object_meta,
                        num_rows: stats.num_rows,
                        num_columns: stats.column_statistics.len(),
                        table_size_bytes: stats.total_byte_size,
                        statistics_size_bytes: 0, // TODO: set to the real size in the future
                    },
                )
            })
            .collect()
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
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
        assert!(cache.get_with_extra(&meta.location, &meta).is_none());

        cache.put_with_extra(
            &meta.location,
            Statistics::new_unknown(&Schema::new(vec![Field::new(
                "test_column",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            )]))
            .into(),
            &meta,
        );
        assert!(cache.get_with_extra(&meta.location, &meta).is_some());

        // file size changed
        let mut meta2 = meta.clone();
        meta2.size = 2048;
        assert!(cache.get_with_extra(&meta2.location, &meta2).is_none());

        // file last_modified changed
        let mut meta2 = meta.clone();
        meta2.last_modified = DateTime::parse_from_rfc3339("2022-09-27T22:40:00+02:00")
            .unwrap()
            .into();
        assert!(cache.get_with_extra(&meta2.location, &meta2).is_none());

        // different file
        let mut meta2 = meta;
        meta2.location = Path::from("test2");
        assert!(cache.get_with_extra(&meta2.location, &meta2).is_none());
    }
}
