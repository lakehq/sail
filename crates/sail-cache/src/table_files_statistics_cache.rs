use std::sync::{Arc, LazyLock};

use datafusion::common::Statistics;
use datafusion::execution::cache::CacheAccessor;
use log::{debug, error};
use moka::sync::Cache;
use object_store::path::Path;
use object_store::ObjectMeta;

use crate::try_parse_non_zero_u64;

pub static GLOBAL_FILE_STATISTICS_CACHE: LazyLock<Arc<MokaFileStatisticsCache>> =
    LazyLock::new(|| {
        let max_entries =
            std::env::var("SAIL_PARQUET__TABLE_FILES_STATISTICS_CACHE_MAX_ENTRIES").ok();
        Arc::new(MokaFileStatisticsCache::new(max_entries))
    });

pub struct MokaFileStatisticsCache {
    statistics: Cache<Path, (ObjectMeta, Arc<Statistics>)>,
}

impl MokaFileStatisticsCache {
    pub fn new(max_entries: Option<String>) -> Self {
        let mut builder = Cache::builder();
        if let Some(max_entries) = max_entries {
            if let Some(max_entries) = try_parse_non_zero_u64(&max_entries) {
                debug!("Setting max entries for MokaFileStatisticsCache to {max_entries}");
                builder = builder.max_capacity(max_entries);
            } else {
                debug!(
                    "Disabled or invalid max entries for MokaFileStatisticsCache: {max_entries}"
                );
            }
        } else {
            debug!("No max entries set for MokaFileStatisticsCache");
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
        error!("Put cache in MokaFileStatisticsCache without Extra not supported.");
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

    fn remove(&mut self, k: &Path) -> Option<Arc<Statistics>> {
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
        "MokaFileStatisticsCache".to_string()
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
    fn test_statistics_cache() {
        let meta = ObjectMeta {
            location: Path::from("test"),
            last_modified: DateTime::parse_from_rfc3339("2022-09-27T22:36:00+02:00")
                .unwrap()
                .into(),
            size: 1024,
            e_tag: None,
            version: None,
        };
        let cache = MokaFileStatisticsCache::new(None);
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
