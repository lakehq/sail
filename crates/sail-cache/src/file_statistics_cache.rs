use std::time::Duration;

use datafusion::execution::cache::TableScopedPath;
use datafusion::execution::cache::cache_manager::CachedFileMetadata;
use moka::sync::Cache;

use crate::object_store_cache::ObjectStoreCache;

pub type MokaFileStatisticsCache = ObjectStoreCache<TableScopedPath, CachedFileMetadata>;

impl MokaFileStatisticsCache {
    pub fn new(ttl: Option<u64>, limit: Option<u64>) -> Self {
        let mut builder = Cache::builder();
        let ttl = ttl.map(Duration::from_secs);
        if let Some(ttl) = ttl {
            builder = builder.time_to_live(ttl);
        }
        if let Some(limit) = limit {
            builder = builder.max_capacity(limit);
        }
        Self::new_inner("MokaFileStatisticsCache", limit, ttl, builder.build(), true)
    }
}

#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::DateTime;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::common::Statistics;
    use datafusion::execution::cache::{Cache as _, SchemaFingerprint};
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
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let root = MokaFileStatisticsCache::new(None, None);
        let cache = root.for_store(&store).for_file(&meta);
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
        root.put(&key, cached.clone());
        assert!(root.get(&key).is_some());
        assert!(cache.get(&key).is_none());
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
