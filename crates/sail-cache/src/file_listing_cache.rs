use std::time::Duration;

use datafusion::execution::cache::TableScopedPath;
use datafusion::execution::cache::cache_manager::CachedFileList;
use moka::sync::Cache;

use crate::object_store_cache::ObjectStoreCache;

pub type MokaFileListingCache = ObjectStoreCache<TableScopedPath, CachedFileList>;

impl MokaFileListingCache {
    pub fn new(ttl: Option<u64>, limit: Option<u64>) -> Self {
        let mut builder = Cache::builder();
        let ttl = ttl.map(Duration::from_secs);
        if let Some(ttl) = ttl {
            builder = builder.time_to_live(ttl);
        }
        if let Some(limit) = limit {
            builder = builder.max_capacity(limit);
        }
        Self::new_inner("MokaFileListingCache", limit, ttl, builder.build(), false)
    }
}

#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::DateTime;
    use datafusion::execution::cache::Cache as _;
    use object_store::ObjectMeta;

    use super::*;

    #[test]
    fn test_file_listing_cache() {
        let meta = ObjectMeta {
            location: object_store::path::Path::from("test"),
            last_modified: DateTime::parse_from_rfc3339("2022-09-27T22:36:00+02:00")
                .unwrap()
                .into(),
            size: 1024,
            e_tag: None,
            version: None,
        };

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let cache = MokaFileListingCache::new(None, None).for_store(&store);
        let key = TableScopedPath {
            table: None,
            path: meta.location.clone(),
        };
        assert!(cache.get(&key).is_none());

        cache.put(&key, CachedFileList::new(vec![meta.clone()]));
        assert_eq!(
            cache.get(&key).unwrap().files.first().unwrap().clone(),
            meta.clone()
        );
    }
}
