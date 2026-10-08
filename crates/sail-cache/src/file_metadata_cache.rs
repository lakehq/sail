use std::time::Duration;

use datafusion::common::{HashMap, Result, TableReference};
use datafusion::execution::cache::cache_manager::CachedFileMetadataEntry;
use datafusion::execution::cache::{Cache as DataFusionCache, CacheEntryInfo, CacheValue};
use log::debug;
use moka::policy::EvictionPolicy;
use moka::sync::Cache;
use object_store::path::Path;

pub struct MokaFileMetadataCache {
    size_limit: Option<u64>,
    ttl: Option<Duration>,
    metadata: Cache<Path, CachedFileMetadataEntry>,
}

impl MokaFileMetadataCache {
    const NAME: &'static str = "MokaFileMetadataCache";

    pub fn new(ttl: Option<u64>, size_limit: Option<u64>) -> Self {
        let mut builder = Cache::builder().eviction_policy(EvictionPolicy::lru());

        let ttl = ttl.map(Duration::from_secs);
        if let Some(ttl) = ttl {
            debug!("Setting TTL for {} to {ttl:?}", Self::NAME);
            builder = builder.time_to_live(ttl);
        }
        if let Some(size_limit) = size_limit {
            debug!(
                "Setting size limit for {} to {size_limit} byte(s)",
                Self::NAME
            );
            builder = builder
                .weigher(|_key: &Path, entry: &CachedFileMetadataEntry| -> u32 {
                    entry.file_metadata.memory_size() as u32
                })
                .max_capacity(size_limit);
        } else {
            debug!("No size limit set for {}", Self::NAME);
        }

        Self {
            size_limit,
            ttl,
            metadata: builder.build(),
        }
    }
}

impl DataFusionCache<Path, CachedFileMetadataEntry> for MokaFileMetadataCache {
    fn get(&self, key: &Path) -> Option<CachedFileMetadataEntry> {
        self.metadata.get(key)
    }

    fn put(&self, key: &Path, value: CachedFileMetadataEntry) -> Option<CachedFileMetadataEntry> {
        let previous = self.metadata.get(key);
        self.metadata.insert(key.clone(), value);
        previous
    }

    fn remove(&self, key: &Path) -> Option<CachedFileMetadataEntry> {
        self.metadata.remove(key)
    }

    fn contains_key(&self, key: &Path) -> bool {
        self.metadata.contains_key(key)
    }

    fn len(&self) -> usize {
        self.metadata.entry_count() as usize
    }

    fn clear(&self) {
        self.metadata.invalidate_all();
    }

    fn name(&self) -> String {
        Self::NAME.to_string()
    }

    fn cache_limit(&self) -> usize {
        self.size_limit
            .map(|size| size as usize)
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

    fn drop_table_entries(&self, _table_ref: &TableReference) -> Result<()> {
        Ok(())
    }

    fn list_entries(&self) -> HashMap<Path, CacheEntryInfo<CachedFileMetadataEntry>> {
        self.metadata
            .iter()
            .map(|(path, entry)| {
                (
                    path.as_ref().clone(),
                    CacheEntryInfo {
                        size_bytes: entry.size(),
                        value: entry,
                        hits: 0,
                        expires: None,
                    },
                )
            })
            .collect()
    }
}

#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Arc;

    use chrono::DateTime;
    use datafusion::execution::cache::cache_manager::FileMetadata;
    use object_store::ObjectMeta;
    use object_store::path::Path;

    use super::*;

    pub struct TestFileMetadata {
        metadata: String,
    }

    impl FileMetadata for TestFileMetadata {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn memory_size(&self) -> usize {
            self.metadata.len()
        }

        fn extra_info(&self) -> HashMap<String, String> {
            HashMap::new()
        }
    }

    #[test]
    fn test_file_metadata_cache() {
        let object_meta = ObjectMeta {
            location: Path::from("test"),
            last_modified: DateTime::parse_from_rfc3339("2025-07-29T12:12:12+00:00")
                .unwrap()
                .into(),
            size: 1024,
            e_tag: None,
            version: None,
        };

        let file_metadata: Arc<dyn FileMetadata> = Arc::new(TestFileMetadata {
            metadata: "retrieved_metadata".to_owned(),
        });
        let entry = CachedFileMetadataEntry::new(object_meta.clone(), Arc::clone(&file_metadata));

        let cache = MokaFileMetadataCache::new(None, None);
        assert!(cache.get(&object_meta.location).is_none());

        // put
        cache.put(&object_meta.location, entry.clone());

        // get and contains of a valid entry
        assert!(cache.contains_key(&object_meta.location));
        let value = cache.get(&object_meta.location);
        assert!(value.is_some());
        let cached_entry = value.unwrap();
        assert!(cached_entry.is_valid_for(&object_meta));
        let test_meta = Arc::downcast::<TestFileMetadata>(cached_entry.file_metadata);
        assert!(test_meta.is_ok());
        assert_eq!(test_meta.unwrap().metadata, "retrieved_metadata");

        // different file
        let mut object_meta2 = object_meta.clone();
        object_meta2.location = Path::from("test2");
        assert!(cache.get(&object_meta2.location).is_none());
        assert!(!cache.contains_key(&object_meta2.location));

        // remove
        cache.remove(&object_meta.location);
        assert!(cache.get(&object_meta.location).is_none());
        assert!(!cache.contains_key(&object_meta.location));
    }
}
