use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use datafusion::execution::cache::cache_manager::{
    FileMetadata, FileMetadataCache, FileMetadataCacheEntry,
};
use datafusion::execution::cache::CacheAccessor;
use log::debug;
use moka::policy::EvictionPolicy;
use moka::sync::Cache;
use object_store::path::Path;
use object_store::ObjectMeta;

pub struct MokaFileMetadataCache {
    size_limit: Option<u64>,
    metadata: Cache<Path, (ObjectMeta, Arc<dyn FileMetadata>)>,
}

impl MokaFileMetadataCache {
    const NAME: &'static str = "MokaFileMetadataCache";

    pub fn new(ttl: Option<u64>, size_limit: Option<u64>) -> Self {
        let mut builder = Cache::builder().eviction_policy(EvictionPolicy::lru());

        if let Some(ttl) = ttl {
            let ttl = Duration::from_secs(ttl);
            debug!("Setting TTL for {} to {ttl:?}", Self::NAME);
            builder = builder.time_to_live(ttl);
        }
        if let Some(size_limit) = size_limit {
            debug!(
                "Setting size limit for {} to {size_limit} byte(s)",
                Self::NAME
            );
            builder = builder
                .weigher(
                    |_key: &Path, (_, meta): &(ObjectMeta, Arc<dyn FileMetadata>)| -> u32 {
                        meta.memory_size() as u32
                    },
                )
                .max_capacity(size_limit);
        } else {
            debug!("No size limit set for {}", Self::NAME);
        }

        Self {
            size_limit,
            metadata: builder.build(),
        }
    }
}

impl FileMetadataCache for MokaFileMetadataCache {
    fn cache_limit(&self) -> usize {
        self.size_limit
            .map(|size| size as usize)
            .unwrap_or(usize::MAX)
    }

    fn update_cache_limit(&self, _limit: usize) {
        // TODO: support dynamic update of cache limit
    }

    fn list_entries(&self) -> HashMap<Path, FileMetadataCacheEntry> {
        self.metadata
            .iter()
            .map(|(path, (object_meta, meta))| {
                (
                    path.as_ref().clone(),
                    FileMetadataCacheEntry {
                        object_meta,
                        size_bytes: meta.memory_size(),
                        // TODO: get hits from the cache
                        hits: 0,
                        extra: meta.extra_info(),
                    },
                )
            })
            .collect()
    }
}

impl CacheAccessor<ObjectMeta, Arc<dyn FileMetadata>> for MokaFileMetadataCache {
    type Extra = ObjectMeta;

    fn get(&self, k: &ObjectMeta) -> Option<Arc<dyn FileMetadata>> {
        self.metadata
            .get(&k.location)
            .and_then(|(extra, metadata)| {
                if extra.size == k.size && extra.last_modified == k.last_modified {
                    Some(Arc::clone(&metadata))
                } else {
                    None
                }
            })
    }

    fn get_with_extra(&self, k: &ObjectMeta, _e: &Self::Extra) -> Option<Arc<dyn FileMetadata>> {
        self.get(k)
    }

    fn put(&self, key: &ObjectMeta, value: Arc<dyn FileMetadata>) -> Option<Arc<dyn FileMetadata>> {
        self.metadata
            .insert(key.location.clone(), (key.clone(), value));
        None
    }

    fn put_with_extra(
        &self,
        key: &ObjectMeta,
        value: Arc<dyn FileMetadata>,
        _e: &Self::Extra,
    ) -> Option<Arc<dyn FileMetadata>> {
        self.put(key, value)
    }

    fn remove(&self, k: &ObjectMeta) -> Option<Arc<dyn FileMetadata>> {
        self.metadata
            .remove(&k.location)
            .map(|(_, metadata)| metadata)
    }

    fn contains_key(&self, k: &ObjectMeta) -> bool {
        self.metadata
            .get(&k.location)
            .map(|(extra, _)| extra.size == k.size && extra.last_modified == k.last_modified)
            .unwrap_or(false)
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
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Arc;

    use chrono::DateTime;
    use datafusion::execution::cache::cache_manager::FileMetadata;
    use object_store::path::Path;
    use object_store::ObjectMeta;

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

        let metadata: Arc<dyn FileMetadata> = Arc::new(TestFileMetadata {
            metadata: "retrieved_metadata".to_owned(),
        });

        let cache = MokaFileMetadataCache::new(None, None);
        assert!(cache.get(&object_meta).is_none());

        // put
        cache.put(&object_meta, metadata);

        // get and contains of a valid entry
        assert!(cache.contains_key(&object_meta));
        let value = cache.get(&object_meta);
        assert!(value.is_some());
        let test_file_metadata = Arc::downcast::<TestFileMetadata>(value.unwrap());
        assert!(test_file_metadata.is_ok());
        assert_eq!(test_file_metadata.unwrap().metadata, "retrieved_metadata");

        // file size changed
        let mut object_meta2 = object_meta.clone();
        object_meta2.size = 2048;
        assert!(cache.get(&object_meta2).is_none());
        assert!(!cache.contains_key(&object_meta2));

        // file last_modified changed
        let mut object_meta2 = object_meta.clone();
        object_meta2.last_modified = DateTime::parse_from_rfc3339("2025-07-29T13:13:13+00:00")
            .unwrap()
            .into();
        assert!(cache.get(&object_meta2).is_none());
        assert!(!cache.contains_key(&object_meta2));

        // different file
        let mut object_meta2 = object_meta.clone();
        object_meta2.location = Path::from("test2");
        assert!(cache.get(&object_meta2).is_none());
        assert!(!cache.contains_key(&object_meta2));

        // remove
        cache.remove(&object_meta);
        assert!(cache.get(&object_meta).is_none());
        assert!(!cache.contains_key(&object_meta));
    }
}
