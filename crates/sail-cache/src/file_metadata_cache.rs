use std::time::Duration;

use datafusion::execution::cache::cache_manager::CachedFileMetadataEntry;
use moka::sync::Cache;
use object_store::path::Path;

use crate::object_store_cache::{ObjectStoreCache, ScopedKey};

pub type MokaFileMetadataCache = ObjectStoreCache<Path, CachedFileMetadataEntry>;

impl MokaFileMetadataCache {
    pub fn new(ttl: Option<u64>, limit: Option<u64>) -> Self {
        let mut builder = Cache::builder().eviction_policy(moka::policy::EvictionPolicy::lru());
        let ttl = ttl.map(Duration::from_secs);
        if let Some(ttl) = ttl {
            builder = builder.time_to_live(ttl);
        }
        if let Some(limit) = limit {
            builder = builder.weigher(|_key: &ScopedKey<Path>, entry: &CachedFileMetadataEntry| {
                u32::try_from(entry.file_metadata.memory_size()).unwrap_or(u32::MAX)
            });
            builder = builder.max_capacity(limit);
        }
        Self::new_inner("MokaFileMetadataCache", limit, ttl, builder.build(), true)
    }
}

#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Arc;

    use chrono::DateTime;
    use datafusion::common::HashMap;
    use datafusion::execution::cache::Cache as _;
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

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let root = MokaFileMetadataCache::new(None, None);
        root.put(&object_meta.location, entry.clone());
        assert!(root.get(&object_meta.location).is_some());
        let cache = root.for_store(&store).for_file(&object_meta);
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
    #[test]
    fn metadata_requires_matching_store_and_file_version() {
        let root = MokaFileMetadataCache::new(None, Some(100));
        let a: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let b: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let meta = ObjectMeta {
            location: Path::from("same.parquet"),
            last_modified: DateTime::UNIX_EPOCH,
            size: 1000,
            e_tag: Some("etag-a".into()),
            version: Some("version-a".into()),
        };
        let metadata: Arc<dyn FileMetadata> = Arc::new(TestFileMetadata {
            metadata: "footer".into(),
        });
        root.for_store(&a).for_file(&meta).put(
            &meta.location,
            CachedFileMetadataEntry::new(meta.clone(), Arc::clone(&metadata)),
        );
        assert!(
            root.for_store(&b)
                .for_file(&meta)
                .get(&meta.location)
                .is_none()
        );
        assert!(root.for_store(&a).get(&meta.location).is_none());
        let cached = root
            .for_store(&a)
            .for_files([(meta.location.clone(), &meta)])
            .get(&meta.location)
            .unwrap();
        assert!(Arc::ptr_eq(&cached.file_metadata, &metadata));
        for field in 0..4 {
            let mut changed = meta.clone();
            match field {
                0 => changed.size += 1,
                1 => changed.last_modified += chrono::Duration::seconds(1),
                2 => changed.e_tag = Some("etag-b".into()),
                _ => changed.version = Some("version-b".into()),
            }
            assert!(
                root.for_store(&a)
                    .for_file(&changed)
                    .get(&meta.location)
                    .is_none()
            );
        }
    }
}
