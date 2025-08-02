use std::sync::Arc;

use crate::error::{CacheError, CacheResult};
use datafusion::execution::cache::cache_manager::{FileMetadata, FileMetadataCache};
use datafusion::execution::cache::CacheAccessor;
use datafusion::parquet::file::metadata::ParquetMetaData;
use moka::sync::Cache;
use object_store::{path::Path, ObjectMeta};

pub struct MokaFilesMetadataCache {
    metadata: Cache<Path, (ObjectMeta, Arc<dyn FileMetadata>)>,
}

impl MokaFilesMetadataCache {
    pub fn new(memory_limit: Option<String>) -> CacheResult<Self> {
        let mut builder = Cache::builder();

        if let Some(limit) = memory_limit {
            let max_capacity = parse_memory_limit(&limit)?;
            builder = builder
                .weigher(
                    |_key: &Path, value: &(ObjectMeta, Arc<dyn FileMetadata>)| -> u32 {
                        if let Some(parquet_meta) =
                            value.1.as_any().downcast_ref::<ParquetMetaData>()
                        {
                            parquet_meta.memory_size().min(u32::MAX as usize) as u32
                        } else {
                            size_of::<ObjectMeta>() as u32 + 1024
                        }
                    },
                )
                .max_capacity(max_capacity);
        }

        Ok(Self {
            metadata: builder.build(),
        })
    }
}
//
// impl Default for MokaFilesMetadataCache {
//     fn default() -> Self {
//         Self {
//             metadata: Cache::new(10_000),
//         }
//     }
// }

impl FileMetadataCache for MokaFilesMetadataCache {}

impl CacheAccessor<ObjectMeta, Arc<dyn FileMetadata>> for MokaFilesMetadataCache {
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

    fn remove(&mut self, k: &ObjectMeta) -> Option<Arc<dyn FileMetadata>> {
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
        "MokaFilesMetadataCache".to_string()
    }
}

fn parse_memory_limit(limit: &str) -> CacheResult<u64> {
    let (number, unit) = limit.split_at(limit.len() - 1);
    let number: f64 = number.parse().map_err(|_| {
        CacheError::invalid(format!(
            "Failed to parse number from memory limit '{limit}'"
        ))
    })?;

    match unit {
        "K" => Ok((number * 1024.0) as u64),
        "M" => Ok((number * 1024.0 * 1024.0) as u64),
        "G" => Ok((number * 1024.0 * 1024.0 * 1024.0) as u64),
        _ => Err(CacheError::unsupported(format!(
            "Unsupported unit '{unit}' in memory limit '{limit}'"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use datafusion::execution::cache::cache_manager::FileMetadata;
    use object_store::path::Path;
    use object_store::ObjectMeta;
    use std::any::Any;
    use std::sync::Arc;

    pub struct TestFileMetadata {
        metadata: String,
    }

    impl FileMetadata for TestFileMetadata {
        fn as_any(&self) -> &dyn Any {
            self
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

        let mut cache = MokaFilesMetadataCache::new(None).unwrap();
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
