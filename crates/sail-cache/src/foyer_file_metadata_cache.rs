use std::str::FromStr;
use std::sync::{Arc, LazyLock};

use datafusion::datasource::physical_plan::parquet::reader::CachedParquetMetaData;
use datafusion::execution::cache::cache_manager::{FileMetadata, FileMetadataCache};
use datafusion::execution::cache::CacheAccessor;
use foyer::{Cache, CacheBuilder, EvictionConfig, S3FifoConfig};
use log::{debug, info};
use object_store::path::Path;
use object_store::ObjectMeta;

use crate::try_parse_memory_limit;

pub static GLOBAL_FOYER_FILE_METADATA_CACHE: LazyLock<Arc<FoyerFilesMetadataCache>> =
    LazyLock::new(|| {
        let memory_limit = std::env::var("SAIL_PARQUET__FILE_METADATA_CACHE_LIMIT").ok();
        if std::env::var("SAIL_FOYER_USE_CUSTOM").is_ok_and(|v| bool::from_str(&v).unwrap_or(false))
        {
            Arc::new(FoyerFilesMetadataCache::new_custom(memory_limit))
        } else {
            Arc::new(FoyerFilesMetadataCache::new(memory_limit))
        }
    });

pub struct FoyerFilesMetadataCache {
    metadata: Cache<Path, (ObjectMeta, Arc<dyn FileMetadata>)>,
}

impl FoyerFilesMetadataCache {
    pub fn new(memory_limit: Option<String>) -> Self {
        let (capacity, use_weigher) =
            if let Some(limit) = memory_limit.and_then(|l| try_parse_memory_limit(&l)) {
                info!("Setting memory capacity for metadata cache: {limit} bytes");
                (limit, true)
            } else {
                info!("No memory limit set for metadata cache, using default capacity: 1000");
                (1000, false)
            };

        info!("Initializing custom Foyer metadata cache with {capacity} capacity");

        let mut builder = CacheBuilder::new(capacity).with_name("parquet_file_metadata_cache");

        if use_weigher {
            debug!("Using weigher for metadata cache entries");
            builder = builder.with_weighter(
                |_key: &Path, value: &(ObjectMeta, Arc<dyn FileMetadata>)| -> usize {
                    if let Some(parquet_metadata) =
                        value.1.as_any().downcast_ref::<CachedParquetMetaData>()
                    {
                        info!(
                            "Using ParquetMetaData for size calculation in FoyerFilesMetadataCache"
                        );
                        parquet_metadata
                            .parquet_metadata()
                            .memory_size()
                            .min(u32::MAX as usize)
                    } else {
                        info!("Using ObjectMeta for size calculation in FoyerFilesMetadataCache");
                        size_of::<ObjectMeta>() + 1024
                    }
                },
            );
        }

        let cache = builder.build();

        Self { metadata: cache }
    }

    pub fn new_custom(memory_limit: Option<String>) -> Self {
        let cpu_count = num_cpus::get();

        let (capacity, use_weigher) =
            if let Some(limit) = memory_limit.and_then(|l| try_parse_memory_limit(&l)) {
                info!("Setting memory capacity for metadata cache: {limit} bytes");
                (limit, true)
            } else {
                info!("No memory limit set for metadata cache, using default capacity: 1000");
                (1000, false)
            };

        info!("Initializing custom Foyer metadata cache with {capacity} capacity and {cpu_count} shards on {cpu_count} CPUs");

        let mut builder = CacheBuilder::new(capacity)
            .with_name("parquet_file_metadata_cache")
            .with_shards(cpu_count)
            .with_eviction_config(EvictionConfig::S3Fifo(S3FifoConfig {
                small_queue_capacity_ratio: 0.1,
                ghost_queue_capacity_ratio: 1.0,
                small_to_main_freq_threshold: 1,
            }));

        if use_weigher {
            debug!("Using weigher for metadata cache entries");
            builder = builder.with_weighter(
                |_key: &Path, value: &(ObjectMeta, Arc<dyn FileMetadata>)| -> usize {
                    if let Some(parquet_metadata) =
                        value.1.as_any().downcast_ref::<CachedParquetMetaData>()
                    {
                        info!(
                            "Using ParquetMetaData for size calculation in FoyerFilesMetadataCache"
                        );
                        parquet_metadata
                            .parquet_metadata()
                            .memory_size()
                            .min(u32::MAX as usize)
                    } else {
                        info!("Using ObjectMeta for size calculation in FoyerFilesMetadataCache");
                        size_of::<ObjectMeta>() + 1024
                    }
                },
            );
        }

        let cache = builder.build();

        Self { metadata: cache }
    }
}

impl FileMetadataCache for FoyerFilesMetadataCache {}

impl CacheAccessor<ObjectMeta, Arc<dyn FileMetadata>> for FoyerFilesMetadataCache {
    type Extra = ObjectMeta;

    fn get(&self, k: &ObjectMeta) -> Option<Arc<dyn FileMetadata>> {
        let meta = self
            .metadata
            .get(&k.location)
            .map(|s| {
                let (extra, metadata) = s.value();
                if extra.size != k.size || extra.last_modified != k.last_modified {
                    None
                } else {
                    Some(Arc::clone(metadata))
                }
            })
            .unwrap_or(None);
        info!(
            "FoyerFilesMetadataCache GET for key: {k:?}. FOUND: {}, Current usage: {}, capacity: {}",
             meta.is_some(),
            self.metadata.usage(),
            self.metadata.capacity()
        );
        meta
    }

    fn get_with_extra(&self, k: &ObjectMeta, _e: &Self::Extra) -> Option<Arc<dyn FileMetadata>> {
        self.get(k)
    }

    fn put(&self, key: &ObjectMeta, value: Arc<dyn FileMetadata>) -> Option<Arc<dyn FileMetadata>> {
        info!(
            "FoyerFilesMetadataCache PUT for key: {key:?}. Current usage: {}, capacity: {}",
            self.metadata.usage(),
            self.metadata.capacity()
        );
        let entry = self
            .metadata
            .insert(key.location.clone(), (key.clone(), value));
        let (_, metadata) = entry.value();
        Some(Arc::clone(metadata))
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
        info!(
            "FoyerFilesMetadataCache REMOVE for key: {k:?}. Current usage: {}, capacity: {}",
            self.metadata.usage(),
            self.metadata.capacity()
        );
        self.metadata.remove(&k.location).map(|x| {
            let (_, metadata) = x.value();
            Arc::clone(metadata)
        })
    }

    fn contains_key(&self, k: &ObjectMeta) -> bool {
        let contains = self
            .metadata
            .get(&k.location)
            .map(|s| {
                let (extra, _) = s.value();
                extra.size == k.size && extra.last_modified == k.last_modified
            })
            .unwrap_or(false);
        info!(
            "FoyerFilesMetadataCache CONTAINS_KEY for key: {k:?}. CONTAINS KEY: {contains}, Current usage: {}, capacity: {}",
            self.metadata.usage(),
            self.metadata.capacity()
        );
        contains
    }

    fn len(&self) -> usize {
        self.metadata.usage()
    }

    fn clear(&self) {
        self.metadata.clear();
    }

    fn name(&self) -> String {
        "FoyerFilesMetadataCache".to_string()
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

        let mut cache = FoyerFilesMetadataCache::new(None);
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
