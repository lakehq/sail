use std::sync::Arc;

use datafusion::execution::cache::cache_manager::{
    CacheManagerConfig, FileMetadataCache, FileStatisticsCache, ListFilesCache,
};
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{
    FairSpillPool, GreedyMemoryPool, MemoryPool, UnboundedMemoryPool,
};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::DiskManager;
use datafusion_common::Result;
use log::debug;
use sail_cache::file_listing_cache::MokaFileListingCache;
use sail_cache::file_metadata_cache::MokaFileMetadataCache;
use sail_cache::file_statistics_cache::MokaFileStatisticsCache;
use sail_common::config::{
    AppConfig, CacheType, FairMemoryPoolConfig, GreedyMemoryPoolConfig, MemoryPoolConfig,
};
use sail_common::runtime::RuntimeHandle;
use sail_object_store::DynamicObjectStoreRegistry;

pub struct RuntimeEnvFactory {
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
    global_file_listing_cache: Option<Arc<dyn ListFilesCache>>,
    global_file_statistics_cache: Option<Arc<dyn FileStatisticsCache>>,
    global_file_metadata_cache: Option<Arc<MokaFileMetadataCache>>,
}

impl RuntimeEnvFactory {
    pub fn new(config: Arc<AppConfig>, runtime: RuntimeHandle) -> Self {
        Self {
            config,
            runtime,
            global_file_listing_cache: None,
            global_file_statistics_cache: None,
            global_file_metadata_cache: None,
        }
    }

    pub fn create<M>(&mut self, mutator: M) -> Result<Arc<RuntimeEnv>>
    where
        M: FnOnce(RuntimeEnvBuilder) -> Result<RuntimeEnvBuilder>,
    {
        let registry = DynamicObjectStoreRegistry::new(self.runtime.clone());
        let cache_config = CacheManagerConfig::default()
            .with_files_statistics_cache(Some(self.create_file_statistics_cache()))
            .with_list_files_cache(Some(self.create_file_listing_cache()))
            .with_file_metadata_cache(Some(self.create_file_metadata_cache()));
        let builder = RuntimeEnvBuilder::default()
            .with_object_store_registry(Arc::new(registry))
            .with_cache_manager(cache_config)
            .with_memory_pool(self.create_memory_pool())
            .with_disk_manager_builder(self.create_disk_manager_builder());
        let builder = mutator(builder)?;
        Ok(Arc::new(builder.build()?))
    }

    fn create_memory_pool(&self) -> Arc<dyn MemoryPool> {
        match self.config.runtime.memory_pool {
            MemoryPoolConfig::Unbounded => Arc::new(UnboundedMemoryPool::default()),
            MemoryPoolConfig::Greedy(GreedyMemoryPoolConfig { max_size }) => {
                Arc::new(GreedyMemoryPool::new(max_size))
            }
            MemoryPoolConfig::Fair(FairMemoryPoolConfig { max_size }) => {
                Arc::new(FairSpillPool::new(max_size))
            }
        }
    }

    fn create_disk_manager_builder(&self) -> DiskManagerBuilder {
        let max_size = self.config.runtime.temporary_files.max_size;
        let paths = self.config.runtime.temporary_files.paths.as_slice();

        let mut builder = DiskManager::builder();
        builder.set_max_temp_directory_size(max_size as u64);
        if max_size == 0 {
            builder.set_mode(DiskManagerMode::Disabled);
        } else if paths.is_empty() {
            builder.set_mode(DiskManagerMode::OsTmpDirectory);
        } else {
            let paths = paths.iter().map(|x| x.into()).collect();
            builder.set_mode(DiskManagerMode::Directories(paths));
        }
        builder
    }

    fn create_file_statistics_cache(&mut self) -> Arc<dyn FileStatisticsCache> {
        let ttl = self.config.parquet.file_statistics_cache.ttl;
        let max_entries = self.config.parquet.file_statistics_cache.max_entries;
        match &self.config.parquet.file_statistics_cache.r#type {
            CacheType::None => {
                debug!("Not using file statistics cache");
                Arc::new(MokaFileStatisticsCache::new(ttl, Some(0)))
            }
            CacheType::Global => {
                debug!("Using global file statistics cache");
                self.global_file_statistics_cache
                    .get_or_insert_with(|| {
                        Arc::new(MokaFileStatisticsCache::new(ttl, max_entries))
                            as Arc<dyn FileStatisticsCache>
                    })
                    .clone()
            }
            CacheType::Session => {
                debug!("Using session file statistics cache");
                Arc::new(MokaFileStatisticsCache::new(ttl, max_entries))
            }
        }
    }

    fn create_file_listing_cache(&mut self) -> Arc<dyn ListFilesCache> {
        let ttl = self.config.execution.file_listing_cache.ttl;
        let max_entries = self.config.execution.file_listing_cache.max_entries;
        match &self.config.execution.file_listing_cache.r#type {
            CacheType::None => {
                debug!("Not using file listing cache");
                Arc::new(MokaFileListingCache::new(ttl, Some(0)))
            }
            CacheType::Global => {
                debug!("Using global file listing cache");
                self.global_file_listing_cache
                    .get_or_insert_with(|| {
                        Arc::new(MokaFileListingCache::new(ttl, max_entries))
                            as Arc<dyn ListFilesCache>
                    })
                    .clone()
            }
            CacheType::Session => {
                debug!("Using session file listing cache");
                Arc::new(MokaFileListingCache::new(ttl, max_entries))
            }
        }
    }

    fn create_file_metadata_cache(&mut self) -> Arc<dyn FileMetadataCache> {
        let ttl = self.config.parquet.file_metadata_cache.ttl;
        let size_limit = self.config.parquet.file_metadata_cache.size_limit;
        match self.config.parquet.file_metadata_cache.r#type {
            CacheType::None => {
                debug!("Not using file metadata cache");
                Arc::new(MokaFileMetadataCache::new(ttl, Some(0)))
            }
            CacheType::Global => {
                debug!("Using global file metadata cache");
                self.global_file_metadata_cache
                    .get_or_insert_with(|| Arc::new(MokaFileMetadataCache::new(ttl, size_limit)))
                    .clone()
            }
            CacheType::Session => {
                debug!("Using session file metadata cache");
                Arc::new(MokaFileMetadataCache::new(ttl, size_limit))
            }
        }
    }
}
