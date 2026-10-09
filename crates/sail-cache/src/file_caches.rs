use std::sync::{Arc, OnceLock};

use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::prelude::SessionConfig;
use object_store::ObjectStore;

use crate::file_listing_cache::MokaFileListingCache;
use crate::file_metadata_cache::MokaFileMetadataCache;
use crate::file_statistics_cache::MokaFileStatisticsCache;

/// Configured cache storage shared by all object-store views in a session.
pub struct FileCaches {
    pub metadata: Arc<MokaFileMetadataCache>,
    pub statistics: Option<Arc<MokaFileStatisticsCache>>,
    pub listing: Option<Arc<MokaFileListingCache>>,
}

impl FileCaches {
    pub fn from_config(config: &SessionConfig) -> Arc<Self> {
        static DISABLED: OnceLock<Arc<FileCaches>> = OnceLock::new();
        config.get_extension::<Self>().unwrap_or_else(|| {
            Arc::clone(DISABLED.get_or_init(|| {
                Arc::new(Self {
                    metadata: Arc::new(MokaFileMetadataCache::new(None, Some(0))),
                    statistics: None,
                    listing: None,
                })
            }))
        })
    }

    pub fn metadata_cache(
        config: &SessionConfig,
        store: &Arc<dyn ObjectStore>,
    ) -> Arc<MokaFileMetadataCache> {
        Arc::new(Self::from_config(config).metadata.for_store(store))
    }

    pub fn statistics_cache(
        config: &SessionConfig,
        store: &Arc<dyn ObjectStore>,
    ) -> Option<MokaFileStatisticsCache> {
        Self::from_config(config)
            .statistics
            .as_ref()
            .map(|cache| cache.for_store(store))
    }

    pub fn listing_cache(
        config: &SessionConfig,
        store: &Arc<dyn ObjectStore>,
    ) -> Option<MokaFileListingCache> {
        Self::from_config(config)
            .listing
            .as_ref()
            .map(|cache| cache.for_store(store))
    }

    pub fn cache_manager_config(&self) -> CacheManagerConfig {
        // Path-only DataFusion callers retain caching in the unidentified-store namespace.
        CacheManagerConfig::default()
            .with_file_metadata_cache(Some(Arc::clone(&self.metadata) as _))
            .with_file_statistics_cache(
                self.statistics.as_ref().map(|cache| Arc::clone(cache) as _),
            )
            .with_file_statistics_cache_limit(if self.statistics.is_some() {
                usize::MAX
            } else {
                0
            })
            .with_list_files_cache(self.listing.as_ref().map(|cache| Arc::clone(cache) as _))
            .with_list_files_cache_limit(if self.listing.is_some() {
                usize::MAX
            } else {
                0
            })
    }
}
