mod core;
mod handler;

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use sail_cache::file_listing_cache::MokaFileListingCache;
use sail_cache::file_metadata_cache::MokaFileMetadataCache;
use sail_cache::file_statistics_cache::MokaFileStatisticsCache;

pub struct SessionManagerActor<K> {
    options: super::options::SessionManagerOptions,
    sessions: HashMap<K, SessionContext>,
    global_file_listing_cache: Option<Arc<MokaFileListingCache>>,
    global_file_statistics_cache: Option<Arc<MokaFileStatisticsCache>>,
    global_file_metadata_cache: Option<Arc<MokaFileMetadataCache>>,
}
