use std::sync::{Arc, LazyLock};
use std::time::Duration;

use datafusion::execution::cache::CacheAccessor;
use log::debug;
use moka::sync::Cache;
use object_store::path::Path;
use object_store::ObjectMeta;

use crate::try_parse_non_zero_u64;

pub static GLOBAL_LIST_FILES_CACHE: LazyLock<Arc<MokaListFilesCache>> = LazyLock::new(|| {
    let ttl = std::env::var("SAIL_RUNTIME__LIST_FILES_CACHE_TTL").ok();
    Arc::new(MokaListFilesCache::new(ttl))
});

pub struct MokaListFilesCache {
    statistics: Cache<Path, Arc<Vec<ObjectMeta>>>,
}

impl MokaListFilesCache {
    pub fn new(ttl: Option<String>) -> Self {
        let mut builder = Cache::builder();

        if let Some(ttl) = ttl {
            if let Some(ttl) = try_parse_non_zero_u64(&ttl) {
                debug!("Setting TTL for MokaListFilesCache to {ttl}");
                builder = builder.time_to_live(Duration::from_secs(ttl));
            } else {
                debug!("Disabled or invalid TTL for MokaListFilesCache: {ttl}");
            }
        } else {
            debug!("No TTL set for MokaListFilesCache");
        }

        Self {
            statistics: builder.build(),
        }
    }
}

impl CacheAccessor<Path, Arc<Vec<ObjectMeta>>> for MokaListFilesCache {
    type Extra = ObjectMeta;

    fn get(&self, k: &Path) -> Option<Arc<Vec<ObjectMeta>>> {
        self.statistics.get(k)
    }

    fn get_with_extra(&self, k: &Path, _e: &Self::Extra) -> Option<Arc<Vec<ObjectMeta>>> {
        self.get(k)
    }

    fn put(&self, key: &Path, value: Arc<Vec<ObjectMeta>>) -> Option<Arc<Vec<ObjectMeta>>> {
        self.statistics.insert(key.clone(), value);
        None
    }

    fn put_with_extra(
        &self,
        key: &Path,
        value: Arc<Vec<ObjectMeta>>,
        _e: &Self::Extra,
    ) -> Option<Arc<Vec<ObjectMeta>>> {
        self.put(key, value)
    }

    fn remove(&mut self, k: &Path) -> Option<Arc<Vec<ObjectMeta>>> {
        self.statistics.remove(k)
    }

    fn contains_key(&self, k: &Path) -> bool {
        self.statistics.contains_key(k)
    }

    fn len(&self) -> usize {
        self.statistics.entry_count() as usize
    }

    fn clear(&self) {
        self.statistics.invalidate_all()
    }

    fn name(&self) -> String {
        "MokaListFilesCache".to_string()
    }
}
