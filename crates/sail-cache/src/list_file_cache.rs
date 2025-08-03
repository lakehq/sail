use std::sync::{Arc, LazyLock};
use std::time::Duration;

use datafusion::execution::cache::CacheAccessor;
use log::debug;
use moka::policy::EvictionPolicy;
use moka::sync::Cache;
use object_store::path::Path;
use object_store::ObjectMeta;

use crate::try_parse_non_zero_u64;

pub static GLOBAL_LIST_FILES_CACHE: LazyLock<Arc<MokaListFilesCache>> = LazyLock::new(|| {
    let ttl = std::env::var("SAIL_RUNTIME__LIST_FILES_CACHE_TTL").ok();
    let max_entries = std::env::var("SAIL_RUNTIME__LIST_FILES_CACHE_MAX_ENTRIES").ok();
    Arc::new(MokaListFilesCache::new(ttl, max_entries))
});

pub struct MokaListFilesCache {
    statistics: Cache<Path, Arc<Vec<ObjectMeta>>>,
}

impl MokaListFilesCache {
    pub fn new(ttl: Option<String>, max_entries: Option<String>) -> Self {
        let mut builder = Cache::builder().eviction_policy(EvictionPolicy::lru());

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

        if let Some(max_entries) = max_entries {
            if let Some(max_entries) = try_parse_non_zero_u64(&max_entries) {
                debug!("Setting max entries for MokaListFilesCache to {max_entries}");
                builder = builder.max_capacity(max_entries);
            } else {
                debug!("Disabled or invalid max entries for MokaListFilesCache: {max_entries}");
            }
        } else {
            debug!("No max entries set for MokaListFilesCache");
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

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use object_store::path::Path;
    use object_store::ObjectMeta;

    use super::*;

    #[test]
    fn test_list_file_cache() {
        let meta = ObjectMeta {
            location: Path::from("test"),
            last_modified: DateTime::parse_from_rfc3339("2022-09-27T22:36:00+02:00")
                .unwrap()
                .into(),
            size: 1024,
            e_tag: None,
            version: None,
        };

        let cache = MokaListFilesCache::new(None, None);
        assert!(cache.get(&meta.location).is_none());

        cache.put(&meta.location, vec![meta.clone()].into());
        assert_eq!(
            cache.get(&meta.location).unwrap().first().unwrap().clone(),
            meta.clone()
        );
    }
}
