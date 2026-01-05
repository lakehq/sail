use std::sync::Arc;
use std::time::Duration;

use datafusion::execution::cache::CacheAccessor;
use datafusion::execution::cache::cache_manager::ListFilesCache;
use log::debug;
use moka::sync::Cache;
use object_store::path::Path;
use object_store::ObjectMeta;

pub struct MokaFileListingCache {
    objects: Cache<Path, Arc<Vec<ObjectMeta>>>,
    ttl: Option<Duration>,
    max_entries: Option<u64>,
}

impl MokaFileListingCache {
    const NAME: &'static str = "MokaFileListingCache";

    pub fn new(ttl: Option<u64>, max_entries: Option<u64>) -> Self {
        let mut builder = Cache::builder();

        let ttl = ttl.map(Duration::from_secs);
        if let Some(ttl) = ttl {
            debug!("Setting TTL for {} to {:?} second(s)", Self::NAME, ttl);
            builder = builder.time_to_live(ttl);
        }
        if let Some(max_entries) = max_entries {
            debug!(
                "Setting maximum number of entries for {} to {max_entries}",
                Self::NAME
            );
            builder = builder.max_capacity(max_entries);
        }

        Self {
            objects: builder.build(),
            ttl,
            max_entries,
        }
    }
}

impl CacheAccessor<Path, Arc<Vec<ObjectMeta>>> for MokaFileListingCache {
    type Extra = Option<Path>;

    fn get(&self, k: &Path) -> Option<Arc<Vec<ObjectMeta>>> {
        self.objects.get(k)
    }

    fn get_with_extra(&self, k: &Path, e: &Self::Extra) -> Option<Arc<Vec<ObjectMeta>>> {
        match e {
            None => self.get(k),
            Some(prefix) => {
                let objects = self.get(k)?;

                let base = k.to_string();
                let prefix_str = prefix.to_string();
                let full_prefix = if prefix_str.starts_with(&base) {
                    prefix_str
                } else if base.is_empty() {
                    prefix_str
                } else {
                    format!("{base}/{}", prefix_str.trim_start_matches('/'))
                };

                let filtered = objects
                    .iter()
                    .filter(|meta| meta.location.to_string().starts_with(&full_prefix))
                    .cloned()
                    .collect::<Vec<_>>();
                Some(Arc::new(filtered))
            }
        }
    }

    fn put(&self, key: &Path, value: Arc<Vec<ObjectMeta>>) -> Option<Arc<Vec<ObjectMeta>>> {
        self.objects.insert(key.clone(), value);
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

    fn remove(&self, k: &Path) -> Option<Arc<Vec<ObjectMeta>>> {
        self.objects.remove(k)
    }

    fn contains_key(&self, k: &Path) -> bool {
        self.objects.contains_key(k)
    }

    fn len(&self) -> usize {
        self.objects.entry_count() as usize
    }

    fn clear(&self) {
        self.objects.invalidate_all()
    }

    fn name(&self) -> String {
        Self::NAME.to_string()
    }
}

impl ListFilesCache for MokaFileListingCache {
    fn cache_limit(&self) -> usize {
        self.max_entries
            .map(|limit| limit as usize)
            .unwrap_or(usize::MAX)
    }

    fn cache_ttl(&self) -> Option<Duration> {
        self.ttl
    }

    fn update_cache_limit(&self, _limit: usize) {
        // TODO: support dynamic update of cache limit
    }

    fn update_cache_ttl(&self, _ttl: Option<Duration>) {
        // TODO: support dynamic update of cache ttl
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
    fn test_file_listing_cache() {
        let meta = ObjectMeta {
            location: Path::from("test"),
            last_modified: DateTime::parse_from_rfc3339("2022-09-27T22:36:00+02:00")
                .unwrap()
                .into(),
            size: 1024,
            e_tag: None,
            version: None,
        };

        let cache = MokaFileListingCache::new(None, None);
        assert!(cache.get(&meta.location).is_none());

        cache.put(&meta.location, vec![meta.clone()].into());
        assert_eq!(
            cache.get(&meta.location).unwrap().first().unwrap().clone(),
            meta.clone()
        );
    }
}
