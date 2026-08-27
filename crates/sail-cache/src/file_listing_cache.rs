use std::time::Duration;

use datafusion::common::{HashMap, Result, TableReference};
use datafusion::execution::cache::cache_manager::CachedFileList;
use datafusion::execution::cache::{
    Cache as DataFusionCache, CacheEntryInfo, CacheValue, TableScopedPath,
};
use log::debug;
use moka::sync::Cache;

pub struct MokaFileListingCache {
    objects: Cache<TableScopedPath, CachedFileList>,
    ttl: Option<Duration>,
    max_entries: Option<u64>,
}

impl MokaFileListingCache {
    const NAME: &'static str = "MokaFileListingCache";

    pub fn new(ttl: Option<u64>, max_entries: Option<u64>) -> Self {
        let mut builder = Cache::builder();

        let ttl = ttl.map(Duration::from_secs);
        if let Some(ttl) = ttl {
            debug!("Setting TTL for {} to {ttl:?}", Self::NAME);
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

impl DataFusionCache<TableScopedPath, CachedFileList> for MokaFileListingCache {
    fn get(&self, k: &TableScopedPath) -> Option<CachedFileList> {
        self.objects.get(k)
    }

    fn put(&self, key: &TableScopedPath, value: CachedFileList) -> Option<CachedFileList> {
        let previous = self.objects.get(key);
        self.objects.insert(key.clone(), value);
        previous
    }

    fn remove(&self, k: &TableScopedPath) -> Option<CachedFileList> {
        self.objects.remove(k)
    }

    fn contains_key(&self, k: &TableScopedPath) -> bool {
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

    fn list_entries(&self) -> HashMap<TableScopedPath, CacheEntryInfo<CachedFileList>> {
        self.objects
            .iter()
            .map(|(table_scoped_path, cached)| {
                (
                    (*table_scoped_path).clone(),
                    CacheEntryInfo {
                        size_bytes: cached.size(),
                        value: cached,
                        hits: 0,
                        expires: None,
                    },
                )
            })
            .collect()
    }

    fn drop_table_entries(&self, table_ref: &TableReference) -> Result<()> {
        let keys_to_drop: Vec<TableScopedPath> = self
            .objects
            .iter()
            .filter_map(|(key, _)| {
                (key.table.as_ref() == Some(table_ref)).then_some((*key).clone())
            })
            .collect();

        for key in keys_to_drop {
            self.objects.invalidate(&key);
        }

        Ok(())
    }
}

#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use object_store::ObjectMeta;

    use super::*;

    #[test]
    fn test_file_listing_cache() {
        let meta = ObjectMeta {
            location: object_store::path::Path::from("test"),
            last_modified: DateTime::parse_from_rfc3339("2022-09-27T22:36:00+02:00")
                .unwrap()
                .into(),
            size: 1024,
            e_tag: None,
            version: None,
        };

        let cache = MokaFileListingCache::new(None, None);
        let key = TableScopedPath {
            table: None,
            path: meta.location.clone(),
        };
        assert!(cache.get(&key).is_none());

        cache.put(&key, CachedFileList::new(vec![meta.clone()]));
        assert_eq!(
            cache.get(&key).unwrap().files.first().unwrap().clone(),
            meta.clone()
        );
    }
}
