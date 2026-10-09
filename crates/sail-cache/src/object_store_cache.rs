use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::common::{HashMap, Result, TableReference};
use datafusion::execution::cache::{
    Cache as DataFusionCache, CacheEntryInfo, CacheKey, CacheValue,
};
use moka::Equivalent;
use moka::sync::Cache;
use object_store::{ObjectMeta, ObjectStore};

// Retaining the Arc prevents a dropped store's address from being reused by another store.
#[derive(Clone, Debug)]
struct StoreIdentity(Arc<dyn ObjectStore>);

impl PartialEq for StoreIdentity {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for StoreIdentity {}

impl Hash for StoreIdentity {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::ptr::hash(Arc::as_ptr(&self.0).cast::<()>(), state);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ScopedKey<K> {
    store: Option<StoreIdentity>,
    key: K,
    version: Option<FileVersion>,
}

#[derive(Hash)]
struct ScopedKeyRef<'a, K> {
    store: Option<&'a StoreIdentity>,
    key: &'a K,
    version: Option<&'a FileVersion>,
}

impl<K: Eq> Equivalent<ScopedKey<K>> for ScopedKeyRef<'_, K> {
    fn equivalent(&self, other: &ScopedKey<K>) -> bool {
        self.store == other.store.as_ref()
            && self.key == &other.key
            && self.version == other.version.as_ref()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct FileVersion {
    size: u64,
    last_modified: chrono::DateTime<chrono::Utc>,
    e_tag: Option<String>,
    version: Option<String>,
}

impl From<&ObjectMeta> for FileVersion {
    fn from(meta: &ObjectMeta) -> Self {
        Self {
            size: meta.size,
            last_modified: meta.last_modified,
            e_tag: meta.e_tag.clone(),
            version: meta.version.clone(),
        }
    }
}

#[derive(Clone, Debug)]
enum Versions<K> {
    Unspecified,
    File(FileVersion),
    Files(Arc<HashMap<K, FileVersion>>),
}

/// Store-scoped views share one Moka cache, including its capacity and expiration policy.
#[derive(Clone)]
pub struct ObjectStoreCache<K, V> {
    name: &'static str,
    limit: Option<u64>,
    ttl: Option<Duration>,
    entries: Cache<ScopedKey<K>, V>,
    store: Option<StoreIdentity>,
    versions: Versions<K>,
    invalidation: Option<Arc<Mutex<u64>>>,
    generation: u64,
}

impl<K, V> std::fmt::Debug for ObjectStoreCache<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(self.name)
            .field("limit", &self.limit)
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl<K: CacheKey + 'static, V: CacheValue + 'static> ObjectStoreCache<K, V> {
    pub(crate) fn new_inner(
        name: &'static str,
        limit: Option<u64>,
        ttl: Option<Duration>,
        entries: Cache<ScopedKey<K>, V>,
        versioned: bool,
    ) -> Self {
        Self {
            name,
            limit,
            ttl,
            entries,
            store: None,
            versions: Versions::Unspecified,
            invalidation: (!versioned).then(|| Arc::new(Mutex::new(0))),
            generation: 0,
        }
    }

    pub fn for_store(&self, store: &Arc<dyn ObjectStore>) -> Self {
        let generation = self
            .invalidation
            .as_ref()
            .and_then(|value| value.lock().ok().map(|value| *value))
            .unwrap_or(0);
        Self {
            store: Some(StoreIdentity(Arc::clone(store))),
            generation,
            ..self.clone()
        }
    }

    pub fn for_file(&self, meta: &ObjectMeta) -> Self {
        Self {
            versions: Versions::File(FileVersion::from(meta)),
            ..self.clone()
        }
    }

    pub fn for_files<'a>(&self, files: impl IntoIterator<Item = (K, &'a ObjectMeta)>) -> Self {
        Self {
            versions: Versions::Files(Arc::new(
                files
                    .into_iter()
                    .map(|(key, meta)| (key, FileVersion::from(meta)))
                    .collect(),
            )),
            ..self.clone()
        }
    }

    pub fn invalidate(&self, predicate: impl Fn(&K) -> bool) {
        // Hold the generation lock through removal so an in-flight listing cannot refill stale entries.
        let mut generation = match &self.invalidation {
            Some(value) => match value.lock() {
                Ok(value) => Some(value),
                Err(_) => return,
            },
            None => None,
        };
        if let Some(value) = &mut generation {
            **value = value.wrapping_add(1);
        }
        for (key, _) in self.entries.iter().filter(|(key, _)| {
            // Unidentified entries may also belong to the store being changed.
            (self.store.is_none() || key.store.is_none() || self.store == key.store)
                && predicate(&key.key)
        }) {
            self.entries.invalidate(key.as_ref());
        }
    }

    fn scoped_key<'a>(&'a self, key: &'a K) -> Option<ScopedKeyRef<'a, K>> {
        // Path-only callers use a separate namespace from identified stores.
        // shortcut: unidentified stores can still collide until those callers provide a store.
        let version = match &self.versions {
            Versions::Unspecified => None,
            Versions::File(version) => Some(version),
            Versions::Files(versions) => Some(versions.get(key)?),
        };
        Some(ScopedKeyRef {
            store: self.store.as_ref(),
            key,
            version,
        })
    }
}

impl<K: CacheKey + 'static, V: CacheValue + 'static> DataFusionCache<K, V>
    for ObjectStoreCache<K, V>
{
    fn get(&self, key: &K) -> Option<V> {
        if self
            .invalidation
            .as_ref()
            .is_some_and(|generation| generation.is_poisoned())
        {
            return None;
        }
        self.entries.get(&self.scoped_key(key)?)
    }

    fn put(&self, key: &K, value: V) -> Option<V> {
        let scoped = self.scoped_key(key)?;
        let generation = self
            .invalidation
            .as_ref()
            .map(|value| value.lock())
            .transpose()
            .ok()?;
        // Only store-bound listing requests carry a generation captured before their fetch.
        // shortcut: path-only calls retain the legacy race with concurrent invalidation.
        if self.store.is_some()
            && generation
                .as_ref()
                .is_some_and(|value| **value != self.generation)
        {
            return None;
        }
        let previous = self.entries.get(&scoped);
        self.entries.insert(
            ScopedKey {
                store: scoped.store.cloned(),
                key: key.clone(),
                version: scoped.version.cloned(),
            },
            value,
        );
        previous
    }

    fn remove(&self, key: &K) -> Option<V> {
        self.entries.remove(&self.scoped_key(key)?)
    }

    fn contains_key(&self, key: &K) -> bool {
        self.scoped_key(key)
            .is_some_and(|key| self.entries.contains_key(&key))
    }

    fn len(&self) -> usize {
        if self.store.is_none() {
            return self.entries.entry_count() as usize;
        }
        self.entries
            .iter()
            .filter(|(key, _)| {
                self.scoped_key(&key.key)
                    .is_some_and(|scoped| scoped.equivalent(key.as_ref()))
            })
            .count()
    }

    fn clear(&self) {
        self.invalidate(|_| true);
    }

    fn name(&self) -> String {
        self.name.to_string()
    }

    fn cache_limit(&self) -> usize {
        self.limit.map(|limit| limit as usize).unwrap_or(usize::MAX)
    }

    fn update_cache_limit(&self, _limit: usize) {
        // Sail configures the shared capacity at startup, independently of DataFusion defaults.
    }

    fn cache_ttl(&self) -> Option<Duration> {
        self.ttl
    }

    fn update_cache_ttl(&self, _ttl: Option<Duration>) {
        // Sail configures expiration at startup.
    }

    fn drop_table_entries(&self, table: &TableReference) -> Result<()> {
        self.invalidate(|key| key.table_ref() == Some(table));
        Ok(())
    }

    fn list_entries(&self) -> HashMap<K, CacheEntryInfo<V>> {
        self.entries
            .iter()
            .filter(|(key, _)| {
                self.scoped_key(&key.key)
                    .is_some_and(|scoped| scoped.equivalent(key.as_ref()))
            })
            .map(|(key, value)| {
                (
                    key.key.clone(),
                    CacheEntryInfo {
                        size_bytes: value.size(),
                        value,
                        hits: 0,
                        expires: None,
                    },
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::execution::cache::TableScopedPath;
    use datafusion::execution::cache::cache_manager::CachedFileList;
    use object_store::memory::InMemory;
    use object_store::path::Path;

    use super::*;
    use crate::file_listing_cache::MokaFileListingCache;

    fn key(path: &str) -> TableScopedPath {
        TableScopedPath {
            table: None,
            path: Path::from(path),
        }
    }

    #[test]
    fn store_views_isolate_entries_and_share_capacity() {
        let root = MokaFileListingCache::new(Some(1800), Some(2));
        let stores: Vec<Arc<dyn ObjectStore>> = (0..3)
            .map(|_| Arc::new(InMemory::new()) as Arc<dyn ObjectStore>)
            .collect();
        let a = root.for_store(&stores[0]);
        let b = root.for_store(&stores[1]);
        let path = key("same/path");
        a.put(&path, CachedFileList::new(vec![]));
        assert!(root.get(&path).is_none());
        assert!(b.get(&path).is_none());
        assert!(root.for_store(&Arc::clone(&stores[0])).get(&path).is_some());
        assert_eq!(a.cache_limit(), 2);
        assert_eq!(a.cache_ttl(), Some(Duration::from_secs(1800)));
        b.put(&path, CachedFileList::new(vec![]));
        root.entries.run_pending_tasks();
        assert_eq!(root.entries.entry_count(), 2);
        root.for_store(&stores[2])
            .put(&path, CachedFileList::new(vec![]));
        root.entries.run_pending_tasks();
        assert_eq!(root.entries.entry_count(), 2);
    }

    #[test]
    fn invalidation_blocks_in_flight_refills_and_preserves_unrelated_entries() {
        let root = MokaFileListingCache::new(None, None);
        let store_a: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store_b: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let old_a = root.for_store(&store_a);
        let b = root.for_store(&store_b);
        let path = key("table");
        let unrelated = key("other");
        old_a.put(&path, CachedFileList::new(vec![]));
        old_a.put(&unrelated, CachedFileList::new(vec![]));
        b.put(&path, CachedFileList::new(vec![]));
        old_a.invalidate(|key| key == &path);
        assert!(old_a.get(&path).is_none());
        assert!(old_a.get(&unrelated).is_some());
        assert!(b.get(&path).is_some());
        old_a.put(&path, CachedFileList::new(vec![]));
        assert!(old_a.get(&path).is_none());
        let fresh_a = root.for_store(&store_a);
        fresh_a.put(&path, CachedFileList::new(vec![]));
        assert!(fresh_a.get(&path).is_some());
        fresh_a.clear();
        assert!(fresh_a.get(&unrelated).is_none());
        assert!(b.get(&path).is_some());
    }

    #[test]
    fn path_only_cache_stays_active_and_is_invalidated_with_matching_store_entries() {
        let root = MokaFileListingCache::new(None, None);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let scoped = root.for_store(&store);
        let path = key("table");
        let unrelated = key("other");
        root.put(&path, CachedFileList::new(vec![]));
        root.put(&unrelated, CachedFileList::new(vec![]));
        assert!(root.get(&path).is_some());
        assert!(scoped.get(&path).is_none());
        scoped.put(&path, CachedFileList::new(vec![]));
        assert!(scoped.get(&path).is_some());

        scoped.invalidate(|key| key == &path);
        assert!(root.get(&path).is_none());
        assert!(scoped.get(&path).is_none());
        assert!(root.get(&unrelated).is_some());
        root.put(&path, CachedFileList::new(vec![]));
        assert!(root.get(&path).is_some());
        assert!(scoped.get(&path).is_none());
    }

    #[test]
    fn zero_capacity_disables_scoped_views() {
        let root = MokaFileListingCache::new(None, Some(0));
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = root.for_store(&store);
        cache.put(&key("table"), CachedFileList::new(vec![]));
        root.entries.run_pending_tasks();
        assert!(cache.get(&key("table")).is_none());
    }
}
