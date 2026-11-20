// SPDX-License-Identifier: Apache-2.0

//! Scoping of file cache keys to the object store that holds the files.
//!
//! The file metadata, file statistics, and file listing caches are keyed by object paths, which
//! do not include the object store. Without a scope, the same path in different stores (for
//! example, two S3 buckets) would share cache entries.
//!
//! The scope also includes the session ID for `memory` URLs, since each session has its own
//! object store registry, which creates a new, empty in-memory store for such a URL. Without the
//! session ID, the same path in the in-memory stores of different sessions would share cache
//! entries, even though the caches themselves can be shared across sessions. Other stores are
//! shared across sessions, so their cache entries are too.

use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use datafusion::common::{HashMap, Result, TableReference};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::cache::cache_manager::{CachedFileMetadataEntry, FileMetadataCache};
use datafusion::execution::cache::{Cache, CacheEntryInfo};
use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::path::{Path, PathPart};

/// Returns the cache scope of the object store at `object_store_url` in the session with
/// `session_id`. Only `memory` URLs include the session ID.
fn cache_scope<'a>(object_store_url: &'a ObjectStoreUrl, session_id: &str) -> Cow<'a, str> {
    let url = object_store_url.as_str();
    if url.starts_with("memory:") {
        Cow::Owned(format!("{url} session {session_id}"))
    } else {
        Cow::Borrowed(url)
    }
}

/// Returns a cache key for `path` in the object store at `object_store_url` in the session with
/// `session_id`.
pub fn scoped_path(object_store_url: &ObjectStoreUrl, session_id: &str, path: &Path) -> Path {
    std::iter::once(PathPart::from(
        cache_scope(object_store_url, session_id).as_ref(),
    ))
    .chain(path.parts())
    .collect()
}

/// Returns the table reference that scopes the file listing cache entries of `url` to its
/// object store in the session with `session_id`.
///
/// DataFusion keys its file listing cache by the table reference of the listing URL and the
/// listing prefix, so every listing of a prefix must use this reference for the same store.
pub fn listing_table_ref(url: &ListingTableUrl, session_id: &str) -> TableReference {
    TableReference::bare(cache_scope(&url.object_store(), session_id).as_ref())
}

/// Returns `url` with the table reference that scopes its file listing cache entries to its
/// object store in the session with `session_id`. DataFusion uses this reference when it lists
/// files for a scan.
pub fn scoped_listing_url(url: &ListingTableUrl, session_id: &str) -> ListingTableUrl {
    url.clone()
        .with_table_ref(listing_table_ref(url, session_id))
}

/// Returns a view of the shared file metadata cache whose keys are scoped to one object store
/// in the session with `session_id`.
///
/// Lookups, insertions, removals, and `list_entries` are scoped to the object store. The size,
/// capacity, and TTL methods, `clear`, and `drop_table_entries` act on the shared cache.
pub fn scoped_file_metadata_cache(
    cache: Arc<FileMetadataCache>,
    object_store_url: &ObjectStoreUrl,
    session_id: &str,
) -> Arc<FileMetadataCache> {
    Arc::new(ObjectStoreScopedFileMetadataCache {
        inner: cache,
        scope: PathPart::from(cache_scope(object_store_url, session_id).into_owned()),
    })
}

struct ObjectStoreScopedFileMetadataCache {
    inner: Arc<FileMetadataCache>,
    scope: PathPart<'static>,
}

impl ObjectStoreScopedFileMetadataCache {
    fn scoped(&self, key: &Path) -> Path {
        std::iter::once(self.scope.clone())
            .chain(key.parts())
            .collect()
    }

    fn unscoped(&self, key: &Path) -> Option<Path> {
        let mut parts = key.parts();
        let first = parts.next()?;
        (first == self.scope).then(|| parts.collect())
    }
}

impl Cache<Path, CachedFileMetadataEntry> for ObjectStoreScopedFileMetadataCache {
    fn get(&self, key: &Path) -> Option<CachedFileMetadataEntry> {
        self.inner.get(&self.scoped(key))
    }

    fn put(&self, key: &Path, value: CachedFileMetadataEntry) -> Option<CachedFileMetadataEntry> {
        self.inner.put(&self.scoped(key), value)
    }

    fn remove(&self, key: &Path) -> Option<CachedFileMetadataEntry> {
        self.inner.remove(&self.scoped(key))
    }

    fn contains_key(&self, key: &Path) -> bool {
        self.inner.contains_key(&self.scoped(key))
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn clear(&self) {
        self.inner.clear()
    }

    fn name(&self) -> String {
        format!("{} scoped to {}", self.inner.name(), self.scope.as_ref())
    }

    fn cache_limit(&self) -> usize {
        self.inner.cache_limit()
    }

    fn update_cache_limit(&self, limit: usize) {
        self.inner.update_cache_limit(limit)
    }

    fn cache_ttl(&self) -> Option<Duration> {
        self.inner.cache_ttl()
    }

    fn update_cache_ttl(&self, ttl: Option<Duration>) {
        self.inner.update_cache_ttl(ttl)
    }

    fn drop_table_entries(&self, table_ref: &TableReference) -> Result<()> {
        self.inner.drop_table_entries(table_ref)
    }

    fn list_entries(&self) -> HashMap<Path, CacheEntryInfo<CachedFileMetadataEntry>> {
        self.inner
            .list_entries()
            .into_iter()
            .filter_map(|(key, value)| Some((self.unscoped(&key)?, value)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use datafusion::execution::cache::cache_manager::{
        CacheManager, CacheManagerConfig, FileMetadata,
    };
    use object_store::ObjectMeta;

    use super::*;

    struct TestFileMetadata;

    impl FileMetadata for TestFileMetadata {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn memory_size(&self) -> usize {
            1
        }

        fn extra_info(&self) -> HashMap<String, String> {
            HashMap::new()
        }
    }

    #[test]
    fn scoped_paths_differ_only_by_object_store() -> Result<()> {
        let bucket_a = ObjectStoreUrl::parse("s3://bucket-a")?;
        let bucket_b = ObjectStoreUrl::parse("s3://bucket-b")?;
        let path = Path::from("warehouse/table/data.parquet");
        let other = Path::from("warehouse/table/other.parquet");
        assert_eq!(
            scoped_path(&bucket_a, "s", &path),
            scoped_path(&bucket_a, "s", &path)
        );
        assert_ne!(
            scoped_path(&bucket_a, "s", &path),
            scoped_path(&bucket_b, "s", &path)
        );
        assert_ne!(
            scoped_path(&bucket_a, "s", &path),
            scoped_path(&bucket_a, "s", &other)
        );
        assert_ne!(scoped_path(&bucket_a, "s", &path), path);
        Ok(())
    }

    #[test]
    fn listing_table_ref_matches_scoped_listing_url_and_differs_by_object_store() -> Result<()> {
        let bucket_a = ListingTableUrl::parse("s3://bucket-a/data/")?;
        let bucket_b = ListingTableUrl::parse("s3://bucket-b/data/")?;
        assert_eq!(
            scoped_listing_url(&bucket_a, "s").get_table_ref(),
            &Some(listing_table_ref(&bucket_a, "s"))
        );
        assert_ne!(
            listing_table_ref(&bucket_a, "s"),
            listing_table_ref(&bucket_b, "s")
        );
        Ok(())
    }

    #[test]
    fn only_memory_scopes_differ_by_session() -> Result<()> {
        let path = Path::from("warehouse/table/data.parquet");
        let memory = ObjectStoreUrl::parse("memory://")?;
        assert_eq!(
            scoped_path(&memory, "a", &path),
            scoped_path(&memory, "a", &path)
        );
        assert_ne!(
            scoped_path(&memory, "a", &path),
            scoped_path(&memory, "b", &path)
        );
        let memory = ListingTableUrl::parse("memory:///data/")?;
        assert_ne!(
            listing_table_ref(&memory, "a"),
            listing_table_ref(&memory, "b")
        );
        for url in ["s3://bucket", "file:///"] {
            let object_store_url = ObjectStoreUrl::parse(url)?;
            assert_eq!(
                scoped_path(&object_store_url, "a", &path),
                scoped_path(&object_store_url, "b", &path)
            );
            let listing_url = ListingTableUrl::parse(format!("{url}/data/"))?;
            assert_eq!(
                listing_table_ref(&listing_url, "a"),
                listing_table_ref(&listing_url, "b")
            );
        }
        Ok(())
    }

    #[test]
    fn scoped_views_of_one_file_metadata_cache_keep_object_stores_apart() -> Result<()> {
        let shared =
            CacheManager::try_new(&CacheManagerConfig::default())?.get_file_metadata_cache();
        let bucket_a = scoped_file_metadata_cache(
            Arc::clone(&shared),
            &ObjectStoreUrl::parse("s3://bucket-a")?,
            "s",
        );
        let bucket_b = scoped_file_metadata_cache(
            Arc::clone(&shared),
            &ObjectStoreUrl::parse("s3://bucket-b")?,
            "s",
        );
        let path = Path::from("warehouse/table/data.parquet");
        let entry = CachedFileMetadataEntry::new(
            ObjectMeta {
                location: path.clone(),
                last_modified: chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
                size: 100,
                e_tag: None,
                version: None,
            },
            Arc::new(TestFileMetadata),
        );
        bucket_a.put(&path, entry);
        assert!(bucket_a.contains_key(&path));
        assert!(bucket_a.get(&path).is_some());
        assert!(!bucket_b.contains_key(&path));
        assert!(bucket_b.get(&path).is_none());
        assert!(!shared.contains_key(&path));
        assert_eq!(
            bucket_a.list_entries().into_keys().collect::<Vec<_>>(),
            vec![path]
        );
        assert!(bucket_b.list_entries().is_empty());
        Ok(())
    }
}
