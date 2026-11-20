// SPDX-License-Identifier: Apache-2.0

//! Scoping of file cache keys to the object store that holds the files.
//!
//! The file metadata, file statistics, and file listing caches are keyed by object paths, which
//! do not include the object store. Without a scope, the same path in different stores (for
//! example, two S3 buckets) would share cache entries.

use std::sync::Arc;
use std::time::Duration;

use datafusion::common::{HashMap, Result, TableReference};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::cache::cache_manager::{CachedFileMetadataEntry, FileMetadataCache};
use datafusion::execution::cache::{Cache, CacheEntryInfo};
use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::path::{Path, PathPart};

/// Returns a cache key for `path` in the object store at `object_store_url`.
pub fn object_store_scoped_path(object_store_url: &ObjectStoreUrl, path: &Path) -> Path {
    std::iter::once(PathPart::from(object_store_url.as_str()))
        .chain(path.parts())
        .collect()
}

/// Returns the table reference that scopes the file listing cache entries of `url` to its
/// object store.
///
/// DataFusion keys its file listing cache by the table reference of the listing URL and the
/// listing prefix, so every listing of a prefix must use this reference for the same store.
pub fn listing_cache_scope(url: &ListingTableUrl) -> TableReference {
    TableReference::bare(url.object_store().as_str())
}

/// Returns `url` with the table reference that scopes its file listing cache entries to its
/// object store. DataFusion uses this reference when it lists files for a scan.
pub fn listing_cache_scoped_url(url: &ListingTableUrl) -> ListingTableUrl {
    url.clone().with_table_ref(listing_cache_scope(url))
}

/// Returns a view of the shared file metadata cache whose keys are scoped to one object store.
///
/// Only lookups, insertions, and removals are scoped. The size and capacity methods and
/// `clear` act on the shared cache.
pub fn object_store_scoped_file_metadata_cache(
    cache: Arc<FileMetadataCache>,
    object_store_url: &ObjectStoreUrl,
) -> Arc<FileMetadataCache> {
    Arc::new(ObjectStoreScopedFileMetadataCache {
        inner: cache,
        object_store_url: object_store_url.clone(),
    })
}

struct ObjectStoreScopedFileMetadataCache {
    inner: Arc<FileMetadataCache>,
    object_store_url: ObjectStoreUrl,
}

impl ObjectStoreScopedFileMetadataCache {
    fn scoped(&self, key: &Path) -> Path {
        object_store_scoped_path(&self.object_store_url, key)
    }

    fn unscoped(&self, key: &Path) -> Option<Path> {
        let mut parts = key.parts();
        let first = parts.next()?;
        (first == PathPart::from(self.object_store_url.as_str())).then(|| parts.collect())
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
        format!(
            "{} scoped to {}",
            self.inner.name(),
            self.object_store_url.as_str()
        )
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
            object_store_scoped_path(&bucket_a, &path),
            object_store_scoped_path(&bucket_a, &path)
        );
        assert_ne!(
            object_store_scoped_path(&bucket_a, &path),
            object_store_scoped_path(&bucket_b, &path)
        );
        assert_ne!(
            object_store_scoped_path(&bucket_a, &path),
            object_store_scoped_path(&bucket_a, &other)
        );
        assert_ne!(object_store_scoped_path(&bucket_a, &path), path);
        Ok(())
    }

    #[test]
    fn listing_cache_scope_matches_scoped_url_and_differs_by_object_store() -> Result<()> {
        let bucket_a = ListingTableUrl::parse("s3://bucket-a/data/")?;
        let bucket_b = ListingTableUrl::parse("s3://bucket-b/data/")?;
        assert_eq!(
            listing_cache_scoped_url(&bucket_a).get_table_ref(),
            &Some(listing_cache_scope(&bucket_a))
        );
        assert_ne!(
            listing_cache_scope(&bucket_a),
            listing_cache_scope(&bucket_b)
        );
        Ok(())
    }

    #[test]
    fn scoped_views_of_one_file_metadata_cache_keep_object_stores_apart() -> Result<()> {
        let shared =
            CacheManager::try_new(&CacheManagerConfig::default())?.get_file_metadata_cache();
        let bucket_a = object_store_scoped_file_metadata_cache(
            Arc::clone(&shared),
            &ObjectStoreUrl::parse("s3://bucket-a")?,
        );
        let bucket_b = object_store_scoped_file_metadata_cache(
            Arc::clone(&shared),
            &ObjectStoreUrl::parse("s3://bucket-b")?,
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
