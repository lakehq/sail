// Read-through object-store caching layer backed by Foyer.
//
// Attribution
// ===========
// The page-splitting / read-through design in this file is adapted from
// lancedb/ocra (Apache-2.0), <https://github.com/lancedb/ocra>, specifically
// its `src/read_through.rs` (the `ReadThroughCache` object-store wrapper) and
// `src/memory.rs` (the `InMemoryCache` page backend). The original code uses
// the Moka cache; here it is reimplemented against Foyer's in-memory
// `Cache` (which additionally supports disk tiering, so the working set may
// exceed RAM) and updated for `object_store` 0.13.2 (whose ergonomic methods
// such as `get_range`/`head` live in the blanket `ObjectStoreExt` trait and so
// are intercepted here at `get_opts`). Per-site `// Adapted from ocra ...`
// comments mark the blocks that are direct ports. Sail-specific wiring (the
// `CacheConfig`, env-gated construction, the `ObjectStore` trait surface for
// 0.13.2, and the tests) is original.
//
// ocra is licensed under the Apache License, Version 2.0; this file is part of
// Sail, also Apache-2.0. The original copyright and license notices are
// preserved by this attribution.

use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use foyer::{Cache, CacheBuilder};
use futures::stream::BoxStream;
use futures::{stream, StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, Error, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, RenameOptions, Result,
};

/// Environment variable that enables the object-store cache when set to a
/// truthy value (`1`, `true`, `yes`, or `on`, case-insensitive).
pub const ENV_CACHE_ENABLE: &str = "SAIL_OBJECT_STORE_CACHE";
/// Environment variable overriding the page size, in bytes.
pub const ENV_CACHE_PAGE_SIZE: &str = "SAIL_OBJECT_STORE_CACHE_PAGE_SIZE";
/// Environment variable overriding the in-memory page capacity, in bytes.
pub const ENV_CACHE_MEMORY: &str = "SAIL_OBJECT_STORE_CACHE_MEMORY";
/// Environment variable overriding the metadata (head) capacity, in bytes.
pub const ENV_CACHE_METADATA: &str = "SAIL_OBJECT_STORE_CACHE_METADATA";

/// Default page size: 1 MiB. A larger page favors remote stores (fewer, larger
/// range requests); a smaller page reduces read amplification for tiny reads.
pub const DEFAULT_PAGE_SIZE: usize = 1024 * 1024;
/// Default in-memory page capacity: 1 GiB (weighed by page byte length).
pub const DEFAULT_MEMORY_CAPACITY_BYTES: usize = 1024 * 1024 * 1024;
/// Default metadata (head) capacity: 64 MiB worth of `ObjectMeta` entries.
pub const DEFAULT_METADATA_CAPACITY_BYTES: usize = 64 * 1024 * 1024;

/// Configuration for [`CachingObjectStore`].
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Size of each cache page, in bytes. Reads are aligned to this size.
    pub page_size: usize,
    /// Total in-memory capacity for data pages, in bytes.
    pub memory_capacity_bytes: usize,
    /// Total in-memory capacity for cached object metadata, in bytes.
    pub metadata_capacity_bytes: usize,
    /// Maximum number of pages fetched concurrently for a single read.
    pub parallelism: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        let parallelism = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(8);
        Self {
            page_size: DEFAULT_PAGE_SIZE,
            memory_capacity_bytes: DEFAULT_MEMORY_CAPACITY_BYTES,
            metadata_capacity_bytes: DEFAULT_METADATA_CAPACITY_BYTES,
            parallelism,
        }
    }
}

impl CacheConfig {
    /// Build a [`CacheConfig`] from the defaults, overlaying any
    /// `SAIL_OBJECT_STORE_CACHE_*` environment overrides that are set and parse
    /// as positive integers.
    pub fn from_env() -> Self {
        let mut config = Self::default();
        if let Some(value) = env_usize(ENV_CACHE_PAGE_SIZE) {
            config.page_size = value;
        }
        if let Some(value) = env_usize(ENV_CACHE_MEMORY) {
            config.memory_capacity_bytes = value;
        }
        if let Some(value) = env_usize(ENV_CACHE_METADATA) {
            config.metadata_capacity_bytes = value;
        }
        config
    }
}

/// Returns whether the object-store cache is enabled via [`ENV_CACHE_ENABLE`].
pub fn cache_enabled_from_env() -> bool {
    match std::env::var(ENV_CACHE_ENABLE) {
        Ok(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => false,
    }
}

/// Wrap `store` in a [`CachingObjectStore`] when the cache is enabled via the
/// environment; otherwise return `store` unchanged. This keeps the cache
/// strictly opt-in: with no environment configuration the behavior is identical
/// to the un-wrapped store.
pub fn maybe_wrap_from_env(store: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    if cache_enabled_from_env() {
        Arc::new(CachingObjectStore::new(store, CacheConfig::from_env()))
    } else {
        store
    }
}

fn env_usize(key: &str) -> Option<usize> {
    std::env::var(key)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
}

/// A read-through, page-aligned cache that wraps an inner [`ObjectStore`].
///
/// Range reads are split into fixed-size pages; each missing page is fetched
/// once from the inner store (concurrent misses for the same page are
/// deduplicated by Foyer) and cached. Object metadata (`head`) is cached
/// separately. Mutations to a location invalidate its cached pages and
/// metadata before delegating to the inner store.
#[derive(Clone)]
pub struct CachingObjectStore {
    state: Arc<CacheState>,
}

impl CachingObjectStore {
    /// Create a new caching store wrapping `inner` with the given `config`.
    pub fn new(inner: Arc<dyn ObjectStore>, config: CacheConfig) -> Self {
        // A page size of zero would make alignment math meaningless; clamp it.
        let page_size = config.page_size.max(1) as u64;
        let parallelism = config.parallelism.max(1);

        // Adapted from ocra `memory.rs`: data pages are weighed by their byte
        // length so the byte capacity is respected; metadata is weighed by an
        // estimate of each `ObjectMeta`'s footprint. (Moka's `weigher` ->
        // Foyer's `with_weighter`.)
        let pages: Cache<(u64, u32), Bytes> =
            CacheBuilder::new(config.memory_capacity_bytes.max(1))
                .with_weighter(|_key, value: &Bytes| value.len())
                .build();
        let metas: Cache<u64, ObjectMeta> =
            CacheBuilder::new(config.metadata_capacity_bytes.max(1))
                .with_weighter(|_key, value: &ObjectMeta| estimate_meta_weight(value))
                .build();

        let state = CacheState {
            store: inner,
            pages,
            metas,
            location_ids: DashMap::new(),
            next_location_id: AtomicU64::new(0),
            page_size,
            parallelism,
        };
        Self {
            state: Arc::new(state),
        }
    }
}

impl fmt::Display for CachingObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CachingObjectStore({})", self.state.store)
    }
}

impl fmt::Debug for CachingObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachingObjectStore")
            .field("inner", &self.state.store)
            .field("page_size", &self.state.page_size)
            .finish()
    }
}

struct CacheState {
    store: Arc<dyn ObjectStore>,
    /// Page cache keyed by `(location id, page id)`, valued by page bytes.
    pages: Cache<(u64, u32), Bytes>,
    /// Metadata cache keyed by `location id`.
    metas: Cache<u64, ObjectMeta>,
    /// Map of path to a small integer id, keeping cache keys compact and
    /// enabling O(1) invalidation (drop the id; orphaned entries age out).
    /// Adapted from ocra `memory.rs` (`location_lookup`).
    location_ids: DashMap<Path, u64>,
    next_location_id: AtomicU64,
    page_size: u64,
    parallelism: usize,
}

/// A unit of work for fetching one (slice of a) page.
#[derive(Clone)]
struct PagePlan {
    page_id: u32,
    /// Absolute byte offset of the page start within the object.
    offset: u64,
    /// Absolute byte offset of the page end (clamped to the object size).
    page_end: u64,
    /// Sub-range within the page to return.
    in_page: Range<usize>,
}

struct CachedHead {
    location_id: u64,
    meta: ObjectMeta,
}

impl CacheState {
    /// Resolve (or assign) the compact id for a location.
    /// Adapted from ocra `memory.rs` `location_id`.
    fn location_id(&self, location: &Path) -> u64 {
        {
            if let Some(id) = self.location_ids.get(location) {
                return *id;
            }
        }
        *self
            .location_ids
            .entry(location.clone())
            .or_insert_with(|| self.next_location_id.fetch_add(1, Ordering::Relaxed))
    }

    /// Drop a location's cache identity so its pages and metadata become
    /// unreachable (and are evicted by capacity over time).
    /// Adapted from ocra `memory.rs` `invalidate`.
    fn invalidate(&self, location: &Path) {
        self.location_ids.remove(location);
    }

    /// Return cached object metadata, fetching from the inner store on a miss.
    /// Adapted from ocra `read_through.rs`/`memory.rs` `head`.
    async fn cached_head(&self, location: &Path) -> Result<CachedHead> {
        let id = self.location_id(location);
        if let Some(entry) = self.metas.get(&id) {
            return Ok(CachedHead {
                location_id: id,
                meta: entry.value().clone(),
            });
        }
        let store = self.store.clone();
        let loc = location.clone();
        let entry = self
            .metas
            .get_or_fetch(&id, move || async move { store.head(&loc).await })
            .await
            .map_err(|err| map_cache_error(location, err))?;
        Ok(CachedHead {
            location_id: id,
            meta: entry.value().clone(),
        })
    }

    /// Compute the per-page fetch plan covering the absolute byte range `range`.
    /// Adapted from ocra `read_through.rs` `get_range` (the page-alignment and
    /// intersection math).
    fn page_plan(&self, range: Range<u64>, size: u64) -> Vec<PagePlan> {
        if range.start >= range.end {
            return Vec::new();
        }
        let page_size = self.page_size;
        let start = (range.start / page_size) * page_size;
        let page_count = (range.end - start).div_ceil(page_size) as usize;
        let mut plans = Vec::with_capacity(page_count);
        let mut offset = start;
        while offset < range.end {
            let page_id = (offset / page_size) as u32;
            let intersection_start = offset.max(range.start);
            let intersection_end = (offset + page_size).min(range.end);
            let in_page =
                (intersection_start - offset) as usize..(intersection_end - offset) as usize;
            let page_end = (offset + page_size).min(size);
            plans.push(PagePlan {
                page_id,
                offset,
                page_end,
                in_page,
            });
            offset += page_size;
        }
        plans
    }

    /// Fetch a single page (read-through, deduplicated by Foyer) and return the
    /// requested sub-slice. Adapted from ocra `read_through.rs` (the
    /// `get_range_with` read-through closure) + `memory.rs` `get_with`.
    async fn fetch_page_slice(
        &self,
        location_id: u64,
        location: &Path,
        plan: PagePlan,
    ) -> Result<Bytes> {
        let key = (location_id, plan.page_id);
        if let Some(entry) = self.pages.get(&key) {
            return Ok(page_slice(entry.value(), plan.in_page));
        }
        let store = self.store.clone();
        let loc = location.clone();
        let offset = plan.offset;
        let page_end = plan.page_end;
        let entry = self
            .pages
            .get_or_fetch(&key, move || async move {
                store.get_range(&loc, offset..page_end).await
            })
            .await
            .map_err(|err| map_cache_error(location, err))?;
        Ok(page_slice(entry.value(), plan.in_page))
    }

    async fn fetch_owned_page_slice(
        self: Arc<Self>,
        location_id: u64,
        location: Arc<Path>,
        plan: PagePlan,
    ) -> Result<Bytes> {
        self.fetch_page_slice(location_id, location.as_ref(), plan)
            .await
    }

    /// Read an absolute byte range, assembling it from cached pages.
    /// Adapted from ocra `read_through.rs` `get_range`.
    async fn read_range(
        self: Arc<Self>,
        location_id: u64,
        location: Arc<Path>,
        meta: &ObjectMeta,
        range: Range<u64>,
    ) -> Result<Bytes> {
        let mut plans = self.page_plan(range, meta.size);
        if plans.is_empty() {
            return Ok(Bytes::new());
        }
        if plans.len() == 1 {
            return self
                .fetch_owned_page_slice(location_id, location, plans.remove(0))
                .await;
        }
        let pages: Vec<Bytes> = stream::iter(plans)
            .map(|plan| {
                self.clone()
                    .fetch_owned_page_slice(location_id, Arc::clone(&location), plan)
            })
            .buffered(self.parallelism)
            .try_collect()
            .await?;

        let total: usize = pages.iter().map(Bytes::len).sum();
        let mut buf = Vec::with_capacity(total);
        for page in pages {
            buf.extend_from_slice(&page);
        }
        Ok(Bytes::from(buf))
    }

    /// Build a streaming [`GetResult`] for `range`, yielding one chunk per page
    /// so a full-object read does not buffer the whole object in memory.
    /// Adapted from ocra `read_through.rs` `get` (the page stream).
    fn range_get_result(
        self: Arc<Self>,
        location_id: u64,
        location: Arc<Path>,
        meta: ObjectMeta,
        range: Range<u64>,
    ) -> GetResult {
        let mut plans = self.page_plan(range.clone(), meta.size);
        let parallelism = self.parallelism;
        let stream = if plans.len() == 1 {
            stream::once(self.fetch_owned_page_slice(location_id, location, plans.remove(0)))
                .boxed()
        } else {
            stream::iter(plans)
                .map(move |plan| {
                    self.clone()
                        .fetch_owned_page_slice(location_id, Arc::clone(&location), plan)
                })
                .buffered(parallelism)
                .boxed()
        };
        GetResult {
            payload: GetResultPayload::Stream(stream),
            meta,
            range,
            attributes: Attributes::default(),
        }
    }
}

fn page_slice(page: &Bytes, range: Range<usize>) -> Bytes {
    let end = range.end.min(page.len());
    let start = range.start.min(end);
    page.slice(start..end)
}

fn estimate_meta_weight(meta: &ObjectMeta) -> usize {
    // A rough footprint estimate so `metadata_capacity_bytes` bounds the head
    // cache: the path string plus fixed overhead for the struct and e-tag.
    meta.location.as_ref().len() + 128
}

/// Map a Foyer cache error back to an [`object_store::Error`], preserving the
/// `NotFound` variant (which callers such as DataFusion rely on) when the
/// underlying loader produced one.
fn map_cache_error(location: &Path, err: foyer::Error) -> Error {
    if let Some(Error::NotFound { path, .. }) = err.downcast_ref::<Error>() {
        return Error::NotFound {
            path: path.clone(),
            source: format!("{err}").into(),
        };
    }
    Error::Generic {
        store: "CachingObjectStore",
        source: format!("{location}: {err}").into(),
    }
}

#[async_trait::async_trait]
#[warn(clippy::missing_trait_methods)]
impl ObjectStore for CachingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        // Adapted from ocra `read_through.rs` `put_opts`: invalidate then write.
        self.state.invalidate(location);
        self.state.store.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.state.invalidate(location);
        self.state.store.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        // Conditional and versioned reads cannot be served from the page cache
        // (it is keyed only by path); delegate them straight to the inner store.
        if options.version.is_some()
            || options.if_match.is_some()
            || options.if_none_match.is_some()
            || options.if_modified_since.is_some()
            || options.if_unmodified_since.is_some()
        {
            return self.state.store.get_opts(location, options).await;
        }

        if options.head {
            let CachedHead { meta, .. } = self.state.cached_head(location).await?;
            return Ok(GetResult {
                payload: GetResultPayload::Stream(stream::empty().boxed()),
                meta,
                range: 0..0,
                attributes: Attributes::default(),
            });
        }

        let CachedHead { location_id, meta } = self.state.cached_head(location).await?;
        let range = match &options.range {
            Some(get_range) => get_range
                .as_range(meta.size)
                .map_err(|err| Error::Generic {
                    store: "CachingObjectStore",
                    source: Box::new(err),
                })?,
            None => 0..meta.size,
        };
        Ok(self.state.clone().range_get_result(
            location_id,
            Arc::new(location.clone()),
            meta,
            range,
        ))
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        // Serve each range through the page cache (rather than the default,
        // which would still route through our cached `get_opts`, but this keeps
        // a single `head` lookup and bounds concurrency explicitly).
        let CachedHead { location_id, meta } = self.state.cached_head(location).await?;
        let state = self.state.clone();
        let location = Arc::new(location.clone());
        stream::iter(ranges.iter().cloned())
            .map(|range| {
                let state = state.clone();
                let location = Arc::clone(&location);
                state.read_range(location_id, location, &meta, range)
            })
            .buffered(self.state.parallelism)
            .try_collect()
            .await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        // Invalidate each location as the inner store confirms its deletion.
        let state = self.state.clone();
        self.state
            .store
            .delete_stream(locations)
            .inspect(move |result| {
                if let Ok(path) = result {
                    state.invalidate(path);
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.state.store.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.state.store.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.state.store.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        // Adapted from ocra `read_through.rs` `copy`: invalidate destination.
        self.state.invalidate(to);
        self.state.store.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.state.invalidate(from);
        self.state.invalidate(to);
        self.state.store.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use object_store::local::LocalFileSystem;
    use object_store::ObjectStoreExt;
    use tempfile::tempdir;

    use super::*;

    /// An inner store that counts `get_opts` calls (which every ranged read and
    /// `head` route through), so tests can assert cache hits avoid inner reads.
    #[derive(Debug)]
    struct CountingObjectStore {
        inner: Arc<dyn ObjectStore>,
        get_opts_calls: Arc<AtomicUsize>,
    }

    impl CountingObjectStore {
        fn new(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                get_opts_calls: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn calls(&self) -> usize {
            self.get_opts_calls.load(Ordering::SeqCst)
        }
    }

    impl fmt::Display for CountingObjectStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "CountingObjectStore({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CountingObjectStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
            self.get_opts_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, Result<Path>>,
        ) -> BoxStream<'static, Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn test_config(page_size: usize) -> CacheConfig {
        CacheConfig {
            page_size,
            memory_capacity_bytes: 8 * 1024 * 1024,
            metadata_capacity_bytes: 1024 * 1024,
            parallelism: 4,
        }
    }

    /// Pattern data where `byte[i] == (i % 256)`.
    fn pattern(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 256) as u8).collect()
    }

    /// Write `data` to a temp file and return (tempdir, store, cache, path).
    fn setup(
        data: &[u8],
        page_size: usize,
    ) -> (
        tempfile::TempDir,
        Arc<CountingObjectStore>,
        CachingObjectStore,
        Path,
    ) {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("data.bin");
        std::fs::write(&file_path, data).unwrap();
        let local = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let counting = Arc::new(CountingObjectStore::new(Arc::new(local)));
        let cache = CachingObjectStore::new(counting.clone(), test_config(page_size));
        let path = Path::from("data.bin");
        (dir, counting, cache, path)
    }

    #[tokio::test]
    async fn get_range_hit_avoids_second_inner_read() {
        let data = pattern(3000);
        let (_dir, counting, cache, path) = setup(&data, 1024);

        let first = cache.get_range(&path, 0..100).await.unwrap();
        assert_eq!(&first[..], &data[0..100]);
        let after_first = counting.calls();
        assert!(after_first > 0);

        // A fully-cached repeat read must not touch the inner store at all.
        let second = cache.get_range(&path, 0..100).await.unwrap();
        assert_eq!(first, second);
        assert_eq!(counting.calls(), after_first);
    }

    #[tokio::test]
    async fn get_range_spanning_pages_concatenates() {
        let data = pattern(3000);
        let (_dir, _counting, cache, path) = setup(&data, 1024);

        // 500..2500 spans page 0 (partial), page 1 (full), page 2 (partial).
        let got = cache.get_range(&path, 500..2500).await.unwrap();
        assert_eq!(got.len(), 2000);
        assert_eq!(&got[..], &data[500..2500]);
    }

    #[tokio::test]
    async fn get_ranges_preserves_order_across_pages() {
        let data = pattern(3000);
        let (_dir, _counting, cache, path) = setup(&data, 1024);
        let ranges = [2048..2100, 100..200, 900..1200];

        let got = cache.get_ranges(&path, &ranges).await.unwrap();

        assert_eq!(got.len(), ranges.len());
        for (bytes, range) in got.iter().zip(ranges) {
            assert_eq!(
                bytes.as_ref(),
                &data[range.start as usize..range.end as usize]
            );
        }
    }

    #[tokio::test]
    async fn get_range_end_of_file_partial_page() {
        let data = pattern(3000);
        let (_dir, _counting, cache, path) = setup(&data, 1024);

        // Last page is only 3000 - 2048 = 952 bytes.
        let meta = cache.head(&path).await.unwrap();
        assert_eq!(meta.size, 3000);
        let got = cache.get_range(&path, 2048..meta.size).await.unwrap();
        assert_eq!(got.len(), 952);
        assert_eq!(&got[..], &data[2048..3000]);

        let tail = cache.get_range(&path, 2500..3000).await.unwrap();
        assert_eq!(&tail[..], &data[2500..3000]);
    }

    #[tokio::test]
    async fn head_caches_object_meta() {
        let data = pattern(3000);
        let (_dir, counting, cache, path) = setup(&data, 1024);

        let meta = cache.head(&path).await.unwrap();
        assert_eq!(meta.size, 3000);
        let after_first = counting.calls();
        assert_eq!(after_first, 1);

        let meta2 = cache.head(&path).await.unwrap();
        assert_eq!(meta2.size, 3000);
        // Second head served from the metadata cache: no new inner read.
        assert_eq!(counting.calls(), after_first);
    }

    #[tokio::test]
    async fn put_invalidates_cached_pages() {
        let (_dir, _counting, cache, path) = setup(&pattern(1000), 1024);

        cache
            .put(&path, PutPayload::from(Bytes::from(vec![1u8; 1000])))
            .await
            .unwrap();
        let v1 = cache.get_range(&path, 0..10).await.unwrap();
        assert_eq!(&v1[..], &[1u8; 10]);

        // Overwriting must invalidate the cached pages so the next read is fresh.
        cache
            .put(&path, PutPayload::from(Bytes::from(vec![2u8; 1000])))
            .await
            .unwrap();
        let v2 = cache.get_range(&path, 0..10).await.unwrap();
        assert_eq!(&v2[..], &[2u8; 10]);
    }

    #[tokio::test]
    async fn delete_invalidates_cached_metadata() {
        let (_dir, _counting, cache, path) = setup(&pattern(1000), 1024);

        // Prime the page and metadata caches.
        let _ = cache.get_range(&path, 0..10).await.unwrap();
        assert_eq!(cache.head(&path).await.unwrap().size, 1000);

        cache.delete(&path).await.unwrap();

        // After deletion the invalidated metadata must not be served from cache.
        let result = cache.head(&path).await;
        assert!(matches!(result, Err(Error::NotFound { .. })));
    }
}
