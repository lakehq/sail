use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

pub(super) const STAGING_DIRECTORY: &str = ".sail-staging";

/// Only the raw top-level component is reserved, not percent-encoded lookalikes.
pub(super) fn is_reserved_path(table: &Path, path: &Path) -> bool {
    path.prefix_match(table)
        .and_then(|mut parts| parts.next())
        .is_some_and(|part| part.as_ref() == STAGING_DIRECTORY)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ListingPolicy {
    /// Filesystem recursion must not enter the reserved directory.
    Hierarchical,
    /// Flat object namespaces can stream metadata without traversing directories.
    Flat,
}

impl ListingPolicy {
    pub(super) fn from_url(url: &url::Url) -> Self {
        match url.scheme() {
            "file" | "hdfs" => Self::Hierarchical,
            _ => Self::Flat,
        }
    }
}

/// A listing view, not an ACL: explicit attempt roots and ordinary object operations
/// remain accessible under the caller's grants. Exclusion happens before traversal
/// and cache insertion for filesystems. Flat stores retain native streaming listing,
/// filtering reserved metadata before cache insertion without delimiter fan-out.
///
/// Static overwrite preserves this namespace, including complete/unknown fences and
/// failed sources. Records are retained indefinitely. An external destructive prefix
/// reset is a no-more-replay boundary and requires quiescing retries first.
#[derive(Debug, Clone)]
pub(super) struct TableListingStore {
    inner: Arc<dyn ObjectStore>,
    table: Path,
    policy: ListingPolicy,
}

impl TableListingStore {
    pub(super) fn new(inner: Arc<dyn ObjectStore>, table: Path, policy: ListingPolicy) -> Self {
        Self {
            inner,
            table,
            policy,
        }
    }
}

impl std::fmt::Display for TableListingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TableListingStore({}, {})", self.inner, self.table)
    }
}

#[async_trait]
impl ObjectStore for TableListingStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(path, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(path, options).await
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let prefix = prefix.cloned().unwrap_or_default();
        if is_reserved_path(&self.table, &prefix) {
            return futures::stream::empty().boxed();
        }
        if !self.table.prefix_matches(&prefix) {
            return self.inner.list(Some(&prefix));
        }
        if self.policy == ListingPolicy::Flat {
            let table = self.table.clone();
            return self
                .inner
                .list(Some(&prefix))
                .try_filter(move |meta| {
                    futures::future::ready(!is_reserved_path(&table, &meta.location))
                })
                .boxed();
        }
        let store = self.clone();
        // Enumerate only the ancestors of the reserved subtree with a delimiter.
        // Ordinary children retain the backend's recursive listing implementation.
        let state = (store, vec![prefix], futures::stream::empty().boxed());
        futures::stream::try_unfold(state, |(store, mut pending, mut current)| async move {
            loop {
                if let Some(meta) = current.try_next().await? {
                    return Ok(Some((meta, (store, pending, current))));
                }
                let Some(prefix) = pending.pop() else {
                    return Ok(None);
                };
                if store.table.prefix_matches(&prefix) {
                    let listed = store.list_with_delimiter(Some(&prefix)).await?;
                    pending.extend(listed.common_prefixes);
                    current = futures::stream::iter(listed.objects.into_iter().map(Ok)).boxed();
                } else {
                    current = store.inner.list(Some(&prefix));
                }
            }
        })
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        if prefix.is_some_and(|prefix| is_reserved_path(&self.table, prefix)) {
            return Ok(ListResult {
                objects: vec![],
                common_prefixes: vec![],
            });
        }
        let mut listed = self.inner.list_with_delimiter(prefix).await?;
        listed
            .objects
            .retain(|meta| !is_reserved_path(&self.table, &meta.location));
        listed
            .common_prefixes
            .retain(|path| !is_reserved_path(&self.table, path));
        Ok(listed)
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
pub(super) mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::execution::cache::TableScopedPath;
    use datafusion::execution::cache::cache_manager::CacheManagerConfig;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::prelude::SessionContext;
    use datafusion_common::Result;
    use datafusion_datasource::ListingTableUrl;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;

    use super::*;
    use crate::listing::utils::{hive_partitioned_files, list_all_files, sample_listing_files};

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum Event {
        List(Path),
        Delimiter(Path),
        Copy(Path, Path),
        Delete(Vec<Path>),
    }

    #[derive(Debug, Clone, Copy)]
    pub enum DeleteFailure {
        PerObject(usize),
        Transport,
    }

    #[derive(Debug, Default)]
    pub struct RecordingStore {
        pub inner: Arc<InMemory>,
        pub events: Arc<Mutex<Vec<Event>>>,
        pub yielded: Arc<AtomicUsize>,
        pub deletion_streams: AtomicUsize,
        pub delete_failure: Option<DeleteFailure>,
        pub denied_listing: Option<Path>,
        pub allowed: Option<Path>,
        pub delimiter_not_implemented: bool,
    }

    impl RecordingStore {
        fn check(&self, path: &Path) -> object_store::Result<()> {
            if self
                .allowed
                .as_ref()
                .is_some_and(|allowed| !path.prefix_matches(allowed))
            {
                return Err(injected_error());
            }
            Ok(())
        }

        fn check_listing(&self, path: &Path) -> object_store::Result<()> {
            self.check(path)?;
            if self
                .denied_listing
                .as_ref()
                .is_some_and(|denied| path.prefix_matches(denied))
            {
                return Err(injected_error());
            }
            Ok(())
        }
    }

    fn injected_error() -> object_store::Error {
        object_store::Error::Generic {
            store: "recording",
            source: std::io::Error::other("injected storage failure").into(),
        }
    }

    impl std::fmt::Display for RecordingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RecordingStore")
        }
    }

    #[async_trait]
    impl ObjectStore for RecordingStore {
        async fn put_opts(
            &self,
            path: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.check(path)?;
            self.inner.put_opts(path, payload, options).await
        }
        async fn put_multipart_opts(
            &self,
            path: &Path,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.check(path)?;
            self.inner.put_multipart_opts(path, options).await
        }
        async fn get_opts(
            &self,
            path: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.check(path)?;
            self.inner.get_opts(path, options).await
        }
        fn delete_stream(
            &self,
            paths: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.deletion_streams.fetch_add(1, Ordering::SeqCst);
            let inner = self.inner.clone();
            let events = self.events.clone();
            let failure = self.delete_failure;
            let allowed = self.allowed.clone();
            futures::stream::once(async move {
                let paths = paths.try_collect::<Vec<_>>().await?;
                events.lock().unwrap().push(Event::Delete(paths.clone()));
                if matches!(failure, Some(DeleteFailure::Transport)) {
                    return Err(injected_error());
                }
                Ok(futures::stream::iter(paths.into_iter().enumerate())
                    .then(move |(index, path)| {
                        let inner = inner.clone();
                        let allowed = allowed.clone();
                        async move {
                            if matches!(failure, Some(DeleteFailure::PerObject(i)) if i == index)
                                || allowed.is_some_and(|allowed| !path.prefix_matches(&allowed))
                            {
                                return Err(injected_error());
                            }
                            inner.delete(&path).await?;
                            Ok(path)
                        }
                    })
                    .boxed())
            })
            .try_flatten()
            .boxed()
        }
        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            let prefix = prefix.cloned().unwrap_or_default();
            self.events
                .lock()
                .unwrap()
                .push(Event::List(prefix.clone()));
            if let Err(error) = self.check_listing(&prefix) {
                return futures::stream::once(futures::future::ready(Err(error))).boxed();
            }
            // Recursive listing an ancestor would enter the inaccessible child.
            if self
                .denied_listing
                .as_ref()
                .is_some_and(|denied| denied.prefix_matches(&prefix))
            {
                return futures::stream::once(futures::future::ready(Err(injected_error())))
                    .boxed();
            }
            let yielded = self.yielded.clone();
            self.inner
                .list(Some(&prefix))
                .map_ok(move |meta| {
                    yielded.fetch_add(1, Ordering::SeqCst);
                    meta
                })
                .boxed()
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            let prefix = prefix.cloned().unwrap_or_default();
            self.events
                .lock()
                .unwrap()
                .push(Event::Delimiter(prefix.clone()));
            self.check_listing(&prefix)?;
            if self.delimiter_not_implemented {
                return Err(object_store::Error::NotImplemented {
                    operation: "list_with_delimiter".into(),
                    implementer: "RecordingStore".into(),
                });
            }
            let listed = self.inner.list_with_delimiter(Some(&prefix)).await?;
            self.yielded
                .fetch_add(listed.objects.len(), Ordering::SeqCst);
            Ok(listed)
        }
        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.check(from)?;
            self.check(to)?;
            self.events
                .lock()
                .unwrap()
                .push(Event::Copy(from.clone(), to.clone()));
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn reserved_listing_is_pruned_before_traversal_and_caching() -> Result<()> {
        for address in [
            "file:///table/",
            "hdfs://namenode/table/",
            "hf://datasets/table/",
            "s3://bucket/table/",
        ] {
            let url = ListingTableUrl::parse(address)?;
            let policy = ListingPolicy::from_url(url.get_url());
            for cache_size in [0, 1024 * 1024] {
                for consumer in ["sample", "input_files", "hive", "datafusion"] {
                    let store = Arc::new(RecordingStore {
                        denied_listing: (policy == ListingPolicy::Hierarchical)
                            .then(|| Path::from("table/.sail-staging")),
                        delimiter_not_implemented: policy == ListingPolicy::Flat,
                        ..Default::default()
                    });
                    for path in [
                        "table/p=1/data",
                        "table/.sail-staging/operation/attempts/corrupt",
                    ] {
                        store
                            .put(&Path::from(path), bytes::Bytes::from_static(b"data").into())
                            .await?;
                    }
                    let runtime = RuntimeEnvBuilder::new()
                        .with_cache_manager(
                            CacheManagerConfig::default().with_list_files_cache_limit(cache_size),
                        )
                        .build_arc()?;
                    let ctx = SessionContext::new_with_config_rt(Default::default(), runtime);
                    ctx.runtime_env()
                        .register_object_store(url.get_url(), store.clone());
                    let state = ctx.state();
                    let listing =
                        TableListingStore::new(store.clone(), url.prefix().clone(), policy);
                    for _ in 0..2 {
                        let count = match consumer {
                            "sample" => {
                                sample_listing_files(&state, std::slice::from_ref(&url), None)
                                    .await?[0]
                                    .objects
                                    .len()
                            }
                            "input_files" => list_all_files(&url, &state, &listing, None)
                                .await?
                                .try_collect::<Vec<_>>()
                                .await?
                                .len(),
                            "hive" => hive_partitioned_files(
                                &state,
                                &listing,
                                &url,
                                &[],
                                &[("p".into(), arrow::datatypes::DataType::Int32)],
                                true,
                                None,
                            )
                            .await?
                            .try_collect::<Vec<_>>()
                            .await?
                            .len(),
                            _ => datafusion::datasource::listing::helpers::pruned_partition_list(
                                &state,
                                &listing,
                                &url,
                                &[],
                                "",
                                &[],
                            )
                            .await?
                            .try_collect::<Vec<_>>()
                            .await?
                            .len(),
                        };
                        assert_eq!(count, 1, "{consumer}, cache={cache_size}");
                    }
                    if let Some(cache) = ctx.runtime_env().cache_manager.get_list_files_cache() {
                        let cached = cache
                            .get(&TableScopedPath {
                                table: None,
                                path: url.prefix().clone(),
                            })
                            .unwrap();
                        assert_eq!(cached.files.len(), 1);
                        assert_eq!(cached.files[0].location, Path::from("table/p=1/data"));
                    }
                    assert!(
                        !store
                            .events
                            .lock()
                            .unwrap()
                            .iter()
                            .any(|event| match event {
                                Event::List(prefix) | Event::Delimiter(prefix) =>
                                    is_reserved_path(url.prefix(), prefix),
                                _ => false,
                            })
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn listing_policy_comes_from_the_url_scheme() -> Result<()> {
        for (address, expected) in [
            ("file:///table/", ListingPolicy::Hierarchical),
            ("hdfs://namenode/table/", ListingPolicy::Hierarchical),
            ("s3://bucket/table/", ListingPolicy::Flat),
            ("hf://datasets/table/", ListingPolicy::Flat),
            ("memory://bucket/table/", ListingPolicy::Flat),
        ] {
            assert_eq!(
                ListingPolicy::from_url(ListingTableUrl::parse(address)?.get_url()),
                expected
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn flat_listing_preserves_lazy_sampling_and_avoids_directory_fanout() -> Result<()> {
        for nested in [false, true] {
            for cache_size in [0, 1024 * 1024] {
                let store = Arc::new(RecordingStore {
                    delimiter_not_implemented: true,
                    ..Default::default()
                });
                store
                    .put(
                        &Path::from("table/.sail-staging/operation/file"),
                        bytes::Bytes::from_static(b"stage").into(),
                    )
                    .await?;
                for index in 0..500 {
                    let path = if nested {
                        format!("table/p={index:04}/file")
                    } else {
                        format!("table/file-{index:04}")
                    };
                    store
                        .put(&Path::from(path), bytes::Bytes::from_static(b"data").into())
                        .await?;
                }
                let runtime = RuntimeEnvBuilder::new()
                    .with_cache_manager(
                        CacheManagerConfig::default().with_list_files_cache_limit(cache_size),
                    )
                    .build_arc()?;
                let ctx = SessionContext::new_with_config_rt(Default::default(), runtime);
                let url = ListingTableUrl::parse("s3://bucket/table/")?;
                ctx.runtime_env()
                    .register_object_store(url.get_url(), store.clone());
                let state = ctx.state();
                let samples =
                    sample_listing_files(&state, std::slice::from_ref(&url), None).await?;
                assert_eq!(samples[0].objects.len(), 10);
                assert!(
                    samples[0]
                        .objects
                        .iter()
                        .all(|meta| !is_reserved_path(url.prefix(), &meta.location))
                );
                // The optional cache intentionally materializes a cold native listing;
                // without it, sampling must stop after ten visible objects.
                assert_eq!(
                    store.yielded.load(Ordering::SeqCst),
                    if cache_size == 0 { 11 } else { 501 }
                );
                assert_eq!(
                    *store.events.lock().unwrap(),
                    vec![Event::List(url.prefix().clone())]
                );
                let listing = TableListingStore::new(
                    store.clone(),
                    url.prefix().clone(),
                    ListingPolicy::from_url(url.get_url()),
                );
                let files = list_all_files(&url, &state, &listing, None)
                    .await?
                    .try_collect::<Vec<_>>()
                    .await?;
                assert_eq!(files.len(), 500);
                assert_eq!(
                    store.events.lock().unwrap().len(),
                    if cache_size == 0 { 2 } else { 1 }
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn listing_view_preserves_ordinary_operations_and_errors() -> Result<()> {
        let store = Arc::new(RecordingStore::default());
        for path in [
            "table/.sail-staging/operation/file",
            "table/%2Esail-staging/file",
            "table/.ordinary/file",
            "table/p=1/.sail-staging/file",
        ] {
            store
                .put(
                    &Path::parse(path)?,
                    bytes::Bytes::from_static(b"data").into(),
                )
                .await?;
        }
        let table = Path::from("table");
        let listing =
            TableListingStore::new(store.clone(), table.clone(), ListingPolicy::Hierarchical);
        assert_eq!(
            listing
                .list(Some(&table))
                .try_collect::<Vec<_>>()
                .await?
                .len(),
            3
        );
        assert_eq!(listing.list(None).try_collect::<Vec<_>>().await?.len(), 3);
        let explicit = Path::from("table/.sail-staging/operation");
        assert!(listing.head(&explicit.clone().join("file")).await.is_ok());
        assert_eq!(
            TableListingStore::new(store, explicit.clone(), ListingPolicy::Hierarchical)
                .list(Some(&explicit))
                .try_collect::<Vec<_>>()
                .await?
                .len(),
            1
        );
        let denied = Arc::new(RecordingStore {
            denied_listing: Some(Path::from("table/.ordinary")),
            ..Default::default()
        });
        denied
            .put(
                &Path::from("table/.ordinary/file"),
                bytes::Bytes::from_static(b"data").into(),
            )
            .await?;
        for policy in [ListingPolicy::Hierarchical, ListingPolicy::Flat] {
            assert!(
                TableListingStore::new(denied.clone(), table.clone(), policy)
                    .list(Some(&table))
                    .try_collect::<Vec<_>>()
                    .await
                    .is_err()
            );
        }
        Ok(())
    }
}
