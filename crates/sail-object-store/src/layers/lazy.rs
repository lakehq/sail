use std::fmt;
use std::fmt::Debug;
use std::future::Future;
use std::ops::Range;
use std::sync::Arc;

use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};
use tokio::sync::OnceCell;
use tonic::codegen::Bytes;

struct ObjectStoreCell<S, F> {
    cell: OnceCell<S>,
    initializer: F,
}

impl<S, F> ObjectStoreCell<S, F> {
    fn get(&self) -> Option<&S> {
        self.cell.get()
    }
}

impl<S, F, Fut> ObjectStoreCell<S, F>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<S>>,
{
    async fn get_or_try_init(&self) -> Result<&S> {
        self.cell
            .get_or_try_init(async || (self.initializer)().await)
            .await
    }
}

pub struct LazyObjectStore<S, F> {
    inner: Arc<ObjectStoreCell<S, F>>,
}

impl<S, F> LazyObjectStore<S, F> {
    pub fn new(initializer: F) -> Self {
        Self {
            inner: Arc::new(ObjectStoreCell {
                cell: OnceCell::new(),
                initializer,
            }),
        }
    }
}

impl<S, F> fmt::Display for LazyObjectStore<S, F>
where
    S: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LazyObjectStore({})",
            self.inner
                .get()
                .map(|s| s.to_string())
                .unwrap_or("uninitialized".to_string())
        )
    }
}

impl<S, F> Debug for LazyObjectStore<S, F>
where
    S: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LazyObjectStore({})",
            self.inner
                .get()
                .map(|s| format!("{s:?}"))
                .unwrap_or("uninitialized".to_string())
        )
    }
}

#[async_trait::async_trait]
impl<S, F, Fut> ObjectStore for LazyObjectStore<S, F>
where
    S: ObjectStore,
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<S>> + Send,
{
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        self.inner
            .get_or_try_init()
            .await?
            .put(location, payload)
            .await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner
            .get_or_try_init()
            .await?
            .put_opts(location, payload, opts)
            .await
    }

    async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
        self.inner
            .get_or_try_init()
            .await?
            .put_multipart(location)
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner
            .get_or_try_init()
            .await?
            .put_multipart_opts(location, opts)
            .await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        self.inner.get_or_try_init().await?.get(location).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner
            .get_or_try_init()
            .await?
            .get_opts(location, options)
            .await
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
        self.inner
            .get_or_try_init()
            .await?
            .get_range(location, range)
            .await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.inner
            .get_or_try_init()
            .await?
            .get_ranges(location, ranges)
            .await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.inner.get_or_try_init().await?.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.inner.get_or_try_init().await?.delete(location).await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        futures::stream::once(async {
            match self.inner.get_or_try_init().await {
                Ok(inner) => inner.delete_stream(locations),
                Err(e) => futures::stream::once(async { Err(e) }).boxed(),
            }
        })
        .flatten()
        .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix = prefix.cloned();
        let inner = self.inner.clone();
        futures::stream::once(async move {
            match inner.get_or_try_init().await {
                Ok(inner) => inner.list(prefix.as_ref()),
                Err(e) => futures::stream::once(async { Err(e) }).boxed(),
            }
        })
        .flatten()
        .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix = prefix.cloned();
        let offset = offset.clone();
        let inner = self.inner.clone();
        futures::stream::once(async move {
            match inner.get_or_try_init().await {
                Ok(inner) => inner.list_with_offset(prefix.as_ref(), &offset),
                Err(e) => futures::stream::once(async { Err(e) }).boxed(),
            }
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner
            .get_or_try_init()
            .await?
            .list_with_delimiter(prefix)
            .await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.get_or_try_init().await?.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.get_or_try_init().await?.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner
            .get_or_try_init()
            .await?
            .copy_if_not_exists(from, to)
            .await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner
            .get_or_try_init()
            .await?
            .rename_if_not_exists(from, to)
            .await
    }
}
