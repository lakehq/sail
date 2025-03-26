use std::fmt;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::ops::Range;

use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult, Result,
};
use tokio::sync::OnceCell;
use tonic::codegen::Bytes;

pub struct LazyObjectStore<S, F> {
    inner: OnceCell<S>,
    initializer: F,
}

impl<S, F, Fut> LazyObjectStore<S, F>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<S>>,
{
    pub fn new(initializer: F) -> Self {
        Self {
            inner: OnceCell::new(),
            initializer,
        }
    }

    async fn inner(&self) -> Result<&S> {
        self.inner
            .get_or_try_init(async || (self.initializer)().await)
            .await
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
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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
        self.inner().await?.put(location, payload).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner().await?.put_opts(location, payload, opts).await
    }

    async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
        self.inner().await?.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner().await?.put_multipart_opts(location, opts).await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        self.inner().await?.get(location).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner().await?.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        self.inner().await?.get_range(location, range).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        self.inner().await?.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.inner().await?.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.inner().await?.delete(location).await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        let stream = futures::stream::once(async {
            match self.inner().await {
                Ok(inner) => inner.delete_stream(locations),
                Err(e) => Box::pin(futures::stream::once(async { Err(e) })),
            }
        });
        Box::pin(stream.flatten())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        let prefix = prefix.cloned();
        let stream = futures::stream::once(async move {
            match self.inner().await {
                Ok(inner) => inner.list(prefix.as_ref()),
                Err(e) => Box::pin(futures::stream::once(async { Err(e) })),
            }
        });
        Box::pin(stream.flatten())
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'_, Result<ObjectMeta>> {
        let prefix = prefix.cloned();
        let offset = offset.clone();
        let stream = futures::stream::once(async move {
            match self.inner().await {
                Ok(inner) => inner.list_with_offset(prefix.as_ref(), &offset),
                Err(e) => Box::pin(futures::stream::once(async { Err(e) })),
            }
        });
        Box::pin(stream.flatten())
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner().await?.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner().await?.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner().await?.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner().await?.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner().await?.rename_if_not_exists(from, to).await
    }
}
