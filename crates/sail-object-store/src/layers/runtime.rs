use std::fmt;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, UploadPart,
};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::once;
use tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::Bytes;

#[derive(Debug)]
pub struct RuntimeAwareObjectStore {
    inner: Arc<dyn ObjectStore>,
    handle: Handle,
}

impl RuntimeAwareObjectStore {
    pub fn try_new<F>(initializer: F, handle: Handle) -> Result<Self>
    where
        F: FnOnce() -> Result<Arc<dyn ObjectStore>>,
    {
        let _guard = handle.enter();
        // This initializes the inner object store within the Tokio runtime of the handle.
        // Some object stores create TCP clients during initialization, so we need to
        // ensure that the resources are managed by the correct runtime.
        let inner = initializer()?;
        Ok(Self { inner, handle })
    }

    fn wrap_multipart_upload(
        &self,
        multipart: Box<dyn MultipartUpload>,
    ) -> Box<dyn MultipartUpload> {
        Box::new(RuntimeAwareMultipartUpload::new(
            multipart,
            self.handle.clone(),
        ))
    }

    fn wrap_get_result(&self, result: GetResult) -> GetResult {
        match result {
            GetResult {
                payload: GetResultPayload::File { .. },
                ..
            } => result,
            GetResult {
                payload: GetResultPayload::Stream(stream),
                meta,
                range,
                attributes,
            } => GetResult {
                payload: GetResultPayload::Stream(
                    RuntimeAwareStream::new(move |_| stream, (), self.handle.clone()).boxed(),
                ),
                meta,
                range,
                attributes,
            },
        }
    }
}

impl fmt::Display for RuntimeAwareObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RuntimeAwareObjectStore({})", self.inner)
    }
}

#[async_trait::async_trait]
#[warn(clippy::missing_trait_methods)]
impl ObjectStore for RuntimeAwareObjectStore {
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        let inner = self.inner.clone();
        let location = location.clone();
        self.handle
            .spawn(async move { inner.put(&location, payload).await })
            .await?
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let inner = self.inner.clone();
        let location = location.clone();
        self.handle
            .spawn(async move { inner.put_opts(&location, payload, opts).await })
            .await?
    }

    async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
        let inner = self.inner.clone();
        let location = location.clone();
        let multipart = self
            .handle
            .spawn(async move { inner.put_multipart(&location).await })
            .await??;
        Ok(self.wrap_multipart_upload(multipart))
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let inner = self.inner.clone();
        let location = location.clone();
        let multipart = self
            .handle
            .spawn(async move { inner.put_multipart_opts(&location, opts).await })
            .await??;
        Ok(self.wrap_multipart_upload(multipart))
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let inner = self.inner.clone();
        let location = location.clone();
        let result = self
            .handle
            .spawn(async move { inner.get(&location).await })
            .await??;
        Ok(self.wrap_get_result(result))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let inner = self.inner.clone();
        let location = location.clone();
        let result = self
            .handle
            .spawn(async move { inner.get_opts(&location, options).await })
            .await??;
        Ok(self.wrap_get_result(result))
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
        let inner = self.inner.clone();
        let location = location.clone();
        self.handle
            .spawn(async move { inner.get_range(&location, range).await })
            .await?
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let inner = self.inner.clone();
        let location = location.clone();
        let ranges = ranges.to_vec();
        self.handle
            .spawn(async move { inner.get_ranges(&location, &ranges).await })
            .await?
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let inner = self.inner.clone();
        let location = location.clone();
        self.handle
            .spawn(async move { inner.head(&location).await })
            .await?
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let inner = self.inner.clone();
        let location = location.clone();
        self.handle
            .spawn(async move { inner.delete(&location).await })
            .await?
    }

    fn delete_stream<'a>(
        &'a self,
        _locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        // FIXME: We cannot run `delete_stream` in a runtime-aware manner because
        //  the input and output streams are expected to have the lifetime `'a`,
        //  while tasks spawned by the runtime handle must be `'static`.
        once(Err(object_store::Error::NotImplemented)).boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix = prefix.cloned();
        RuntimeAwareStream::new(
            move |x| x.list(prefix.as_ref()),
            self.inner.clone(),
            self.handle.clone(),
        )
        .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix = prefix.cloned();
        let offset = offset.clone();
        RuntimeAwareStream::new(
            move |x| x.list_with_offset(prefix.as_ref(), &offset),
            self.inner.clone(),
            self.handle.clone(),
        )
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let inner = self.inner.clone();
        let prefix = prefix.cloned();
        self.handle
            .spawn(async move { inner.list_with_delimiter(prefix.as_ref()).await })
            .await?
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let inner = self.inner.clone();
        let from = from.clone();
        let to = to.clone();
        self.handle
            .spawn(async move { inner.copy(&from, &to).await })
            .await?
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let inner = self.inner.clone();
        let from = from.clone();
        let to = to.clone();
        self.handle
            .spawn(async move { inner.rename(&from, &to).await })
            .await?
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let inner = self.inner.clone();
        let from = from.clone();
        let to = to.clone();
        self.handle
            .spawn(async move { inner.copy_if_not_exists(&from, &to).await })
            .await?
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let inner = self.inner.clone();
        let from = from.clone();
        let to = to.clone();
        self.handle
            .spawn(async move { inner.rename_if_not_exists(&from, &to).await })
            .await?
    }
}

#[derive(Debug)]
struct RuntimeAwareMultipartUpload {
    inner: Arc<Mutex<Box<dyn MultipartUpload>>>,
    handle: Handle,
}

impl RuntimeAwareMultipartUpload {
    pub fn new(inner: Box<dyn MultipartUpload>, handle: Handle) -> Self {
        let inner = Arc::new(Mutex::new(inner));
        Self { inner, handle }
    }
}

#[async_trait::async_trait]
impl MultipartUpload for RuntimeAwareMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let inner = self.inner.clone();
        let task = self.handle.spawn(async move {
            let mut inner = inner.lock().await;
            inner.put_part(data).await
        });
        Box::pin(async move { task.await? })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let inner = self.inner.clone();
        self.handle
            .spawn(async move {
                let mut inner = inner.lock().await;
                inner.complete().await
            })
            .await?
    }

    async fn abort(&mut self) -> Result<()> {
        let inner = self.inner.clone();
        self.handle
            .spawn(async move {
                let mut inner = inner.lock().await;
                inner.abort().await
            })
            .await?
    }
}

struct RuntimeAwareStream<T> {
    inner: ReceiverStream<T>,
}

impl<T> RuntimeAwareStream<T>
where
    T: Send + 'static,
{
    pub fn new<F, A>(initializer: F, args: A, handle: Handle) -> Self
    where
        A: Send + 'static,
        F: FnOnce(&A) -> BoxStream<'_, T> + Send + 'static,
    {
        // Testing with larger buffer values showed no performance improvement.
        // Network I/O is the bottleneck, not channel capacity.
        let (tx, rx) = mpsc::channel(1);
        handle.spawn(async move {
            let mut stream = initializer(&args);
            while let Some(item) = stream.next().await {
                if tx.send(item).await.is_err() {
                    break;
                }
            }
        });
        Self {
            inner: ReceiverStream::new(rx),
        }
    }
}

impl<T> Stream for RuntimeAwareStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}
