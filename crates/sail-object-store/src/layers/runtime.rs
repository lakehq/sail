use std::fmt;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
    UploadPart,
};
use tokio::runtime::Handle;
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::AbortOnDropHandle;
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

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let inner = self.inner.clone();
        let location = location.clone();
        let result = self
            .handle
            .spawn(async move { inner.get_opts(&location, options).await })
            .await??;
        Ok(self.wrap_get_result(result))
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let inner = self.inner.clone();
        let location = location.clone();
        let ranges = ranges.to_vec();
        self.handle
            .spawn(async move { inner.get_ranges(&location, &ranges).await })
            .await?
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        RuntimeAwareStream::new(
            move |x| x.delete_stream(locations),
            self.inner.clone(),
            self.handle.clone(),
        )
        .boxed()
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

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let inner = self.inner.clone();
        let from = from.clone();
        let to = to.clone();
        self.handle
            .spawn(async move { inner.copy_opts(&from, &to, options).await })
            .await?
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        let inner = self.inner.clone();
        let from = from.clone();
        let to = to.clone();
        self.handle
            .spawn(async move { inner.rename_opts(&from, &to, options).await })
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
        // The inner `put_part` assigns the part index synchronously, so it must be called here,
        // in the order the parts are requested, rather than from a spawned task whose scheduling
        // order is not guaranteed.
        let part = match self.inner.try_lock() {
            Ok(mut inner) => {
                let _guard = self.handle.enter();
                inner.put_part(data)
            }
            Err(e) => {
                return Box::pin(async move {
                    Err(object_store::Error::Generic {
                        store: "RuntimeAwareMultipartUpload",
                        source: Box::new(e),
                    })
                });
            }
        };
        // The lock is released before the part is uploaded, so parts upload concurrently, and the
        // upload runs on the object store runtime. Dropping the returned future (e.g. when the
        // caller aborts the upload) cancels the in-flight upload instead of leaving it detached.
        let task = AbortOnDropHandle::new(self.handle.spawn(part));
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

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use futures::future::try_join;
    use tokio::runtime::{Id, Runtime};
    use tokio::sync::{Barrier, Notify};
    use tokio::time::timeout;

    use super::*;

    type Records = Arc<Mutex<Vec<(usize, Bytes, Id)>>>;

    /// A fake upload that records, for each part, the index assigned when the part
    /// was requested, the payload, and the runtime on which the part was uploaded.
    /// The index is assigned synchronously in `put_part`, as all `object_store`
    /// implementations do, so it reflects the order in which parts were requested.
    #[derive(Debug)]
    struct RecordingMultipartUpload {
        next_idx: Arc<AtomicUsize>,
        barrier: Arc<Barrier>,
        records: Records,
        started: Arc<Notify>,
        dropped: Arc<Notify>,
    }

    impl RecordingMultipartUpload {
        fn new(parts: usize) -> (Self, Records) {
            let records = Arc::new(Mutex::new(Vec::new()));
            let upload = Self {
                next_idx: Arc::new(AtomicUsize::new(0)),
                barrier: Arc::new(Barrier::new(parts)),
                records: records.clone(),
                started: Arc::new(Notify::new()),
                dropped: Arc::new(Notify::new()),
            };
            (upload, records)
        }
    }

    struct NotifyOnDrop(Arc<Notify>);

    impl Drop for NotifyOnDrop {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }

    #[async_trait::async_trait]
    impl MultipartUpload for RecordingMultipartUpload {
        fn put_part(&mut self, data: PutPayload) -> UploadPart {
            let idx = self.next_idx.fetch_add(1, Ordering::SeqCst);
            let barrier = self.barrier.clone();
            let records = self.records.clone();
            let started = self.started.clone();
            let dropped = NotifyOnDrop(self.dropped.clone());
            Box::pin(async move {
                let _dropped = dropped;
                started.notify_one();
                // All parts must be in flight at the same time to pass the barrier.
                barrier.wait().await;
                records
                    .lock()
                    .await
                    .push((idx, Bytes::from(data), Handle::current().id()));
                Ok(())
            })
        }

        async fn complete(&mut self) -> Result<PutResult> {
            Ok(PutResult {
                e_tag: None,
                version: None,
            })
        }

        async fn abort(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multipart_parts_upload_concurrently() -> Result<(), Box<dyn Error>> {
        let (inner, _) = RecordingMultipartUpload::new(2);
        let mut upload = RuntimeAwareMultipartUpload::new(Box::new(inner), Handle::current());

        let first = upload.put_part(vec![1].into());
        let second = upload.put_part(vec![2].into());

        // The barrier is only released when both parts are in flight at the same
        // time, so this times out if the wrapper serializes the uploads.
        timeout(Duration::from_secs(5), try_join(first, second)).await??;
        Ok(())
    }

    #[test]
    fn multipart_parts_are_created_synchronously() -> Result<(), Box<dyn Error>> {
        let io = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let (inner, _) = RecordingMultipartUpload::new(2);
        let next_idx = inner.next_idx.clone();
        let mut upload = RuntimeAwareMultipartUpload::new(Box::new(inner), io.handle().clone());

        let first = upload.put_part(vec![1].into());
        let second = upload.put_part(vec![2].into());

        assert_eq!(next_idx.load(Ordering::SeqCst), 2);
        drop((first, second));
        Ok(())
    }

    #[test]
    fn multipart_parts_upload_on_object_store_runtime() -> Result<(), Box<dyn Error>> {
        let primary = Runtime::new()?;
        let io = Runtime::new()?;
        let (inner, records) = RecordingMultipartUpload::new(1);
        let mut upload = RuntimeAwareMultipartUpload::new(Box::new(inner), io.handle().clone());

        primary.block_on(async move {
            timeout(Duration::from_secs(5), upload.put_part(vec![1].into())).await
        })??;

        let records = records.blocking_lock();
        let runtime_ids: Vec<Id> = records.iter().map(|(_, _, id)| *id).collect();
        assert_eq!(runtime_ids, vec![io.handle().id()]);
        assert_ne!(runtime_ids, vec![primary.handle().id()]);
        Ok(())
    }

    #[test]
    fn dropping_upload_part_cancels_runtime_task() -> Result<(), Box<dyn Error>> {
        let primary = Runtime::new()?;
        let io = Runtime::new()?;
        // Only one part is launched, so the two-party barrier keeps it pending.
        let (inner, _) = RecordingMultipartUpload::new(2);
        let started = inner.started.clone();
        let dropped = inner.dropped.clone();
        let mut upload = RuntimeAwareMultipartUpload::new(Box::new(inner), io.handle().clone());
        let part = upload.put_part(vec![1].into());

        primary.block_on(async move {
            let task = tokio::spawn(part);
            timeout(Duration::from_secs(5), started.notified()).await?;
            task.abort();
            timeout(Duration::from_secs(5), dropped.notified()).await?;
            Ok::<_, Box<dyn Error>>(())
        })?;
        Ok(())
    }
}
