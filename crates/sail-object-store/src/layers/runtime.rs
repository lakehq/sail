use std::fmt;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

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
use tokio::time::timeout;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tokio_util::task::{AbortOnDropHandle, TaskTracker};
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
        let handle = self.handle.clone();
        self.handle
            .spawn(async move {
                let multipart = inner.put_multipart_opts(&location, opts).await?;
                // Install cleanup before returning across the runtime boundary.
                Ok(
                    Box::new(RuntimeAwareMultipartUpload::new(multipart, handle))
                        as Box<dyn MultipartUpload>,
                )
            })
            .await?
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
    inner: Arc<Mutex<MultipartUploadState>>,
    handle: Handle,
    parts: TaskTracker,
    parts_cancel: CancellationToken,
}

#[derive(Debug)]
struct MultipartUploadState {
    upload: Box<dyn MultipartUpload>,
    finished: bool,
}

impl RuntimeAwareMultipartUpload {
    pub fn new(inner: Box<dyn MultipartUpload>, handle: Handle) -> Self {
        let inner = Arc::new(Mutex::new(MultipartUploadState {
            upload: inner,
            finished: false,
        }));
        Self {
            inner,
            handle,
            parts: TaskTracker::new(),
            parts_cancel: CancellationToken::new(),
        }
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
                inner.upload.put_part(data)
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
        let part = self.parts_cancel.clone().run_until_cancelled_owned(part);
        let task = AbortOnDropHandle::new(self.parts.spawn_on(part, &self.handle));
        Box::pin(async move {
            task.await?.unwrap_or_else(|| {
                Err(object_store::Error::Generic {
                    store: "RuntimeAwareMultipartUpload",
                    source: "multipart upload aborted".into(),
                })
            })
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let inner = self.inner.clone();
        self.handle
            .spawn(async move {
                let mut inner = inner.lock().await;
                let result = inner.upload.complete().await;
                if result.is_ok() {
                    inner.finished = true;
                }
                result
            })
            .await?
    }

    async fn abort(&mut self) -> Result<()> {
        // Give local part tasks a bounded window to process cancellation before aborting the
        // provider upload.
        self.parts_cancel.cancel();
        self.parts.close();
        let _ = timeout(Duration::from_secs(15), self.parts.wait()).await;
        let inner = self.inner.clone();
        self.handle
            .spawn(async move {
                let mut inner = inner.lock().await;
                if inner.finished {
                    return Ok(());
                }
                let result = inner.upload.abort().await;
                if result.is_ok() {
                    inner.finished = true;
                }
                result
            })
            .await?
    }
}

impl Drop for RuntimeAwareMultipartUpload {
    fn drop(&mut self) {
        self.parts_cancel.cancel();
        self.parts.close();
        let parts = self.parts.clone();
        let inner = self.inner.clone();
        self.handle.spawn(async move {
            let _ = timeout(Duration::from_secs(15), parts.wait()).await;
            let mut inner = inner.lock().await;
            // Completion runs under the same lock, including when its caller is cancelled.
            if !inner.finished {
                if let Err(error) = inner.upload.abort().await {
                    log::warn!("Failed to abort abandoned multipart upload: {error}");
                } else {
                    inner.finished = true;
                }
            }
        });
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

    #[derive(Debug)]
    struct RecordingMultipartUpload {
        next_idx: Arc<AtomicUsize>,
        barrier: Arc<Barrier>,
        records: Records,
        started: Arc<Notify>,
        dropped: Arc<Notify>,
        aborted: Arc<AtomicUsize>,
        released: Arc<Notify>,
        fail_part: bool,
        completion: Option<(Arc<Notify>, Arc<Notify>)>,
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
                aborted: Arc::new(AtomicUsize::new(0)),
                released: Arc::new(Notify::new()),
                fail_part: false,
                completion: None,
            };
            (upload, records)
        }
    }

    impl Drop for RecordingMultipartUpload {
        fn drop(&mut self) {
            self.released.notify_one();
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
            let fail = self.fail_part;
            Box::pin(async move {
                let _dropped = dropped;
                started.notify_one();
                // All parts must be in flight at the same time to pass the barrier.
                barrier.wait().await;
                if fail {
                    return Err(object_store::Error::Generic {
                        store: "RecordingMultipartUpload",
                        source: "part upload failed".into(),
                    });
                }
                records
                    .lock()
                    .await
                    .push((idx, Bytes::from(data), Handle::current().id()));
                Ok(())
            })
        }

        async fn complete(&mut self) -> Result<PutResult> {
            if let Some((started, resume)) = &self.completion {
                started.notify_one();
                resume.notified().await;
            }
            Ok(PutResult {
                e_tag: None,
                version: None,
            })
        }

        async fn abort(&mut self) -> Result<()> {
            self.aborted.fetch_add(1, Ordering::SeqCst);
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

    #[tokio::test]
    async fn dropped_multipart_upload_aborts_in_flight_parts() -> Result<(), Box<dyn Error>> {
        let (inner, _) = RecordingMultipartUpload::new(2);
        let started = inner.started.clone();
        let aborted = inner.aborted.clone();
        let released = inner.released.clone();
        let mut upload = RuntimeAwareMultipartUpload::new(Box::new(inner), Handle::current());
        let part = upload.put_part(vec![1].into());
        timeout(Duration::from_secs(5), started.notified()).await?;
        drop(upload);
        assert!(part.await.is_err());
        timeout(Duration::from_secs(5), released.notified()).await?;
        assert_eq!(aborted.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn failed_part_is_aborted_when_finishing_buffered_upload() -> Result<(), Box<dyn Error>> {
        let (mut inner, _) = RecordingMultipartUpload::new(1);
        inner.fail_part = true;
        let aborted = inner.aborted.clone();
        let released = inner.released.clone();
        let upload = RuntimeAwareMultipartUpload::new(Box::new(inner), Handle::current());
        let mut writer = object_store::WriteMultipart::new(Box::new(upload));
        writer.put(Bytes::from_static(b"part"));
        assert!(writer.finish().await.is_err());
        timeout(Duration::from_secs(5), released.notified()).await?;
        assert_eq!(aborted.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn finished_multipart_upload_is_not_aborted_again() -> Result<(), Box<dyn Error>> {
        for abort in [false, true] {
            let (inner, _) = RecordingMultipartUpload::new(1);
            let aborted = inner.aborted.clone();
            let released = inner.released.clone();
            let mut upload = RuntimeAwareMultipartUpload::new(Box::new(inner), Handle::current());
            if abort {
                upload.abort().await?;
            } else {
                upload.complete().await?;
            }
            drop(upload);
            timeout(Duration::from_secs(5), released.notified()).await?;
            assert_eq!(aborted.load(Ordering::SeqCst), usize::from(abort));
        }
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_completion_does_not_abort_committed_upload() -> Result<(), Box<dyn Error>> {
        let (mut inner, _) = RecordingMultipartUpload::new(1);
        let started = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        inner.completion = Some((started.clone(), resume.clone()));
        let aborted = inner.aborted.clone();
        let released = inner.released.clone();
        let mut upload = RuntimeAwareMultipartUpload::new(Box::new(inner), Handle::current());
        let task = tokio::spawn(async move { upload.complete().await });
        timeout(Duration::from_secs(5), started.notified()).await?;
        task.abort();
        assert!(task.await.is_err());
        resume.notify_one();
        timeout(Duration::from_secs(5), released.notified()).await?;
        assert_eq!(aborted.load(Ordering::SeqCst), 0);
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
