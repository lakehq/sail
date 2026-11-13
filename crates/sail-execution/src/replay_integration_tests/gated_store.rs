use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};

use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};
use tokio::sync::Semaphore;

#[derive(Debug)]
pub(super) struct GatedStore {
    inner: InMemory,
    manifest: bool,
    blocked: AtomicBool,
    pub entered: Semaphore,
    pub release: Semaphore,
}

impl GatedStore {
    pub fn new(manifest: bool) -> Self {
        Self {
            inner: InMemory::new(),
            manifest,
            blocked: AtomicBool::new(false),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        }
    }
}

impl Display for GatedStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("GatedStore")
    }
}

#[tonic::async_trait]
impl ObjectStore for GatedStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        if location.as_ref().ends_with("manifest.json") == self.manifest
            && !self.blocked.swap(true, Ordering::SeqCst)
        {
            self.entered.add_permits(1);
            self.release
                .acquire()
                .await
                .map_err(|error| object_store::Error::Generic {
                    store: "GatedStore",
                    source: Box::new(error),
                })?
                .forget();
        }
        // The operation completes even if its initiating task has been canceled.
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
