use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use bytes::Bytes;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use tokio::sync::Barrier;

#[derive(Debug, Clone, Copy)]
pub(crate) enum ReplacementFault {
    LostAck,
    PutThenAlreadyExists,
    DifferentBytes,
    Missing,
    ConditionalConflict,
    CompetingVersion,
    PreflightList,
    BootstrapRace,
    ReadbackFailure,
    PostflightList,
    CancelAfterPut,
}

#[derive(Debug)]
pub(crate) struct ReplacementStore {
    pub(crate) inner: object_store::memory::InMemory,
    fault: ReplacementFault,
    pub(crate) metadata_puts: AtomicUsize,
    pub(crate) lists: AtomicUsize,
    pub(crate) disabled: AtomicBool,
    pub(crate) bootstrap_entered: Arc<Barrier>,
    pub(crate) bootstrap_release: Arc<Barrier>,
}

impl ReplacementStore {
    pub(crate) fn new(fault: ReplacementFault) -> Self {
        Self {
            inner: object_store::memory::InMemory::new(),
            fault,
            metadata_puts: AtomicUsize::new(0),
            lists: AtomicUsize::new(0),
            disabled: AtomicBool::new(false),
            bootstrap_entered: Arc::new(Barrier::new(2)),
            bootstrap_release: Arc::new(Barrier::new(2)),
        }
    }
}

impl std::fmt::Display for ReplacementStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "replacement fault store")
    }
}

#[async_trait::async_trait]
impl ObjectStore for ReplacementStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        if !path.as_ref().ends_with(".metadata.json") {
            return self.inner.put_opts(path, payload, options).await;
        }
        let put = self.metadata_puts.fetch_add(1, Ordering::SeqCst);
        if self.disabled.load(Ordering::SeqCst) {
            return self.inner.put_opts(path, payload, options).await;
        }
        match self.fault {
            ReplacementFault::BootstrapRace => {
                if put == 0 {
                    self.bootstrap_entered.wait().await;
                    self.bootstrap_release.wait().await;
                }
                return self.inner.put_opts(path, payload, options).await;
            }
            ReplacementFault::PreflightList | ReplacementFault::PostflightList => {
                return self.inner.put_opts(path, payload, options).await;
            }
            ReplacementFault::CancelAfterPut => {
                let result = self.inner.put_opts(path, payload, options).await?;
                self.bootstrap_entered.wait().await;
                self.bootstrap_release.wait().await;
                return Ok(result);
            }
            ReplacementFault::Missing => {}
            ReplacementFault::DifferentBytes | ReplacementFault::ConditionalConflict => {
                self.inner
                    .put(path, Bytes::from_static(b"different metadata").into())
                    .await?;
                if matches!(self.fault, ReplacementFault::ConditionalConflict) {
                    return self.inner.put_opts(path, payload, options).await;
                }
            }
            ReplacementFault::LostAck | ReplacementFault::ReadbackFailure => {
                self.inner.put_opts(path, payload, options).await?;
            }
            ReplacementFault::PutThenAlreadyExists => {
                self.inner
                    .put_opts(path, payload.clone(), options.clone())
                    .await?;
                return self.inner.put_opts(path, payload, options).await;
            }
            ReplacementFault::CompetingVersion => {
                self.inner
                    .put(
                        &Path::from("table/metadata/v2.metadata.json.gz"),
                        Bytes::from_static(b"competitor").into(),
                    )
                    .await?;
                return self.inner.put_opts(path, payload, options).await;
            }
        }
        Err(object_store::Error::Generic {
            store: "replacement fault",
            source: std::io::Error::other("lost acknowledgement").into(),
        })
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }
    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        if matches!(self.fault, ReplacementFault::ReadbackFailure)
            && !self.disabled.load(Ordering::SeqCst)
            && path.as_ref().ends_with(".metadata.json")
            && !path.as_ref().ends_with("/v1.metadata.json")
            && !path.as_ref().contains("/00000-")
        {
            return Err(object_store::Error::Generic {
                store: "replacement fault",
                source: std::io::Error::other("readback failed").into(),
            });
        }
        self.inner.get_opts(path, options).await
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        assert!(
            matches!(
                self.fault,
                ReplacementFault::PreflightList
                    | ReplacementFault::BootstrapRace
                    | ReplacementFault::ConditionalConflict
            ),
            "replacement must not delete potentially published files"
        );
        let paths = paths.inspect_ok(|path| {
            assert!(
                path.as_ref().ends_with(".avro"),
                "only definitely unpublished manifests may be deleted: {path}"
            );
        });
        self.inner.delete_stream(Box::pin(paths))
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let call = self.lists.fetch_add(1, Ordering::SeqCst);
        if matches!(self.fault, ReplacementFault::PostflightList)
            && self.metadata_puts.load(Ordering::SeqCst) > 0
            && !self.disabled.load(Ordering::SeqCst)
        {
            return Box::pin(futures::stream::once(async {
                Err(object_store::Error::Generic {
                    store: "replacement fault",
                    source: std::io::Error::other("postflight listing failed").into(),
                })
            }));
        }
        // commit_replacement validates the base twice before its publication preflight.
        if matches!(self.fault, ReplacementFault::PreflightList)
            && call == 2
            && !self.disabled.load(Ordering::SeqCst)
        {
            return Box::pin(futures::stream::once(async {
                Err(object_store::Error::Generic {
                    store: "replacement fault",
                    source: std::io::Error::other("preflight listing failed").into(),
                })
            }));
        }
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
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
