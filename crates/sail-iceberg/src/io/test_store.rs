//! Object-store observations and fault injection for manifest/commit invariants.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

#[derive(Debug, Clone, Copy)]
pub(crate) enum PutFault {
    Reject,
    Collision,
    LostAck,
    Pause,
    PauseAfterWrite,
}

#[derive(Debug)]
pub(crate) struct TestStore {
    pub inner: Arc<dyn ObjectStore>,
    pub gets: Mutex<HashMap<String, usize>>,
    pub puts: AtomicUsize,
    pub deletes: Arc<Mutex<Vec<Path>>>,
    pub fault: Option<(String, PutFault)>,
    pub reject_put_number: Option<usize>,
    pub reject_delete: Option<String>,
    pub reject_list_after_put: bool,
    pub reject_list: AtomicBool,
    pub paused: tokio::sync::Notify,
    pub resume: tokio::sync::Notify,
}

impl TestStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            gets: Mutex::new(HashMap::new()),
            puts: AtomicUsize::new(0),
            deletes: Arc::new(Mutex::new(vec![])),
            fault: None,
            reject_put_number: None,
            reject_delete: None,
            reject_list_after_put: false,
            reject_list: AtomicBool::new(false),
            paused: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        }
    }
}

impl std::fmt::Display for TestStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TestStore")
    }
}

fn injected_error(message: &'static str) -> object_store::Error {
    object_store::Error::Generic {
        store: "test-store",
        source: Box::new(std::io::Error::other(message)),
    }
}

#[async_trait::async_trait]
#[expect(clippy::expect_used)]
impl ObjectStore for TestStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let number = self.puts.fetch_add(1, Ordering::SeqCst) + 1;
        if self.reject_put_number == Some(number) {
            return Err(injected_error("injected upload failure"));
        }
        let fault = self
            .fault
            .as_ref()
            .filter(|(pattern, _)| location.as_ref().contains(pattern))
            .map(|(_, fault)| *fault);
        match fault {
            Some(PutFault::Reject) => return Err(injected_error("injected upload failure")),
            Some(PutFault::Collision) => {
                return Err(object_store::Error::AlreadyExists {
                    path: location.to_string(),
                    source: Box::new(std::io::Error::other("injected Create collision")),
                });
            }
            Some(PutFault::Pause) => {
                self.paused.notify_one();
                std::future::pending::<()>().await;
            }
            _ => {}
        }
        let result = self.inner.put_opts(location, payload, opts).await?;
        if matches!(fault, Some(PutFault::PauseAfterWrite)) {
            self.paused.notify_one();
            self.resume.notified().await;
        }
        if matches!(fault, Some(PutFault::LostAck)) {
            return Err(injected_error("injected lost acknowledgement"));
        }
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        *self
            .gets
            .lock()
            .expect("get counts")
            .entry(location.to_string())
            .or_default() += 1;
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let inner = self.inner.clone();
        let deletes = self.deletes.clone();
        let reject = self.reject_delete.clone();
        locations
            .then(move |location| {
                let inner = inner.clone();
                let deletes = deletes.clone();
                let reject = reject.clone();
                async move {
                    let location = location?;
                    deletes
                        .lock()
                        .expect("delete observations")
                        .push(location.clone());
                    if reject
                        .as_ref()
                        .is_some_and(|pattern| location.as_ref().contains(pattern))
                    {
                        return Err(injected_error("injected cleanup failure"));
                    }
                    inner.delete(&location).await?;
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        if self.reject_list.load(Ordering::SeqCst)
            || (self.reject_list_after_put && self.puts.load(Ordering::SeqCst) > 0)
        {
            return futures::stream::once(async {
                Err(injected_error("injected listing failure"))
            })
            .boxed();
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
