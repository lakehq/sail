// SPDX-License-Identifier: Apache-2.0

//! An object store for the tests of the write protocol.

use std::collections::{BTreeMap, HashMap};
use std::fmt::{Display, Formatter};
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion_common::{Result, internal_err};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::memory::InMemory;
use object_store::multipart::{MultipartStore, PartId};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartId, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use tokio::sync::Semaphore;

use crate::listing::staging::Publisher;

/// The way in which the files of a write are published in a test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    Rename,
    Multipart,
}

impl Mode {
    pub const ALL: [Mode; 2] = [Mode::Rename, Mode::Multipart];

    /// The kind of the request that publishes a file.
    pub fn publish_request(&self) -> &'static str {
        match self {
            Mode::Rename => "RENAME",
            Mode::Multipart => "COMPLETE",
        }
    }
}

/// A fault of the requests of one kind.
struct Fault {
    kind: &'static str,
    /// The text that the request must contain, if the fault is for
    /// the requests for one location.
    location: Option<String>,
    /// The number of requests that are not affected before the fault.
    skip: usize,
    /// The number of requests that are affected.
    times: usize,
    /// Whether the request takes effect and only its response is an error.
    lost_response: bool,
}

/// Holds the requests of one kind until it is opened.
pub(crate) struct Gate {
    semaphore: Semaphore,
    waiting: AtomicUsize,
}

impl Gate {
    pub fn open(&self) {
        self.semaphore.add_permits(Semaphore::MAX_PERMITS / 2);
    }

    /// Waits until the number of requests have arrived at the gate.
    pub async fn wait_for(&self, requests: usize) {
        while self.waiting.load(Ordering::SeqCst) < requests {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }
}

#[derive(Default)]
struct State {
    requests: Vec<String>,
    faults: Vec<Fault>,
    gates: HashMap<&'static str, Arc<Gate>>,
    /// The location of each multipart upload that is neither completed nor aborted.
    uploads: BTreeMap<MultipartId, String>,
}

#[derive(Default)]
struct Shared {
    state: Mutex<State>,
}

impl Shared {
    /// Records a request, and waits and fails as the test has asked for.
    /// Returns whether the response of the request is to be lost.
    async fn request(&self, kind: &'static str, detail: String) -> object_store::Result<bool> {
        let (gate, fault) = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let gate = state.gates.get(kind).cloned();
            let mut fault = None;
            let matches = |x: &Fault| {
                x.kind == kind && x.location.as_ref().is_none_or(|l| detail.contains(l))
            };
            if let Some(i) = state.faults.iter().position(matches) {
                let x = &mut state.faults[i];
                if x.skip > 0 {
                    x.skip -= 1;
                } else {
                    fault = Some(x.lost_response);
                    x.times -= 1;
                    if x.times == 0 {
                        state.faults.remove(i);
                    }
                }
            }
            state.requests.push(format!("{kind} {detail}"));
            (gate, fault)
        };
        if let Some(gate) = gate {
            gate.waiting.fetch_add(1, Ordering::SeqCst);
            drop(gate.semaphore.acquire().await);
        }
        match fault {
            Some(true) => Ok(true),
            Some(false) => Err(failure(kind)),
            None => Ok(false),
        }
    }
}

fn failure(kind: &'static str) -> object_store::Error {
    object_store::Error::Generic {
        store: "FaultStore",
        source: format!("{kind} failure").into(),
    }
}

/// Replaces the random part of the names of staged objects.
fn normalize(text: &str) -> String {
    const PREFIX: &str = "_staged-";
    // The name has two hyphenated identifiers that are separated by a hyphen.
    const RANDOM: usize = 36 + 1 + 36;
    let mut result = String::new();
    let mut rest = text;
    while let Some(i) = rest.find(PREFIX) {
        let start = i + PREFIX.len();
        result.push_str(&rest[..start]);
        result.push('*');
        rest = &rest[(start + RANDOM).min(rest.len())..];
    }
    result.push_str(rest);
    result
}

/// An in-memory object store that records the requests it receives, that
/// fails or holds the requests that a test selects, and that tracks
/// the multipart uploads that are open.
#[derive(Clone, Default)]
pub(crate) struct FaultStore {
    inner: Arc<InMemory>,
    shared: Arc<Shared>,
}

impl std::fmt::Debug for FaultStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FaultStore")
    }
}

impl Display for FaultStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FaultStore")
    }
}

impl FaultStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn publisher(self: &Arc<Self>, mode: Mode) -> Publisher {
        match mode {
            Mode::Rename => Publisher::Rename {
                store: self.clone(),
            },
            Mode::Multipart => Publisher::Multipart {
                store: self.clone(),
                multipart: self.clone(),
            },
        }
    }

    /// The object store without the faults and without the record of requests.
    pub fn inner(&self) -> &Arc<InMemory> {
        &self.inner
    }

    fn fault(&self, fault: Fault) {
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.faults.push(fault);
    }

    /// Fails one request of the kind after `skip` such requests.
    pub fn fail(&self, kind: &'static str, skip: usize) {
        self.fault(Fault {
            kind,
            location: None,
            skip,
            times: 1,
            lost_response: false,
        });
    }

    /// Fails all requests of the kind whose description contains the location.
    pub fn fail_at(&self, kind: &'static str, location: &str) {
        self.fault(Fault {
            kind,
            location: Some(location.to_string()),
            skip: 0,
            times: usize::MAX,
            lost_response: false,
        });
    }

    /// Lets one request of the kind take effect after `skip` such requests,
    /// and returns an error for it.
    pub fn lose_response(&self, kind: &'static str, skip: usize) {
        self.fault(Fault {
            kind,
            location: None,
            skip,
            times: 1,
            lost_response: true,
        });
    }

    pub fn clear_faults(&self) {
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.faults.clear();
    }

    /// Holds the requests of the kind until the gate is opened.
    pub fn hold(&self, kind: &'static str) -> Arc<Gate> {
        let gate = Arc::new(Gate {
            semaphore: Semaphore::new(0),
            waiting: AtomicUsize::new(0),
        });
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.gates.insert(kind, Arc::clone(&gate));
        gate
    }

    /// The requests that were received, with the random part
    /// of the names of staged objects replaced.
    pub fn requests(&self) -> Vec<String> {
        let state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.requests.iter().map(|x| normalize(x)).collect()
    }

    /// The number of requests of the kind that were received.
    pub fn count(&self, kind: &str) -> usize {
        let state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let prefix = format!("{kind} ");
        state
            .requests
            .iter()
            .filter(|x| x.starts_with(&prefix))
            .count()
    }

    pub fn clear_requests(&self) {
        self.shared
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .requests
            .clear();
    }

    /// The locations of the multipart uploads that are neither completed nor aborted.
    pub fn open_uploads(&self) -> Vec<String> {
        let state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut uploads: Vec<_> = state.uploads.values().cloned().collect();
        uploads.sort();
        uploads
    }

    /// The locations of all objects, with the random part of the names
    /// of staged objects replaced.
    pub async fn objects(&self) -> Result<Vec<String>> {
        let objects: Vec<ObjectMeta> = self.inner.list(None).try_collect().await?;
        let mut objects: Vec<_> = objects
            .iter()
            .map(|x| normalize(x.location.as_ref()))
            .collect();
        objects.sort();
        Ok(objects)
    }

    /// Waits until the store has exactly the objects and no open upload.
    /// The staged files of a write that has ended without success are removed
    /// by tasks that run in the background.
    pub async fn wait_for(&self, expected: &[&str]) -> Result<()> {
        let mut objects = vec![];
        let mut uploads = vec![];
        for _ in 0..2000 {
            objects = self.objects().await?;
            uploads = self.open_uploads();
            if objects == expected && uploads.is_empty() {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        internal_err!(
            "expected {expected:?} but found {objects:?} and the open uploads {uploads:?} after the requests {:?}",
            self.requests()
        )
    }

    /// Waits until the number of requests of the kind were received.
    pub async fn wait_for_requests(&self, kind: &str, requests: usize) -> Result<()> {
        for _ in 0..2000 {
            if self.count(kind) >= requests {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        internal_err!(
            "expected {requests} {kind} requests but found the requests {:?}",
            self.requests()
        )
    }
}

#[async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let detail = format!("{location} {}", payload.content_length());
        let lost = self.shared.request("PUT", detail).await?;
        let result = self.inner.put_opts(location, payload, options).await?;
        if lost {
            Err(failure("PUT"))
        } else {
            Ok(result)
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.shared
            .request("PUT_MULTIPART", location.to_string())
            .await?;
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let kind = if options.head { "HEAD" } else { "GET" };
        self.shared.request(kind, location.to_string()).await?;
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let shared = Arc::clone(&self.shared);
        let locations = locations
            .then(move |location| {
                let shared = Arc::clone(&shared);
                async move {
                    let location = location?;
                    shared.request("DELETE", location.to_string()).await?;
                    Ok(location)
                }
            })
            .boxed();
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let shared = Arc::clone(&self.shared);
        let inner = Arc::clone(&self.inner);
        let prefix = prefix.cloned();
        futures::stream::once(async move {
            let detail = prefix.as_ref().map(|x| x.to_string()).unwrap_or_default();
            match shared.request("LIST", detail).await {
                Ok(_) => inner.list(prefix.as_ref()),
                Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
            }
        })
        .flatten()
        .boxed()
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
        self.shared.request("COPY", format!("{from} {to}")).await?;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        let detail = format!("{from} {to}");
        let lost = self.shared.request("RENAME", detail).await?;
        self.inner.rename_opts(from, to, options).await?;
        if lost { Err(failure("RENAME")) } else { Ok(()) }
    }
}

#[async_trait]
impl MultipartStore for FaultStore {
    async fn create_multipart(&self, path: &Path) -> object_store::Result<MultipartId> {
        let lost = self.shared.request("CREATE", path.to_string()).await?;
        let id = self.inner.create_multipart(path).await?;
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.uploads.insert(id.clone(), path.to_string());
        if lost { Err(failure("CREATE")) } else { Ok(id) }
    }

    async fn put_part(
        &self,
        path: &Path,
        id: &MultipartId,
        part_idx: usize,
        data: PutPayload,
    ) -> object_store::Result<PartId> {
        let detail = format!("{path} {part_idx} {}", data.content_length());
        self.shared.request("PART", detail).await?;
        self.inner.put_part(path, id, part_idx, data).await
    }

    async fn complete_multipart(
        &self,
        path: &Path,
        id: &MultipartId,
        parts: Vec<PartId>,
    ) -> object_store::Result<PutResult> {
        let lost = self.shared.request("COMPLETE", path.to_string()).await?;
        let result = self.inner.complete_multipart(path, id, parts).await?;
        self.shared
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .uploads
            .remove(id);
        if lost {
            Err(failure("COMPLETE"))
        } else {
            Ok(result)
        }
    }

    async fn abort_multipart(&self, path: &Path, id: &MultipartId) -> object_store::Result<()> {
        self.shared.request("ABORT", path.to_string()).await?;
        self.inner.abort_multipart(path, id).await?;
        self.shared
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .uploads
            .remove(id);
        Ok(())
    }
}
