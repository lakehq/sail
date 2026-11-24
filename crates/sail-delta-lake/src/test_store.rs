// SPDX-License-Identifier: Apache-2.0

//! An in-memory object store for tests that records the reads and the lookups, fails lookups of
//! chosen paths, can list keys out of order, and can let another writer write a path first.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};

#[derive(Debug, Default)]
pub(crate) struct ObservedStore {
    inner: InMemory,
    gets: Mutex<Vec<Path>>,
    heads: Mutex<Vec<Path>>,
    failing_lookups: Mutex<HashSet<Path>>,
    reverse_listings: AtomicBool,
    racing_writes: Mutex<HashMap<Path, Bytes>>,
}

impl ObservedStore {
    /// Returns the paths of the reads so far.
    #[expect(clippy::unwrap_used)]
    pub(crate) fn gets(&self) -> Vec<Path> {
        self.gets.lock().unwrap().clone()
    }

    /// Returns the paths of the lookups so far.
    #[expect(clippy::unwrap_used)]
    pub(crate) fn heads(&self) -> Vec<Path> {
        self.heads.lock().unwrap().clone()
    }

    /// Makes the listings return keys in reverse lexicographic order, as an object store
    /// that does not list keys in order may.
    pub(crate) fn list_in_reverse_order(&self) {
        self.reverse_listings.store(true, Ordering::Relaxed);
    }

    /// Makes the lookups and reads of the path fail with a server error.
    #[expect(clippy::unwrap_used)]
    pub(crate) fn fail_lookups(&self, path: Path) {
        self.failing_lookups.lock().unwrap().insert(path);
    }

    /// Makes the first write of the path find the bytes that another writer wrote just before.
    #[expect(clippy::unwrap_used)]
    pub(crate) fn write_first(&self, path: Path, bytes: Bytes) {
        self.racing_writes.lock().unwrap().insert(path, bytes);
    }

    fn ordered(
        &self,
        stream: BoxStream<'static, Result<ObjectMeta>>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        if !self.reverse_listings.load(Ordering::Relaxed) {
            return stream;
        }
        futures::stream::once(async move {
            let mut entries = stream.try_collect::<Vec<_>>().await?;
            entries.reverse();
            Ok::<_, object_store::Error>(futures::stream::iter(entries.into_iter().map(Ok)))
        })
        .try_flatten()
        .boxed()
    }
}

impl fmt::Display for ObservedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObservedStore")
    }
}

#[async_trait]
impl ObjectStore for ObservedStore {
    #[expect(clippy::unwrap_used)]
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let racing = self.racing_writes.lock().unwrap().remove(location);
        if let Some(bytes) = racing {
            self.inner
                .put_opts(location, bytes.into(), PutOptions::default())
                .await?;
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    #[expect(clippy::unwrap_used)]
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if options.head {
            self.heads.lock().unwrap().push(location.clone());
        } else {
            self.gets.lock().unwrap().push(location.clone());
        }
        if self.failing_lookups.lock().unwrap().contains(location) {
            return Err(object_store::Error::Generic {
                store: "ObservedStore",
                source: "503 Service Unavailable".into(),
            });
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.ordered(self.inner.list(prefix))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.ordered(self.inner.list_with_offset(prefix, offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
