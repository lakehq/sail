// SPDX-License-Identifier: Apache-2.0

//! An in-memory object store for tests that records the listings and fails lookups of
//! chosen paths.

use std::collections::HashSet;
use std::fmt;
use std::sync::Mutex;

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};

#[derive(Debug, Default)]
pub(crate) struct ObservedStore {
    inner: InMemory,
    lists: Mutex<Vec<String>>,
    failing_lookups: Mutex<HashSet<Path>>,
}

impl ObservedStore {
    /// Returns the prefixes of the listings so far.
    #[expect(clippy::unwrap_used)]
    pub(crate) fn lists(&self) -> Vec<String> {
        self.lists.lock().unwrap().clone()
    }

    /// Makes the lookups and reads of the path fail with a server error.
    #[expect(clippy::unwrap_used)]
    pub(crate) fn fail_lookups(&self, path: Path) {
        self.failing_lookups.lock().unwrap().insert(path);
    }

    #[expect(clippy::unwrap_used)]
    fn record_list(&self, prefix: Option<&Path>) {
        let prefix = prefix.map(|prefix| prefix.to_string()).unwrap_or_default();
        self.lists.lock().unwrap().push(prefix);
    }
}

impl fmt::Display for ObservedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObservedStore")
    }
}

#[async_trait]
impl ObjectStore for ObservedStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
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
        self.record_list(prefix);
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.record_list(prefix);
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.record_list(prefix);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
