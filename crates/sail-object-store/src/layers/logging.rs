use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use futures::stream::BoxStream;
use log::debug;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};
use tonic::codegen::Bytes;

#[derive(Debug)]
pub struct LoggingObjectStore {
    inner: Arc<dyn ObjectStore>,
}

impl LoggingObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }
}

impl fmt::Display for LoggingObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LoggingObjectStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for LoggingObjectStore {
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        debug!("object_store put: location: {location:?}");
        self.inner.put(location, payload).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        debug!("object_store put_opts: location: {location:?} opts: {opts:?}");
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
        debug!("object_store put_multipart: location: {location:?}");
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        debug!("put_multipart_opts: location: {location:?} opts: {opts:?}");
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        debug!("object_store get: location: {location:?}");
        self.inner.get(location).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        debug!("object_store get_opts: location: {location:?} options: {options:?}");
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
        debug!("object_store get_range: location: {location:?} range: {range:?}");
        self.inner.get_range(location, range).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        debug!("object_store get_ranges: location: {location:?} ranges: {ranges:?}");
        self.inner.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        debug!("object_store head: location: {location:?}");
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        debug!("object_store delete: location: {location:?}");
        self.inner.delete(location).await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        debug!("object_store delete_stream");
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        debug!("object_store list: prefix: {prefix:?}");
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        debug!("object_store list_with_offset: prefix: {prefix:?} offset: {offset:?}");
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        debug!("object_store list_with_delimiter: prefix: {prefix:?}");
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        debug!("object_store copy: from: {from:?} to: {to:?}");
        self.inner.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        debug!("object_store rename: from: {from:?} to: {to:?}");
        self.inner.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        debug!("object_store copy_if_not_exists: from: {from:?} to: {to:?}");
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        debug!("object_store rename_if_not_exists: from: {from:?} to: {to:?}");
        self.inner.rename_if_not_exists(from, to).await
    }
}
