use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
};
use tonic::codegen::Bytes;

use super::access_error;

pub(super) type StorageRoute = (String, Arc<dyn ObjectStore>);

/// Select a credential at the I/O boundary, before the cloud provider signs the request.
pub(super) struct CredentialRoutingStore {
    routes: Vec<StorageRoute>,
}

impl CredentialRoutingStore {
    pub fn new(mut routes: Vec<StorageRoute>) -> Self {
        routes.sort_by_key(|(prefix, _)| std::cmp::Reverse(prefix.len()));
        Self { routes }
    }

    fn store(&self, path: &str) -> Result<&Arc<dyn ObjectStore>> {
        self.routes
            .iter()
            .find(|(prefix, _)| path.starts_with(prefix))
            .map(|(_, store)| store)
            .ok_or_else(|| access_error("No delegated credentials cover this object path"))
    }

    fn list_store(&self, prefix: Option<&Path>) -> Result<&Arc<dyn ObjectStore>> {
        // ObjectStore listing adds a directory separator to each nonempty prefix.
        let prefix = prefix
            .filter(|prefix| !prefix.as_ref().is_empty())
            .map(|prefix| format!("{prefix}/"))
            .unwrap_or_default();
        self.store(&prefix)
    }

    fn copy_store(&self, from: &Path, to: &Path) -> Result<&Arc<dyn ObjectStore>> {
        let source = self.store(from.as_ref())?;
        let destination = self.store(to.as_ref())?;
        if !Arc::ptr_eq(source, destination) {
            return Err(access_error(
                "Copy between different credential scopes is unsupported",
            ));
        }
        Ok(source)
    }
}

impl fmt::Debug for CredentialRoutingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CredentialRoutingStore")
    }
}

impl fmt::Display for CredentialRoutingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CredentialRoutingStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for CredentialRoutingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.store(location.as_ref())?
            .put_opts(location, payload, opts)
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.store(location.as_ref())?
            .put_multipart_opts(location, opts)
            .await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.store(location.as_ref())?
            .get_opts(location, options)
            .await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.store(location.as_ref())?
            .get_ranges(location, ranges)
            .await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let routes = self.routes.clone();
        locations
            .then(move |path| {
                let routes = routes.clone();
                async move {
                    let path = path?;
                    let router = Self { routes };
                    let mut deleted = router
                        .store(path.as_ref())?
                        .delete_stream(futures::stream::iter([Ok(path)]).boxed());
                    deleted
                        .try_next()
                        .await?
                        .ok_or_else(|| access_error("Storage delete returned no result"))
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        match self.list_store(prefix) {
            Ok(store) => store.list(prefix),
            Err(error) => futures::stream::once(async move { Err(error) }).boxed(),
        }
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        match self.list_store(prefix) {
            Ok(store) => store.list_with_offset(prefix, offset),
            Err(error) => futures::stream::once(async move { Err(error) }).boxed(),
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.list_store(prefix)?.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.copy_store(from, to)?
            .copy_opts(from, to, options)
            .await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.copy_store(from, to)?
            .rename_opts(from, to, options)
            .await
    }
}
