/// Compatibility wrapper to bridge object_store 0.13 (workspace) with object_store 0.12
/// (required by delta_kernel 0.18.2 internally).
///
/// delta_kernel's `DefaultEngine::new_with_executor` expects an `Arc<dyn object_store_0_12::ObjectStore>`.
/// This wrapper implements the 0.12 interface by delegating to a 0.13 store.
use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectStore, ObjectStoreExt};
use object_store_012::path::Path as Path012;
use object_store_012::{
    GetOptions as GetOptions012, GetResult as GetResult012,
    GetResultPayload as GetResultPayload012, ListResult as ListResult012,
    MultipartUpload as MultipartUpload012, ObjectMeta as ObjectMeta012,
    ObjectStore as ObjectStore012, PutMultipartOptions as PutMultipartOptions012,
    PutOptions as PutOptions012, PutPayload as PutPayload012, PutResult as PutResult012,
    Result as Result012,
};

fn path_from_012(p: &Path012) -> object_store::path::Path {
    object_store::path::Path::from(p.as_ref())
}

fn path_to_012(p: &object_store::path::Path) -> Path012 {
    Path012::from(p.as_ref())
}

fn meta_from_013(m: object_store::ObjectMeta) -> ObjectMeta012 {
    ObjectMeta012 {
        location: path_to_012(&m.location),
        last_modified: m.last_modified,
        size: m.size,
        e_tag: m.e_tag,
        version: m.version,
    }
}

fn error_013_to_012(e: object_store::Error) -> object_store_012::Error {
    object_store_012::Error::Generic {
        store: "compat",
        source: Box::new(e),
    }
}

/// Wrapper that makes an `object_store` 0.13 store appear as a 0.12 store.
pub(crate) struct ObjectStoreCompat {
    inner: Arc<dyn ObjectStore>,
}

impl ObjectStoreCompat {
    pub(crate) fn new(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(Self { inner })
    }
}

impl fmt::Display for ObjectStoreCompat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectStoreCompat({})", self.inner)
    }
}

impl fmt::Debug for ObjectStoreCompat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectStoreCompat({:?})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore012 for ObjectStoreCompat {
    async fn put_opts(
        &self,
        location: &Path012,
        payload: PutPayload012,
        _opts: PutOptions012,
    ) -> Result012<PutResult012> {
        let loc = path_from_012(location);
        let bytes: Bytes = payload.into_iter().flatten().collect::<Vec<u8>>().into();
        let result = self
            .inner
            .put(&loc, bytes.into())
            .await
            .map_err(error_013_to_012)?;
        Ok(PutResult012 {
            e_tag: result.e_tag,
            version: result.version,
        })
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path012,
        _opts: PutMultipartOptions012,
    ) -> Result012<Box<dyn MultipartUpload012>> {
        Err(object_store_012::Error::NotImplemented)
    }

    async fn get_opts(
        &self,
        location: &Path012,
        _options: GetOptions012,
    ) -> Result012<GetResult012> {
        let loc = path_from_012(location);
        let result = self.inner.get(&loc).await.map_err(error_013_to_012)?;
        let meta012 = ObjectMeta012 {
            location: location.clone(),
            last_modified: result.meta.last_modified,
            size: result.meta.size,
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let range = result.range.clone();
        let bytes = result.bytes().await.map_err(error_013_to_012)?;
        let stream: BoxStream<'static, Result012<Bytes>> =
            futures::stream::once(async move { Ok(bytes) }).boxed();
        Ok(GetResult012 {
            payload: GetResultPayload012::Stream(stream),
            meta: meta012,
            range,
            attributes: Default::default(),
        })
    }

    async fn delete(&self, location: &Path012) -> Result012<()> {
        let loc = path_from_012(location);
        self.inner.delete(&loc).await.map_err(error_013_to_012)
    }

    fn list(&self, prefix: Option<&Path012>) -> BoxStream<'static, Result012<ObjectMeta012>> {
        let prefix013 = prefix.map(path_from_012);
        self.inner
            .list(prefix013.as_ref())
            .map_ok(meta_from_013)
            .map_err(error_013_to_012)
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path012>) -> Result012<ListResult012> {
        let prefix013 = prefix.map(path_from_012);
        let result = self
            .inner
            .list_with_delimiter(prefix013.as_ref())
            .await
            .map_err(error_013_to_012)?;
        Ok(ListResult012 {
            common_prefixes: result.common_prefixes.iter().map(path_to_012).collect(),
            objects: result.objects.into_iter().map(meta_from_013).collect(),
        })
    }

    async fn copy(&self, from: &Path012, to: &Path012) -> Result012<()> {
        let from013 = path_from_012(from);
        let to013 = path_from_012(to);
        self.inner
            .copy(&from013, &to013)
            .await
            .map_err(error_013_to_012)
    }

    async fn copy_if_not_exists(&self, from: &Path012, to: &Path012) -> Result012<()> {
        let from013 = path_from_012(from);
        let to013 = path_from_012(to);
        self.inner
            .copy_if_not_exists(&from013, &to013)
            .await
            .map_err(error_013_to_012)
    }
}
