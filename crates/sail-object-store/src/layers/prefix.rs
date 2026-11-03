use std::fmt;
use std::ops::Range;
use std::sync::{Arc, PoisonError, RwLock};

use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
};
use tonic::codegen::Bytes;

type Routes = Arc<RwLock<Vec<(Path, Arc<dyn ObjectStore>)>>>;

/// An object store that sends each request to the store registered for the
/// longest prefix of its path, and every other request to a fallback store.
///
/// A catalog vends storage credentials per table location, so one bucket can
/// need a different credential for each table it holds.
#[derive(Debug)]
pub struct PrefixObjectStore {
    fallback: Arc<dyn ObjectStore>,
    routes: Routes,
}

impl PrefixObjectStore {
    pub fn new(fallback: Arc<dyn ObjectStore>) -> Self {
        Self {
            fallback,
            routes: Arc::new(RwLock::new(vec![])),
        }
    }

    /// Routes requests under `prefix` to `store`, replacing any store
    /// registered for the same prefix.
    pub fn register(&self, prefix: Path, store: Arc<dyn ObjectStore>) {
        let mut routes = self.routes.write().unwrap_or_else(PoisonError::into_inner);
        routes.retain(|(existing, _)| existing != &prefix);
        routes.push((prefix, store));
        routes.sort_by_key(|(prefix, _)| std::cmp::Reverse(prefix.as_ref().len()));
    }

    fn route(&self, location: &Path) -> Arc<dyn ObjectStore> {
        route(&self.routes, &self.fallback, location)
    }

    fn route_prefix(&self, prefix: Option<&Path>) -> Arc<dyn ObjectStore> {
        match prefix {
            Some(prefix) => self.route(prefix),
            None => self.fallback.clone(),
        }
    }

    fn route_pair(&self, from: &Path, to: &Path) -> Result<Arc<dyn ObjectStore>> {
        let store = self.route(from);
        if Arc::ptr_eq(&store, &self.route(to)) {
            Ok(store)
        } else {
            Err(object_store::Error::NotSupported {
                source: format!("{from} and {to} are under different storage credentials").into(),
            })
        }
    }
}

fn route(
    routes: &Routes,
    fallback: &Arc<dyn ObjectStore>,
    location: &Path,
) -> Arc<dyn ObjectStore> {
    routes
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .find(|(prefix, _)| location.prefix_matches(prefix))
        .map(|(_, store)| store.clone())
        .unwrap_or_else(|| fallback.clone())
}

impl fmt::Display for PrefixObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PrefixObjectStore({})", self.fallback)
    }
}

#[async_trait::async_trait]
#[warn(clippy::missing_trait_methods)]
impl ObjectStore for PrefixObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.route(location).put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.route(location)
            .put_multipart_opts(location, opts)
            .await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.route(location).get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.route(location).get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let routes = self.routes.clone();
        let fallback = self.fallback.clone();
        locations
            .and_then(move |location| {
                let store = route(&routes, &fallback, &location);
                let single = stream::iter([Ok(location.clone())]).boxed();
                async move {
                    let deleted = store.delete_stream(single).next().await;
                    deleted.unwrap_or(Ok(location))
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.route_prefix(prefix).list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.route_prefix(prefix).list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.route_prefix(prefix).list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.route_pair(from, to)?
            .copy_opts(from, to, options)
            .await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.route_pair(from, to)?
            .rename_opts(from, to, options)
            .await
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use object_store::memory::InMemory;
    use object_store::{ObjectStoreExt, PutPayload};

    use super::*;

    #[tokio::test]
    async fn routes_each_path_to_the_longest_registered_prefix() {
        let fallback = Arc::new(InMemory::new());
        let table = Arc::new(InMemory::new());
        let nested = Arc::new(InMemory::new());
        let store = PrefixObjectStore::new(fallback.clone());
        store.register(Path::from("wh/t1"), table.clone());
        store.register(Path::from("wh/t1/data"), nested.clone());

        for path in [
            "wh/t1/metadata/v1.json",
            "wh/t1/data/a.parquet",
            "wh/t10/x",
            "other",
        ] {
            store
                .put(&Path::from(path), PutPayload::from_static(b"x"))
                .await
                .unwrap();
        }

        let held = |s: Arc<InMemory>, path: &'static str| async move {
            s.head(&Path::from(path)).await.is_ok()
        };
        assert!(held(table.clone(), "wh/t1/metadata/v1.json").await);
        assert!(held(nested.clone(), "wh/t1/data/a.parquet").await);
        assert!(!held(table.clone(), "wh/t1/data/a.parquet").await);
        // `wh/t10` is not under `wh/t1`.
        assert!(held(fallback.clone(), "wh/t10/x").await);
        assert!(held(fallback.clone(), "other").await);

        let listed = store
            .list(Some(&Path::from("wh/t1/metadata")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);

        let deleted = store
            .delete_stream(
                stream::iter([
                    Ok(Path::from("wh/t1/metadata/v1.json")),
                    Ok(Path::from("other")),
                ])
                .boxed(),
            )
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(deleted.len(), 2);
        assert!(!held(table, "wh/t1/metadata/v1.json").await);
        assert!(!held(fallback, "other").await);
    }

    #[tokio::test]
    async fn a_new_registration_replaces_the_store_for_its_prefix() {
        let store = PrefixObjectStore::new(Arc::new(InMemory::new()));
        let first = Arc::new(InMemory::new());
        let second = Arc::new(InMemory::new());
        store.register(Path::from("wh/t1"), first.clone());
        store.register(Path::from("wh/t1"), second.clone());
        store
            .put(&Path::from("wh/t1/a"), PutPayload::from_static(b"x"))
            .await
            .unwrap();
        assert!(first.head(&Path::from("wh/t1/a")).await.is_err());
        assert!(second.head(&Path::from("wh/t1/a")).await.is_ok());
    }

    #[tokio::test]
    async fn copy_across_credentials_is_refused() {
        let store = PrefixObjectStore::new(Arc::new(InMemory::new()));
        store.register(Path::from("wh/t1"), Arc::new(InMemory::new()));
        store
            .put(&Path::from("wh/t1/a"), PutPayload::from_static(b"x"))
            .await
            .unwrap();
        let error = store
            .copy(&Path::from("wh/t1/a"), &Path::from("wh/t2/a"))
            .await
            .unwrap_err();
        assert!(matches!(error, object_store::Error::NotSupported { .. }));
    }
}
