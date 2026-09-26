use std::sync::Arc;

use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion_common::{DataFusionError, Result};
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::path::Path;
use url::Url;

#[derive(Clone)]
pub struct ResolvedObjectStorePath {
    object_store_url: ObjectStoreUrl,
    prefix: Path,
    store: Arc<dyn ObjectStore>,
}

impl ResolvedObjectStorePath {
    pub fn object_store_url(&self) -> &ObjectStoreUrl {
        &self.object_store_url
    }

    pub fn prefix(&self) -> &Path {
        &self.prefix
    }

    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// Return a fully-qualified URL for a store-relative object location.
    ///
    /// `ObjectStore::head` and `ObjectStore::list` return relative keys.
    /// This converts those keys back into locations that can be resolved through
    /// the same RuntimeEnv registry.
    pub fn qualify(&self, location: &Path) -> Result<String> {
        qualify_object_store_path(&self.object_store_url, location)
    }
}

/// Encode a store-relative key without changing its identity.
pub fn qualify_object_store_path(base: &ObjectStoreUrl, location: &Path) -> Result<String> {
    let mut uri = Url::parse(base.as_str())
        .map_err(|e| DataFusionError::Internal(format!("invalid object store URL: {e}")))?;
    uri.path_segments_mut()
        .map_err(|()| DataFusionError::Internal("object store URL cannot be a base".into()))?
        .clear()
        .extend(location.parts().map(|part| part.as_ref().to_string()));
    Ok(uri.to_string())
}

pub async fn delete_object_store_prefix_objects(
    store: &dyn ObjectStore,
    prefix: &Path,
) -> Result<()> {
    let locations = store
        .list(Some(prefix))
        .map(|result| result.map(|object| object.location));
    let mut deleted = store.delete_stream(Box::pin(locations));
    while let Some(result) = deleted.next().await {
        match result {
            Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(DataFusionError::ObjectStore(Box::new(error))),
        }
    }
    Ok(())
}

pub fn resolve_object_store_location(
    runtime_env: &RuntimeEnv,
    path: &str,
) -> Result<ResolvedObjectStorePath> {
    // Exact paths: never interpret filesystem metacharacters as listing globs.
    let parsed = if std::path::Path::new(path).is_absolute() {
        Url::from_file_path(path).map_err(|()| DataFusionError::Plan("invalid file path".into()))?
    } else {
        match Url::parse(path) {
            Ok(url) => url,
            Err(url::ParseError::RelativeUrlWithoutBase) => {
                Url::from_file_path(std::env::current_dir()?.join(path))
                    .map_err(|()| DataFusionError::Plan("invalid file path".into()))?
            }
            Err(error) => return Err(DataFusionError::External(Box::new(error))),
        }
    };
    let url = ListingTableUrl::try_new(parsed, None)?;
    let object_store_url = url.object_store();
    let store = runtime_env.object_store(&object_store_url)?;
    Ok(ResolvedObjectStorePath {
        object_store_url,
        prefix: url.prefix().clone(),
        store,
    })
}

pub fn resolve_object_store_path(
    runtime_env: &RuntimeEnv,
    path: &str,
) -> Result<ResolvedObjectStorePath> {
    let directory = format!("{}/", path.trim_end_matches('/'));
    let url = ListingTableUrl::parse(&directory)?;
    let object_store_url = url.object_store();
    let store = runtime_env.object_store(&object_store_url)?;
    Ok(ResolvedObjectStorePath {
        object_store_url,
        prefix: url.prefix().clone(),
        store,
    })
}

#[cfg(test)]
mod tests {
    use datafusion::execution::runtime_env::RuntimeEnv;
    use object_store::memory::InMemory;
    use object_store::{ObjectStoreExt, PutPayload};
    use url::Url;

    use super::*;

    #[test]
    fn resolve_location_preserves_exact_object_path() -> Result<()> {
        let runtime_env = RuntimeEnv::default();
        runtime_env.register_object_store(
            &Url::parse("memory:///")
                .map_err(|error| DataFusionError::External(Box::new(error)))?,
            Arc::new(InMemory::new()),
        );

        let resolved = resolve_object_store_location(&runtime_env, "memory:///data/file.vortex")?;
        assert_eq!(resolved.prefix(), &Path::from("data/file.vortex"));
        assert_eq!(
            resolved.qualify(&Path::from("other/file.vortex"))?,
            "memory:///other/file.vortex"
        );
        Ok(())
    }

    #[test]
    fn literal_filesystem_locations_match_file_urls() -> Result<()> {
        let runtime = RuntimeEnv::default();
        let path = std::env::temp_dir().join("a*[b]?#%20.txt");
        let plain = resolve_object_store_location(&runtime, &path.to_string_lossy())?;
        let url = Url::from_file_path(&path)
            .map_err(|()| DataFusionError::Plan("invalid test path".into()))?;
        assert_eq!(
            plain.prefix(),
            resolve_object_store_location(&runtime, url.as_str())?.prefix()
        );
        Ok(())
    }

    #[tokio::test]
    async fn metadata_locations_do_not_target_colliding_keys() -> Result<()> {
        let runtime = RuntimeEnv::default();
        let store = Arc::new(InMemory::new());
        runtime.register_object_store(
            &Url::parse("memory:///").map_err(|e| DataFusionError::External(Box::new(e)))?,
            store.clone(),
        );
        let base = resolve_object_store_location(&runtime, "memory:///data")?;
        let sentinel = Path::from("data/a");
        store
            .put(&sentinel, PutPayload::from_static(b"keep"))
            .await?;
        for key in [
            "data/a#b",
            "data/a?b",
            "data/a%20b",
            "data/a*b",
            "data/a b",
            "data/日本語",
        ] {
            let key = Path::parse(key)?;
            store
                .put(&key, PutPayload::from_static(b"original"))
                .await?;
            let url = base.qualify(&key)?;
            let resolved = resolve_object_store_location(&runtime, &url)?;
            assert_eq!(resolved.prefix(), &key);
            assert_eq!(
                resolved
                    .store()
                    .get(resolved.prefix())
                    .await?
                    .bytes()
                    .await?
                    .as_ref(),
                b"original"
            );
            resolved.store().delete(resolved.prefix()).await?;
            assert!(store.head(&sentinel).await.is_ok());
        }
        Ok(())
    }

    #[tokio::test]
    async fn resolved_path_puts_and_deletes_only_its_prefix() -> Result<()> {
        let runtime_env = RuntimeEnv::default();
        runtime_env.register_object_store(
            &Url::parse("memory:///")
                .map_err(|error| DataFusionError::External(Box::new(error)))?,
            Arc::new(InMemory::new()),
        );
        let resolved = resolve_object_store_path(&runtime_env, "memory:///checkpoint")?;
        let first = resolved.prefix().clone().join("part-00000.parquet");
        let nested = resolved
            .prefix()
            .clone()
            .join("nested")
            .join("part-00001.parquet");
        let outside = Path::from("outside.parquet");

        for (location, bytes) in [
            (&first, b"first".as_slice()),
            (&nested, b"nested".as_slice()),
            (&outside, b"outside".as_slice()),
        ] {
            resolved
                .store()
                .put(location, PutPayload::from(bytes.to_vec()))
                .await
                .map_err(|error| DataFusionError::ObjectStore(Box::new(error)))?;
        }
        delete_object_store_prefix_objects(resolved.store().as_ref(), resolved.prefix()).await?;

        assert!(matches!(
            resolved.store().head(&first).await,
            Err(object_store::Error::NotFound { .. })
        ));
        assert!(matches!(
            resolved.store().head(&nested).await,
            Err(object_store::Error::NotFound { .. })
        ));
        assert!(resolved.store().head(&outside).await.is_ok());
        Ok(())
    }
}
