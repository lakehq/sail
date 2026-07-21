use std::sync::Arc;

use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion_common::{DataFusionError, Result};
use futures::StreamExt;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, PutPayload};

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

    pub fn child(&self, name: &str) -> Path {
        self.prefix.clone().join(name)
    }

    pub async fn put_bytes(&self, location: &Path, bytes: Vec<u8>) -> Result<ObjectMeta> {
        self.store
            .put(location, PutPayload::from(bytes))
            .await
            .map_err(|e| DataFusionError::ObjectStore(Box::new(e)))?;
        self.store
            .head(location)
            .await
            .map_err(|e| DataFusionError::ObjectStore(Box::new(e)))
    }

    pub async fn delete_prefix(&self) -> Result<()> {
        delete_object_store_prefix_objects(self.store.as_ref(), &self.prefix).await
    }
}

pub async fn delete_object_store_prefix_objects(
    store: &dyn ObjectStore,
    prefix: &Path,
) -> Result<()> {
    let locations = store.list(Some(prefix)).map(|result| {
        result
            .map(|object| object.location)
            .map_err(|source| object_store::Error::Generic {
                store: "object store listing",
                source: Box::new(source),
            })
    });
    let mut deleted = store.delete_stream(Box::pin(locations));
    while let Some(result) = deleted.next().await {
        match result {
            Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(DataFusionError::ObjectStore(Box::new(error))),
        }
    }
    Ok(())
}

pub fn resolve_object_store_path(
    runtime_env: &RuntimeEnv,
    path: &str,
) -> Result<ResolvedObjectStorePath> {
    let directory = format!("{}/", path.trim_end_matches('/'));
    let url = ListingTableUrl::parse(&directory)?;
    if !url.get_url().username().is_empty() || url.get_url().password().is_some() {
        return Err(DataFusionError::Plan(
            "object store URL cannot contain user information".to_string(),
        ));
    }
    let object_store_url = url.object_store();
    let store = runtime_env.object_store(&object_store_url)?;
    Ok(ResolvedObjectStorePath {
        object_store_url,
        prefix: url.prefix().clone(),
        store,
    })
}

pub async fn delete_object_store_prefix(runtime_env: &RuntimeEnv, path: &str) -> Result<()> {
    resolve_object_store_path(runtime_env, path)?
        .delete_prefix()
        .await
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use datafusion::execution::runtime_env::RuntimeEnv;
    use object_store::memory::InMemory;
    use url::Url;

    use super::*;

    #[tokio::test]
    async fn resolved_path_puts_and_deletes_prefix() -> Result<()> {
        let runtime_env = RuntimeEnv::default();
        runtime_env.register_object_store(
            &Url::parse("memory:///").map_err(|e| DataFusionError::External(Box::new(e)))?,
            Arc::new(InMemory::new()),
        );
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .as_nanos();
        let path = format!("memory:///{suffix}/checkpoint");
        let resolved = resolve_object_store_path(&runtime_env, &path)?;
        let first = resolved.child("part-00000.arrow");
        let nested = resolved.child("nested/part-00001.arrow");
        let outside = Path::from(format!("{suffix}/outside.arrow"));

        resolved.put_bytes(&first, b"first".to_vec()).await?;
        resolved.put_bytes(&nested, b"nested".to_vec()).await?;
        resolved.put_bytes(&outside, b"outside".to_vec()).await?;

        resolved.delete_prefix().await?;

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

    #[test]
    fn resolved_path_rejects_url_user_information() -> Result<()> {
        let runtime_env = RuntimeEnv::default();
        let error = match resolve_object_store_path(
            &runtime_env,
            "s3://checkpoint-user:checkpoint-secret@bucket/checkpoint",
        ) {
            Ok(_) => {
                return Err(DataFusionError::Plan(
                    "object store credentials were accepted in a URL".to_string(),
                ));
            }
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("object store URL cannot contain user information")
        );
        Ok(())
    }
}
