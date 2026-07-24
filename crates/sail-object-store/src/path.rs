use std::sync::Arc;

use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion_common::{DataFusionError, Result};
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::path::Path;

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
    if url.get_glob().is_some()
        || url.get_url().query().is_some()
        || url.get_url().fragment().is_some()
    {
        return Err(DataFusionError::Plan(
            "object store path cannot contain a query, fragment, or glob".to_string(),
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

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use datafusion::execution::runtime_env::RuntimeEnv;
    use object_store::memory::InMemory;
    use object_store::{ObjectStoreExt, PutPayload};
    use url::Url;

    use super::*;

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

    #[test]
    fn resolved_path_rejects_url_user_information() {
        let runtime_env = RuntimeEnv::default();
        let error = resolve_object_store_path(
            &runtime_env,
            "s3://checkpoint-user:checkpoint-secret@bucket/checkpoint",
        )
        .err()
        .expect("checkpoint URL with user information must fail");
        assert!(
            error
                .to_string()
                .contains("object store URL cannot contain user information")
        );
    }
}
