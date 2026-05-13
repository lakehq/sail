use datafusion::execution::runtime_env::RuntimeEnv;
use futures::{StreamExt, TryStreamExt};
use log::warn;
use object_store::ObjectStoreScheme;

pub async fn cleanup_checkpoint_storage(runtime_env: &RuntimeEnv, storage_uri: &str) {
    let Ok(url) = url::Url::parse(storage_uri) else {
        warn!("failed to parse checkpoint location for cleanup: {storage_uri}");
        return;
    };
    let Ok((_, checkpoint_path)) = ObjectStoreScheme::parse(&url) else {
        warn!("failed to parse checkpoint object-store path for cleanup: {storage_uri}");
        return;
    };
    let Ok(object_store) = runtime_env.object_store_registry.get_store(&url) else {
        warn!("failed to resolve checkpoint object store for cleanup: {storage_uri}");
        return;
    };
    let locations = object_store
        .list(Some(&checkpoint_path))
        .map_ok(|meta| meta.location)
        .boxed();
    if let Err(e) = object_store
        .delete_stream(locations)
        .try_collect::<Vec<_>>()
        .await
    {
        warn!("failed to remove checkpoint location {storage_uri}: {e}");
    }
    cleanup_file_checkpoint_directory(&url, storage_uri).await;
}

async fn cleanup_file_checkpoint_directory(url: &url::Url, storage_uri: &str) {
    if url.scheme() != "file" {
        return;
    }
    let Ok(path) = url.to_file_path() else {
        warn!("failed to convert checkpoint file URL to path for cleanup: {storage_uri}");
        return;
    };
    if let Err(e) = tokio::fs::remove_dir_all(&path).await {
        if e.kind() != std::io::ErrorKind::NotFound {
            warn!("failed to remove checkpoint directory {storage_uri}: {e}");
        }
    }
}
