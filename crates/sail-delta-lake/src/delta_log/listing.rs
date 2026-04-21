use std::sync::Arc;

use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};

use crate::spec::{
    delta_log_root_path, last_checkpoint_path, parse_checkpoint_version, parse_checksum_version,
    parse_commit_version, parse_compacted_json_versions, DeltaResult, LastCheckpointHint,
};

pub(crate) fn parse_delta_log_entry_version(meta: &ObjectMeta) -> Option<i64> {
    let filename = meta.location.as_ref().rsplit('/').next()?;
    parse_commit_version(filename)
        .or_else(|| parse_checkpoint_version(filename))
        .or_else(|| parse_compacted_json_versions(filename).map(|(_, end)| end))
}

pub(crate) fn parse_checksum_version_from_location(location: &Path) -> Option<i64> {
    location
        .as_ref()
        .rsplit('/')
        .next()
        .and_then(parse_checksum_version)
}

pub(crate) fn parse_commit_version_from_location(location: &Path) -> Option<i64> {
    location
        .as_ref()
        .rsplit('/')
        .next()
        .and_then(parse_commit_version)
}

pub(crate) fn parse_checkpoint_version_from_location(location: &Path) -> Option<i64> {
    location
        .as_ref()
        .rsplit('/')
        .next()
        .and_then(parse_checkpoint_version)
}

pub(crate) fn parse_compacted_json_versions_from_location(location: &Path) -> Option<(i64, i64)> {
    location
        .as_ref()
        .rsplit('/')
        .next()
        .and_then(parse_compacted_json_versions)
}

pub(crate) async fn read_last_checkpoint_version_from_store(
    store: Arc<dyn ObjectStore>,
) -> Option<i64> {
    let bytes = store
        .get(&last_checkpoint_path())
        .await
        .ok()?
        .bytes()
        .await
        .ok()?;
    let hint: LastCheckpointHint = serde_json::from_slice(&bytes).ok()?;
    Some(hint.version)
}

pub(crate) async fn list_delta_log_entries_from(
    store: Arc<dyn ObjectStore>,
    _offset_version: i64,
) -> DeltaResult<Vec<ObjectMeta>> {
    // Always list the full _delta_log directory.
    //
    // We previously called `list_with_offset(&log_path, &offset)` where
    // `offset = _delta_log/00000000000000000<version>` (no extension).
    // Azure/OneLake treats this bare offset as a prefix boundary and
    // excludes real files like `<version>.checkpoint.parquet`, breaking
    // tables with a `_last_checkpoint` hint.
    // See: https://github.com/delta-io/delta-kernel-rs/issues/2433
    let log_path = delta_log_root_path();
    Ok(store.list(Some(&log_path)).try_collect::<Vec<_>>().await?)
}

pub(crate) async fn latest_version_from_listing(
    store: Arc<dyn ObjectStore>,
) -> DeltaResult<Option<i64>> {
    let entries = list_delta_log_entries_from(store, 0).await?;

    let mut max_version: Option<i64> = None;
    for meta in entries {
        if let Some(version) = parse_delta_log_entry_version(&meta) {
            max_version = Some(max_version.map_or(version, |curr| curr.max(version)));
        }
    }
    Ok(max_version)
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;

    #[tokio::test]
    async fn latest_version_from_listing_works_without_last_checkpoint_hint() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(
                &Path::from("_delta_log/00000000000000000007.json"),
                b"{}".to_vec().into(),
            )
            .await
            .unwrap();

        assert_eq!(latest_version_from_listing(store).await.unwrap(), Some(7));
    }

    #[tokio::test]
    async fn latest_version_from_listing_uses_checkpoint_when_commits_are_pruned() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let hint = serde_json::to_vec(&LastCheckpointHint {
            version: 20,
            ..Default::default()
        })
        .unwrap();

        store
            .put(&Path::from("_delta_log/_last_checkpoint"), hint.into())
            .await
            .unwrap();
        store
            .put(
                &Path::from("_delta_log/00000000000000000020.checkpoint.parquet"),
                b"parquet".to_vec().into(),
            )
            .await
            .unwrap();

        assert_eq!(latest_version_from_listing(store).await.unwrap(), Some(20));
    }

    #[tokio::test]
    async fn latest_version_from_listing_finds_commits_newer_than_last_checkpoint() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let hint = serde_json::to_vec(&LastCheckpointHint {
            version: 20,
            ..Default::default()
        })
        .unwrap();

        store
            .put(&Path::from("_delta_log/_last_checkpoint"), hint.into())
            .await
            .unwrap();
        store
            .put(
                &Path::from("_delta_log/00000000000000000020.checkpoint.parquet"),
                b"parquet".to_vec().into(),
            )
            .await
            .unwrap();
        store
            .put(
                &Path::from("_delta_log/00000000000000000021.json"),
                b"{}".to_vec().into(),
            )
            .await
            .unwrap();

        assert_eq!(latest_version_from_listing(store).await.unwrap(), Some(21));
    }
}
