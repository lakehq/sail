use std::collections::BTreeSet;
use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore};
use uuid::Uuid;

use super::{
    parse_checkpoint_version_from_location, parse_checksum_version_from_location,
    parse_commit_version_from_location,
};
use crate::spec::{delta_log_root_path, DeltaResult};
use crate::storage::LogStore;

#[derive(Debug, Clone, Copy)]
struct LogRetentionWindow {
    latest_version: i64,
    cutoff_timestamp: i64,
}

impl LogRetentionWindow {
    fn new(latest_version: i64, cutoff_timestamp: i64) -> Self {
        Self {
            latest_version,
            cutoff_timestamp,
        }
    }

    fn includes_commit(self, meta: &ObjectMeta, version: i64) -> bool {
        version <= self.latest_version
            && meta.last_modified.timestamp_millis() <= self.cutoff_timestamp
    }

    fn includes_checkpoint(self, version: i64) -> bool {
        version <= self.latest_version
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeltaLogFile {
    Commit(i64),
    Checksum(i64),
    Checkpoint(i64),
}

impl DeltaLogFile {
    fn from_meta(meta: &ObjectMeta) -> Option<Self> {
        parse_commit_version_from_location(&meta.location)
            .map(Self::Commit)
            .or_else(|| parse_checksum_version_from_location(&meta.location).map(Self::Checksum))
            .or_else(|| {
                parse_checkpoint_version_from_location(&meta.location).map(Self::Checkpoint)
            })
    }

    fn version(self) -> i64 {
        match self {
            Self::Commit(version) | Self::Checksum(version) | Self::Checkpoint(version) => version,
        }
    }

    fn is_checkpoint(self) -> bool {
        matches!(self, Self::Checkpoint(_))
    }

    fn is_retained_by(self, retention: LogRetentionWindow, meta: &ObjectMeta) -> bool {
        match self {
            Self::Commit(version) => retention.includes_commit(meta, version),
            Self::Checksum(_) => false,
            Self::Checkpoint(version) => retention.includes_checkpoint(version),
        }
    }

    fn expires_before(self, retention_checkpoint_version: i64) -> bool {
        self.version() < retention_checkpoint_version
    }
}

#[derive(Debug, Default)]
struct RetentionCleanupBoundary {
    cutoff_commit_version: Option<i64>,
    checkpoint_versions: BTreeSet<i64>,
}

impl RetentionCleanupBoundary {
    fn observe(&mut self, meta: &ObjectMeta, retention: LogRetentionWindow) {
        let Some(file) = DeltaLogFile::from_meta(meta) else {
            return;
        };

        if !file.is_retained_by(retention, meta) {
            return;
        }

        if file.is_checkpoint() {
            self.checkpoint_versions.insert(file.version());
        } else {
            self.cutoff_commit_version = self.cutoff_commit_version.max(Some(file.version()));
        }
    }

    fn retention_checkpoint_version(&self) -> Option<i64> {
        let cutoff_commit_version = self.cutoff_commit_version?;
        self.checkpoint_versions
            .range(..=cutoff_commit_version)
            .next_back()
            .copied()
    }
}

pub(crate) async fn cleanup_expired_delta_log_files(
    latest_version: i64,
    log_store: &dyn LogStore,
    cutoff_timestamp: i64,
    operation_id: Option<Uuid>,
) -> DeltaResult<usize> {
    let object_store = log_store.object_store(operation_id);
    let retention = LogRetentionWindow::new(latest_version, cutoff_timestamp);

    let Some(retention_checkpoint_version) =
        find_retention_checkpoint_version(object_store.clone(), retention).await
    else {
        return Ok(0);
    };

    delete_logs_before_checkpoint_version(object_store, retention_checkpoint_version).await
}

async fn find_retention_checkpoint_version(
    object_store: Arc<dyn ObjectStore>,
    retention: LogRetentionWindow,
) -> Option<i64> {
    let mut boundary = RetentionCleanupBoundary::default();
    let log_path = delta_log_root_path();
    let mut log_entries = object_store.list(Some(&log_path));
    while let Some(meta) = log_entries.next().await {
        if let Ok(meta) = meta {
            boundary.observe(&meta, retention);
        }
    }

    boundary.retention_checkpoint_version()
}

async fn delete_logs_before_checkpoint_version(
    object_store: Arc<dyn ObjectStore>,
    retention_checkpoint_version: i64,
) -> DeltaResult<usize> {
    let log_path = delta_log_root_path();
    let locations = object_store
        .list(Some(&log_path))
        .filter_map(|meta| async move {
            let meta = meta.ok()?;
            expired_log_location(&meta, retention_checkpoint_version).map(Ok)
        })
        .boxed();

    Ok(object_store
        .delete_stream(locations)
        .try_collect::<Vec<_>>()
        .await?
        .len())
}

fn expired_log_location(
    meta: &ObjectMeta,
    retention_checkpoint_version: i64,
) -> Option<object_store::path::Path> {
    DeltaLogFile::from_meta(meta)
        .filter(|file| file.expires_before(retention_checkpoint_version))
        .map(|_| meta.location.clone())
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use futures::TryStreamExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use url::Url;

    use super::*;
    use crate::spec::{checkpoint_path, checksum_path, commit_path};
    use crate::storage::{default_logstore, LogStoreRef, StorageConfig};

    fn test_log_store(store: Arc<dyn ObjectStore>) -> LogStoreRef {
        default_logstore(
            store.clone(),
            store,
            &Url::parse("memory:///").unwrap(),
            &StorageConfig,
        )
    }

    async fn put_log_file(store: &Arc<dyn ObjectStore>, path: Path) {
        store.put(&path, b"{}".to_vec().into()).await.unwrap();
    }

    async fn list_log_file_paths(store: &Arc<dyn ObjectStore>) -> Vec<String> {
        let mut paths = store
            .list(Some(&delta_log_root_path()))
            .map_ok(|meta| meta.location.as_ref().to_string())
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        paths.sort();
        paths
    }

    #[tokio::test]
    async fn cleanup_expired_delta_log_files_deletes_entries_before_retention_checkpoint() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put_log_file(&store, commit_path(0)).await;
        put_log_file(&store, commit_path(1)).await;
        put_log_file(&store, checksum_path(1)).await;
        put_log_file(&store, checkpoint_path(1)).await;
        put_log_file(&store, commit_path(2)).await;
        put_log_file(&store, checkpoint_path(2)).await;
        put_log_file(&store, commit_path(3)).await;

        let deleted = cleanup_expired_delta_log_files(
            3,
            test_log_store(store.clone()).as_ref(),
            i64::MAX,
            None,
        )
        .await
        .unwrap();

        assert_eq!(deleted, 4);
        assert_eq!(
            list_log_file_paths(&store).await,
            vec![
                "_delta_log/00000000000000000002.checkpoint.parquet".to_string(),
                "_delta_log/00000000000000000002.json".to_string(),
                "_delta_log/00000000000000000003.json".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn cleanup_expired_delta_log_files_skips_when_no_checkpoint_is_eligible() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put_log_file(&store, commit_path(0)).await;
        put_log_file(&store, commit_path(1)).await;
        put_log_file(&store, checkpoint_path(2)).await;
        put_log_file(&store, commit_path(2)).await;

        let deleted = cleanup_expired_delta_log_files(
            1,
            test_log_store(store.clone()).as_ref(),
            i64::MAX,
            None,
        )
        .await
        .unwrap();

        assert_eq!(deleted, 0);
        assert_eq!(
            list_log_file_paths(&store).await,
            vec![
                "_delta_log/00000000000000000000.json".to_string(),
                "_delta_log/00000000000000000001.json".to_string(),
                "_delta_log/00000000000000000002.checkpoint.parquet".to_string(),
                "_delta_log/00000000000000000002.json".to_string(),
            ]
        );
    }
}
