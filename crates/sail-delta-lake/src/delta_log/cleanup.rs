use std::collections::BTreeSet;
use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore};
use uuid::Uuid;

use super::{
    parse_checkpoint_version_from_location, parse_checksum_version_from_location,
    parse_commit_version_from_location, resolve_version_timestamp,
};
use crate::kernel::snapshot::DeltaSnapshot;
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

    fn includes_commit(self, version_timestamp: i64, version: i64) -> bool {
        version <= self.latest_version && version_timestamp <= self.cutoff_timestamp
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
    fn observe_checkpoint(&mut self, version: i64, retention: LogRetentionWindow) {
        if retention.includes_checkpoint(version) {
            self.checkpoint_versions.insert(version);
        }
    }

    fn observe_commit(
        &mut self,
        version: i64,
        version_timestamp: i64,
        retention: LogRetentionWindow,
    ) {
        if retention.includes_commit(version_timestamp, version) {
            self.cutoff_commit_version = self.cutoff_commit_version.max(Some(version));
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
    table_state: &DeltaSnapshot,
    log_store: &dyn LogStore,
    cutoff_timestamp: i64,
    operation_id: Option<Uuid>,
) -> DeltaResult<usize> {
    let latest_version = table_state.version();
    let object_store = log_store.object_store(operation_id);
    let retention = LogRetentionWindow::new(latest_version, cutoff_timestamp);

    let Some(retention_checkpoint_version) =
        find_retention_checkpoint_version(table_state, log_store, object_store.clone(), retention)
            .await?
    else {
        return Ok(0);
    };

    delete_logs_before_checkpoint_version(object_store, retention_checkpoint_version).await
}

async fn find_retention_checkpoint_version(
    table_state: &DeltaSnapshot,
    log_store: &dyn LogStore,
    object_store: Arc<dyn ObjectStore>,
    retention: LogRetentionWindow,
) -> DeltaResult<Option<i64>> {
    let mut boundary = RetentionCleanupBoundary::default();
    let mut commit_entries = Vec::new();
    let log_path = delta_log_root_path();
    let mut log_entries = object_store.list(Some(&log_path));
    while let Some(meta) = log_entries.next().await {
        let Ok(meta) = meta else {
            continue;
        };
        let Some(file) = DeltaLogFile::from_meta(&meta) else {
            continue;
        };
        match file {
            DeltaLogFile::Commit(version) if version <= retention.latest_version => {
                commit_entries.push((version, meta));
            }
            DeltaLogFile::Checkpoint(version) => {
                boundary.observe_checkpoint(version, retention);
            }
            _ => {}
        }
    }
    commit_entries.sort_by_key(|(version, _)| *version);

    for (version, _) in commit_entries {
        let version_timestamp = resolve_version_timestamp(
            log_store,
            version,
            table_state.version_timestamp(version),
            table_state.protocol(),
            table_state.metadata(),
        )
        .await?;
        boundary.observe_commit(version, version_timestamp, retention);
    }

    Ok(boundary.retention_checkpoint_version())
}

async fn delete_logs_before_checkpoint_version(
    object_store: Arc<dyn ObjectStore>,
    retention_checkpoint_version: i64,
) -> DeltaResult<usize> {
    let log_path = delta_log_root_path();
    let locations = object_store
        .list(Some(&log_path))
        .filter_map(move |meta| async move {
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
    use object_store::{ObjectStore, ObjectStoreExt};
    use url::Url;

    use super::*;
    use crate::kernel::snapshot::DeltaSnapshot;
    use crate::spec::{
        checkpoint_path, checksum_path, commit_path, Action, CommitInfo, DataType, Metadata,
        Protocol, StructField, StructType, TableFeature, VersionChecksum,
    };
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

    fn test_metadata(
        configuration: impl IntoIterator<Item = (&'static str, &'static str)>,
    ) -> Metadata {
        Metadata::try_new(
            None,
            None,
            StructType::try_new([StructField::not_null("id", DataType::LONG)]).unwrap(),
            Vec::new(),
            0,
            configuration
                .into_iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        )
        .unwrap()
    }

    async fn put_commit(store: &Arc<dyn ObjectStore>, version: i64, actions: &[Action]) {
        let mut bytes = Vec::new();
        for (index, action) in actions.iter().enumerate() {
            if index > 0 {
                bytes.push(b'\n');
            }
            serde_json::to_writer(&mut bytes, action).unwrap();
        }
        store
            .put(&commit_path(version), bytes.into())
            .await
            .unwrap();
    }

    async fn put_checksum(
        store: &Arc<dyn ObjectStore>,
        version: i64,
        protocol: &Protocol,
        metadata: &Metadata,
        in_commit_timestamp_opt: Option<i64>,
    ) {
        let checksum = VersionChecksum {
            txn_id: None,
            table_size_bytes: 0,
            num_files: 0,
            num_metadata: 1,
            num_protocol: 1,
            in_commit_timestamp_opt,
            set_transactions: None,
            domain_metadata: None,
            metadata: metadata.clone(),
            protocol: protocol.clone(),
            file_size_histogram: None,
            all_files: None,
        };
        store
            .put(
                &checksum_path(version),
                serde_json::to_vec(&checksum).unwrap().into(),
            )
            .await
            .unwrap();
    }

    async fn load_snapshot(log_store: &LogStoreRef, version: i64) -> Arc<DeltaSnapshot> {
        Arc::new(
            DeltaSnapshot::try_new(log_store.as_ref(), Default::default(), Some(version), None)
                .await
                .unwrap(),
        )
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
        let protocol = Protocol::new(1, 2, None, None);
        let metadata = test_metadata([]);
        put_commit(
            &store,
            0,
            &[
                Action::CommitInfo(CommitInfo::default()),
                Action::Protocol(protocol.clone()),
                Action::Metadata(metadata.clone()),
            ],
        )
        .await;
        put_commit(&store, 1, &[Action::CommitInfo(CommitInfo::default())]).await;
        put_commit(&store, 2, &[Action::CommitInfo(CommitInfo::default())]).await;
        put_commit(&store, 3, &[Action::CommitInfo(CommitInfo::default())]).await;

        let log_store = test_log_store(store.clone());
        let snapshot = load_snapshot(&log_store, 3).await;

        put_checksum(&store, 1, &protocol, &metadata, None).await;
        put_log_file(&store, checkpoint_path(1)).await;
        put_log_file(&store, checkpoint_path(2)).await;

        let deleted =
            cleanup_expired_delta_log_files(snapshot.as_ref(), log_store.as_ref(), i64::MAX, None)
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
        let protocol = Protocol::new(1, 2, None, None);
        let metadata = test_metadata([]);
        put_commit(
            &store,
            0,
            &[
                Action::CommitInfo(CommitInfo::default()),
                Action::Protocol(protocol),
                Action::Metadata(metadata),
            ],
        )
        .await;
        put_commit(&store, 1, &[Action::CommitInfo(CommitInfo::default())]).await;

        let log_store = test_log_store(store.clone());
        let snapshot = load_snapshot(&log_store, 1).await;

        put_log_file(&store, checkpoint_path(2)).await;
        put_commit(&store, 2, &[Action::CommitInfo(CommitInfo::default())]).await;

        let deleted =
            cleanup_expired_delta_log_files(snapshot.as_ref(), log_store.as_ref(), i64::MAX, None)
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

    #[tokio::test]
    async fn cleanup_expired_delta_log_files_uses_ict_cutoff_instead_of_object_mtime() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let protocol = Protocol::new(1, 7, None, Some(vec![TableFeature::InCommitTimestamp]));
        let metadata = test_metadata([("delta.enableInCommitTimestamps", "true")]);
        put_commit(
            &store,
            0,
            &[
                Action::CommitInfo(CommitInfo {
                    in_commit_timestamp: Some(100),
                    ..Default::default()
                }),
                Action::Protocol(protocol.clone()),
                Action::Metadata(metadata.clone()),
            ],
        )
        .await;
        put_commit(
            &store,
            1,
            &[Action::CommitInfo(CommitInfo {
                in_commit_timestamp: Some(200),
                ..Default::default()
            })],
        )
        .await;
        put_commit(
            &store,
            2,
            &[Action::CommitInfo(CommitInfo {
                in_commit_timestamp: Some(300),
                ..Default::default()
            })],
        )
        .await;
        put_commit(
            &store,
            3,
            &[Action::CommitInfo(CommitInfo {
                in_commit_timestamp: Some(400),
                ..Default::default()
            })],
        )
        .await;

        let log_store = test_log_store(store.clone());
        let snapshot = load_snapshot(&log_store, 3).await;

        put_checksum(&store, 1, &protocol, &metadata, Some(200)).await;
        put_log_file(&store, checkpoint_path(1)).await;
        put_log_file(&store, checkpoint_path(2)).await;

        let deleted =
            cleanup_expired_delta_log_files(snapshot.as_ref(), log_store.as_ref(), 350, None)
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
}
