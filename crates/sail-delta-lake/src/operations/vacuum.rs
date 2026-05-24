use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path as OsPath;
use object_store::ObjectStore;

use crate::spec::{DeltaError, DeltaResult, DeletionVectorDescriptor, Remove, StorageType};
use crate::table::DeltaSnapshot;

/// Minimum retention hours enforced by the safety check (7 days).
pub const MIN_RETENTION_HOURS: u64 = 168;

/// Compute the cutoff timestamp in milliseconds for the given retention window.
///
/// Returns the Unix timestamp in milliseconds before which files may be deleted.
pub fn retention_cutoff_ms(retention_hours: u64) -> i64 {
    let retention_ms = (retention_hours as i64) * 3_600_000;
    Utc::now().timestamp_millis() - retention_ms
}

/// Compute the effective retention window in hours, defaulting to the table's
/// `delta.deletedFileRetentionDuration` if no explicit hours were supplied.
pub fn resolve_retention_hours(
    explicit_hours: Option<u64>,
    table_property_duration: Duration,
) -> u64 {
    explicit_hours.unwrap_or(table_property_duration.as_secs() / 3600)
}

/// Collect all data-file paths that must be preserved (not deleted) by VACUUM.
///
/// All paths are expressed as `object_store::path::Path` strings relative to
/// the table root, matching exactly what `object_store.list()` returns for the
/// prefixed store.  This avoids any URL round-trip and coordinate mismatch.
///
/// Includes:
/// - Active `Add` file paths.
/// - Active `Add` deletion-vector paths (UUID-relative only; absolute DVs are
///   external and cannot appear in the table listing).
/// - Unexpired `Remove` (tombstone) paths whose `deletion_timestamp` is
///   newer than `retention_cutoff_ms` — files that may still be read during
///   concurrent time-travel reads within the retention window.
/// - Unexpired `Remove` deletion-vector paths.
pub async fn collect_referenced_paths(
    snapshot: &DeltaSnapshot,
    log_store: &dyn crate::storage::LogStore,
    retention_cutoff_ms: i64,
) -> DeltaResult<HashSet<String>> {
    let mut referenced = HashSet::new();

    for add in snapshot.adds() {
        referenced.insert(normalize_data_path(&add.path));
        if let Some(dv) = &add.deletion_vector {
            if let Some(rel) = dv_relative_path(dv)? {
                referenced.insert(rel);
            }
        }
    }

    let tombstones: Vec<Remove> = snapshot.all_tombstones(log_store).await?.collect();

    for remove in &tombstones {
        // Keep tombstones within the retention window so concurrent readers can
        // still access the data during time-travel.
        if remove.deletion_timestamp.unwrap_or(0) > retention_cutoff_ms {
            referenced.insert(normalize_data_path(&remove.path));
            if let Some(dv) = &remove.deletion_vector {
                if let Some(rel) = dv_relative_path(dv)? {
                    referenced.insert(rel);
                }
            }
        }
    }

    Ok(referenced)
}

/// Identify data files in the table directory that are eligible for deletion.
///
/// A file is a candidate if:
/// 1. It is NOT in the `referenced_paths` set.
/// 2. Its `last_modified` timestamp (ms) is ≤ `retention_cutoff_ms`.
/// 3. It does NOT reside under `_delta_log/`.
///
/// Returns `(object_store_path, size_bytes)` pairs.
pub async fn find_vacuum_candidates(
    object_store: &Arc<dyn ObjectStore>,
    referenced_paths: &HashSet<String>,
    retention_cutoff_ms: i64,
) -> DeltaResult<Vec<(OsPath, i64)>> {
    let mut candidates = Vec::new();
    let mut listing = object_store.list(None);
    while let Some(meta) = listing.next().await {
        let meta = meta.map_err(DeltaError::from)?;
        let path_str = meta.location.as_ref();
        if is_delta_log_path(path_str) {
            continue;
        }
        let last_modified_ms = meta.last_modified.timestamp_millis();
        if last_modified_ms > retention_cutoff_ms {
            continue;
        }
        if referenced_paths.contains(path_str) {
            continue;
        }
        candidates.push((meta.location, meta.size as i64));
    }
    Ok(candidates)
}

/// Delete a list of candidate files from the object store.
///
/// Returns the number of files successfully deleted.
pub async fn delete_files(
    object_store: Arc<dyn ObjectStore>,
    candidates: Vec<OsPath>,
) -> DeltaResult<usize> {
    let locations =
        futures::stream::iter(candidates.into_iter().map(Ok::<_, object_store::Error>)).boxed();
    let deleted = object_store
        .delete_stream(locations)
        .try_collect::<Vec<_>>()
        .await
        .map_err(DeltaError::from)?;
    Ok(deleted.len())
}

/// Normalize a Delta `Add`/`Remove` path to the same form that
/// `object_store.list()` produces for the prefixed store.
///
/// `Add.path` is deserialized as a decoded (un-percent-encoded) string per
/// `serde_path::deserialize`. `object_store::Path` also stores paths decoded.
/// Both sides are therefore in the same coordinate system; we only need to
/// strip any accidental leading slash via `OsPath::from`.
fn normalize_data_path(path: &str) -> String {
    OsPath::from(path).to_string()
}

/// Derive the object-store-relative path of a UUID-based deletion vector file.
///
/// Returns `None` for inline or absolute-path DVs (which either have no file
/// or live outside the table prefix and cannot appear in the table listing).
fn dv_relative_path(dv: &DeletionVectorDescriptor) -> DeltaResult<Option<String>> {
    match dv.storage_type {
        StorageType::UuidRelativePath => {
            use crate::deletion_vector::resolve_dv_absolute_path;
            // Reuse the existing resolver with a dummy base URL to extract the
            // UUID-derived relative portion, then strip the base prefix.
            // The relative path is <prefix>/deletion_vector_<uuid>.bin.
            let base = url::Url::parse("memory:///")
                .map_err(|e| DeltaError::generic(format!("internal URL parse error: {e}")))?;
            if let Some(abs) = resolve_dv_absolute_path(&base, dv)? {
                // abs.path() => "/<prefix>/deletion_vector_<uuid>.bin"
                let rel = abs.path().trim_start_matches('/').to_string();
                Ok(Some(rel))
            } else {
                Ok(None)
            }
        }
        // Absolute-path DVs are external; not in the table listing.
        // Inline DVs have no file.
        StorageType::AbsolutePath | StorageType::Inline => Ok(None),
    }
}

fn is_delta_log_path(path: &str) -> bool {
    path.starts_with("_delta_log/") || path == "_delta_log"
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use chrono::Utc;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt};
    use url::Url;

    use super::*;
    use crate::spec::{
        Action, Add, CommitInfo, DataType, Metadata, Protocol, Remove, StructField, StructType,
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

    async fn put_commit(store: &Arc<dyn ObjectStore>, version: i64, actions: &[Action]) {
        let path = crate::spec::commit_path(version);
        let mut bytes = Vec::new();
        for (i, action) in actions.iter().enumerate() {
            if i > 0 {
                bytes.push(b'\n');
            }
            serde_json::to_writer(&mut bytes, action).unwrap();
        }
        store.put(&path, bytes.into()).await.unwrap();
    }

    async fn put_data_file(store: &Arc<dyn ObjectStore>, path: &str) {
        store
            .put(&Path::from(path), b"fake_parquet_data".to_vec().into())
            .await
            .unwrap();
    }

    fn make_metadata() -> Metadata {
        Metadata::try_new(
            None,
            None,
            StructType::try_new([StructField::not_null("id", DataType::LONG)]).unwrap(),
            Vec::new(),
            0,
            Default::default(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn vacuum_dry_run_identifies_orphan_files() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let protocol = Protocol::new(1, 2, None, None);
        let metadata = make_metadata();

        put_commit(
            &store,
            0,
            &[
                Action::CommitInfo(CommitInfo::default()),
                Action::Protocol(protocol.clone()),
                Action::Metadata(metadata.clone()),
                Action::Add(Add {
                    path: "part-00000.parquet".to_string(),
                    size: 100,
                    ..Default::default()
                }),
            ],
        )
        .await;

        put_data_file(&store, "part-00000.parquet").await;
        put_data_file(&store, "orphan-00001.parquet").await;

        let log_store = test_log_store(store.clone());
        let snapshot = Arc::new(
            crate::table::DeltaSnapshot::try_new(
                log_store.as_ref(),
                Default::default(),
                None,
                None,
            )
            .await
            .unwrap(),
        );

        let cutoff = i64::MAX;
        let referenced =
            collect_referenced_paths(snapshot.as_ref(), log_store.as_ref(), cutoff)
                .await
                .unwrap();
        let raw_store = log_store.object_store(None);
        let candidates = find_vacuum_candidates(&raw_store, &referenced, cutoff)
            .await
            .unwrap();

        let paths: Vec<_> = candidates
            .iter()
            .map(|(p, _)| p.as_ref().to_string())
            .collect();
        assert!(paths.iter().any(|p| p.contains("orphan-00001")));
        assert!(!paths.iter().any(|p| p.contains("part-00000")));
    }

    #[tokio::test]
    async fn vacuum_respects_retention_cutoff() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let protocol = Protocol::new(1, 2, None, None);
        let metadata = make_metadata();

        let now_ms = Utc::now().timestamp_millis();

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
        put_commit(
            &store,
            1,
            &[
                Action::CommitInfo(CommitInfo::default()),
                Action::Remove(Remove {
                    path: "old-part.parquet".to_string(),
                    deletion_timestamp: Some(now_ms - 200_000),
                    ..Default::default()
                }),
            ],
        )
        .await;

        put_data_file(&store, "old-part.parquet").await;

        let log_store = test_log_store(store.clone());
        let snapshot = Arc::new(
            crate::table::DeltaSnapshot::try_new(
                log_store.as_ref(),
                Default::default(),
                None,
                None,
            )
            .await
            .unwrap(),
        );

        // cutoff is 100 ms ago — the file was removed 200s ago, so it's beyond the cutoff
        let cutoff = now_ms - 100;
        let referenced =
            collect_referenced_paths(snapshot.as_ref(), log_store.as_ref(), cutoff)
                .await
                .unwrap();
        let raw_store = log_store.object_store(None);
        let candidates = find_vacuum_candidates(&raw_store, &referenced, cutoff)
            .await
            .unwrap();

        // old-part was removed long before the cutoff, so it should be a candidate
        let paths: Vec<_> = candidates
            .iter()
            .map(|(p, _)| p.as_ref().to_string())
            .collect();
        assert!(paths.iter().any(|p| p.contains("old-part")));
    }

    #[tokio::test]
    async fn vacuum_keeps_active_file() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let protocol = Protocol::new(1, 2, None, None);
        let metadata = make_metadata();

        put_commit(
            &store,
            0,
            &[
                Action::CommitInfo(CommitInfo::default()),
                Action::Protocol(protocol.clone()),
                Action::Metadata(metadata.clone()),
                Action::Add(Add {
                    path: "active.parquet".to_string(),
                    size: 100,
                    ..Default::default()
                }),
            ],
        )
        .await;

        put_data_file(&store, "active.parquet").await;

        let log_store = test_log_store(store.clone());
        let snapshot = Arc::new(
            crate::table::DeltaSnapshot::try_new(
                log_store.as_ref(),
                Default::default(),
                None,
                None,
            )
            .await
            .unwrap(),
        );

        let cutoff = i64::MAX;
        let referenced =
            collect_referenced_paths(snapshot.as_ref(), log_store.as_ref(), cutoff)
                .await
                .unwrap();
        let raw_store = log_store.object_store(None);
        let candidates = find_vacuum_candidates(&raw_store, &referenced, cutoff)
            .await
            .unwrap();

        let paths: Vec<_> = candidates
            .iter()
            .map(|(p, _)| p.as_ref().to_string())
            .collect();
        assert!(!paths.iter().any(|p| p.contains("active")));
    }

    #[tokio::test]
    async fn vacuum_keeps_tombstone_within_retention_window() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let protocol = Protocol::new(1, 2, None, None);
        let metadata = make_metadata();
        let now_ms = Utc::now().timestamp_millis();

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
        put_commit(
            &store,
            1,
            &[
                Action::CommitInfo(CommitInfo::default()),
                // Removed 1 second ago — within the retention window.
                Action::Remove(Remove {
                    path: "recent-remove.parquet".to_string(),
                    deletion_timestamp: Some(now_ms - 1_000),
                    ..Default::default()
                }),
            ],
        )
        .await;

        put_data_file(&store, "recent-remove.parquet").await;

        let log_store = test_log_store(store.clone());
        let snapshot = Arc::new(
            crate::table::DeltaSnapshot::try_new(
                log_store.as_ref(),
                Default::default(),
                None,
                None,
            )
            .await
            .unwrap(),
        );

        // Cutoff 7 days ago — the file removed 1s ago is within the window.
        let cutoff = now_ms - 7 * 24 * 3_600_000;
        let referenced =
            collect_referenced_paths(snapshot.as_ref(), log_store.as_ref(), cutoff)
                .await
                .unwrap();
        let raw_store = log_store.object_store(None);
        let candidates = find_vacuum_candidates(&raw_store, &referenced, cutoff)
            .await
            .unwrap();

        let paths: Vec<_> = candidates
            .iter()
            .map(|(p, _)| p.as_ref().to_string())
            .collect();
        // File is still within the retention window and must NOT be a candidate.
        assert!(!paths.iter().any(|p| p.contains("recent-remove")));
    }

    #[tokio::test]
    async fn vacuum_excludes_delta_log_files() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let protocol = Protocol::new(1, 2, None, None);
        let metadata = make_metadata();

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

        put_data_file(&store, "orphan.parquet").await;
        // Simulate some file under a partition directory that looks like delta log.
        put_data_file(&store, "year=2024/data.parquet").await;

        let log_store = test_log_store(store.clone());
        let snapshot = Arc::new(
            crate::table::DeltaSnapshot::try_new(
                log_store.as_ref(),
                Default::default(),
                None,
                None,
            )
            .await
            .unwrap(),
        );

        let cutoff = i64::MAX;
        let referenced =
            collect_referenced_paths(snapshot.as_ref(), log_store.as_ref(), cutoff)
                .await
                .unwrap();
        let raw_store = log_store.object_store(None);
        let candidates = find_vacuum_candidates(&raw_store, &referenced, cutoff)
            .await
            .unwrap();

        let paths: Vec<_> = candidates
            .iter()
            .map(|(p, _)| p.as_ref().to_string())
            .collect();
        // _delta_log/* must never appear.
        assert!(!paths.iter().any(|p| p.contains("_delta_log")));
        // Partition data without an Add action should be a candidate.
        assert!(paths.iter().any(|p| p.contains("year=2024")));
        assert!(paths.iter().any(|p| p.contains("orphan")));
    }

    #[tokio::test]
    async fn vacuum_uuid_dv_path_is_preserved() {
        use crate::deletion_vector::new_uuid_dv_path;
        use crate::spec::{DeletionVectorDescriptor, StorageType};

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let protocol = Protocol::new(1, 2, None, None);
        let metadata = make_metadata();

        let (path_or_inline_dv, dv_relative_path, _uuid) = new_uuid_dv_path().unwrap();
        let dv = DeletionVectorDescriptor {
            storage_type: StorageType::UuidRelativePath,
            path_or_inline_dv,
            offset: Some(1),
            size_in_bytes: 10,
            cardinality: 1,
        };

        put_commit(
            &store,
            0,
            &[
                Action::CommitInfo(CommitInfo::default()),
                Action::Protocol(protocol),
                Action::Metadata(metadata),
                Action::Add(Add {
                    path: "part-with-dv.parquet".to_string(),
                    size: 100,
                    deletion_vector: Some(dv),
                    ..Default::default()
                }),
            ],
        )
        .await;

        put_data_file(&store, "part-with-dv.parquet").await;
        put_data_file(&store, &dv_relative_path).await;

        let log_store = test_log_store(store.clone());
        let snapshot = Arc::new(
            crate::table::DeltaSnapshot::try_new(
                log_store.as_ref(),
                Default::default(),
                None,
                None,
            )
            .await
            .unwrap(),
        );

        let cutoff = i64::MAX;
        let referenced =
            collect_referenced_paths(snapshot.as_ref(), log_store.as_ref(), cutoff)
                .await
                .unwrap();
        let raw_store = log_store.object_store(None);
        let candidates = find_vacuum_candidates(&raw_store, &referenced, cutoff)
            .await
            .unwrap();

        let paths: Vec<_> = candidates
            .iter()
            .map(|(p, _)| p.as_ref().to_string())
            .collect();
        // The DV file must NOT be a candidate for deletion.
        assert!(
            !paths.iter().any(|p| p.contains("deletion_vector")),
            "DV file should not be a vacuum candidate; candidates: {paths:?}"
        );
        // The data file must also NOT be a candidate.
        assert!(!paths.iter().any(|p| p.contains("part-with-dv")));
    }
}
