use std::collections::BTreeMap;
use std::sync::Arc;

use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectMeta, ObjectStore, ObjectStoreExt};

use crate::spec::{
    CheckpointFileKind, DELTA_LOG_DIR, DeltaError, DeltaResult, LastCheckpointHint,
    delta_log_prefix_path, delta_log_root_path, is_uuid_checkpoint_filename, last_checkpoint_path,
    parse_checkpoint_filename, parse_checkpoint_version, parse_checksum_version,
    parse_commit_version, parse_compacted_json_versions,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum CheckpointIdentity {
    Classic,
    MultiPart { parts: u64 },
    Uuid { filename: String },
}

impl CheckpointIdentity {
    fn part_count(&self) -> u64 {
        match self {
            Self::Classic | Self::Uuid { .. } => 1,
            Self::MultiPart { parts } => *parts,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CheckpointFileSet {
    version: i64,
    identity: CheckpointIdentity,
    files: Vec<ObjectMeta>,
}

impl CheckpointFileSet {
    pub(crate) fn version(&self) -> i64 {
        self.version
    }

    #[cfg(test)]
    pub(crate) fn files(&self) -> &[ObjectMeta] {
        &self.files
    }

    pub(crate) fn into_files(self) -> Vec<ObjectMeta> {
        self.files
    }

    pub(crate) fn is_multi_part(&self) -> bool {
        matches!(self.identity, CheckpointIdentity::MultiPart { .. })
    }

    pub(crate) fn contains_location(&self, location: &Path) -> bool {
        self.files.iter().any(|file| file.location == *location)
    }
}

/// Groups checkpoint files into complete checkpoint instances. The returned sets are ordered by
/// version and then by checkpoint preference at one version: classic, multi-part (with larger part
/// counts last), and UUID-named V2.
pub(crate) fn complete_checkpoint_file_sets(
    checkpoint_files: impl IntoIterator<Item = ObjectMeta>,
) -> Vec<CheckpointFileSet> {
    let mut grouped_files: BTreeMap<(i64, CheckpointIdentity), BTreeMap<u64, ObjectMeta>> =
        BTreeMap::new();

    for file in checkpoint_files {
        if file.size == 0 {
            continue;
        }
        let Some(filename) = file.location.filename() else {
            continue;
        };
        let Some(parsed) = parse_checkpoint_filename(filename) else {
            continue;
        };
        let (identity, part) = match parsed.kind {
            CheckpointFileKind::Classic => (CheckpointIdentity::Classic, 1),
            CheckpointFileKind::MultiPart { part, parts } => {
                (CheckpointIdentity::MultiPart { parts }, part)
            }
            CheckpointFileKind::Uuid { .. } => (
                CheckpointIdentity::Uuid {
                    filename: filename.to_string(),
                },
                1,
            ),
        };
        grouped_files
            .entry((parsed.version, identity))
            .or_default()
            .insert(part, file);
    }

    grouped_files
        .into_iter()
        .filter_map(|((version, identity), parts)| {
            let expected_parts = identity.part_count();
            let actual_parts = u64::try_from(parts.len()).ok()?;
            if actual_parts != expected_parts || !parts.keys().copied().eq(1..=expected_parts) {
                return None;
            }
            Some(CheckpointFileSet {
                version,
                identity,
                files: parts.into_values().collect(),
            })
        })
        .collect()
}

fn delta_log_top_level_filename(location: &Path) -> Option<&str> {
    let log_root = delta_log_root_path();
    let relative = location
        .as_ref()
        .strip_prefix(log_root.as_ref())?
        .strip_prefix('/')?;
    (!relative.is_empty() && !relative.contains('/')).then_some(relative)
}

pub(crate) fn parse_checksum_version_from_location(location: &Path) -> Option<i64> {
    delta_log_top_level_filename(location).and_then(parse_checksum_version)
}

pub(crate) fn parse_commit_version_from_location(location: &Path) -> Option<i64> {
    delta_log_top_level_filename(location).and_then(parse_commit_version)
}

pub(crate) fn parse_checkpoint_version_from_location(location: &Path) -> Option<i64> {
    delta_log_top_level_filename(location).and_then(parse_checkpoint_version)
}

pub(crate) fn parse_compacted_json_versions_from_location(location: &Path) -> Option<(i64, i64)> {
    delta_log_top_level_filename(location).and_then(parse_compacted_json_versions)
}

pub(crate) async fn read_last_checkpoint_hint_from_store(
    store: Arc<dyn ObjectStore>,
) -> Option<LastCheckpointHint> {
    let bytes = store
        .get(&last_checkpoint_path())
        .await
        .ok()?
        .bytes()
        .await
        .ok()?;
    serde_json::from_slice(&bytes).ok()
}

pub(crate) async fn read_last_checkpoint_version_from_store(
    store: Arc<dyn ObjectStore>,
) -> Option<i64> {
    read_last_checkpoint_hint_from_store(store)
        .await
        .map(|hint| hint.version)
}

pub(crate) fn v2_checkpoint_path_from_hint(hint: &LastCheckpointHint) -> DeltaResult<Option<Path>> {
    let Some(v2_checkpoint) = &hint.v2_checkpoint else {
        return Ok(None);
    };
    let path = v2_checkpoint.path.trim_start_matches('/');
    let filename = path
        .strip_prefix(&format!("{DELTA_LOG_DIR}/"))
        .unwrap_or(path);
    if filename.is_empty() || filename.contains('/') || !is_uuid_checkpoint_filename(filename) {
        return Err(DeltaError::generic(format!(
            "_last_checkpoint contains an invalid V2 checkpoint path: {}",
            v2_checkpoint.path
        )));
    }
    let location = Path::from(format!("{DELTA_LOG_DIR}/{filename}"));
    if parse_checkpoint_version_from_location(&location) != Some(hint.version) {
        return Err(DeltaError::generic(format!(
            "_last_checkpoint V2 path {} does not match checkpoint version {}",
            v2_checkpoint.path, hint.version
        )));
    }
    Ok(Some(location))
}

pub(crate) async fn list_delta_log_entries_from(
    store: Arc<dyn ObjectStore>,
    offset_version: i64,
) -> DeltaResult<Vec<ObjectMeta>> {
    // `delta_log_prefix_path(version)` is a prefix, not a concrete filename, so
    // files for `version` still compare greater than the offset.
    let log_path = delta_log_root_path();
    let offset = delta_log_prefix_path(offset_version);
    let entries = match store
        .list_with_offset(Some(&log_path), &offset)
        .try_collect::<Vec<_>>()
        .await
    {
        Ok(entries) => entries,
        Err(ObjectStoreError::NotSupported { .. } | ObjectStoreError::NotImplemented { .. }) => {
            match store.list_with_delimiter(Some(&log_path)).await {
                Ok(result) => result.objects,
                Err(
                    ObjectStoreError::NotSupported { .. } | ObjectStoreError::NotImplemented { .. },
                ) => store.list(Some(&log_path)).try_collect::<Vec<_>>().await?,
                Err(err) => return Err(err.into()),
            }
        }
        Err(err) => return Err(err.into()),
    };
    Ok(entries
        .into_iter()
        .filter(|meta| {
            meta.location.as_ref() > offset.as_ref()
                && delta_log_top_level_filename(&meta.location).is_some()
        })
        .collect())
}

pub(crate) async fn latest_version_from_listing(
    store: Arc<dyn ObjectStore>,
) -> DeltaResult<Option<i64>> {
    let offset_version = read_last_checkpoint_version_from_store(store.clone())
        .await
        .map(|v| v.saturating_sub(1))
        .unwrap_or(0);
    let entries = list_delta_log_entries_from(store.clone(), offset_version).await?;
    let latest_version = latest_version_from_entries(entries);
    if latest_version.is_none() && offset_version > 0 {
        let entries = list_delta_log_entries_from(store, 0).await?;
        return Ok(latest_version_from_entries(entries));
    }
    Ok(latest_version)
}

fn latest_version_from_entries(entries: Vec<ObjectMeta>) -> Option<i64> {
    let mut max_version: Option<i64> = None;
    let mut checkpoint_files = Vec::new();
    for meta in entries {
        if parse_checkpoint_version_from_location(&meta.location).is_some() {
            checkpoint_files.push(meta);
            continue;
        }
        if let Some(version) = parse_commit_version_from_location(&meta.location).or_else(|| {
            parse_compacted_json_versions_from_location(&meta.location).map(|(_, end)| end)
        }) {
            max_version = Some(max_version.map_or(version, |curr| curr.max(version)));
        }
    }
    if let Some(checkpoint) = complete_checkpoint_file_sets(checkpoint_files).last() {
        let version = checkpoint.version();
        max_version = Some(max_version.map_or(version, |current| current.max(version)));
    }
    max_version
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;

    #[tokio::test]
    async fn latest_version_from_listing_works_without_last_checkpoint_hint() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let sidecar = Path::from(
            "_delta_log/_sidecars/00000000000000000042.checkpoint.0000000001.0000000001.uuid.parquet",
        );
        store
            .put(&sidecar, b"sidecar".to_vec().into())
            .await
            .unwrap();
        store
            .put(
                &Path::from("_delta_log/00000000000000000007.json"),
                b"{}".to_vec().into(),
            )
            .await
            .unwrap();

        assert_eq!(parse_checkpoint_version_from_location(&sidecar), None);
        assert_eq!(
            latest_version_from_listing(store.clone()).await.unwrap(),
            Some(7)
        );
        let entries = list_delta_log_entries_from(store, 0).await.unwrap();
        assert_eq!(entries.len(), 1);
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

    #[tokio::test]
    async fn latest_version_from_listing_uses_complete_multi_part_checkpoint() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let hint = serde_json::to_vec(&LastCheckpointHint {
            version: 20,
            parts: Some(3),
            ..Default::default()
        })
        .unwrap();
        store
            .put(&Path::from("_delta_log/_last_checkpoint"), hint.into())
            .await
            .unwrap();
        for part in 1..=3 {
            store
                .put(
                    &Path::from(format!(
                        "_delta_log/00000000000000000020.checkpoint.{part:010}.0000000003.parquet"
                    )),
                    format!("part {part}").into_bytes().into(),
                )
                .await
                .unwrap();
        }

        assert_eq!(latest_version_from_listing(store).await.unwrap(), Some(20));
    }

    #[tokio::test]
    async fn latest_version_from_listing_ignores_incomplete_multi_part_checkpoint() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let hint = serde_json::to_vec(&LastCheckpointHint {
            version: 20,
            parts: Some(3),
            ..Default::default()
        })
        .unwrap();
        store
            .put(&Path::from("_delta_log/_last_checkpoint"), hint.into())
            .await
            .unwrap();
        for part in [1, 3] {
            store
                .put(
                    &Path::from(format!(
                        "_delta_log/00000000000000000020.checkpoint.{part:010}.0000000003.parquet"
                    )),
                    format!("part {part}").into_bytes().into(),
                )
                .await
                .unwrap();
        }

        assert_eq!(latest_version_from_listing(store).await.unwrap(), None);
    }

    #[tokio::test]
    async fn latest_version_from_listing_falls_back_past_incomplete_multi_part_checkpoint() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let hint = serde_json::to_vec(&LastCheckpointHint {
            version: 20,
            parts: Some(3),
            ..Default::default()
        })
        .unwrap();
        store
            .put(&Path::from("_delta_log/_last_checkpoint"), hint.into())
            .await
            .unwrap();
        store
            .put(
                &Path::from("_delta_log/00000000000000000010.checkpoint.parquet"),
                b"checkpoint".to_vec().into(),
            )
            .await
            .unwrap();
        for part in [1, 3] {
            store
                .put(
                    &Path::from(format!(
                        "_delta_log/00000000000000000020.checkpoint.{part:010}.0000000003.parquet"
                    )),
                    format!("part {part}").into_bytes().into(),
                )
                .await
                .unwrap();
        }

        assert_eq!(latest_version_from_listing(store).await.unwrap(), Some(10));
    }

    #[tokio::test]
    async fn list_delta_log_entries_from_keeps_empty_results_empty() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(
                &Path::from("_delta_log/00000000000000000020.checkpoint.parquet"),
                b"parquet".to_vec().into(),
            )
            .await
            .unwrap();

        let entries = list_delta_log_entries_from(store, 21).await.unwrap();
        assert!(entries.is_empty());
    }
}
