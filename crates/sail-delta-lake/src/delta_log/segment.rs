use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use log::debug;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};

use super::timestamps::version_uses_in_commit_timestamps;
use super::{list_delta_log_entries_from, read_last_checkpoint_version_from_store};
use crate::spec::{
    checksum_path, parse_checkpoint_version, parse_checksum_version, parse_commit_version,
    DeltaError, DeltaResult, DomainMetadata, Metadata, Protocol, Transaction, VersionChecksum,
};
use crate::storage::LogStore;

const CHECKSUM_LOOKBACK_WINDOW: i64 = 100;

#[derive(Debug, Clone)]
pub(crate) struct ReplayedTableHeader {
    pub version: i64,
    pub protocol: Protocol,
    pub metadata: Metadata,
    pub txns: Arc<HashMap<String, Transaction>>,
    pub domain_metadata: Arc<HashMap<String, DomainMetadata>>,
    pub commit_timestamps: Arc<BTreeMap<i64, i64>>,
}

#[derive(Debug)]
pub(crate) enum ResolvedLogSegment {
    ExactChecksum {
        header: ReplayedTableHeader,
    },
    Incremental {
        base: ReplayedTableHeader,
        checkpoint: Option<ObjectMeta>,
        commit_files: Vec<(i64, ObjectMeta)>,
        target_version: i64,
    },
    FullReplay {
        checkpoint: Option<ObjectMeta>,
        commit_files: Vec<(i64, ObjectMeta)>,
        target_version: i64,
    },
}

pub(crate) struct LogSegmentResolver<'a> {
    log_store: &'a dyn LogStore,
    target_version: i64,
    replay_hint: Option<&'a ReplayedTableHeader>,
}

impl<'a> LogSegmentResolver<'a> {
    pub(crate) fn new(
        log_store: &'a dyn LogStore,
        target_version: i64,
        replay_hint: Option<&'a ReplayedTableHeader>,
    ) -> Self {
        Self {
            log_store,
            target_version,
            replay_hint,
        }
    }

    pub(crate) async fn resolve_for_header(&self) -> DeltaResult<ResolvedLogSegment> {
        let version = self.target_version;

        let store = self.log_store.object_store(None);
        if let Some(header) = try_read_checksum_header(store.clone(), version).await {
            debug!("crc-header: exact checksum hit target_version={version}");
            return Ok(ResolvedLogSegment::ExactChecksum { header });
        }

        let lower_bound = {
            let lookback = version.saturating_sub(CHECKSUM_LOOKBACK_WINDOW);
            let hint_floor = self
                .replay_hint
                .map(|h| h.version.saturating_add(1))
                .unwrap_or(0);
            lookback.max(hint_floor)
        };

        let last_cp_hint_version = read_last_checkpoint_version_from_store(store.clone())
            .await
            .map(|v| v.min(version).saturating_sub(1))
            .unwrap_or(0);
        let list_offset = lower_bound.min(last_cp_hint_version);

        let (checksum_candidates, checkpoint_candidate, all_commits) =
            list_log_files(store.clone(), list_offset, version).await?;

        let mut older_crc_base: Option<ReplayedTableHeader> = None;
        for (crc_version, meta) in &checksum_candidates {
            if *crc_version < lower_bound || *crc_version >= version {
                continue;
            }
            let bytes = match store.get(&meta.location).await {
                Ok(r) => match r.bytes().await {
                    Ok(b) => b,
                    Err(err) => {
                        debug!(
                            "crc-header: failed to read older checksum at version {crc_version}: {err}"
                        );
                        continue;
                    }
                },
                Err(object_store::Error::NotFound { .. }) => continue,
                Err(err) => {
                    debug!(
                        "crc-header: failed to fetch older checksum at version {crc_version}: {err}"
                    );
                    continue;
                }
            };
            let checksum: VersionChecksum = match serde_json::from_slice(&bytes) {
                Ok(c) => c,
                Err(err) => {
                    debug!(
                        "crc-header: failed to deserialize older checksum at version {crc_version}: {err}"
                    );
                    continue;
                }
            };
            if let Some(header) = validate_and_build_header(*crc_version, checksum) {
                older_crc_base = Some(header);
                break;
            }
        }

        if let Some(ref base) = older_crc_base {
            debug!(
                "crc-header: older checksum hint hit target_version={version}, checksum_version={}",
                base.version
            );
        }

        let used_older_crc = older_crc_base.is_some();
        let base = match older_crc_base.or_else(|| self.replay_hint.cloned()) {
            Some(b) => {
                if !used_older_crc {
                    debug!(
                        "crc-header: reused snapshot hint target_version={version}, hint_version={}",
                        b.version
                    );
                }
                b
            }
            None => {
                debug!("crc-header: no usable checksum or replay hint target_version={version}");
                let start_version = match &checkpoint_candidate {
                    Some(cp_meta) => cp_meta_version(cp_meta)?.saturating_add(1),
                    None => 0,
                };
                let commit_files: Vec<(i64, ObjectMeta)> = all_commits
                    .into_iter()
                    .filter(|(v, _)| *v >= start_version && *v <= version)
                    .collect();
                return Ok(ResolvedLogSegment::FullReplay {
                    checkpoint: checkpoint_candidate,
                    commit_files,
                    target_version: version,
                });
            }
        };

        let use_checkpoint = checkpoint_candidate
            .as_ref()
            .map(cp_meta_version)
            .transpose()?
            .map(|v| v > base.version)
            .unwrap_or(false);

        let (checkpoint, commit_files, start_version) = if use_checkpoint {
            let cp_meta = checkpoint_candidate.ok_or_else(|| {
                DeltaError::generic("checkpoint_candidate was None after use_checkpoint check")
            })?;
            let cp_version = cp_meta_version(&cp_meta)?;
            let commits: Vec<(i64, ObjectMeta)> = all_commits
                .into_iter()
                .filter(|(v, _)| *v > cp_version && *v <= version)
                .collect();
            (Some(cp_meta), commits, cp_version.saturating_add(1))
        } else {
            let start = base.version.saturating_add(1);
            let commits: Vec<(i64, ObjectMeta)> = all_commits
                .into_iter()
                .filter(|(v, _)| *v >= start && *v <= version)
                .collect();
            (None, commits, start)
        };

        validate_commit_contiguity(&commit_files, start_version, version)?;

        Ok(ResolvedLogSegment::Incremental {
            base,
            checkpoint,
            commit_files,
            target_version: version,
        })
    }

    pub(crate) async fn resolve_for_full_state(&self) -> DeltaResult<ResolvedLogSegment> {
        let version = self.target_version;
        let store = self.log_store.object_store(None);

        let last_cp_hint_version = read_last_checkpoint_version_from_store(store.clone())
            .await
            .map(|v| v.min(version).saturating_sub(1))
            .unwrap_or(0);

        let (_, checkpoint, all_commits) =
            list_log_files(store, last_cp_hint_version, version).await?;

        let start_version = match &checkpoint {
            Some(cp_meta) => cp_meta_version(cp_meta)?.saturating_add(1),
            None => 0,
        };
        let commit_files: Vec<(i64, ObjectMeta)> = all_commits
            .into_iter()
            .filter(|(v, _)| *v >= start_version && *v <= version)
            .collect();
        validate_commit_contiguity(&commit_files, start_version, version)?;

        Ok(ResolvedLogSegment::FullReplay {
            checkpoint,
            commit_files,
            target_version: version,
        })
    }
}

fn cp_meta_version(meta: &ObjectMeta) -> DeltaResult<i64> {
    meta.location
        .as_ref()
        .rsplit('/')
        .next()
        .and_then(parse_checkpoint_version)
        .ok_or_else(|| {
            DeltaError::generic(format!(
                "checkpoint path does not contain a parseable version: {}",
                meta.location
            ))
        })
}

async fn try_read_checksum_header(
    store: Arc<dyn ObjectStore>,
    version: i64,
) -> Option<ReplayedTableHeader> {
    let path = checksum_path(version);
    let bytes = store.get(&path).await.ok()?.bytes().await.ok()?;
    let checksum: VersionChecksum = match serde_json::from_slice(&bytes) {
        Ok(c) => c,
        Err(err) => {
            debug!("crc-header: failed to deserialize checksum at version {version}: {err}");
            return None;
        }
    };
    validate_and_build_header(version, checksum)
}

fn validate_and_build_header(
    version: i64,
    checksum: VersionChecksum,
) -> Option<ReplayedTableHeader> {
    if checksum.num_metadata != 1 || checksum.num_protocol != 1 {
        debug!(
            "crc-header: invalid checksum at version {version}: \
             num_metadata={}, num_protocol={}",
            checksum.num_metadata, checksum.num_protocol
        );
        return None;
    }
    let txns = checksum
        .set_transactions
        .unwrap_or_default()
        .into_iter()
        .map(|txn| (txn.app_id.clone(), txn))
        .collect::<HashMap<_, _>>();
    let domain_metadata = checksum
        .domain_metadata
        .unwrap_or_default()
        .into_iter()
        .map(|domain| (domain.domain.clone(), domain))
        .collect::<HashMap<_, _>>();
    let commit_timestamps =
        if version_uses_in_commit_timestamps(version, &checksum.protocol, &checksum.metadata) {
            checksum
                .in_commit_timestamp_opt
                .into_iter()
                .map(|timestamp| (version, timestamp))
                .collect()
        } else {
            BTreeMap::new()
        };
    Some(ReplayedTableHeader {
        version,
        protocol: checksum.protocol,
        metadata: checksum.metadata,
        txns: Arc::new(txns),
        domain_metadata: Arc::new(domain_metadata),
        commit_timestamps: Arc::new(commit_timestamps),
    })
}

pub(crate) async fn list_log_files(
    store: Arc<dyn ObjectStore>,
    list_offset_version: i64,
    max_version: i64,
) -> DeltaResult<(
    Vec<(i64, ObjectMeta)>,
    Option<ObjectMeta>,
    Vec<(i64, ObjectMeta)>,
)> {
    let entries = list_delta_log_entries_from(store, list_offset_version).await?;

    let mut checkpoint_candidates: Vec<(i64, ObjectMeta)> = Vec::new();
    let mut commit_candidates: Vec<(i64, ObjectMeta)> = Vec::new();
    let mut checksum_candidates: Vec<(i64, ObjectMeta)> = Vec::new();

    for meta in entries {
        let filename = match meta.location.as_ref().rsplit('/').next() {
            Some(f) => f,
            None => continue,
        };
        if let Some(v) = parse_checkpoint_version(filename) {
            if v <= max_version {
                checkpoint_candidates.push((v, meta));
            }
            continue;
        }
        if let Some(v) = parse_commit_version(filename) {
            if v <= max_version {
                commit_candidates.push((v, meta));
            }
            continue;
        }
        if let Some(v) = parse_checksum_version(filename) {
            if v <= max_version {
                checksum_candidates.push((v, meta));
            }
        }
    }

    // TODO(v2-checkpoints): This groups checkpoint candidates by version only. It does not yet
    // distinguish classic vs. V2 checkpoint layouts, nor does it validate multipart completeness;
    // readers rely on later replay-time handling of checkpointMetadata/sidecar fields instead.
    let latest_checkpoint_version = checkpoint_candidates.iter().map(|(v, _)| *v).max();
    let checkpoint = latest_checkpoint_version.map(|latest_v| {
        let mut files: Vec<ObjectMeta> = checkpoint_candidates
            .into_iter()
            .filter_map(|(v, m)| (v == latest_v).then_some(m))
            .collect();
        files.sort_by(|a, b| a.location.as_ref().cmp(b.location.as_ref()));
        files.remove(0)
    });

    commit_candidates.sort_by(|(av, _), (bv, _)| av.cmp(bv));
    checksum_candidates.sort_by(|(av, _), (bv, _)| bv.cmp(av));

    Ok((checksum_candidates, checkpoint, commit_candidates))
}

fn validate_commit_contiguity(
    commit_files: &[(i64, ObjectMeta)],
    start_version: i64,
    end_version: i64,
) -> DeltaResult<()> {
    if start_version > end_version {
        return Ok(());
    }
    let mut expected = start_version;
    for (v, _) in commit_files {
        if *v < start_version || *v > end_version {
            continue;
        }
        if *v != expected {
            return Err(DeltaError::generic(format!(
                "Missing commit file: expected version {expected}, found {v}"
            )));
        }
        expected = expected.saturating_add(1);
    }
    if expected.saturating_sub(1) != end_version {
        return Err(DeltaError::generic(format!(
            "Missing commit file: expected final version {end_version}, replay reached {}",
            expected.saturating_sub(1)
        )));
    }
    Ok(())
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::spec::StructType;

    fn make_test_checksum(
        num_metadata: i64,
        num_protocol: i64,
        in_commit_timestamp_opt: Option<i64>,
    ) -> VersionChecksum {
        use crate::spec::{Metadata, Protocol, StructType};

        let protocol = Protocol::new(1, 2, None, None);
        let metadata = Metadata::try_new(
            None,
            None,
            StructType::try_new([]).unwrap(),
            vec![],
            0,
            Default::default(),
        )
        .unwrap();

        VersionChecksum {
            txn_id: None,
            table_size_bytes: 0,
            num_files: 0,
            num_metadata,
            num_protocol,
            in_commit_timestamp_opt,
            set_transactions: None,
            domain_metadata: None,
            metadata,
            protocol,
            file_size_histogram: None,
            all_files: None,
        }
    }

    #[test]
    fn valid_checksum_builds_header() {
        let checksum = make_test_checksum(1, 1, None);
        let header = validate_and_build_header(42, checksum);
        assert!(header.is_some());
        let h = header.unwrap();
        assert_eq!(h.version, 42);
        assert!(h.txns.is_empty());
        assert!(h.commit_timestamps.is_empty());
    }

    #[test]
    fn checksum_header_keeps_in_commit_timestamp() {
        let mut checksum = make_test_checksum(1, 1, Some(123));
        checksum.protocol = Protocol::new(
            1,
            7,
            None,
            Some(vec![crate::spec::TableFeature::InCommitTimestamp]),
        );
        checksum.metadata = Metadata::try_new(
            None,
            None,
            StructType::try_new([]).unwrap(),
            vec![],
            0,
            HashMap::from([(
                "delta.enableInCommitTimestamps".to_string(),
                "true".to_string(),
            )]),
        )
        .unwrap();
        let header = validate_and_build_header(42, checksum);
        assert!(header.is_some());
        let header = header.unwrap();
        assert_eq!(header.commit_timestamps.get(&42), Some(&123));
    }

    #[test]
    fn checksum_header_ignores_pre_enable_in_commit_timestamp() {
        let checksum = make_test_checksum(1, 1, Some(123));
        let header = validate_and_build_header(42, checksum);
        assert!(header.is_some());
        let header = header.unwrap();
        assert!(header.commit_timestamps.is_empty());
    }

    #[test]
    fn checksum_with_num_metadata_zero_is_rejected() {
        let checksum = make_test_checksum(0, 1, None);
        assert!(validate_and_build_header(1, checksum).is_none());
    }

    #[test]
    fn checksum_with_num_protocol_zero_is_rejected() {
        let checksum = make_test_checksum(1, 0, None);
        assert!(validate_and_build_header(1, checksum).is_none());
    }

    #[test]
    fn checksum_with_num_metadata_two_is_rejected() {
        let checksum = make_test_checksum(2, 1, None);
        assert!(validate_and_build_header(1, checksum).is_none());
    }

    #[test]
    fn checksum_with_both_invalid_is_rejected() {
        let checksum = make_test_checksum(0, 0, None);
        assert!(validate_and_build_header(1, checksum).is_none());
    }

    fn dummy_meta(version: i64) -> ObjectMeta {
        use object_store::path::Path;

        ObjectMeta {
            location: Path::from(format!("_delta_log/{version:020}.json")),
            last_modified: chrono::Utc::now(),
            size: 0,
            e_tag: None,
            version: None,
        }
    }

    #[test]
    fn contiguous_commits_pass() {
        let files = vec![
            (3i64, dummy_meta(3)),
            (4, dummy_meta(4)),
            (5, dummy_meta(5)),
        ];
        assert!(validate_commit_contiguity(&files, 3, 5).is_ok());
    }

    #[test]
    fn empty_range_passes() {
        let files: Vec<(i64, ObjectMeta)> = vec![];
        assert!(validate_commit_contiguity(&files, 6, 5).is_ok());
    }

    #[test]
    fn gap_in_commits_fails() {
        let files = vec![(3i64, dummy_meta(3)), (5, dummy_meta(5))];
        let err = validate_commit_contiguity(&files, 3, 5).unwrap_err();
        assert!(err.to_string().contains("Missing commit file"));
    }

    #[test]
    fn missing_final_commit_fails() {
        let files = vec![(3i64, dummy_meta(3)), (4, dummy_meta(4))];
        let err = validate_commit_contiguity(&files, 3, 5).unwrap_err();
        assert!(err.to_string().contains("Missing commit file"));
    }

    #[test]
    fn single_commit_passes() {
        let files = vec![(7i64, dummy_meta(7))];
        assert!(validate_commit_contiguity(&files, 7, 7).is_ok());
    }

    #[tokio::test]
    async fn lower_bound_ignores_last_checkpoint_hint() {
        use object_store::memory::InMemory;
        use object_store::path::Path;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let checksum_55 = make_test_checksum(1, 1, None);
        let crc_bytes = serde_json::to_vec(&checksum_55).unwrap();
        store
            .put(
                &Path::from("_delta_log/00000000000000000055.crc"),
                crc_bytes.into(),
            )
            .await
            .unwrap();

        let last_cp = serde_json::json!({"version": 120});
        store
            .put(
                &Path::from("_delta_log/_last_checkpoint"),
                serde_json::to_vec(&last_cp).unwrap().into(),
            )
            .await
            .unwrap();

        let (checksums, _, _) = list_log_files(store, 50, 150).await.unwrap();
        let found = checksums.iter().find(|(v, _)| *v == 55).map(|(v, _)| *v);
        assert_eq!(
            found,
            Some(55),
            "should find CRC at version 55 even though _last_checkpoint points to 120"
        );
    }

    #[tokio::test]
    async fn lower_bound_respects_replay_hint_floor() {
        use object_store::memory::InMemory;
        use object_store::path::Path;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let checksum_55 = make_test_checksum(1, 1, None);
        let crc_bytes = serde_json::to_vec(&checksum_55).unwrap();
        store
            .put(
                &Path::from("_delta_log/00000000000000000055.crc"),
                crc_bytes.into(),
            )
            .await
            .unwrap();

        let (checksums, _, _) = list_log_files(store, 61, 150).await.unwrap();
        let found = checksums
            .iter()
            .find(|(v, _)| *v >= 61 && *v < 150)
            .map(|(v, _)| *v);
        assert_eq!(
            found, None,
            "resolver should ignore CRCs below the replay_hint floor"
        );
    }

    #[tokio::test]
    async fn list_log_files_includes_target_version_when_offset_is_prefix() {
        use object_store::memory::InMemory;
        use object_store::path::Path;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(
                &Path::from("_delta_log/00000000000000000000.json"),
                b"{}".to_vec().into(),
            )
            .await
            .unwrap();

        let (_, _, commits) = list_log_files(store, 0, 0).await.unwrap();
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].0, 0);
    }
}
