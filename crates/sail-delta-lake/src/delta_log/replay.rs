// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use datafusion::common::runtime::SpawnedTask;
use log::debug;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::segment::try_read_checksum_header;
use super::{
    CheckpointFileSet, LogSegmentResolver, ReplayedTableHeader, ResolvedLogSegment,
    latest_version_from_listing, list_log_files, read_last_checkpoint_version_from_store,
};
use crate::checkpoint::{
    ReconciledCheckpointState, ReconciledHeaderState, ReplayedTableState, decode_checkpoint_rows,
    read_checkpoint_main_rows_from_checkpoint_file, read_checkpoint_protocol,
    read_checkpoint_rows_from_checkpoint_files, replay_commit_actions_with_compactions,
    replay_commit_header_actions_with_compactions, validate_multi_part_checkpoint_rows,
};
use crate::delta_log::{LogStore, get_protocol_action};
use crate::snapshot::CatalogManagedCommitSet;
use crate::spec::{
    DeltaError as DeltaTableError, DeltaResult, Protocol, is_json_checkpoint_filename,
};

async fn read_checkpoint_header_from_checkpoint_files(
    root_store: Arc<dyn ObjectStore>,
    checkpoint: CheckpointFileSet,
) -> DeltaResult<ReconciledHeaderState> {
    let checkpoint_version = checkpoint.version();
    let multi_part = checkpoint.is_multi_part();
    let mut state = ReconciledHeaderState::default();
    for meta in checkpoint.into_files() {
        if is_json_checkpoint_location(&meta) {
            let rows =
                read_checkpoint_main_rows_from_checkpoint_file(root_store.clone(), meta).await?;
            for row in rows {
                state.apply_checkpoint_row(row)?;
            }
        } else {
            let bytes = root_store.get(&meta.location).await?.bytes().await?;
            state = SpawnedTask::spawn_blocking(move || {
                let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
                    .map_err(DeltaTableError::generic_err)?;

                let parquet_schema = builder.parquet_schema();
                let mask = ProjectionMask::columns(
                    parquet_schema,
                    [
                        "metaData",
                        "protocol",
                        "txn",
                        "domainMetadata",
                        "checkpointMetadata",
                        "sidecar",
                    ],
                );

                let mut batches = builder
                    .with_projection(mask)
                    .build()
                    .map_err(DeltaTableError::generic_err)?;

                for batch_result in &mut batches {
                    let batch = batch_result.map_err(DeltaTableError::generic_err)?;
                    let rows = decode_checkpoint_rows(&batch)?;
                    if multi_part {
                        validate_multi_part_checkpoint_rows(checkpoint_version, &rows)?;
                    }
                    for row in rows {
                        state.apply_checkpoint_row(row)?;
                    }
                }
                Ok::<_, DeltaTableError>(state)
            })
            .await
            .map_err(DeltaTableError::generic_err)??;
        }
    }
    Ok(state)
}

fn is_json_checkpoint_location(meta: &ObjectMeta) -> bool {
    meta.location
        .as_ref()
        .rsplit('/')
        .next()
        .is_some_and(is_json_checkpoint_filename)
}

pub(crate) async fn load_replayed_table_state(
    version: i64,
    log_store: &dyn LogStore,
    catalog_managed_commits: Option<&CatalogManagedCommitSet>,
) -> DeltaResult<ReplayedTableState> {
    if version < 0 {
        return Err(DeltaTableError::generic(format!(
            "Cannot load table state for negative version: {version}"
        )));
    }

    let segment = LogSegmentResolver::new(log_store, version, None, catalog_managed_commits)
        .resolve_for_full_state()
        .await?;

    let ResolvedLogSegment::FullReplay {
        checkpoint,
        commit_files,
        compaction_files,
        target_version,
    } = segment
    else {
        return Err(DeltaTableError::generic(
            "resolve_for_full_state must return FullReplay",
        ));
    };

    let store = log_store.object_store(None);
    let mut state = ReconciledCheckpointState::default();
    let start_commit_version = if let Some(checkpoint_files) = checkpoint {
        let next_version = checkpoint_files.version().saturating_add(1);
        let rows =
            read_checkpoint_rows_from_checkpoint_files(store.clone(), checkpoint_files).await?;
        for row in rows {
            state.apply_checkpoint_row(row)?;
        }
        next_version
    } else {
        0
    };

    let commit_timestamps = replay_commit_actions_with_compactions(
        &mut state,
        store,
        &commit_files,
        &compaction_files,
        start_commit_version,
        target_version,
    )
    .await?;

    let protocol = state
        .protocol
        .ok_or_else(|| DeltaTableError::generic("Cannot load table state without protocol"))?;
    let metadata = state
        .metadata
        .ok_or_else(|| DeltaTableError::generic("Cannot load table state without metadata"))?;
    let txns = state.txns;
    let domain_metadata = state
        .domain_metadata
        .into_iter()
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect::<Vec<_>>();
    let adds = state
        .adds
        .into_iter()
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect::<Vec<_>>();
    let removes = state
        .removes
        .into_iter()
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect::<Vec<_>>();
    Ok(ReplayedTableState {
        version: target_version,
        protocol,
        metadata,
        txns,
        domain_metadata,
        adds,
        removes,
        commit_timestamps,
    })
}

pub(crate) async fn load_replayed_table_header(
    version: i64,
    log_store: &dyn LogStore,
    replay_hint: Option<&ReplayedTableHeader>,
    catalog_managed_commits: Option<&CatalogManagedCommitSet>,
) -> DeltaResult<Option<ReplayedTableHeader>> {
    if version < 0 {
        return Err(DeltaTableError::generic(format!(
            "Cannot load table header for negative version: {version}"
        )));
    }

    let segment = LogSegmentResolver::new(log_store, version, replay_hint, catalog_managed_commits)
        .resolve_for_header()
        .await?;

    match segment {
        ResolvedLogSegment::ExactChecksum { header } => Ok(Some(header)),
        ResolvedLogSegment::Incremental {
            base,
            checkpoint,
            commit_files,
            compaction_files,
            target_version,
        } => {
            let store = log_store.object_store(None);

            let (mut state, start_commit_version, mut commit_timestamps) = match checkpoint {
                Some(checkpoint_files) => {
                    let next_version = checkpoint_files.version().saturating_add(1);
                    let checkpoint_state = read_checkpoint_header_from_checkpoint_files(
                        store.clone(),
                        checkpoint_files,
                    )
                    .await?;
                    (checkpoint_state, next_version, BTreeMap::new())
                }
                None => {
                    let start = base.version.saturating_add(1);
                    let ts = Arc::unwrap_or_clone(base.commit_timestamps.clone());
                    (ReconciledHeaderState::from_header(&base), start, ts)
                }
            };

            if start_commit_version <= target_version {
                commit_timestamps.extend(
                    replay_commit_header_actions_with_compactions(
                        &mut state,
                        store,
                        &commit_files,
                        &compaction_files,
                        start_commit_version,
                        target_version,
                    )
                    .await?,
                );
            }

            let protocol = state.protocol.ok_or_else(|| {
                DeltaTableError::generic("Cannot load table header without protocol")
            })?;
            let metadata = state.metadata.ok_or_else(|| {
                DeltaTableError::generic("Cannot load table header without metadata")
            })?;
            Ok(Some(ReplayedTableHeader {
                version: target_version,
                protocol,
                metadata,
                txns: Arc::new(state.txns),
                domain_metadata: Arc::new(state.domain_metadata),
                commit_timestamps: Arc::new(commit_timestamps),
            }))
        }
        ResolvedLogSegment::FullReplay { .. } => {
            debug!(
                "crc-header: no usable base state, returning None for header fast path target_version={version}"
            );
            Ok(None)
        }
    }
}

/// Returns whether the table protocol satisfies `predicate` at some version up to `version`.
///
/// The search starts from a checksum or the latest checkpoint, and then reads the published log
/// files after it in version order, decoding only their protocol action. It stops at the first
/// protocol that satisfies `predicate`, so the files after it are not read. A missing published
/// commit is an error.
pub(crate) async fn protocol_satisfies_up_to(
    version: i64,
    log_store: &dyn LogStore,
    predicate: impl Fn(&Protocol) -> bool,
) -> DeltaResult<bool> {
    let store = log_store.object_store(None);
    if let Some(header) = try_read_checksum_header(store.clone(), version).await {
        return Ok(predicate(&header.protocol));
    }

    let list_offset = read_last_checkpoint_version_from_store(store.clone())
        .await
        .filter(|hint| (0..=version).contains(hint))
        .map(|hint| hint.saturating_sub(1))
        .unwrap_or(0);
    let (checksum_files, checkpoint, commit_files, compaction_files) =
        list_log_files(store.clone(), list_offset, version).await?;

    let mut next_version = 0;
    if let Some(checkpoint) = checkpoint {
        next_version = checkpoint.version().saturating_add(1);
        if read_checkpoint_protocol(store.clone(), checkpoint)
            .await?
            .as_ref()
            .is_some_and(&predicate)
        {
            return Ok(true);
        }
    }
    // A newer checksum lets the search skip the commits before it. Checksums are listed newest
    // first, and the one at `version` was read above.
    if let Some((checksum_version, _)) = checksum_files
        .iter()
        .find(|(checksum_version, _)| *checksum_version < version)
        && *checksum_version >= next_version
        && let Some(header) = try_read_checksum_header(store.clone(), *checksum_version).await
    {
        if predicate(&header.protocol) {
            return Ok(true);
        }
        next_version = checksum_version.saturating_add(1);
    }

    let commit_files = commit_files
        .iter()
        .map(|(version, meta)| (*version, meta))
        .collect::<HashMap<_, _>>();
    let compaction_files = compaction_files
        .iter()
        .map(|((start, end), meta)| (*start, (*end, meta)))
        .collect::<HashMap<_, _>>();
    while next_version <= version {
        let (end, meta) = match (
            compaction_files.get(&next_version),
            commit_files.get(&next_version),
        ) {
            (Some((end, meta)), _) => (*end, *meta),
            (None, Some(meta)) => (next_version, *meta),
            // Commits are published in order, also for a catalog-managed table, so the log is
            // incomplete.
            (None, None) => {
                return Err(DeltaTableError::generic(format!(
                    "Missing commit file: expected version {next_version}"
                )));
            }
        };
        let bytes = store.get(&meta.location).await?.bytes().await?;
        if get_protocol_action(end, &bytes)?
            .as_ref()
            .is_some_and(&predicate)
        {
            return Ok(true);
        }
        next_version = end.saturating_add(1);
    }
    Ok(false)
}

pub(crate) async fn latest_replayable_version(log_store: &dyn LogStore) -> DeltaResult<i64> {
    let store = log_store.object_store(None);
    latest_version_from_listing(store)
        .await?
        .ok_or(crate::spec::DeltaError::MissingVersion)
}
