use std::collections::BTreeMap;
use std::sync::Arc;

use log::debug;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;

use super::{
    list_delta_log_entries_from, parse_checkpoint_version_from_location,
    parse_commit_version_from_location, read_last_checkpoint_version_from_store,
    LogSegmentResolver, ReplayedTableHeader, ResolvedLogSegment,
};
use crate::kernel::checkpoints::{
    decode_checkpoint_rows, read_checkpoint_rows_from_parquet, replay_commit_actions,
    replay_commit_header_actions, ReconciledCheckpointState, ReconciledHeaderState,
    ReplayedTableState,
};
use crate::spec::{CheckpointActionRow, DeltaError as DeltaTableError, DeltaResult};
use crate::storage::LogStore;

async fn read_checkpoint_header_from_parquet(
    root_store: Arc<dyn ObjectStore>,
    meta: ObjectMeta,
) -> DeltaResult<ReconciledHeaderState> {
    let bytes = root_store.get(&meta.location).await?.bytes().await?;
    tokio::task::spawn_blocking(move || {
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(DeltaTableError::generic_err)?;

        let parquet_schema = builder.parquet_schema();
        let mask = ProjectionMask::columns(
            parquet_schema,
            ["metaData", "protocol", "txn", "domainMetadata"],
        );

        let mut batches = builder
            .with_projection(mask)
            .build()
            .map_err(DeltaTableError::generic_err)?;

        let mut state = ReconciledHeaderState::default();
        for batch_result in &mut batches {
            let batch = batch_result.map_err(DeltaTableError::generic_err)?;
            let rows: Vec<CheckpointActionRow> = decode_checkpoint_rows(&batch)?;
            for row in rows {
                state.apply_checkpoint_row(row)?;
            }
        }
        Ok::<_, DeltaTableError>(state)
    })
    .await
    .map_err(DeltaTableError::generic_err)?
}

pub(crate) async fn load_replayed_table_state(
    version: i64,
    log_store: &dyn LogStore,
) -> DeltaResult<ReplayedTableState> {
    if version < 0 {
        return Err(DeltaTableError::generic(format!(
            "Cannot load table state for negative version: {version}"
        )));
    }

    let segment = LogSegmentResolver::new(log_store, version, None)
        .resolve_for_full_state()
        .await?;

    let ResolvedLogSegment::FullReplay {
        checkpoint,
        commit_files,
        target_version,
    } = segment
    else {
        return Err(DeltaTableError::generic(
            "resolve_for_full_state must return FullReplay",
        ));
    };

    let store = log_store.object_store(None);
    let mut state = ReconciledCheckpointState::default();
    let start_commit_version = if let Some(cp_meta) = checkpoint {
        let rows = read_checkpoint_rows_from_parquet(store.clone(), cp_meta).await?;
        for row in rows {
            state.apply_checkpoint_row(row)?;
        }
        commit_files
            .first()
            .map(|(v, _)| *v)
            .unwrap_or(target_version.saturating_add(1))
    } else {
        0
    };

    let commit_timestamps = replay_commit_actions(
        &mut state,
        store,
        &commit_files,
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
) -> DeltaResult<Option<ReplayedTableHeader>> {
    if version < 0 {
        return Err(DeltaTableError::generic(format!(
            "Cannot load table header for negative version: {version}"
        )));
    }

    let segment = LogSegmentResolver::new(log_store, version, replay_hint)
        .resolve_for_header()
        .await?;

    match segment {
        ResolvedLogSegment::ExactChecksum { header } => Ok(Some(header)),
        ResolvedLogSegment::Incremental {
            base,
            checkpoint,
            commit_files,
            target_version,
        } => {
            let store = log_store.object_store(None);

            let (mut state, start_commit_version, mut commit_timestamps) = match checkpoint {
                Some(cp_meta) => {
                    let cp_state =
                        read_checkpoint_header_from_parquet(store.clone(), cp_meta).await?;
                    let next_v = commit_files
                        .first()
                        .map(|(v, _)| *v)
                        .unwrap_or(target_version.saturating_add(1));
                    (cp_state, next_v, BTreeMap::new())
                }
                None => {
                    let start = base.version.saturating_add(1);
                    let ts = Arc::unwrap_or_clone(base.commit_timestamps.clone());
                    (ReconciledHeaderState::from_header(&base), start, ts)
                }
            };

            if start_commit_version <= target_version {
                commit_timestamps.extend(
                    replay_commit_header_actions(
                        &mut state,
                        store,
                        &commit_files,
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

pub(crate) async fn latest_replayable_version(log_store: &dyn LogStore) -> DeltaResult<i64> {
    let store = log_store.object_store(None);
    let offset_version = read_last_checkpoint_version_from_store(store.clone())
        .await
        .map(|v| v.saturating_sub(1))
        .unwrap_or(0);
    let log_entries = list_delta_log_entries_from(store, offset_version).await?;

    let latest = log_entries
        .iter()
        .filter_map(|meta| {
            parse_commit_version_from_location(&meta.location)
                .or_else(|| parse_checkpoint_version_from_location(&meta.location))
        })
        .max();

    latest.ok_or(crate::spec::DeltaError::MissingVersion)
}
