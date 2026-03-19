use object_store::ObjectMeta;

use crate::spec::{
    checksum_path, commit_path, Action, DeltaError, DeltaResult, Metadata, Protocol,
    TableProperties, VersionChecksum,
};
use crate::storage::{get_actions, LogStore};

pub(crate) fn in_commit_timestamp_from_actions(actions: &[Action]) -> Option<i64> {
    actions.iter().find_map(|action| match action {
        Action::CommitInfo(info) => info.in_commit_timestamp,
        _ => None,
    })
}

pub(crate) fn resolve_effective_protocol_and_metadata(
    current_protocol: Option<&Protocol>,
    current_metadata: Option<&Metadata>,
    actions: &[Action],
) -> Option<(Protocol, Metadata)> {
    let protocol = actions
        .iter()
        .rev()
        .find_map(|action| match action {
            Action::Protocol(protocol) => Some(protocol.clone()),
            _ => None,
        })
        .or_else(|| current_protocol.cloned())?;
    let metadata = actions
        .iter()
        .rev()
        .find_map(|action| match action {
            Action::Metadata(metadata) => Some(metadata.clone()),
            _ => None,
        })
        .or_else(|| current_metadata.cloned())?;
    Some((protocol, metadata))
}

pub(crate) fn mtime_fallback_allowed(
    version: i64,
    protocol: &Protocol,
    metadata: &Metadata,
) -> bool {
    let table_properties = TableProperties::from(metadata.configuration().iter());
    if !protocol.is_in_commit_timestamps_enabled(&table_properties) {
        return true;
    }
    table_properties
        .in_commit_timestamp_enablement_version()
        .is_some_and(|enablement_version| version < enablement_version)
}

pub(crate) fn resolve_commit_timestamp_from_actions(
    version: i64,
    meta: &ObjectMeta,
    current_protocol: Option<&Protocol>,
    current_metadata: Option<&Metadata>,
    actions: &[Action],
) -> DeltaResult<i64> {
    if let Some(timestamp) = in_commit_timestamp_from_actions(actions) {
        return Ok(timestamp);
    }

    let fallback_allowed =
        resolve_effective_protocol_and_metadata(current_protocol, current_metadata, actions)
            .map(|(protocol, metadata)| mtime_fallback_allowed(version, &protocol, &metadata))
            .unwrap_or(true);

    if fallback_allowed {
        Ok(meta.last_modified.timestamp_millis())
    } else {
        Err(DeltaError::generic(format!(
            "commit {version} is missing inCommitTimestamp and object metadata fallback is disallowed"
        )))
    }
}

async fn read_in_commit_timestamp_from_checksum(
    log_store: &dyn LogStore,
    version: i64,
) -> DeltaResult<Option<i64>> {
    let path = checksum_path(version);
    let store = log_store.object_store(None);
    let bytes = match store.get(&path).await {
        Ok(result) => result.bytes().await?,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let checksum: VersionChecksum = serde_json::from_slice(&bytes)?;
    Ok(checksum.in_commit_timestamp_opt)
}

async fn read_in_commit_timestamp_from_commit_json(
    log_store: &dyn LogStore,
    version: i64,
) -> DeltaResult<Option<i64>> {
    let Some(bytes) = log_store.read_commit_entry(version).await? else {
        return Ok(None);
    };
    let actions = get_actions(version, &bytes)?;
    Ok(in_commit_timestamp_from_actions(&actions))
}

pub(crate) async fn resolve_version_timestamp(
    log_store: &dyn LogStore,
    version: i64,
    cached_timestamp: Option<i64>,
    protocol: &Protocol,
    metadata: &Metadata,
) -> DeltaResult<i64> {
    if let Some(timestamp) = cached_timestamp {
        return Ok(timestamp);
    }
    if let Some(timestamp) = read_in_commit_timestamp_from_checksum(log_store, version).await? {
        return Ok(timestamp);
    }
    if let Some(timestamp) = read_in_commit_timestamp_from_commit_json(log_store, version).await? {
        return Ok(timestamp);
    }
    if mtime_fallback_allowed(version, protocol, metadata) {
        let commit_uri = commit_path(version);
        let meta = log_store.object_store(None).head(&commit_uri).await?;
        Ok(meta.last_modified.timestamp_millis())
    } else {
        Err(DeltaError::generic(format!(
            "commit {version} requires inCommitTimestamp, but neither the checksum nor the commit JSON provided one"
        )))
    }
}
