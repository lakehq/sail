use object_store::{ObjectMeta, ObjectStoreExt};

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

pub(crate) fn version_uses_in_commit_timestamps(
    version: i64,
    protocol: &Protocol,
    metadata: &Metadata,
) -> bool {
    let table_properties = TableProperties::from(metadata.configuration().iter());
    if !protocol.is_in_commit_timestamps_enabled(&table_properties) {
        return false;
    }

    match table_properties.in_commit_timestamp_enablement_version() {
        Some(enablement_version) => version >= enablement_version,
        None => true,
    }
}

pub(crate) fn resolve_commit_timestamp_from_actions(
    version: i64,
    meta: &ObjectMeta,
    current_protocol: Option<&Protocol>,
    current_metadata: Option<&Metadata>,
    actions: &[Action],
) -> DeltaResult<i64> {
    let uses_in_commit_timestamp =
        resolve_effective_protocol_and_metadata(current_protocol, current_metadata, actions)
            .map(|(protocol, metadata)| {
                version_uses_in_commit_timestamps(version, &protocol, &metadata)
            })
            .unwrap_or(false);

    if uses_in_commit_timestamp {
        in_commit_timestamp_from_actions(actions).ok_or_else(|| {
            DeltaError::generic(format!(
                "commit {version} is missing inCommitTimestamp and object metadata fallback is disallowed"
            ))
        })
    } else {
        Ok(meta.last_modified.timestamp_millis())
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
    if !version_uses_in_commit_timestamps(version, protocol, metadata) {
        if let Some(timestamp) = cached_timestamp {
            return Ok(timestamp);
        }
        let commit_uri = commit_path(version);
        let meta = log_store.object_store(None).head(&commit_uri).await?;
        return Ok(meta.last_modified.timestamp_millis());
    }

    if let Some(timestamp) = cached_timestamp {
        return Ok(timestamp);
    }
    if let Some(timestamp) = read_in_commit_timestamp_from_checksum(log_store, version).await? {
        return Ok(timestamp);
    }
    if let Some(timestamp) = read_in_commit_timestamp_from_commit_json(log_store, version).await? {
        return Ok(timestamp);
    }
    Err(DeltaError::generic(format!(
        "commit {version} requires inCommitTimestamp, but neither the checksum nor the commit JSON provided one"
    )))
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use chrono::DateTime;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
    use url::Url;

    use super::*;
    use crate::spec::{CommitInfo, DataType, Metadata, StructField, StructType, TableFeature};
    use crate::storage::{default_logstore, LogStoreRef, StorageConfig};

    fn test_log_store(store: Arc<dyn ObjectStore>) -> LogStoreRef {
        default_logstore(
            store.clone(),
            store,
            &Url::parse("memory:///").unwrap(),
            &StorageConfig,
        )
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

    fn commit_meta(version: i64, last_modified_millis: i64) -> DeltaResult<ObjectMeta> {
        let last_modified = DateTime::from_timestamp_millis(last_modified_millis)
            .ok_or_else(|| DeltaError::generic("test timestamp must be valid"))?;
        Ok(ObjectMeta {
            location: Path::from(format!("_delta_log/{version:020}.json")),
            last_modified,
            size: 0,
            e_tag: None,
            version: None,
        })
    }

    async fn put_commit(
        store: &Arc<dyn ObjectStore>,
        version: i64,
        actions: &[Action],
    ) -> DeltaResult<()> {
        let mut bytes = Vec::new();
        for (index, action) in actions.iter().enumerate() {
            if index > 0 {
                bytes.push(b'\n');
            }
            serde_json::to_writer(&mut bytes, action)?;
        }
        store.put(&commit_path(version), bytes.into()).await?;
        Ok(())
    }

    async fn put_checksum(
        store: &Arc<dyn ObjectStore>,
        version: i64,
        protocol: &Protocol,
        metadata: &Metadata,
        in_commit_timestamp_opt: Option<i64>,
    ) -> DeltaResult<()> {
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
            .await?;
        Ok(())
    }

    #[test]
    fn resolve_commit_timestamp_from_actions_ignores_pre_enable_ict() -> DeltaResult<()> {
        let protocol = Protocol::new(1, 2, None, None);
        let metadata = test_metadata([]);
        let commit_timestamp = resolve_commit_timestamp_from_actions(
            0,
            &commit_meta(0, 4_567)?,
            Some(&protocol),
            Some(&metadata),
            &[Action::CommitInfo(CommitInfo {
                in_commit_timestamp: Some(123),
                ..Default::default()
            })],
        )?;

        assert_eq!(commit_timestamp, 4_567);
        Ok(())
    }

    #[tokio::test]
    async fn resolve_version_timestamp_ignores_pre_enable_ict_in_checksum_and_commit(
    ) -> DeltaResult<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let log_store = test_log_store(store.clone());
        let pre_enable_protocol = Protocol::new(1, 2, None, None);
        let pre_enable_metadata = test_metadata([]);
        let enabled_protocol =
            Protocol::new(1, 7, None, Some(vec![TableFeature::InCommitTimestamp]));
        let enabled_metadata = test_metadata([
            ("delta.enableInCommitTimestamps", "true"),
            ("delta.inCommitTimestampEnablementVersion", "2"),
            ("delta.inCommitTimestampEnablementTimestamp", "300"),
        ]);

        put_commit(
            &store,
            0,
            &[Action::CommitInfo(CommitInfo {
                in_commit_timestamp: Some(10_000),
                ..Default::default()
            })],
        )
        .await?;
        put_checksum(
            &store,
            0,
            &pre_enable_protocol,
            &pre_enable_metadata,
            Some(10_000),
        )
        .await?;

        let expected = store
            .head(&commit_path(0))
            .await?
            .last_modified
            .timestamp_millis();
        let resolved = resolve_version_timestamp(
            log_store.as_ref(),
            0,
            None,
            &enabled_protocol,
            &enabled_metadata,
        )
        .await?;

        assert_eq!(resolved, expected);
        Ok(())
    }

    #[test]
    fn version_uses_in_commit_timestamps_honors_enablement_boundary() {
        let protocol = Protocol::new(1, 7, None, Some(vec![TableFeature::InCommitTimestamp]));
        let metadata = test_metadata([
            ("delta.enableInCommitTimestamps", "true"),
            ("delta.inCommitTimestampEnablementVersion", "2"),
            ("delta.inCommitTimestampEnablementTimestamp", "300"),
        ]);

        assert!(!version_uses_in_commit_timestamps(1, &protocol, &metadata));
        assert!(version_uses_in_commit_timestamps(2, &protocol, &metadata));
    }
}
