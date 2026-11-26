// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};

use datafusion::execution::TaskContext;
use datafusion_common::{DataFusionError, Result, not_impl_err, plan_err};
use futures::TryStreamExt;
use object_store::path::Path;
use sail_common::spec::CreateTableMode;
use sail_common_datafusion::catalog::delta::is_delta_coordinated_property;
use sail_common_datafusion::catalog::identity_partition_fields;
use sail_common_datafusion::lakesource::{CloneMetrics, LakeSourceClone, LakeSourceMetadata};
use sail_data_source::{canonical_local_path, resolve_table_url};
use url::Url;

use crate::DeltaTableError;
use crate::snapshot::{DeltaSnapshot, DeltaSnapshotConfig};
use crate::spec::utils::{absolute_uri_parts, decode_path, encode_path};
use crate::spec::{CommitAction, DeltaOperation};
use crate::table::{
    create_logstore_with_object_store, open_table_with_object_store_and_table_config,
};
use crate::transaction::{CommitBuilder, CommitProperties};

fn external(error: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

/// Classify the stored URI before decoding so an escaped colon stays relative.
pub(crate) fn data_file_url(root: &Url, path: &str) -> Result<Url> {
    let encoded = encode_path(&decode_path(path).map_err(external)?);
    let url = if absolute_uri_parts(path).is_some() {
        Url::parse(&encoded).map_err(external)?
    } else {
        root.join(&if path.starts_with('/') {
            encoded
        } else {
            format!("./{encoded}")
        })
        .map_err(external)?
    };
    if url.scheme() != root.scheme() || url.authority() != root.authority() {
        return not_impl_err!(
            "Delta absolute data paths across object-store authorities are not supported"
        );
    }
    Ok(url)
}

fn table_url(path: &str) -> Result<Url> {
    let mut url = resolve_table_url(path.to_string())?;
    if url.query().is_some() || url.fragment().is_some() {
        return plan_err!("Delta table locations cannot contain query strings or fragments");
    }
    if url.scheme() == "file" {
        url = Url::from_directory_path(canonical_local_path(&url)?)
            .map_err(|_| DataFusionError::Plan("invalid local Delta path".into()))?;
    } else if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    Ok(url)
}

fn validate_snapshot(snapshot: &DeltaSnapshot) -> Result<()> {
    let protocol = snapshot.protocol();
    if protocol.min_reader_version() > 1
        || protocol.min_writer_version() > 2
        || protocol.reader_features().is_some()
        || protocol.writer_features().is_some()
    {
        return not_impl_err!(
            "SHALLOW CLONE currently supports Delta reader version 1 and writer versions 1-2 without table features"
        );
    }
    if snapshot.metadata().configuration().keys().any(|key| {
        is_delta_coordinated_property(key) || key.to_ascii_lowercase().starts_with("delta.feature.")
    }) {
        return not_impl_err!(
            "SHALLOW CLONE of catalog-coordinated or feature-enabled Delta tables"
        );
    }
    if snapshot.adds().iter().any(|add| {
        add.deletion_vector.is_some()
            || add.base_row_id.is_some()
            || add.default_row_commit_version.is_some()
            || add.clustering_provider.is_some()
    }) {
        return not_impl_err!(
            "SHALLOW CLONE does not support deletion vectors, row tracking or clustering"
        );
    }
    Ok(())
}

pub(crate) async fn prepare(
    ctx: &TaskContext,
    source: futures::future::BoxFuture<'_, Result<String>>,
    destination: &str,
    mode: CreateTableMode,
) -> Result<LakeSourceClone> {
    let destination_url = table_url(destination)?;
    let destination_root = Path::from_url_path(destination_url.path())?;
    let runtime = ctx.runtime_env();
    let store = runtime
        .object_store_registry
        .get_store(&destination_url)
        .map_err(external)?;
    let destination_table = match open_table_with_object_store_and_table_config(
        destination_url.clone(),
        store.clone(),
        Default::default(),
        DeltaSnapshotConfig::default(),
    )
    .await
    {
        Ok(table) => Some(table),
        Err(DeltaTableError::InvalidTableLocation(_) | DeltaTableError::FileNotFound(_)) => None,
        Err(error) => return Err(error.into()),
    };
    let old = destination_table
        .as_ref()
        .map(|table| table.snapshot().cloned())
        .transpose()?;
    if let Some(old) = &old {
        if mode.ignore_if_exists() {
            return Ok(LakeSourceClone {
                metrics: None,
                metadata: LakeSourceMetadata {
                    schema: old.logical_arrow_schema()?,
                    properties: old
                        .metadata()
                        .configuration()
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                    partition_by: Some(identity_partition_fields(
                        old.metadata().partition_columns(),
                    )),
                },
                publish: Box::pin(async { Ok(()) }),
                post_commit: Box::pin(async {}),
            });
        }
        if !mode.is_replace() {
            return plan_err!("SHALLOW CLONE destination already exists");
        }
        validate_snapshot(old)?;
    } else {
        if mode.replace_requires_existing() {
            return plan_err!("REPLACE TABLE SHALLOW CLONE destination does not exist");
        }
        if store
            .list(Some(&destination_root))
            .try_next()
            .await?
            .is_some()
        {
            return plan_err!("SHALLOW CLONE destination is not empty and is not a Delta table");
        }
    }
    let source_url = table_url(&source.await?)?;
    if source_url.scheme() != destination_url.scheme()
        || source_url.authority() != destination_url.authority()
    {
        return not_impl_err!("SHALLOW CLONE across object-store authorities is not supported");
    }
    // Match the decoded, segment-aware prefixes used by StorageConfig::decorate_store.
    let source_root = Path::from_url_path(source_url.path())?;
    if source_root.prefix_matches(&destination_root)
        || destination_root.prefix_matches(&source_root)
    {
        return plan_err!(
            "SHALLOW CLONE source and destination must be distinct, non-overlapping locations"
        );
    }
    let source_table = open_table_with_object_store_and_table_config(
        source_url.clone(),
        store.clone(),
        Default::default(),
        DeltaSnapshotConfig::default(),
    )
    .await?;
    let source = source_table.snapshot()?;
    validate_snapshot(source)?;
    let metadata = source
        .metadata()
        .for_clone(old.as_deref().map(DeltaSnapshot::metadata));
    let protocol = old
        .as_ref()
        .map(|old| old.protocol().merge_upgrade_requirements(source.protocol()))
        .unwrap_or_else(|| source.protocol().clone());
    let result_metadata = LakeSourceMetadata {
        schema: source.logical_arrow_schema()?,
        properties: metadata
            .configuration()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        partition_by: Some(identity_partition_fields(metadata.partition_columns())),
    };
    let mut actions = vec![
        CommitAction::Protocol(protocol),
        CommitAction::Metadata(metadata),
    ];
    // Match physical files across equivalent URI spellings, retaining the destination's
    // log path for metadata updates. Delta reconciles actions by the stored path and DV.
    let mut remaining = HashMap::new();
    for add in old.iter().flat_map(|s| s.adds()) {
        let url = data_file_url(&destination_url, &add.path)?;
        if remaining.insert(url, add).is_some() {
            return plan_err!(
                "SHALLOW CLONE destination has multiple entries for the same data file"
            );
        }
    }
    let mut metrics = CloneMetrics::default();
    let mut seen = HashSet::new();
    for add in source.adds() {
        let mut add = add.clone();
        let absolute = data_file_url(&source_url, &add.path)?;
        // Check both the stored location and the physical location behind local symlinks.
        let physical = if absolute.scheme() == "file" {
            Some(
                Url::from_file_path(canonical_local_path(&absolute)?)
                    .map_err(|_| DataFusionError::Plan("invalid local Delta path".into()))?,
            )
        } else {
            None
        };
        for url in std::iter::once(&absolute).chain(&physical) {
            if Path::from_url_path(url.path())?.prefix_matches(&destination_root) {
                return plan_err!("SHALLOW CLONE cannot reference files inside its destination");
            }
        }
        let absolute = physical.unwrap_or(absolute);
        if !seen.insert(absolute.clone()) {
            return plan_err!("SHALLOW CLONE source has multiple entries for the same data file");
        }
        metrics.source_num_of_files += 1;
        metrics.source_table_size += add.size;
        if let Some(previous) = remaining.remove(&absolute) {
            add.path.clone_from(&previous.path);
            add.commit_version = previous.commit_version;
            add.commit_timestamp = previous.commit_timestamp;
            add.data_change = previous.data_change;
            if &add == previous {
                continue;
            }
            add.data_change = add.partition_values != previous.partition_values;
        } else {
            add.path = absolute.to_string();
            add.data_change = true;
        }
        actions.push(CommitAction::Add(add));
    }
    let removed_at = chrono::Utc::now().timestamp_millis();
    for removed in remaining.into_values() {
        metrics.num_removed_files += 1;
        metrics.removed_files_size += removed.size;
        actions.push(CommitAction::Remove(
            removed.clone().into_remove(removed_at),
        ));
    }
    let operation = DeltaOperation::Clone {
        source: source_url.to_string(),
        source_version: source.version(),
        is_shallow: true,
    };
    let log_store = create_logstore_with_object_store(store, destination_url, Default::default())?;
    let history_metrics: HashMap<_, _> = [
        ("sourceTableSize", metrics.source_table_size),
        ("sourceNumOfFiles", metrics.source_num_of_files),
        ("numRemovedFiles", metrics.num_removed_files),
        ("numCopiedFiles", metrics.num_copied_files),
        ("removedFilesSize", metrics.removed_files_size),
        ("copiedFilesSize", metrics.copied_files_size),
    ]
    .into_iter()
    .map(|(key, value)| {
        (
            key.to_string(),
            serde_json::Value::String(value.to_string()),
        )
    })
    .collect();
    let commit = CommitBuilder::from(
        CommitProperties::from_session_config(ctx.session_config())?
            .with_operation_metrics(history_metrics),
    )
    .with_actions(actions)
    .build(old, log_store, operation);
    // Protocol/append-only validation is still preparation, before catalog publication.
    let prepared = commit.into_prepared_commit_future().await?;
    let (sender, receiver) = tokio::sync::oneshot::channel();
    Ok(LakeSourceClone {
        metrics: Some(metrics),
        metadata: result_metadata,
        publish: Box::pin(async move {
            let post_commit = prepared.await?;
            let _ = sender.send(post_commit);
            Ok(())
        }),
        post_commit: Box::pin(async move {
            if let Ok(post_commit) = receiver.await
                && let Err(error) = post_commit.await
            {
                log::warn!("Delta clone post-commit maintenance failed: {error}");
            }
        }),
    })
}
