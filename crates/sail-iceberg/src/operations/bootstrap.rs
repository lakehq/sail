// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/// Bootstrap helper for creating the first snapshot in a new or empty Iceberg table
///
/// This module provides utilities for bootstrapping Iceberg tables when:
/// 1. The table metadata file doesn't exist (new table)
/// 2. The table metadata exists but has no current snapshot (e.g., after CREATE TABLE)
use std::sync::Arc;

use bytes::Bytes;
use datafusion_common::{DataFusionError, Result};
use url::Url;

use crate::io::StoreContext;
use crate::operations::{SnapshotProduceOperation, SnapshotProducer, Transaction};
use crate::physical_plan::commit::IcebergCommitInfo;
use crate::spec::metadata::format::FormatVersion;
use crate::spec::metadata::table_metadata::SnapshotLog;
use crate::spec::partition::PartitionSpec;
use crate::spec::schema::Schema as IcebergSchema;
use crate::spec::snapshots::{SnapshotBuilder, SnapshotReference, SnapshotRetention};
use crate::spec::TableMetadata;
use crate::utils::WritePathMode;

/// Strategy for persisting metadata during bootstrap
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistStrategy {
    /// Overwrite the existing metadata file in place (for syncing with external catalogs)
    InPlace,
    /// Generate and write a new version of the metadata file (standard Iceberg approach)
    NewVersion,
}

/// Bootstrap operation for SnapshotProducer
struct BootstrapOperation;

impl SnapshotProduceOperation for BootstrapOperation {
    fn operation(&self) -> &'static str {
        "append"
    }
}

/// Bootstrap a new table when no metadata file exists
///
/// This creates:
/// - A new manifest with the data files
/// - A new manifest list
/// - A new snapshot
/// - A new table metadata file (version 1)
/// - A version-hint file
pub async fn bootstrap_new_table(
    table_url: &Url,
    store_ctx: &StoreContext,
    commit_info: &IcebergCommitInfo,
) -> Result<TableMetadata> {
    let iceberg_schema: IcebergSchema = commit_info
        .schema
        .clone()
        .ok_or_else(|| DataFusionError::Plan("Missing schema for bootstrap".to_string()))?;
    let partition_spec: PartitionSpec = commit_info
        .partition_spec
        .clone()
        .unwrap_or_else(PartitionSpec::unpartitioned_spec);

    // Create a minimal transaction context (no parent snapshot)
    let empty_snapshot = SnapshotBuilder::new()
        .with_snapshot_id(0)
        .with_sequence_number(0)
        .with_manifest_list(String::new())
        .with_summary(crate::spec::snapshots::Summary::new(
            crate::spec::Operation::Append,
        ))
        .with_schema_id(iceberg_schema.schema_id())
        .build()
        .map_err(DataFusionError::Execution)?;

    let tx = Transaction::new(table_url.to_string(), empty_snapshot);
    let manifest_meta = crate::spec::manifest::ManifestMetadata::new(
        Arc::new(iceberg_schema.clone()),
        iceberg_schema.schema_id(),
        partition_spec.clone(),
        FormatVersion::V2,
        crate::spec::ManifestContentType::Data,
    );

    // Use SnapshotProducer in bootstrap mode
    let producer = SnapshotProducer::new(
        &tx,
        commit_info.data_files.clone(),
        Some(store_ctx.clone()),
        Some(manifest_meta),
    )
    .with_bootstrap(true)
    .with_write_path_mode(WritePathMode::Absolute);

    let action_commit = producer
        .commit(BootstrapOperation)
        .await
        .map_err(DataFusionError::Execution)?;

    // Extract the new snapshot from the updates
    let updates = action_commit.into_updates();
    let snapshot = updates
        .iter()
        .find_map(|upd| match upd {
            crate::spec::catalog::TableUpdate::AddSnapshot { snapshot } => Some(snapshot.clone()),
            _ => None,
        })
        .ok_or_else(|| DataFusionError::Plan("No snapshot in bootstrap commit".to_string()))?;

    // Build minimal TableMetadata V2
    let commit_timestamp_ms = crate::utils::timestamp::monotonic_timestamp_ms();
    let table_meta = TableMetadata {
        format_version: FormatVersion::V2,
        table_uuid: None,
        location: table_url.to_string(),
        last_sequence_number: 1,
        last_updated_ms: commit_timestamp_ms,
        last_column_id: iceberg_schema.highest_field_id(),
        schemas: vec![iceberg_schema.clone()],
        current_schema_id: iceberg_schema.schema_id(),
        partition_specs: vec![partition_spec.clone()],
        default_spec_id: partition_spec.spec_id(),
        last_partition_id: partition_spec.highest_field_id().unwrap_or(0),
        properties: std::collections::HashMap::new(),
        current_snapshot_id: Some(snapshot.snapshot_id()),
        snapshots: vec![snapshot.clone()],
        snapshot_log: vec![SnapshotLog {
            timestamp_ms: commit_timestamp_ms,
            snapshot_id: snapshot.snapshot_id(),
        }],
        metadata_log: vec![],
        sort_orders: vec![],
        default_sort_order_id: None,
        refs: std::iter::once((
            crate::spec::snapshots::MAIN_BRANCH.to_string(),
            SnapshotReference {
                snapshot_id: snapshot.snapshot_id(),
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            },
        ))
        .collect(),
        statistics: vec![],
        partition_statistics: vec![],
    };

    // Write metadata v00001
    let new_meta_bytes = table_meta
        .to_json()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let new_meta_rel = format!("metadata/{:05}-{}.metadata.json", 1, uuid::Uuid::new_v4());
    let meta_path = object_store::path::Path::from(new_meta_rel.as_str());
    store_ctx
        .prefixed
        .put(
            &meta_path,
            object_store::PutPayload::from(Bytes::from(new_meta_bytes)),
        )
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Write version-hint
    let hint_path = object_store::path::Path::from("metadata/version-hint.text");
    store_ctx
        .prefixed
        .put(
            &hint_path,
            object_store::PutPayload::from(Bytes::from("1".as_bytes().to_vec())),
        )
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    Ok(table_meta)
}

/// Bootstrap the first snapshot for an existing table that has no current snapshot
///
/// This is used when a table was created via CREATE TABLE but has no data yet.
/// The persist_strategy determines how the metadata is written:
/// - InPlace: Overwrites the existing metadata file (for external catalog sync)
/// - NewVersion: Creates a new metadata version (standard Iceberg)
pub async fn bootstrap_first_snapshot(
    table_url: &Url,
    store_ctx: &StoreContext,
    commit_info: &IcebergCommitInfo,
    mut table_meta: TableMetadata,
    latest_meta_path: &str,
    persist_strategy: PersistStrategy,
) -> Result<TableMetadata> {
    let schema_iceberg = table_meta
        .current_schema()
        .cloned()
        .ok_or_else(|| DataFusionError::Plan("No current schema in table metadata".to_string()))?;

    let partition_spec = table_meta
        .default_partition_spec()
        .cloned()
        .unwrap_or_else(PartitionSpec::unpartitioned_spec);

    // Create a minimal transaction context (no parent snapshot)
    let empty_snapshot = SnapshotBuilder::new()
        .with_snapshot_id(0)
        .with_sequence_number(0)
        .with_manifest_list(String::new())
        .with_summary(crate::spec::snapshots::Summary::new(
            crate::spec::Operation::Append,
        ))
        .with_schema_id(schema_iceberg.schema_id())
        .build()
        .map_err(DataFusionError::Execution)?;

    let tx = Transaction::new(table_url.to_string(), empty_snapshot);
    let manifest_meta = crate::spec::manifest::ManifestMetadata::new(
        Arc::new(schema_iceberg.clone()),
        schema_iceberg.schema_id(),
        partition_spec.clone(),
        FormatVersion::V2,
        crate::spec::ManifestContentType::Data,
    );

    // Use SnapshotProducer in bootstrap mode
    let producer = SnapshotProducer::new(
        &tx,
        commit_info.data_files.clone(),
        Some(store_ctx.clone()),
        Some(manifest_meta),
    )
    .with_bootstrap(true)
    .with_write_path_mode(WritePathMode::Absolute);

    let action_commit = producer
        .commit(BootstrapOperation)
        .await
        .map_err(DataFusionError::Execution)?;

    // Extract the new snapshot from the updates
    let updates = action_commit.into_updates();
    let snapshot = updates
        .iter()
        .find_map(|upd| match upd {
            crate::spec::catalog::TableUpdate::AddSnapshot { snapshot } => Some(snapshot.clone()),
            _ => None,
        })
        .ok_or_else(|| DataFusionError::Plan("No snapshot in bootstrap commit".to_string()))?;

    // Update table metadata with the new snapshot
    let commit_timestamp_ms = crate::utils::timestamp::monotonic_timestamp_ms();
    table_meta.current_snapshot_id = Some(snapshot.snapshot_id());
    table_meta.snapshots.push(snapshot.clone());
    table_meta.snapshot_log.push(SnapshotLog {
        timestamp_ms: commit_timestamp_ms,
        snapshot_id: snapshot.snapshot_id(),
    });
    table_meta.last_sequence_number = 1;
    table_meta.last_updated_ms = commit_timestamp_ms;

    // Add main branch reference if not present
    if !table_meta
        .refs
        .contains_key(crate::spec::snapshots::MAIN_BRANCH)
    {
        table_meta.refs.insert(
            crate::spec::snapshots::MAIN_BRANCH.to_string(),
            SnapshotReference {
                snapshot_id: snapshot.snapshot_id(),
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            },
        );
    }

    // Serialize and write metadata
    let new_meta_bytes = table_meta
        .to_json()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    match persist_strategy {
        PersistStrategy::InPlace => {
            // Overwrite the existing metadata file in place
            let rel_name = if latest_meta_path.starts_with(&table_url.to_string()) {
                latest_meta_path
                    .strip_prefix(&table_url.to_string())
                    .unwrap_or("metadata/00000.metadata.json")
                    .to_string()
            } else if let Some(fname) = latest_meta_path.rsplit('/').next() {
                format!("metadata/{}", fname)
            } else {
                "metadata/00000.metadata.json".to_string()
            };
            let rel_path = object_store::path::Path::from(rel_name.as_str());
            store_ctx
                .prefixed
                .put(
                    &rel_path,
                    object_store::PutPayload::from(Bytes::from(new_meta_bytes)),
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // Extract metadata filename for version-hint
            let metadata_filename = if let Some(fname) = rel_name.rsplit('/').next() {
                fname.to_string()
            } else {
                rel_name.clone()
            };

            // Write version-hint
            let hint_path = object_store::path::Path::from("metadata/version-hint.text");
            store_ctx
                .prefixed
                .put(
                    &hint_path,
                    object_store::PutPayload::from(Bytes::from(
                        metadata_filename.as_bytes().to_vec(),
                    )),
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }
        PersistStrategy::NewVersion => {
            // Create a new metadata version
            let version = table_meta.metadata_log.len() + 1;
            let new_meta_rel = format!(
                "metadata/{:05}-{}.metadata.json",
                version,
                uuid::Uuid::new_v4()
            );
            let meta_path = object_store::path::Path::from(new_meta_rel.as_str());
            store_ctx
                .prefixed
                .put(
                    &meta_path,
                    object_store::PutPayload::from(Bytes::from(new_meta_bytes)),
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // Write version-hint
            let hint_path = object_store::path::Path::from("metadata/version-hint.text");
            store_ctx
                .prefixed
                .put(
                    &hint_path,
                    object_store::PutPayload::from(Bytes::from(format!("{}", version))),
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }
    }

    Ok(table_meta)
}
