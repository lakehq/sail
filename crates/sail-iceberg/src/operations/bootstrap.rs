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
use object_store::ObjectStoreExt;
use url::Url;

use crate::io::StoreContext;
use crate::operations::helpers::format_version_for_schema;
use crate::operations::{SnapshotProduceOperation, SnapshotProducer, Transaction};
use crate::physical_plan::commit::IcebergCommitInfo;
use crate::spec::metadata::table_metadata::SnapshotLog;
use crate::spec::partition::PartitionSpec;
use crate::spec::schema::Schema as IcebergSchema;
use crate::spec::snapshots::{SnapshotBuilder, SnapshotReference, SnapshotRetention};
use crate::spec::{FormatVersion, TableMetadata};
use crate::table::metadata_loader::{
    encode_metadata_file, metadata_file_extension_from_properties, metadata_file_version_from_path,
};
use crate::utils::WritePathMode;

/// Strategy for persisting metadata during bootstrap
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistStrategy {
    /// Overwrite the existing metadata file in place (for syncing with external catalogs)
    InPlace,
    /// Generate and write a new version of the metadata file (standard Iceberg approach)
    NewVersion,
}

fn version_hint_from_metadata_path(path: &str) -> Result<String> {
    metadata_file_version_from_path(path)
        .map(|version| version.to_string())
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "cannot determine Iceberg metadata version from path: {path}"
            ))
        })
}

fn in_place_version_hint_from_metadata_path(path: &str) -> Result<String> {
    let file_name = path.rsplit('/').next().ok_or_else(|| {
        DataFusionError::Plan(format!(
            "cannot determine Iceberg metadata file name from path: {path}"
        ))
    })?;
    // Numeric hints are only unambiguous for Hadoop-style vN metadata files.
    // Catalog-style metadata files (00000-uuid.metadata.json) must be hinted by
    // file name so static readers do not translate "0" to v0.metadata.json.
    if file_name.starts_with('v') {
        version_hint_from_metadata_path(path)
    } else if metadata_file_version_from_path(path).is_some() {
        Ok(file_name.to_string())
    } else {
        Err(DataFusionError::Plan(format!(
            "cannot determine Iceberg metadata version from path: {path}"
        )))
    }
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
    let (format_version, table_properties) =
        crate::properties::metadata_properties_from_table_properties(
            &commit_info.table_properties,
        )?;
    let format_version = format_version.max(format_version_for_schema(&iceberg_schema));

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
        format_version,
        crate::spec::ManifestContentType::Data,
    );
    let row_lineage_start_row_id = (format_version >= FormatVersion::V3).then_some(0);

    // Use SnapshotProducer in bootstrap mode
    let producer = SnapshotProducer::new(
        &tx,
        commit_info.data_files.clone(),
        Some(store_ctx.clone()),
        Some(manifest_meta),
    )
    .with_bootstrap(true)
    .with_row_lineage_start_row_id(row_lineage_start_row_id)
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

    // Build minimal TableMetadata, using v3 when the schema requires v3 types.
    let commit_timestamp_ms = crate::utils::timestamp::monotonic_timestamp_ms();
    let table_meta = TableMetadata {
        format_version,
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
        properties: table_properties,
        current_snapshot_id: Some(snapshot.snapshot_id()),
        next_row_id: snapshot.added_rows.and_then(|added_rows| {
            row_lineage_start_row_id.map(|start_row_id| start_row_id + added_rows)
        }),
        encryption_keys: vec![],
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
    let mut table_meta = table_meta;
    table_meta.ensure_required_format_fields();

    // Write metadata v1 using the Hadoop table convention for path-based compatibility.
    let new_meta_json = table_meta
        .to_json()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let file_extension = metadata_file_extension_from_properties(&table_meta.properties)?;
    let new_meta_rel = format!("metadata/v1{file_extension}");
    let new_meta_bytes = encode_metadata_file(&new_meta_rel, &new_meta_json)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
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
    let format_version = table_meta
        .format_version
        .max(format_version_for_schema(&schema_iceberg));
    table_meta.format_version = format_version;
    let row_lineage_start_row_id = table_meta.row_lineage_start_row_id();

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
        format_version,
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
    .with_row_lineage_start_row_id(row_lineage_start_row_id)
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
    if let Some(added_rows) = snapshot.added_rows {
        table_meta.advance_next_row_id(added_rows);
    }

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
    let new_meta_json = table_meta
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
            let new_meta_bytes = encode_metadata_file(&rel_name, &new_meta_json)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let rel_path = object_store::path::Path::from(rel_name.as_str());
            store_ctx
                .prefixed
                .put(
                    &rel_path,
                    object_store::PutPayload::from(Bytes::from(new_meta_bytes)),
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let version_hint = in_place_version_hint_from_metadata_path(&rel_name)?;

            // Write version-hint
            let hint_path = object_store::path::Path::from("metadata/version-hint.text");
            store_ctx
                .prefixed
                .put(
                    &hint_path,
                    object_store::PutPayload::from(Bytes::from(version_hint.into_bytes())),
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }
        PersistStrategy::NewVersion => {
            // Create a new metadata version
            let version = metadata_file_version_from_path(latest_meta_path)
                .map(|version| version + 1)
                .unwrap_or_else(|| table_meta.metadata_log.len() as i32 + 1);
            let file_extension = metadata_file_extension_from_properties(&table_meta.properties)?;
            let new_meta_rel = format!("metadata/v{version}{file_extension}");
            let new_meta_bytes = encode_metadata_file(&new_meta_rel, &new_meta_json)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_hint_from_metadata_path_uses_numeric_version() -> Result<()> {
        assert_eq!(
            version_hint_from_metadata_path("metadata/v1.metadata.json")?,
            "1"
        );
        assert_eq!(
            version_hint_from_metadata_path("metadata/v2.gz.metadata.json")?,
            "2"
        );
        Ok(())
    }

    #[test]
    fn in_place_version_hint_matches_metadata_naming_convention() -> Result<()> {
        assert_eq!(
            in_place_version_hint_from_metadata_path("metadata/v1.metadata.json")?,
            "1"
        );
        assert_eq!(
            in_place_version_hint_from_metadata_path("metadata/00003-1234.metadata.json")?,
            "00003-1234.metadata.json"
        );
        Ok(())
    }
}
