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
use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use datafusion_common::{DataFusionError, Result};
use object_store::ObjectStoreExt;
use url::Url;

use crate::io::StoreContext;
use crate::operations::helpers::format_version_for_schema;
use crate::operations::{ActionCommit, SnapshotProducer, Transaction};
use crate::physical_plan::commit::IcebergCommitInfo;
use crate::spec::metadata::table_metadata::SnapshotLog;
use crate::spec::partition::PartitionSpec;
use crate::spec::schema::Schema as IcebergSchema;
use crate::spec::snapshots::{MAIN_BRANCH, SnapshotBuilder, SnapshotReference, SnapshotRetention};
use crate::spec::{FormatVersion, TableMetadata};
use crate::table::metadata_loader::{
    encode_metadata_file, metadata_file_extension_from_properties, metadata_file_version_from_path,
};
use crate::utils::WritePathMode;

/// Strategy for persisting metadata during bootstrap
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistStrategy {
    /// Generate and write a new version of the metadata file (standard Iceberg approach)
    NewVersion,
    /// Generate and write a new UUID-style metadata file for catalog-backed tables.
    NewUuidVersion,
}

/// Metadata file naming style for bootstrapping a table with no metadata files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NewTableMetadataStyle {
    /// Hadoop/path table convention: `metadata/v1.metadata.json`.
    Hadoop,
    /// Catalog table convention: `metadata/00000-<uuid>.metadata.json`.
    Uuid,
}

#[derive(Debug)]
pub struct BootstrapResult {
    pub table_metadata: TableMetadata,
    pub metadata_file: String,
}

pub(crate) async fn bootstrap_snapshot_action_commit(
    table_url: &Url,
    store_ctx: &StoreContext,
    commit_info: &IcebergCommitInfo,
    table_meta: &TableMetadata,
) -> Result<ActionCommit> {
    let mut table_meta = table_meta.clone();
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
    let row_lineage_start_row_id = table_meta.row_lineage_start_row_id();

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

    let tx = Transaction::new(
        table_url.to_string(),
        empty_snapshot,
        table_meta.last_sequence_number,
    );
    let manifest_meta = crate::spec::manifest::ManifestMetadata::new(
        Arc::new(schema_iceberg.clone()),
        schema_iceberg.schema_id(),
        partition_spec.clone(),
        format_version,
        crate::spec::ManifestContentType::Data,
    );

    let producer = SnapshotProducer::new(
        &tx,
        commit_info.data_files.clone(),
        Some(store_ctx.clone()),
        Some(manifest_meta),
    )
    .with_bootstrap(true)
    .with_added_delete_files(commit_info.delete_files.clone())
    .with_partition_specs(table_meta.partition_specs.clone())
    .with_row_lineage_start_row_id(row_lineage_start_row_id)
    .with_write_path_mode(WritePathMode::Absolute);

    producer
        .commit(commit_info.snapshot_update_kind)
        .await
        .map_err(DataFusionError::Execution)
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
    Ok(bootstrap_new_table_with_style(
        table_url,
        store_ctx,
        commit_info,
        NewTableMetadataStyle::Hadoop,
    )
    .await?
    .table_metadata)
}

pub async fn bootstrap_new_table_with_style(
    table_url: &Url,
    store_ctx: &StoreContext,
    commit_info: &IcebergCommitInfo,
    metadata_style: NewTableMetadataStyle,
) -> Result<BootstrapResult> {
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

    let tx = Transaction::new(table_url.to_string(), empty_snapshot, 0);
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
    .with_added_delete_files(commit_info.delete_files.clone())
    .with_partition_specs(vec![partition_spec.clone()])
    .with_row_lineage_start_row_id(row_lineage_start_row_id)
    .with_write_path_mode(WritePathMode::Absolute);

    let action_commit = producer
        .commit(commit_info.snapshot_update_kind)
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
        last_sequence_number: snapshot.sequence_number(),
        last_updated_ms: commit_timestamp_ms,
        last_column_id: iceberg_schema.highest_field_id(),
        schemas: vec![iceberg_schema.clone()],
        current_schema_id: iceberg_schema.schema_id(),
        partition_specs: vec![partition_spec.clone()],
        default_spec_id: partition_spec.spec_id(),
        last_partition_id: partition_spec.last_assigned_field_id(),
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

    // Write metadata using the selected table/catalog convention.
    let new_meta_json = table_meta
        .to_json()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let file_extension = metadata_file_extension_from_properties(&table_meta.properties)?;
    let (new_meta_rel, hint) = match metadata_style {
        NewTableMetadataStyle::Hadoop => (format!("metadata/v1{file_extension}"), "1".to_string()),
        NewTableMetadataStyle::Uuid => {
            let file = format!("00000-{}{}", uuid::Uuid::new_v4(), file_extension);
            (format!("metadata/{file}"), file)
        }
    };
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
            object_store::PutPayload::from(Bytes::from(hint.into_bytes())),
        )
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    Ok(BootstrapResult {
        table_metadata: table_meta,
        metadata_file: new_meta_rel,
    })
}

pub async fn bootstrap_empty_table_metadata(
    table_url: &Url,
    store_ctx: &StoreContext,
    iceberg_schema: IcebergSchema,
    partition_spec: PartitionSpec,
    table_properties: &[(String, String)],
    metadata_style: NewTableMetadataStyle,
) -> Result<BootstrapResult> {
    let (format_version, table_properties) =
        crate::properties::metadata_properties_from_table_properties(table_properties)?;
    let format_version = format_version.max(format_version_for_schema(&iceberg_schema));
    let commit_timestamp_ms = crate::utils::timestamp::monotonic_timestamp_ms();

    let mut table_meta = TableMetadata {
        format_version,
        table_uuid: None,
        location: table_url.to_string(),
        last_sequence_number: 0,
        last_updated_ms: commit_timestamp_ms,
        last_column_id: iceberg_schema.highest_field_id(),
        schemas: vec![iceberg_schema.clone()],
        current_schema_id: iceberg_schema.schema_id(),
        partition_specs: vec![partition_spec.clone()],
        default_spec_id: partition_spec.spec_id(),
        last_partition_id: partition_spec.last_assigned_field_id(),
        properties: table_properties,
        current_snapshot_id: Some(-1),
        next_row_id: (format_version >= FormatVersion::V3).then_some(0),
        encryption_keys: vec![],
        snapshots: vec![],
        snapshot_log: vec![],
        metadata_log: vec![],
        sort_orders: vec![],
        default_sort_order_id: None,
        refs: HashMap::new(),
        statistics: vec![],
        partition_statistics: vec![],
    };
    table_meta.ensure_required_format_fields();

    let new_meta_json = table_meta
        .to_json()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let file_extension = metadata_file_extension_from_properties(&table_meta.properties)?;
    let (new_meta_rel, hint) = match metadata_style {
        NewTableMetadataStyle::Hadoop => (format!("metadata/v1{file_extension}"), "1".to_string()),
        NewTableMetadataStyle::Uuid => {
            let file = format!("00000-{}{}", uuid::Uuid::new_v4(), file_extension);
            (format!("metadata/{file}"), file)
        }
    };
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

    let hint_path = object_store::path::Path::from("metadata/version-hint.text");
    store_ctx
        .prefixed
        .put(
            &hint_path,
            object_store::PutPayload::from(Bytes::from(hint.into_bytes())),
        )
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    Ok(BootstrapResult {
        table_metadata: table_meta,
        metadata_file: new_meta_rel,
    })
}

pub async fn replace_empty_table_metadata(
    table_url: &Url,
    store_ctx: &StoreContext,
    iceberg_schema: IcebergSchema,
    partition_spec: PartitionSpec,
    table_properties: &[(String, String)],
    previous_metadata: &TableMetadata,
    latest_meta_path: &str,
    metadata_style: NewTableMetadataStyle,
) -> Result<BootstrapResult> {
    let (requested_format_version, table_properties) =
        crate::properties::metadata_properties_from_table_properties(table_properties)?;
    let format_version = previous_metadata
        .format_version
        .max(requested_format_version)
        .max(format_version_for_schema(&iceberg_schema));
    let commit_timestamp_ms = crate::utils::timestamp::monotonic_timestamp_ms();

    let mut metadata_log = previous_metadata.metadata_log.clone();
    metadata_log.push(crate::spec::metadata::table_metadata::MetadataLog {
        timestamp_ms: previous_metadata.last_updated_ms,
        metadata_file: latest_meta_path.to_string(),
    });

    let next_row_id = if format_version >= FormatVersion::V3 {
        let mut row_lineage_metadata = previous_metadata.clone();
        row_lineage_metadata.format_version = format_version;
        row_lineage_metadata.row_lineage_start_row_id()
    } else {
        None
    };

    let last_column_id = previous_metadata
        .last_column_id
        .max(iceberg_schema.highest_field_id());
    let last_partition_id = previous_metadata
        .last_partition_id
        .max(partition_spec.last_assigned_field_id());
    let mut schemas = previous_metadata.schemas.clone();
    schemas.push(iceberg_schema.clone());
    let mut partition_specs = previous_metadata.partition_specs.clone();
    partition_specs.push(partition_spec.clone());

    let mut refs = previous_metadata.refs.clone();
    refs.remove(MAIN_BRANCH);
    let mut table_meta = TableMetadata {
        format_version,
        table_uuid: previous_metadata.table_uuid,
        location: table_url.to_string(),
        last_sequence_number: previous_metadata.last_sequence_number,
        last_updated_ms: commit_timestamp_ms,
        last_column_id,
        schemas,
        current_schema_id: iceberg_schema.schema_id(),
        partition_specs,
        default_spec_id: partition_spec.spec_id(),
        last_partition_id,
        properties: table_properties,
        current_snapshot_id: Some(-1),
        next_row_id,
        encryption_keys: previous_metadata.encryption_keys.clone(),
        snapshots: previous_metadata.snapshots.clone(),
        snapshot_log: previous_metadata.snapshot_log.clone(),
        metadata_log,
        sort_orders: previous_metadata.sort_orders.clone(),
        default_sort_order_id: previous_metadata.default_sort_order_id,
        refs,
        statistics: previous_metadata.statistics.clone(),
        partition_statistics: previous_metadata.partition_statistics.clone(),
    };
    table_meta.ensure_required_format_fields();

    let new_meta_json = table_meta
        .to_json()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let version = metadata_file_version_from_path(latest_meta_path)
        .map(|version| version + 1)
        .unwrap_or_else(|| table_meta.metadata_log.len() as i32 + 1);
    let file_extension = metadata_file_extension_from_properties(&table_meta.properties)?;
    let (new_meta_rel, hint) = match metadata_style {
        NewTableMetadataStyle::Hadoop => (
            format!("metadata/v{version}{file_extension}"),
            version.to_string(),
        ),
        NewTableMetadataStyle::Uuid => {
            let file = format!("{version:05}-{}{}", uuid::Uuid::new_v4(), file_extension);
            (format!("metadata/{file}"), file)
        }
    };
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

    let hint_path = object_store::path::Path::from("metadata/version-hint.text");
    store_ctx
        .prefixed
        .put(
            &hint_path,
            object_store::PutPayload::from(Bytes::from(hint.into_bytes())),
        )
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    Ok(BootstrapResult {
        table_metadata: table_meta,
        metadata_file: new_meta_rel,
    })
}

/// Bootstrap the first snapshot for an existing table that has no current snapshot
///
/// This is used when a table was created via CREATE TABLE but has no data yet.
/// The persist_strategy determines how the metadata is written:
/// - NewVersion: Creates a new metadata version (standard Iceberg)
/// - NewUuidVersion: Creates a new UUID-style metadata version for catalog-backed tables
pub async fn bootstrap_first_snapshot(
    table_url: &Url,
    store_ctx: &StoreContext,
    commit_info: &IcebergCommitInfo,
    mut table_meta: TableMetadata,
    latest_meta_path: &str,
    previous_metadata_file: Option<&str>,
    persist_strategy: PersistStrategy,
) -> Result<BootstrapResult> {
    let schema_iceberg = table_meta
        .current_schema()
        .cloned()
        .ok_or_else(|| DataFusionError::Plan("No current schema in table metadata".to_string()))?;
    let format_version = table_meta
        .format_version
        .max(format_version_for_schema(&schema_iceberg));
    table_meta.format_version = format_version;
    let action_commit =
        bootstrap_snapshot_action_commit(table_url, store_ctx, commit_info, &table_meta).await?;

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
    let previous_metadata_timestamp_ms = table_meta.last_updated_ms;
    let commit_timestamp_ms = crate::utils::timestamp::monotonic_timestamp_ms();
    table_meta.current_snapshot_id = Some(snapshot.snapshot_id());
    table_meta.snapshots.push(snapshot.clone());
    table_meta.snapshot_log.push(SnapshotLog {
        timestamp_ms: commit_timestamp_ms,
        snapshot_id: snapshot.snapshot_id(),
    });
    table_meta
        .metadata_log
        .push(crate::spec::metadata::table_metadata::MetadataLog {
            timestamp_ms: previous_metadata_timestamp_ms,
            metadata_file: previous_metadata_file
                .unwrap_or(latest_meta_path)
                .to_string(),
        });
    table_meta.last_sequence_number = snapshot.sequence_number();
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

    let metadata_file = match persist_strategy {
        PersistStrategy::NewVersion | PersistStrategy::NewUuidVersion => {
            // Create a new metadata version
            let version = metadata_file_version_from_path(latest_meta_path)
                .map(|version| version + 1)
                .unwrap_or_else(|| table_meta.metadata_log.len() as i32 + 1);
            let file_extension = metadata_file_extension_from_properties(&table_meta.properties)?;
            let (new_meta_rel, hint) = match persist_strategy {
                PersistStrategy::NewVersion => (
                    format!("metadata/v{version}{file_extension}"),
                    version.to_string(),
                ),
                PersistStrategy::NewUuidVersion => {
                    let file = format!("{version:05}-{}{}", uuid::Uuid::new_v4(), file_extension);
                    (format!("metadata/{file}"), file)
                }
            };
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
                    object_store::PutPayload::from(Bytes::from(hint.into_bytes())),
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            new_meta_rel
        }
    };

    Ok(BootstrapResult {
        table_metadata: table_meta,
        metadata_file,
    })
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use object_store::ObjectStore;

    use super::*;
    use crate::spec::{
        BlobMetadata, NestedField, NullOrder, Operation, PartitionStatisticsFile, PrimitiveType,
        SortDirection, SortField, SortOrder, StatisticsFile, Transform, Type,
    };

    #[test]
    fn replace_preserves_historical_snapshot_metadata_without_main_reference() {
        futures::executor::block_on(async {
            let table_url = Url::parse("file:///tmp/replaced-table/").expect("table URL");
            let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
            let store_ctx = StoreContext::new(store, &table_url).expect("store context");
            let original_schema = IcebergSchema::builder()
                .with_schema_id(1)
                .with_fields([Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                ))])
                .build()
                .expect("original schema");
            let original_spec = PartitionSpec::builder().with_spec_id(1).build();
            let table_properties = vec![("format-version".to_string(), "2".to_string())];
            let bootstrap = bootstrap_empty_table_metadata(
                &table_url,
                &store_ctx,
                original_schema,
                original_spec,
                &table_properties,
                NewTableMetadataStyle::Hadoop,
            )
            .await
            .expect("bootstrap metadata");

            let mut previous = bootstrap.table_metadata;
            let historical_snapshot = SnapshotBuilder::new()
                .with_snapshot_id(17)
                .with_sequence_number(4)
                .with_timestamp_ms(123)
                .with_manifest_list("file:///tmp/replaced-table/metadata/snap-17.avro")
                .with_summary(crate::spec::snapshots::Summary::new(Operation::Append))
                .with_schema_id(1)
                .build()
                .expect("historical snapshot");
            previous.last_sequence_number = historical_snapshot.sequence_number();
            previous.current_snapshot_id = Some(historical_snapshot.snapshot_id());
            previous.snapshots = vec![historical_snapshot.clone()];
            previous.snapshot_log = vec![SnapshotLog {
                timestamp_ms: historical_snapshot.timestamp_ms,
                snapshot_id: historical_snapshot.snapshot_id(),
            }];
            previous.refs.insert(
                MAIN_BRANCH.to_string(),
                SnapshotReference {
                    snapshot_id: historical_snapshot.snapshot_id(),
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            );
            previous.refs.insert(
                "before_replace".to_string(),
                SnapshotReference {
                    snapshot_id: historical_snapshot.snapshot_id(),
                    retention: SnapshotRetention::Tag {
                        max_ref_age_ms: Some(86_400_000),
                    },
                },
            );
            previous.sort_orders = vec![SortOrder {
                order_id: 7,
                fields: vec![SortField {
                    source_id: 1,
                    source_ids: vec![],
                    transform: Transform::Identity,
                    direction: SortDirection::Ascending,
                    null_order: NullOrder::Last,
                }],
            }];
            previous.default_sort_order_id = Some(7);
            previous.statistics = vec![StatisticsFile {
                snapshot_id: historical_snapshot.snapshot_id(),
                statistics_path: "file:///tmp/replaced-table/metadata/stats.puffin".to_string(),
                file_size_in_bytes: 101,
                file_footer_size_in_bytes: 11,
                key_metadata: Some("key".to_string()),
                blob_metadata: vec![BlobMetadata {
                    r#type: "apache-datasketches-theta-v1".to_string(),
                    snapshot_id: historical_snapshot.snapshot_id(),
                    sequence_number: historical_snapshot.sequence_number(),
                    fields: vec![1],
                    properties: HashMap::from([("ndv".to_string(), "3".to_string())]),
                }],
            }];
            previous.partition_statistics = vec![PartitionStatisticsFile {
                snapshot_id: historical_snapshot.snapshot_id(),
                statistics_path: "file:///tmp/replaced-table/metadata/partition-stats.parquet"
                    .to_string(),
                file_size_in_bytes: 202,
            }];

            let replacement_schema = IcebergSchema::builder()
                .with_schema_id(2)
                .with_fields([Arc::new(NestedField::required(
                    2,
                    "value",
                    Type::Primitive(PrimitiveType::String),
                ))])
                .build()
                .expect("replacement schema");
            let replacement_spec = PartitionSpec::builder().with_spec_id(2).build();
            let replacement = replace_empty_table_metadata(
                &table_url,
                &store_ctx,
                replacement_schema,
                replacement_spec,
                &table_properties,
                &previous,
                &bootstrap.metadata_file,
                NewTableMetadataStyle::Hadoop,
            )
            .await
            .expect("replacement metadata")
            .table_metadata;

            assert_eq!(replacement.current_snapshot_id, Some(-1));
            assert_eq!(replacement.last_sequence_number, 4);
            assert_eq!(replacement.snapshots, vec![historical_snapshot]);
            assert_eq!(replacement.snapshot_log.len(), 1);
            assert_eq!(replacement.snapshot_log[0].snapshot_id, 17);
            assert!(!replacement.refs.contains_key(MAIN_BRANCH));
            assert_eq!(
                replacement
                    .refs
                    .get("before_replace")
                    .map(|reference| reference.snapshot_id),
                Some(17)
            );
            assert_eq!(replacement.sort_orders, previous.sort_orders);
            assert_eq!(replacement.default_sort_order_id, Some(7));
            assert_eq!(replacement.statistics, previous.statistics);
            assert_eq!(
                replacement.partition_statistics,
                previous.partition_statistics
            );
        });
    }
}
