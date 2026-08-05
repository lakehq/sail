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
use crate::operations::{PreparedSnapshotCommit, SnapshotProducer, Transaction};
use crate::physical_plan::commit::IcebergCommitInfo;
use crate::spec::metadata::table_metadata::SnapshotLog;
use crate::spec::partition::PartitionSpec;
use crate::spec::schema::Schema as IcebergSchema;
use crate::spec::snapshots::{MAIN_BRANCH, SnapshotBuilder, SnapshotReference, SnapshotRetention};
use crate::spec::{FormatVersion, SortOrder, TableMetadata};
use crate::table::metadata_loader::{
    encode_metadata_file, metadata_file_extension_from_properties, metadata_file_version_from_path,
    write_version_hint,
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

fn initial_metadata_version(metadata_style: NewTableMetadataStyle) -> i32 {
    match metadata_style {
        NewTableMetadataStyle::Hadoop => 1,
        NewTableMetadataStyle::Uuid => 0,
    }
}

async fn write_metadata_version(
    store_ctx: &StoreContext,
    table_metadata: TableMetadata,
    version: i32,
    metadata_style: NewTableMetadataStyle,
) -> Result<BootstrapResult> {
    let metadata_json = table_metadata
        .to_json()
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let file_extension = metadata_file_extension_from_properties(&table_metadata.properties)?;
    let (metadata_file, version_hint) = match metadata_style {
        NewTableMetadataStyle::Hadoop => (
            format!("metadata/v{version}{file_extension}"),
            version.to_string(),
        ),
        NewTableMetadataStyle::Uuid => {
            let file_name = format!("{version:05}-{}{}", uuid::Uuid::new_v4(), file_extension);
            (format!("metadata/{file_name}"), file_name)
        }
    };
    let metadata_bytes = encode_metadata_file(&metadata_file, &metadata_json)
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let metadata_path = object_store::path::Path::from(metadata_file.as_str());
    store_ctx
        .prefixed
        .put(
            &metadata_path,
            object_store::PutPayload::from(Bytes::from(metadata_bytes)),
        )
        .await
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    write_version_hint(&store_ctx.prefixed, &version_hint).await;

    Ok(BootstrapResult {
        table_metadata,
        metadata_file,
    })
}

pub(crate) async fn prepare_bootstrap_snapshot(
    table_url: &Url,
    store_ctx: &StoreContext,
    commit_info: &IcebergCommitInfo,
    table_metadata: &TableMetadata,
) -> Result<PreparedSnapshotCommit> {
    let iceberg_schema = table_metadata
        .current_schema()
        .cloned()
        .ok_or_else(|| DataFusionError::Plan("No current schema in table metadata".to_string()))?;

    let partition_spec = table_metadata
        .default_partition_spec()
        .cloned()
        .unwrap_or_else(PartitionSpec::unpartitioned_spec);
    let format_version = table_metadata
        .format_version
        .max(format_version_for_schema(&iceberg_schema));
    let mut row_lineage_metadata = table_metadata.clone();
    let row_lineage_start_row_id = row_lineage_metadata.row_lineage_start_row_id();

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

    let transaction = Transaction::new(
        table_url.to_string(),
        empty_snapshot,
        table_metadata.last_sequence_number,
    );
    let manifest_metadata = crate::spec::manifest::ManifestMetadata::new(
        Arc::new(iceberg_schema.clone()),
        iceberg_schema.schema_id(),
        partition_spec.clone(),
        format_version,
        crate::spec::ManifestContentType::Data,
    );

    let snapshot_producer = SnapshotProducer::new(
        &transaction,
        commit_info.data_files.clone(),
        Some(store_ctx.clone()),
        Some(manifest_metadata),
    )
    .with_bootstrap(true)
    .with_added_delete_files(commit_info.delete_files.clone())
    .with_partition_specs(table_metadata.partition_specs.clone())
    .with_row_lineage_start_row_id(row_lineage_start_row_id)
    .with_write_path_mode(WritePathMode::Absolute);

    snapshot_producer
        .prepare(commit_info.snapshot_update_kind)
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

    let transaction = Transaction::new(table_url.to_string(), empty_snapshot, 0);
    let manifest_metadata = crate::spec::manifest::ManifestMetadata::new(
        Arc::new(iceberg_schema.clone()),
        iceberg_schema.schema_id(),
        partition_spec.clone(),
        format_version,
        crate::spec::ManifestContentType::Data,
    );
    let row_lineage_start_row_id = (format_version >= FormatVersion::V3).then_some(0);

    // Use SnapshotProducer in bootstrap mode
    let snapshot_producer = SnapshotProducer::new(
        &transaction,
        commit_info.data_files.clone(),
        Some(store_ctx.clone()),
        Some(manifest_metadata),
    )
    .with_bootstrap(true)
    .with_added_delete_files(commit_info.delete_files.clone())
    .with_partition_specs(vec![partition_spec.clone()])
    .with_row_lineage_start_row_id(row_lineage_start_row_id)
    .with_write_path_mode(WritePathMode::Absolute);

    let prepared_snapshot = snapshot_producer
        .prepare(commit_info.snapshot_update_kind)
        .await
        .map_err(DataFusionError::Execution)?;

    let snapshot = match prepared_snapshot
        .action_commit()
        .updates()
        .iter()
        .find_map(|update| match update {
            crate::spec::catalog::TableUpdate::AddSnapshot { snapshot } => Some(snapshot.clone()),
            _ => None,
        }) {
        Some(snapshot) => snapshot,
        None => {
            prepared_snapshot.cleanup().await;
            return Err(DataFusionError::Plan(
                "No snapshot in bootstrap commit".to_string(),
            ));
        }
    };

    let commit_timestamp_ms = crate::utils::timestamp::monotonic_timestamp_ms();
    let mut table_metadata = TableMetadata {
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
    table_metadata.ensure_required_format_fields();

    let metadata_result = write_metadata_version(
        store_ctx,
        table_metadata,
        initial_metadata_version(metadata_style),
        metadata_style,
    )
    .await;
    if metadata_result.is_err() {
        prepared_snapshot.cleanup().await;
    }
    metadata_result
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

    let mut table_metadata = TableMetadata {
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
    table_metadata.ensure_required_format_fields();

    write_metadata_version(
        store_ctx,
        table_metadata,
        initial_metadata_version(metadata_style),
        metadata_style,
    )
    .await
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

    let mut replacement_metadata = previous_metadata.clone();
    replacement_metadata.format_version = format_version;
    replacement_metadata.location = table_url.to_string();
    replacement_metadata.last_updated_ms = commit_timestamp_ms;
    replacement_metadata.last_column_id = last_column_id;
    replacement_metadata.schemas.push(iceberg_schema.clone());
    replacement_metadata.current_schema_id = iceberg_schema.schema_id();
    replacement_metadata
        .partition_specs
        .push(partition_spec.clone());
    replacement_metadata.default_spec_id = partition_spec.spec_id();
    replacement_metadata.last_partition_id = last_partition_id;
    replacement_metadata.properties = table_properties;
    replacement_metadata.current_snapshot_id = Some(-1);
    replacement_metadata.next_row_id = next_row_id;
    replacement_metadata
        .metadata_log
        .push(crate::spec::metadata::table_metadata::MetadataLog {
            timestamp_ms: previous_metadata.last_updated_ms,
            metadata_file: latest_meta_path.to_string(),
        });
    replacement_metadata.refs.remove(MAIN_BRANCH);
    if format_version >= FormatVersion::V2 {
        if !replacement_metadata
            .sort_orders
            .iter()
            .any(|sort_order| sort_order.order_id == 0)
        {
            replacement_metadata
                .sort_orders
                .push(SortOrder::unsorted_order());
        }
        replacement_metadata.default_sort_order_id = Some(0);
    } else {
        replacement_metadata.default_sort_order_id = None;
    }
    replacement_metadata.ensure_required_format_fields();

    let version = metadata_file_version_from_path(latest_meta_path)
        .map(|version| version + 1)
        .unwrap_or_else(|| replacement_metadata.metadata_log.len() as i32 + 1);
    write_metadata_version(store_ctx, replacement_metadata, version, metadata_style).await
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
    mut table_metadata: TableMetadata,
    latest_meta_path: &str,
    previous_metadata_file: Option<&str>,
    persist_strategy: PersistStrategy,
) -> Result<BootstrapResult> {
    let iceberg_schema = table_metadata
        .current_schema()
        .cloned()
        .ok_or_else(|| DataFusionError::Plan("No current schema in table metadata".to_string()))?;
    let format_version = table_metadata
        .format_version
        .max(format_version_for_schema(&iceberg_schema));
    table_metadata.format_version = format_version;
    let prepared_snapshot =
        prepare_bootstrap_snapshot(table_url, store_ctx, commit_info, &table_metadata).await?;

    let snapshot = match prepared_snapshot
        .action_commit()
        .updates()
        .iter()
        .find_map(|update| match update {
            crate::spec::catalog::TableUpdate::AddSnapshot { snapshot } => Some(snapshot.clone()),
            _ => None,
        }) {
        Some(snapshot) => snapshot,
        None => {
            prepared_snapshot.cleanup().await;
            return Err(DataFusionError::Plan(
                "No snapshot in bootstrap commit".to_string(),
            ));
        }
    };

    let previous_metadata_timestamp_ms = table_metadata.last_updated_ms;
    let commit_timestamp_ms = crate::utils::timestamp::monotonic_timestamp_ms();
    table_metadata.current_snapshot_id = Some(snapshot.snapshot_id());
    table_metadata.snapshots.push(snapshot.clone());
    table_metadata.snapshot_log.push(SnapshotLog {
        timestamp_ms: commit_timestamp_ms,
        snapshot_id: snapshot.snapshot_id(),
    });
    table_metadata
        .metadata_log
        .push(crate::spec::metadata::table_metadata::MetadataLog {
            timestamp_ms: previous_metadata_timestamp_ms,
            metadata_file: previous_metadata_file
                .unwrap_or(latest_meta_path)
                .to_string(),
        });
    table_metadata.last_sequence_number = snapshot.sequence_number();
    table_metadata.last_updated_ms = commit_timestamp_ms;
    if let Some(added_rows) = snapshot.added_rows {
        table_metadata.advance_next_row_id(added_rows);
    }

    table_metadata
        .refs
        .entry(MAIN_BRANCH.to_string())
        .or_insert_with(|| SnapshotReference {
            snapshot_id: snapshot.snapshot_id(),
            retention: SnapshotRetention::Branch {
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
            },
        });

    let version = metadata_file_version_from_path(latest_meta_path)
        .map(|version| version + 1)
        .unwrap_or_else(|| table_metadata.metadata_log.len() as i32 + 1);
    let metadata_style = match persist_strategy {
        PersistStrategy::NewVersion => NewTableMetadataStyle::Hadoop,
        PersistStrategy::NewUuidVersion => NewTableMetadataStyle::Uuid,
    };
    let metadata_result =
        write_metadata_version(store_ctx, table_metadata, version, metadata_style).await;
    if metadata_result.is_err() {
        prepared_snapshot.cleanup().await;
    }
    metadata_result
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use std::ops::Range;

    use futures::TryStreamExt;
    use futures::stream::BoxStream;
    use object_store::path::Path;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };

    use super::*;
    use crate::spec::{
        BlobMetadata, NestedField, NullOrder, Operation, PartitionStatisticsFile, PrimitiveType,
        SortDirection, SortField, SortOrder, StatisticsFile, Transform, Type,
    };

    #[derive(Debug)]
    struct SuffixRejectingStore {
        memory_store: Arc<object_store::memory::InMemory>,
        rejected_suffix: &'static str,
    }

    impl std::fmt::Display for SuffixRejectingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "SuffixRejectingStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for SuffixRejectingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            if location.as_ref().ends_with(self.rejected_suffix) {
                return Err(object_store::Error::Generic {
                    store: "fail-put",
                    source: Box::new(std::io::Error::other("injected put failure")),
                });
            }
            self.memory_store.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.memory_store.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.memory_store.get_opts(location, options).await
        }

        async fn get_ranges(
            &self,
            location: &Path,
            ranges: &[Range<u64>],
        ) -> object_store::Result<Vec<Bytes>> {
            self.memory_store.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.memory_store.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.memory_store.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.memory_store.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.memory_store.copy_opts(from, to, options).await
        }
    }

    #[test]
    fn bootstrap_metadata_ignores_version_hint_failure() {
        futures::executor::block_on(async {
            let table_url = Url::parse("file:///tmp/version-hint-failure/").expect("table URL");
            let memory_store = Arc::new(object_store::memory::InMemory::new());
            let store: Arc<dyn ObjectStore> = Arc::new(SuffixRejectingStore {
                memory_store,
                rejected_suffix: "metadata/version-hint.text",
            });
            let store_ctx = StoreContext::new(store, &table_url).expect("store context");
            let schema = IcebergSchema::builder()
                .with_schema_id(1)
                .with_fields([Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                ))])
                .build()
                .expect("schema");

            let bootstrap = bootstrap_empty_table_metadata(
                &table_url,
                &store_ctx,
                schema,
                PartitionSpec::builder().with_spec_id(1).build(),
                &[("format-version".to_string(), "2".to_string())],
                NewTableMetadataStyle::Hadoop,
            )
            .await
            .expect("metadata publication must not depend on the version hint");

            store_ctx
                .prefixed
                .head(&Path::from(bootstrap.metadata_file))
                .await
                .expect("published metadata file");
        });
    }

    #[test]
    fn bootstrap_metadata_failure_cleans_snapshot_artifacts() {
        futures::executor::block_on(async {
            let table_url =
                Url::parse("file:///tmp/bootstrap-metadata-failure/").expect("table URL");
            let memory_store = Arc::new(object_store::memory::InMemory::new());
            let store: Arc<dyn ObjectStore> = Arc::new(SuffixRejectingStore {
                memory_store: Arc::clone(&memory_store),
                rejected_suffix: ".metadata.json",
            });
            let store_ctx = StoreContext::new(store, &table_url).expect("store context");
            let schema = IcebergSchema::builder()
                .with_schema_id(1)
                .with_fields([Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                ))])
                .build()
                .expect("schema");
            let commit_info = IcebergCommitInfo {
                table_uri: table_url.to_string(),
                row_count: 0,
                data_files: vec![],
                delete_files: vec![],
                manifest_path: String::new(),
                manifest_list_path: String::new(),
                updates: vec![],
                requirements: vec![],
                table_properties: vec![("format-version".to_string(), "2".to_string())],
                lakehouse_table: None,
                snapshot_update_kind: crate::operations::SnapshotUpdateKind::FastAppend,
                schema: Some(schema),
                partition_spec: Some(PartitionSpec::builder().with_spec_id(1).build()),
            };

            let result = bootstrap_new_table_with_style(
                &table_url,
                &store_ctx,
                &commit_info,
                NewTableMetadataStyle::Hadoop,
            )
            .await;

            assert!(result.is_err());
            let remaining = memory_store
                .list(None)
                .try_collect::<Vec<_>>()
                .await
                .expect("list objects after failed bootstrap");
            assert!(remaining.is_empty(), "remaining objects: {remaining:?}");
        });
    }

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
            assert!(
                previous
                    .sort_orders
                    .iter()
                    .all(|sort_order| replacement.sort_orders.contains(sort_order))
            );
            assert_eq!(replacement.default_sort_order_id, Some(0));
            assert!(
                replacement
                    .sort_orders
                    .iter()
                    .any(|sort_order| sort_order.order_id == 0 && sort_order.fields.is_empty())
            );
            assert_eq!(replacement.statistics, previous.statistics);
            assert_eq!(
                replacement.partition_statistics,
                previous.partition_statistics
            );
        });
    }
}
