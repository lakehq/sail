use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::execution::TaskContext;
use datafusion_common::{DataFusionError, Result, plan_err};
use sail_catalog::error::CatalogError;
use sail_catalog::manager::CatalogManager;
use sail_common_datafusion::catalog::TableReplacement;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::datasource::type_converter::arrow_schema_to_iceberg;
use crate::io::StoreContext;
use crate::operations::bootstrap::prepare_bootstrap_snapshot;
use crate::operations::helpers::format_version_for_schema;
use crate::operations::metadata_commit::{
    MetadataFileCommit, MetadataFileStyle, MetadataWriteOutcome, PreviousMetadataFile,
    previous_metadata_log_entry,
};
use crate::physical_plan::commit::IcebergCommitInfo;
use crate::schema_evolution::SchemaEvolver;
use crate::spec::{
    FormatVersion, MAIN_BRANCH, NullOrder, SnapshotLog, SortDirection, SortField, SortOrder,
    TableMetadata, TableUpdate, Transform,
};
use crate::table::find_latest_metadata_file;
use crate::table::metadata_loader::metadata_file_version_from_path;

/// Coordinator-only private replacement state. Workers receive only the resulting writer schema,
/// spec, properties and order, not the table's history or a live catalog reservation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergTableReplacement {
    pub catalog: TableReplacement,
    pub base_metadata_file: Option<String>,
    pub metadata: TableMetadata,
}

pub(crate) fn prepare_replacement(
    table_url: &Url,
    mut catalog: TableReplacement,
    base_metadata_file: Option<String>,
    base: Option<&TableMetadata>,
    input: &ArrowSchema,
) -> Result<IcebergTableReplacement> {
    if catalog.existing != base.is_some() {
        return plan_err!("Iceberg replacement catalog/storage existence mismatch");
    }
    let input = arrow_schema_to_iceberg(input)?;
    let schema = match base {
        Some(base) => SchemaEvolver::assign_replacement_schema_ids(&input, base)?,
        None => SchemaEvolver::assign_schema_field_ids(&input)?,
    };
    let requested_spec =
        crate::lake_source::create_table_partition_spec(&schema, &catalog.partition_by)?;
    let spec = match base {
        Some(base) => requested_spec
            .assign_ids(base)
            .map_err(DataFusionError::Plan)?,
        None => requested_spec,
    };
    // A v1 spec retains void fields; removing their source columns cannot produce a valid v1 spec.
    for field in spec.fields() {
        let source = schema.field_by_id(field.source_id).ok_or_else(|| {
            DataFusionError::Plan(
                "replacement partition source is absent from the new schema".into(),
            )
        })?;
        field
            .transform
            .result_type(&source.field_type)
            .map_err(DataFusionError::Plan)?;
    }
    let table_properties = catalog
        .property_updates
        .iter()
        .filter(|(key, _)| !key.starts_with("option."))
        .cloned()
        .collect::<Vec<_>>();
    let (requested_version, properties) =
        crate::properties::metadata_properties_from_table_properties(&table_properties)?;
    let explicit_version = table_properties
        .iter()
        .any(|(key, _)| key == "format-version");
    let format_version = match base {
        Some(base) if explicit_version && requested_version < base.format_version => {
            return plan_err!(
                "cannot downgrade Iceberg table format version from v{} to v{}",
                base.format_version as i32,
                requested_version as i32
            );
        }
        Some(base) if !explicit_version => base.format_version,
        _ => requested_version,
    }
    .max(format_version_for_schema(&schema));
    let mut metadata = base.cloned().unwrap_or_else(|| TableMetadata {
        format_version,
        table_uuid: None,
        location: table_url.to_string(),
        last_sequence_number: 0,
        last_updated_ms: crate::utils::timestamp::monotonic_timestamp_ms(),
        last_column_id: 0,
        schemas: vec![],
        current_schema_id: 0,
        partition_specs: vec![],
        default_spec_id: 0,
        last_partition_id: 999,
        properties: HashMap::new(),
        current_snapshot_id: None,
        next_row_id: None,
        encryption_keys: vec![],
        snapshots: vec![],
        snapshot_log: vec![],
        metadata_log: vec![],
        sort_orders: vec![],
        default_sort_order_id: None,
        refs: HashMap::new(),
        statistics: vec![],
        partition_statistics: vec![],
    });
    metadata.format_version = format_version;
    metadata.properties.extend(properties);
    for key in ["write.format.default", "write.delete.format.default"] {
        if let Some(format) = metadata.properties.get(key)
            && !format.eq_ignore_ascii_case("parquet")
        {
            return plan_err!(
                "unsupported Iceberg replacement file format {format} for {key}; only parquet is supported"
            );
        }
    }
    crate::properties::validate_write_properties(&metadata.properties)?;
    // Spark binds every historical sort order against the replacement schema when writing,
    // even if the new default is unsorted. Keep history intact and reject unbindable changes.
    for order in &metadata.sort_orders {
        for field in &order.fields {
            let source_ids = if field.source_ids.is_empty() {
                std::slice::from_ref(&field.source_id)
            } else {
                &field.source_ids
            };
            for source_id in source_ids {
                let source = schema.field_by_id(*source_id).ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "unsupported Iceberg replacement: historical sort order {} source {source_id} is absent from the new schema",
                        order.order_id
                    ))
                })?;
                if !matches!(source.field_type.as_ref(), crate::spec::Type::Primitive(_)) {
                    return plan_err!(
                        "unsupported Iceberg replacement: historical sort order {} source {source_id} must remain primitive",
                        order.order_id
                    );
                }
                field.transform.result_type(&source.field_type).map_err(|error| {
                    DataFusionError::Plan(format!(
                        "unsupported Iceberg replacement: historical sort order {} cannot bind source {source_id}: {error}",
                        order.order_id
                    ))
                })?;
            }
        }
    }
    metadata.last_column_id = metadata.last_column_id.max(schema.highest_field_id());
    metadata.current_schema_id = schema.schema_id();
    if !metadata
        .schemas
        .iter()
        .any(|old| old.schema_id() == schema.schema_id())
    {
        metadata.schemas.push(schema.clone());
    }
    metadata.default_spec_id = spec.spec_id();
    metadata.last_partition_id = metadata
        .last_partition_id
        .max(spec.last_assigned_field_id());
    if !metadata
        .partition_specs
        .iter()
        .any(|old| old.spec_id() == spec.spec_id())
    {
        metadata.partition_specs.push(spec);
    }
    let fields = catalog
        .sort_by
        .iter()
        .map(|sort| {
            let source_id = schema.field_id_by_name(&sort.column).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "replacement sort column not found: {}",
                    sort.column
                ))
            })?;
            Ok(SortField {
                source_id,
                source_ids: vec![],
                transform: Transform::Identity,
                direction: if sort.ascending {
                    SortDirection::Ascending
                } else {
                    SortDirection::Descending
                },
                null_order: NullOrder::Last,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let order_id = if fields.is_empty() {
        0
    } else {
        metadata
            .sort_orders
            .iter()
            .find(|order| order.fields == fields)
            .map(|order| order.order_id)
            .unwrap_or_else(|| {
                metadata
                    .sort_orders
                    .iter()
                    .map(|order| order.order_id)
                    .max()
                    .unwrap_or(0)
                    + 1
            })
    };
    if !metadata
        .sort_orders
        .iter()
        .any(|order| order.order_id == order_id)
    {
        metadata.sort_orders.push(SortOrder { order_id, fields });
    }
    metadata.default_sort_order_id = Some(order_id as i32);
    metadata.current_snapshot_id = None;
    metadata.refs.remove(MAIN_BRANCH);
    if metadata.format_version >= FormatVersion::V3 {
        metadata.row_lineage_start_row_id();
    }
    metadata.ensure_required_format_fields();
    // Preserve only catalog options from bookkeeping, never stale table properties. The
    // authoritative metadata plus explicit updates defines the installed catalog status.
    let mut effective_properties = catalog
        .properties
        .iter()
        .chain(catalog.property_updates.iter())
        .filter(|(key, _)| key.starts_with("option."))
        .cloned()
        .collect::<HashMap<_, _>>();
    effective_properties.extend(metadata.properties.clone());
    effective_properties.insert(
        "format-version".into(),
        (metadata.format_version as i32).to_string(),
    );
    catalog.properties = effective_properties.into_iter().collect();
    catalog.properties.sort();
    Ok(IcebergTableReplacement {
        catalog,
        base_metadata_file,
        metadata,
    })
}

/// Reject a changed base rather than retrying writer outputs with potentially invalid IDs.
async fn validate_base(
    store: &StoreContext,
    table_url: &Url,
    expected: Option<&str>,
) -> Result<()> {
    let actual = match find_latest_metadata_file(&store.base, table_url).await {
        Ok(path) => Some(path),
        Err(error) if error.to_string().contains("No metadata files found") => None,
        Err(error) => return Err(error),
    };
    if actual.as_deref() != expected {
        return plan_err!(
            "Iceberg replacement conflict: base metadata changed; replan the operation"
        );
    }
    Ok(())
}

pub(crate) async fn commit_replacement(
    context: &Arc<TaskContext>,
    table_url: &Url,
    store: &StoreContext,
    replacement: IcebergTableReplacement,
    commit: &IcebergCommitInfo,
) -> Result<()> {
    validate_base(store, table_url, replacement.base_metadata_file.as_deref()).await?;
    let mut metadata = replacement.metadata;
    let mut snapshot = prepare_bootstrap_snapshot(table_url, store, commit, &metadata).await?;
    let previous = replacement
        .base_metadata_file
        .as_ref()
        .map(|file| {
            previous_metadata_log_entry(
                table_url,
                PreviousMetadataFile::ObjectPath(file),
                metadata.last_updated_ms,
            )
        })
        .transpose()?;
    for update in snapshot.action_commit().updates() {
        match update {
            TableUpdate::AddSnapshot { snapshot } => {
                metadata.last_sequence_number = snapshot.sequence_number();
                if let Some(rows) = snapshot.added_rows {
                    metadata.advance_next_row_id(rows);
                }
                metadata.snapshots.push(snapshot.clone());
            }
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } => {
                if ref_name == MAIN_BRANCH {
                    metadata.current_snapshot_id = Some(reference.snapshot_id);
                    metadata.snapshot_log.push(SnapshotLog {
                        timestamp_ms: crate::utils::timestamp::monotonic_timestamp_ms(),
                        snapshot_id: reference.snapshot_id,
                    });
                }
                metadata.refs.insert(ref_name.clone(), reference.clone());
            }
            _ => return plan_err!("unexpected Iceberg replacement snapshot update"),
        }
    }
    if let Some(previous) = previous {
        metadata.metadata_log.push(previous);
    }
    metadata.last_updated_ms = crate::utils::timestamp::monotonic_timestamp_ms();
    let version = replacement
        .base_metadata_file
        .as_deref()
        .and_then(metadata_file_version_from_path)
        .map_or(1, |version| version + 1);
    let publication =
        MetadataFileCommit::prepare(table_url, &metadata, version, MetadataFileStyle::Versioned)?;
    let mut catalog = replacement.catalog;
    // Metadata pointer properties are informational for this path-managed catalog, but must
    // describe the same version as the installed definition.
    catalog.properties.retain(|(key, _)| {
        !sail_common_datafusion::catalog::managed::is_metadata_location_key(key)
    });
    catalog
        .properties
        .push(("metadata_location".into(), publication.location().into()));
    let manager = context.extension::<CatalogManager>()?;
    let table_url = table_url.clone();
    let store = store.clone();
    let base = replacement.base_metadata_file;
    manager
        .publish_table_replacement(
            catalog,
            Box::pin(async move {
                validate_base(&store, &table_url, base.as_deref()).await?;
                match publication.replacement_version_exists(&store).await {
                    Ok(false) => {}
                    Ok(true) => {
                        snapshot.cleanup().await;
                        return Err(CatalogError::Conflict(
                            "Iceberg replacement metadata version already exists".into(),
                        ));
                    }
                    Err(error) => {
                        snapshot.cleanup().await;
                        return Err(error.into());
                    }
                }
                snapshot.publication_started();
                match publication.write_replacement(&store).await {
                    Ok(MetadataWriteOutcome::Written) => {
                        snapshot.commit_succeeded();
                        publication.write_hint(&store).await;
                        Ok(())
                    }
                    Ok(MetadataWriteOutcome::Conflict) => {
                        snapshot.publication_did_not_happen();
                        snapshot.cleanup().await;
                        Err(CatalogError::Conflict(
                            "Iceberg replacement metadata version already exists".into(),
                        ))
                    }
                    Err(error) => Err(CatalogError::CommitStateUnknown(error.to_string().into())),
                }
            }),
        )
        .await
        .map_err(|error| DataFusionError::External(Box::new(error)))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use bytes::Bytes;
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::prelude::{SessionConfig, SessionContext};
    use futures::TryStreamExt;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt};
    use sail_catalog::manager::CatalogManagerOptions;
    use sail_catalog::provider::{
        CatalogProvider, CreateTableColumnOptions, CreateTableMode, CreateTableOptions,
        DropTableOptions, Namespace,
    };
    use sail_catalog_memory::MemoryCatalogProvider;
    use sail_common_datafusion::catalog::TableKind;

    use super::*;
    use crate::io::{load_manifest, load_manifest_list};
    use crate::operations::SnapshotUpdateKind;
    use crate::operations::replacement_test_support::{ReplacementFault, ReplacementStore};
    use crate::spec::{DataContentType, DataFile, DataFileFormat};

    fn catalog(existing: bool) -> TableReplacement {
        TableReplacement {
            catalog_table: vec!["memory".into(), "db".into(), "t".into()],
            catalog_generation: 7,
            database_generation: 8,
            table_generation: 9,
            require_existing: existing,
            existing,
            columns: vec![("replacement".into(), DataType::Int64, false)],
            location: "memory:///table".into(),
            partition_by: vec![],
            sort_by: vec![],
            properties: vec![("owner".into(), "old".into())],
            property_updates: vec![("owner".into(), "new".into())],
            is_external: true,
        }
    }

    fn historical_metadata() -> TableMetadata {
        serde_json::from_value(serde_json::json!({
            "format-version": 3, "table-uuid": "12345678-1234-1234-1234-123456789abc",
            "location": "memory:///table", "last-sequence-number": 4,
            "last-updated-ms": 100, "last-column-id": 10, "current-schema-id": 2,
            "schemas": [{"type":"struct", "schema-id":2,"fields":[{"id":10,"name":"old","required":false,"type":"int"}]}],
            "partition-specs": [{"spec-id":3,"fields":[{"source-id":10,"field-id":1002,"name":"old","transform":"identity"}]}],
            "default-spec-id":3,"last-partition-id":1002,
            "properties":{"retained":"yes","owner":"old"},
            "current-snapshot-id":42,"next-row-id":123,
            "snapshots":[{"snapshot-id":42,"sequence-number":4,"timestamp-ms":100,"manifest-list":"memory:///table/metadata/old.avro","summary":{"operation":"append"},"schema-id":2,"first-row-id":0,"added-rows":123}],
            "snapshot-log":[{"timestamp-ms":100,"snapshot-id":42}],
            "metadata-log":[{"timestamp-ms":90,"metadata-file":"memory:///table/metadata/v1.metadata.json"}],
            "sort-orders":[{"order-id":4,"fields":[{"source-id":10,"transform":"identity","direction":"asc","null-order":"nulls-first"}]}],
            "default-sort-order-id":4,
            "refs":{"main":{"snapshot-id":42,"type":"branch"},"audit":{"snapshot-id":42,"type":"branch"},"frozen":{"snapshot-id":42,"type":"tag"}}
        })).unwrap()
    }

    #[test]
    fn replacement_private_metadata_preserves_history_and_resets_defaults() {
        let base = historical_metadata();
        let before = serde_json::to_value(&base).unwrap();
        let input = ArrowSchema::new(vec![
            Field::new("replacement", DataType::Int64, false),
            Field::new("old", DataType::Int32, true),
        ]);
        let staged = prepare_replacement(
            &Url::parse("memory:///table").unwrap(),
            catalog(true),
            Some("table/metadata/v2.metadata.json".into()),
            Some(&base),
            &input,
        )
        .unwrap();
        let metadata = staged.metadata;
        let after = serde_json::to_value(&metadata).unwrap();
        for key in [
            "table-uuid",
            "snapshots",
            "snapshot-log",
            "metadata-log",
            "next-row-id",
            "last-sequence-number",
            "location",
        ] {
            assert_eq!(after[key], before[key], "{key}");
        }
        assert_eq!(serde_json::to_value(&base).unwrap(), before);
        assert_eq!(metadata.current_snapshot_id, None);
        assert!(!metadata.refs.contains_key("main"));
        assert_eq!(after["refs"]["audit"], before["refs"]["audit"]);
        assert_eq!(after["refs"]["frozen"], before["refs"]["frozen"]);
        assert_eq!(after["schemas"][0], before["schemas"][0]);
        assert_eq!(after["partition-specs"][0], before["partition-specs"][0]);
        assert_eq!(after["sort-orders"][0], before["sort-orders"][0]);
        assert!(metadata.current_schema_id > base.current_schema_id);
        assert!(metadata.last_column_id > base.last_column_id);
        assert!(metadata.last_partition_id >= base.last_partition_id);
        assert!(
            metadata
                .partition_specs
                .iter()
                .find(|spec| spec.spec_id() == metadata.default_spec_id)
                .unwrap()
                .fields()
                .is_empty()
        );
        assert_eq!(metadata.default_sort_order_id, Some(0));
        assert_eq!(metadata.properties.get("retained").unwrap(), "yes");
        assert_eq!(metadata.properties.get("owner").unwrap(), "new");
        assert_eq!(
            staged.base_metadata_file.as_deref(),
            Some("table/metadata/v2.metadata.json")
        );
    }

    #[test]
    fn replacement_absent_empty_and_invalid_properties_are_private() {
        let url = Url::parse("memory:///table").unwrap();
        let input = ArrowSchema::new(vec![Field::new("replacement", DataType::Int64, false)]);
        let fresh = prepare_replacement(&url, catalog(false), None, None, &input).unwrap();
        assert!(fresh.metadata.snapshots.is_empty());
        assert!(fresh.metadata.table_uuid.is_some());
        assert!(fresh.metadata.current_snapshot_id.is_none());
        let empty_replacement = prepare_replacement(
            &url,
            catalog(true),
            Some("table/metadata/v1.metadata.json".into()),
            Some(&fresh.metadata),
            &input,
        )
        .unwrap();
        assert!(empty_replacement.metadata.snapshots.is_empty());
        assert_eq!(
            empty_replacement.metadata.table_uuid,
            fresh.metadata.table_uuid
        );
        let base = historical_metadata();
        let before = serde_json::to_value(&base).unwrap();
        assert!(prepare_replacement(&url, catalog(false), None, Some(&base), &input).is_err());
        assert!(prepare_replacement(&url, catalog(true), None, None, &input).is_err());
        for (key, value) in [("format-version", "garbage"), ("format-version", "99")] {
            let mut request = catalog(true);
            request.property_updates.push((key.into(), value.into()));
            assert!(
                prepare_replacement(&url, request, None, Some(&base), &input).is_err(),
                "{key}={value}"
            );
            assert_eq!(serde_json::to_value(&base).unwrap(), before);
        }
    }

    #[test]
    fn replacement_version_uses_authoritative_default_and_rejects_downgrades() {
        let url = Url::parse("memory:///table").unwrap();
        let input = ArrowSchema::new(vec![Field::new("replacement", DataType::Int64, false)]);
        let fresh = prepare_replacement(&url, catalog(false), None, None, &input)
            .unwrap()
            .metadata;
        assert_eq!(fresh.format_version, FormatVersion::V2);
        for version in [FormatVersion::V1, FormatVersion::V2, FormatVersion::V3] {
            let mut base = fresh.clone();
            base.format_version = version;
            for requested in [
                None,
                Some(FormatVersion::V1),
                Some(FormatVersion::V2),
                Some(FormatVersion::V3),
            ] {
                let mut request = catalog(true);
                request.property_updates.clear();
                // Registration bookkeeping is not an explicit version request.
                request
                    .properties
                    .push(("format-version".into(), "3".into()));
                if let Some(requested) = requested {
                    request
                        .property_updates
                        .push(("format-version".into(), (requested as i32).to_string()));
                }
                let before = serde_json::to_value(&base).unwrap();
                let result = prepare_replacement(&url, request, None, Some(&base), &input);
                if requested.is_some_and(|requested| requested < version) {
                    assert!(result.unwrap_err().to_string().contains("cannot downgrade"));
                } else {
                    let replacement = result.unwrap();
                    let expected = requested.unwrap_or(version);
                    assert_eq!(replacement.metadata.format_version, expected);
                    assert!(
                        !replacement
                            .metadata
                            .properties
                            .contains_key("format-version")
                    );
                    assert!(
                        replacement
                            .catalog
                            .properties
                            .contains(&("format-version".into(), (expected as i32).to_string()))
                    );
                }
                assert_eq!(serde_json::to_value(&base).unwrap(), before);
            }
        }
        // As on creation, schema-required versions are reflected in the effective catalog.
        let nanos = ArrowSchema::new(vec![Field::new(
            "replacement",
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None),
            false,
        )]);
        for existing in [false, true] {
            let replacement = prepare_replacement(
                &url,
                catalog(existing),
                None,
                existing.then_some(&fresh),
                &nanos,
            )
            .unwrap();
            assert_eq!(replacement.metadata.format_version, FormatVersion::V3);
            assert!(
                replacement
                    .catalog
                    .properties
                    .contains(&("format-version".into(), "3".into()))
            );
        }
    }

    #[test]
    fn replacement_applies_only_explicit_property_updates_to_authoritative_base() {
        let url = Url::parse("memory:///table").unwrap();
        let input = ArrowSchema::new(vec![Field::new("old", DataType::Int32, true)]);
        let mut base = historical_metadata();
        base.properties.insert("owner".into(), "external".into());
        for override_owner in [false, true] {
            let mut request = catalog(true);
            request.properties.extend([
                ("removed".into(), "stale".into()),
                ("option.example".into(), "preserved".into()),
                ("format-version".into(), "1".into()),
            ]);
            request.property_updates = if override_owner {
                vec![("owner".into(), "requested".into())]
            } else {
                vec![]
            };
            let replacement =
                prepare_replacement(&url, request, None, Some(&base), &input).unwrap();
            let expected = if override_owner {
                "requested"
            } else {
                "external"
            };
            assert_eq!(
                replacement.metadata.properties.get("owner").unwrap(),
                expected
            );
            assert!(!replacement.metadata.properties.contains_key("removed"));
            assert!(
                !replacement
                    .catalog
                    .properties
                    .iter()
                    .any(|(key, _)| key == "removed")
            );
            assert!(
                replacement
                    .catalog
                    .properties
                    .contains(&("owner".into(), expected.into()))
            );
            assert!(
                replacement
                    .catalog
                    .properties
                    .contains(&("option.example".into(), "preserved".into()))
            );
            assert!(
                replacement
                    .catalog
                    .properties
                    .contains(&("format-version".into(), "3".into()))
            );
            assert!(
                !replacement
                    .metadata
                    .properties
                    .contains_key("option.example")
            );
        }
    }

    #[test]
    fn replacement_rejects_unbindable_historical_sort_orders_without_changing_history() {
        let url = Url::parse("memory:///table").unwrap();
        let mut base = historical_metadata();
        base.sort_orders[0].fields[0].transform = Transform::Bucket(16);
        base.sort_orders.push(SortOrder::unsorted_order());
        base.default_sort_order_id = Some(0);
        let before = serde_json::to_value(&base).unwrap();
        for field in [
            Field::new("replacement", DataType::Int64, false),
            Field::new("old", DataType::Boolean, true),
        ] {
            let input = ArrowSchema::new(vec![field]);
            let error =
                prepare_replacement(&url, catalog(true), None, Some(&base), &input).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("unsupported Iceberg replacement: historical sort order 4")
            );
            assert_eq!(serde_json::to_value(&base).unwrap(), before);
        }
        let input = ArrowSchema::new(vec![
            Field::new("old", DataType::Int64, true),
            Field::new("replacement", DataType::Utf8, false),
        ]);
        let replacement =
            prepare_replacement(&url, catalog(true), None, Some(&base), &input).unwrap();
        assert_eq!(replacement.metadata.sort_orders, base.sort_orders);
        assert_eq!(replacement.metadata.default_sort_order_id, Some(0));
    }

    #[tokio::test]
    async fn replacement_publication_faults_preserve_artifacts_and_catalog_access() {
        for fault in [
            ReplacementFault::LostAck,
            ReplacementFault::PutThenAlreadyExists,
            ReplacementFault::PreflightList,
        ] {
            for existing in [false, true] {
                // Write planning supplies a normalized directory URL to replacement operations.
                let url = crate::lake_source::IcebergLakeSource::parse_table_url(vec![
                    "memory:///table".into(),
                ])
                .await
                .unwrap();
                let store = Arc::new(ReplacementStore::new(fault));
                let context = StoreContext::new(store.clone(), &url).unwrap();
                let namespace = Namespace::try_from(vec!["db".to_string()]).unwrap();
                let provider = Arc::new(MemoryCatalogProvider::new(
                    "memory".into(),
                    namespace.clone(),
                    None,
                ));
                let manager = Arc::new(
                    CatalogManager::try_new(CatalogManagerOptions {
                        catalogs: HashMap::from([(
                            "memory".into(),
                            provider.clone() as Arc<dyn CatalogProvider>,
                        )]),
                        default_catalog: "memory".into(),
                        default_database: vec!["db".into()],
                        global_temporary_database: vec!["global_temp".into()],
                    })
                    .unwrap(),
                );
                let task = SessionContext::new_with_config(
                    SessionConfig::new().with_extension(manager.clone()),
                )
                .task_ctx();
                let options = CreateTableOptions {
                    columns: vec![CreateTableColumnOptions {
                        name: "replacement".into(),
                        data_type: DataType::Int64,
                        nullable: false,
                        comment: None,
                        default: None,
                        generated_always_as: None,
                        identity: None,
                    }],
                    comment: None,
                    constraints: vec![],
                    location: Some(url.to_string()),
                    format: "iceberg".into(),
                    partition_by: vec![],
                    sort_by: vec![],
                    bucket_by: None,
                    mode: CreateTableMode::CreateOrReplace,
                    properties: vec![],
                    is_external: true,
                    is_write_precondition: true,
                };
                let input =
                    ArrowSchema::new(vec![Field::new("replacement", DataType::Int64, false)]);
                let base = if existing {
                    let mut old = options.clone();
                    old.columns[0].name = "old".into();
                    provider.create_table(&namespace, "t", old).await.unwrap();
                    let schema = ArrowSchema::new(vec![Field::new("old", DataType::Int64, false)]);
                    let base = prepare_replacement(&url, catalog(false), None, None, &schema)
                        .unwrap()
                        .metadata;
                    store
                        .inner
                        .put(
                            &Path::from("table/metadata/v1.metadata.json"),
                            Bytes::from(base.to_json().unwrap()).into(),
                        )
                        .await
                        .unwrap();
                    Some(base)
                } else {
                    None
                };
                let before = provider
                    .get_table(&namespace, "t")
                    .await
                    .ok()
                    .map(|status| status.kind);
                let base_bytes = if existing {
                    Some(
                        store
                            .inner
                            .get(&Path::from("table/metadata/v1.metadata.json"))
                            .await
                            .unwrap()
                            .bytes()
                            .await
                            .unwrap(),
                    )
                } else {
                    None
                };
                let staged = manager
                    .stage_table_replacement(&["t"], options.clone())
                    .await
                    .unwrap();
                let replacement = prepare_replacement(
                    &url,
                    staged,
                    existing.then(|| "table/metadata/v1.metadata.json".into()),
                    base.as_ref(),
                    &input,
                )
                .unwrap();
                store
                    .inner
                    .put(
                        &Path::from("table/data/task.parquet"),
                        Bytes::from_static(b"task data").into(),
                    )
                    .await
                    .unwrap();
                let commit = IcebergCommitInfo {
                    table_uri: url.to_string(),
                    row_count: 1,
                    action_summary: None,
                    data_files: vec![DataFile {
                        content: DataContentType::Data,
                        file_path: "memory:///table/data/task.parquet".into(),
                        file_format: DataFileFormat::Parquet,
                        partition: vec![],
                        record_count: 1,
                        file_size_in_bytes: 9,
                        column_sizes: HashMap::new(),
                        value_counts: HashMap::new(),
                        null_value_counts: HashMap::new(),
                        nan_value_counts: HashMap::new(),
                        lower_bounds: HashMap::new(),
                        upper_bounds: HashMap::new(),
                        block_size_in_bytes: None,
                        key_metadata: None,
                        split_offsets: vec![],
                        equality_ids: vec![],
                        sort_order_id: None,
                        first_row_id: None,
                        partition_spec_id: replacement.metadata.default_spec_id,
                        referenced_data_file: None,
                        content_offset: None,
                        content_size_in_bytes: None,
                    }],
                    delete_files: vec![],
                    manifest_path: String::new(),
                    manifest_list_path: String::new(),
                    updates: vec![],
                    requirements: vec![],
                    table_properties: vec![],
                    lakehouse_table: None,
                    snapshot_update_kind: SnapshotUpdateKind::FullOverwrite,
                    schema: None,
                    partition_spec: None,
                };
                let result =
                    commit_replacement(&task, &url, &context, replacement.clone(), &commit).await;
                if matches!(fault, ReplacementFault::PreflightList) {
                    let error = result.expect_err("preflight listing must fail");
                    assert!(error.to_string().contains("preflight listing failed"));
                    assert!(!error.to_string().contains("state is unknown"));
                    assert_eq!(store.metadata_puts.load(Ordering::SeqCst), 0);
                    assert_eq!(store.lists.load(Ordering::SeqCst), 3);
                    assert_eq!(
                        provider
                            .get_table(&namespace, "t")
                            .await
                            .ok()
                            .map(|status| status.kind),
                        before
                    );
                    assert_eq!(
                        provider.list_tables(&namespace).await.unwrap().len(),
                        usize::from(existing)
                    );
                    let files = store
                        .inner
                        .list(None)
                        .try_collect::<Vec<_>>()
                        .await
                        .unwrap();
                    assert_eq!(
                        files
                            .iter()
                            .filter(|file| file.location.as_ref().ends_with(".metadata.json"))
                            .count(),
                        usize::from(existing)
                    );
                    assert!(
                        !files
                            .iter()
                            .any(|file| file.location.as_ref().ends_with(".avro")),
                        "unpublished manifests must be cleaned up"
                    );
                    store.disabled.store(true, Ordering::SeqCst);
                    // Retrying the same staged identity must not encounter an uncertain name.
                    commit_replacement(&task, &url, &context, replacement, &commit)
                        .await
                        .unwrap();
                } else {
                    result.unwrap();
                }
                let status = provider.get_table(&namespace, "t").await.unwrap();
                let TableKind::Table {
                    columns,
                    properties,
                    ..
                } = status.kind
                else {
                    panic!("expected table")
                };
                assert_eq!(columns[0].name, "replacement");
                let version = if existing { 2 } else { 1 };
                assert!(
                    properties.contains(&(
                        "metadata_location".into(),
                        format!("memory:///table/metadata/v{version}.metadata.json")
                    )),
                    "{properties:?}"
                );
                let bytes = store
                    .inner
                    .get(&Path::from(format!(
                        "table/metadata/v{version}.metadata.json"
                    )))
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap();
                let metadata = TableMetadata::from_json(&bytes).unwrap();
                let snapshot = metadata.current_snapshot().unwrap();
                let manifests = load_manifest_list(&context, snapshot.manifest_list())
                    .await
                    .unwrap();
                assert_eq!(manifests.entries().len(), 1);
                let manifest = load_manifest(&context, &manifests.entries()[0].manifest_path)
                    .await
                    .unwrap();
                assert_eq!(manifest.entries().len(), 1);
                assert_eq!(manifest.entries()[0].data_file.record_count, 1);
                context
                    .prefixed
                    .head(&Path::from("data/task.parquet"))
                    .await
                    .unwrap();
                if let Some(before) = base_bytes {
                    assert_eq!(
                        store
                            .inner
                            .get(&Path::from("table/metadata/v1.metadata.json"))
                            .await
                            .unwrap()
                            .bytes()
                            .await
                            .unwrap(),
                        before
                    );
                }
                provider
                    .rename_table(&namespace, "t", &namespace, "renamed")
                    .await
                    .unwrap();
                provider
                    .drop_table(
                        &namespace,
                        "renamed",
                        DropTableOptions {
                            if_exists: false,
                            purge: false,
                        },
                    )
                    .await
                    .unwrap();
                provider
                    .create_table(&namespace, "t", options)
                    .await
                    .unwrap();
                assert_eq!(provider.list_tables(&namespace).await.unwrap().len(), 1);
            }
        }
    }

    #[test]
    fn replacement_rejects_unsupported_write_format() {
        let base = historical_metadata();
        let mut request = catalog(true);
        request
            .property_updates
            .push(("write.format.default".into(), "orc".into()));
        let input = ArrowSchema::new(vec![Field::new("replacement", DataType::Int64, false)]);
        assert!(
            prepare_replacement(
                &Url::parse("memory:///table").unwrap(),
                request,
                None,
                Some(&base),
                &input
            )
            .is_err(),
            "write.format.default=orc must not silently select a Parquet writer"
        );
    }
}
