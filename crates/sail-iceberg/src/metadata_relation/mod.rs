//! Iceberg metadata relations.

pub(crate) mod files;
mod history;
mod kind;
mod manifests;
mod metadata_log_entries;
mod provider;
mod refs;
mod snapshots;
mod time;

pub(crate) use kind::IcebergMetadataRelationType;
pub(crate) use provider::metadata_relation_provider;

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::array::{Array, BooleanArray, Int64Array, StringArray};
    use datafusion::arrow::datatypes::DataType;
    use datafusion::common::Result;

    use super::{
        IcebergMetadataRelationType, history, manifests, metadata_log_entries, refs, snapshots,
    };
    use crate::spec::{
        FormatVersion, ManifestContentType, ManifestFile, MetadataLog, Operation, PartitionSpec,
        Schema, Snapshot, SnapshotLog, SnapshotReference, SnapshotRetention, Summary,
        TableMetadata,
    };

    fn snapshot(
        snapshot_id: i64,
        parent_snapshot_id: Option<i64>,
        sequence_number: i64,
        timestamp_ms: i64,
    ) -> Snapshot {
        let mut builder = Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_sequence_number(sequence_number)
            .with_timestamp_ms(timestamp_ms)
            .with_manifest_list(format!("file:///table/metadata/{snapshot_id}.avro"))
            .with_schema_id(7)
            .with_summary(Summary::new(Operation::Append).with_property("added-records", "1"));
        if let Some(parent_snapshot_id) = parent_snapshot_id {
            builder = builder.with_parent_snapshot_id(parent_snapshot_id);
        }
        builder.build().expect("valid test snapshot")
    }

    fn table_metadata() -> TableMetadata {
        let schema = Schema::builder()
            .with_schema_id(7)
            .build()
            .expect("valid test schema");
        TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: None,
            location: "file:///table".to_string(),
            last_sequence_number: 3,
            last_updated_ms: 2_000,
            last_column_id: 0,
            schemas: vec![schema],
            current_schema_id: 7,
            partition_specs: vec![PartitionSpec::unpartitioned_spec()],
            default_spec_id: 0,
            last_partition_id: 999,
            properties: HashMap::new(),
            current_snapshot_id: Some(20),
            next_row_id: None,
            encryption_keys: vec![],
            snapshots: vec![
                snapshot(10, None, 1, 1_000),
                snapshot(30, Some(10), 2, 1_500),
                snapshot(20, Some(10), 3, 2_000),
            ],
            snapshot_log: vec![
                SnapshotLog {
                    timestamp_ms: 1_000,
                    snapshot_id: 10,
                },
                SnapshotLog {
                    timestamp_ms: 1_500,
                    snapshot_id: 30,
                },
                SnapshotLog {
                    timestamp_ms: 2_000,
                    snapshot_id: 20,
                },
            ],
            metadata_log: vec![MetadataLog {
                timestamp_ms: 500,
                metadata_file: "file:///table/metadata/v1.metadata.json".to_string(),
            }],
            sort_orders: vec![],
            default_sort_order_id: None,
            refs: HashMap::from([
                (
                    "main".to_string(),
                    SnapshotReference {
                        snapshot_id: 20,
                        retention: SnapshotRetention::Branch {
                            min_snapshots_to_keep: Some(2),
                            max_snapshot_age_ms: Some(4_000),
                            max_ref_age_ms: Some(5_000),
                        },
                    },
                ),
                (
                    "release".to_string(),
                    SnapshotReference {
                        snapshot_id: 30,
                        retention: SnapshotRetention::Tag {
                            max_ref_age_ms: Some(6_000),
                        },
                    },
                ),
            ]),
            statistics: vec![],
            partition_statistics: vec![],
        }
    }

    #[tokio::test]
    async fn files_scan_is_lazy_pinned_and_partitioned_on_workers() -> Result<()> {
        use std::sync::Arc;

        use bytes::Bytes;
        use datafusion::common::DataFusionError;
        use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
        use datafusion::prelude::{SessionConfig, SessionContext};
        use futures::TryStreamExt;
        use object_store::ObjectStoreExt;
        use object_store::memory::InMemory;
        use object_store::path::Path;
        use url::Url;

        use super::metadata_relation_provider;
        use crate::io::StoreContext;
        use crate::physical_plan::IcebergMetadataRelationExec;
        use crate::spec::manifest_list::ManifestListWriter;
        use crate::spec::{
            DataFile, ManifestEntry, ManifestMetadata, ManifestStatus, ManifestWriter,
        };

        let url = Url::parse("memory://bucket/table").expect("table URL");
        let store = Arc::new(InMemory::new());
        let storage = StoreContext::new(store.clone(), &url)?;
        let mut metadata = table_metadata();
        metadata.location = url.to_string();
        metadata
            .snapshots
            .last_mut()
            .expect("current snapshot")
            .manifest_list = "metadata/current.avro".to_string();
        let mut list = ManifestListWriter::new();
        let mut manifest_bytes = vec![];
        for group in 0..2 {
            let mut writer = ManifestWriter::new(
                Some(20),
                None,
                ManifestMetadata::new(
                    Arc::new(metadata.schemas[0].clone()),
                    7,
                    PartitionSpec::unpartitioned_spec(),
                    FormatVersion::V2,
                    ManifestContentType::Data,
                ),
            );
            for index in 0..3 {
                let file: DataFile = serde_json::from_value(serde_json::json!({
                    "content": "DATA", "file_path": format!("data/{group}-{index}.parquet"),
                    "file_format": "PARQUET", "partition": [], "record_count": 10,
                    "file_size_in_bytes": 100, "partition_spec_id": 0
                }))
                .expect("data file");
                let status = if index == 2 {
                    ManifestStatus::Deleted
                } else {
                    ManifestStatus::Added
                };
                writer.add_entry(ManifestEntry::new(status, Some(20), Some(3), Some(3), file));
            }
            let path = format!("metadata/m{group}.avro");
            list.append(
                writer
                    .clone()
                    .into_manifest_file(path.clone(), 3, 20)
                    .expect("descriptor"),
            );
            manifest_bytes.push((
                path,
                writer.finish().to_avro_bytes_v2().expect("manifest bytes"),
            ));
        }
        storage
            .prefixed
            .put(
                &Path::from("metadata/current.avro"),
                Bytes::from(list.to_bytes(FormatVersion::V2).expect("manifest list")).into(),
            )
            .await?;
        storage
            .prefixed
            .put(
                &Path::from("metadata/v1.metadata.json"),
                Bytes::from(metadata.to_json().expect("metadata JSON")).into(),
            )
            .await?;

        let config = SessionConfig::new()
            .with_target_partitions(2)
            .with_batch_size(1);
        let driver = SessionContext::new_with_config(config.clone());
        driver.register_object_store(&url, store.clone());
        let provider = metadata_relation_provider(
            &driver.state(),
            url.clone(),
            Some("memory://bucket/table/metadata/v1.metadata.json".to_string()),
            IcebergMetadataRelationType::Files,
        )
        .await?;
        // Manifests do not exist until after planning.
        let plan = provider
            .scan(&driver.state(), Some(&vec![1, 5]), &[], None)
            .await?;
        assert_eq!(plan.output_partitioning().partition_count(), 2);
        let scan = plan
            .downcast_ref::<IcebergMetadataRelationExec>()
            .expect("metadata operator");
        let plan = IcebergMetadataRelationExec::try_from_serialized(
            scan.original_schema().clone(),
            &scan.serialized_scan()?,
        )?;
        for (path, bytes) in manifest_bytes {
            storage
                .prefixed
                .put(&Path::from(path), Bytes::from(bytes).into())
                .await?;
        }
        // A newer version and a missing manifest list must not rebind the planned scan.
        metadata.current_snapshot_id = None;
        metadata.snapshots.clear();
        storage
            .prefixed
            .put(
                &Path::from("metadata/v2.metadata.json"),
                Bytes::from(metadata.to_json().expect("new metadata JSON")).into(),
            )
            .await?;
        storage
            .prefixed
            .delete(&Path::from("metadata/current.avro"))
            .await?;
        let worker = SessionContext::new_with_config(config);
        worker.register_object_store(&url, store);
        let mut paths = vec![];
        for partition in 0..2 {
            let batches: Vec<_> = plan
                .execute(partition, worker.task_ctx())?
                .try_collect()
                .await?;
            assert_eq!(batches.len(), 2);
            for batch in batches {
                assert_eq!((batch.num_rows(), batch.num_columns()), (1, 2));
                paths.push(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("paths")
                        .value(0)
                        .to_string(),
                );
            }
        }
        paths.sort();
        assert_eq!(
            paths,
            [
                "data/0-0.parquet",
                "data/0-1.parquet",
                "data/1-0.parquet",
                "data/1-1.parquet"
            ]
        );
        assert!(plan.execute(2, worker.task_ctx()).is_err());

        let mut scan: serde_json::Value = serde_json::from_str(&plan.serialized_scan()?)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        scan["projection"] = serde_json::json!([]);
        let count_plan = IcebergMetadataRelationExec::try_from_serialized(
            plan.original_schema().clone(),
            &scan.to_string(),
        )?;
        let batches: Vec<_> = count_plan
            .execute(0, worker.task_ctx())?
            .try_collect()
            .await?;
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            2
        );
        assert!(batches.iter().all(|batch| batch.num_columns() == 0));

        scan["limit"] = serde_json::json!(0);
        let limit_plan = IcebergMetadataRelationExec::try_from_serialized(
            plan.original_schema().clone(),
            &scan.to_string(),
        )?;
        storage
            .prefixed
            .delete(&Path::from("metadata/v1.metadata.json"))
            .await?;
        assert!(
            limit_plan
                .execute(0, worker.task_ctx())?
                .try_collect::<Vec<_>>()
                .await?
                .is_empty()
        );
        Ok(())
    }

    #[test]
    fn recognizes_all_iceberg_metadata_table_names_case_insensitively() {
        let names = [
            "entries",
            "files",
            "data_files",
            "delete_files",
            "history",
            "metadata_log_entries",
            "snapshots",
            "refs",
            "manifests",
            "partitions",
            "all_data_files",
            "all_delete_files",
            "all_files",
            "all_manifests",
            "all_entries",
            "position_deletes",
        ];
        assert_eq!(
            IcebergMetadataRelationType::ALL
                .iter()
                .copied()
                .map(IcebergMetadataRelationType::name)
                .collect::<Vec<_>>(),
            names
        );
        for name in names {
            let relation_type = IcebergMetadataRelationType::parse(&name.to_ascii_uppercase())
                .expect("recognized Iceberg metadata table");
            assert_eq!(relation_type.name(), name);
        }
        assert_eq!(IcebergMetadataRelationType::parse("unknown_relation"), None);
    }

    #[test]
    fn implemented_metadata_tables_are_supported() {
        for name in [
            "files",
            "history",
            "metadata_log_entries",
            "snapshots",
            "refs",
            "manifests",
        ] {
            assert!(
                IcebergMetadataRelationType::parse(name)
                    .expect("recognized static metadata table")
                    .is_supported()
            );
        }
        for name in ["entries", "position_deletes"] {
            assert!(
                !IcebergMetadataRelationType::parse(name)
                    .expect("recognized deferred metadata table")
                    .is_supported()
            );
        }
    }

    #[test]
    fn builds_spark_compatible_static_metadata_rows() -> Result<()> {
        let metadata = table_metadata();

        let snapshots = snapshots::batch(&metadata)?;
        assert_eq!(snapshots.num_rows(), 3);
        assert_eq!(
            snapshots
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec![
                "committed_at",
                "snapshot_id",
                "parent_id",
                "operation",
                "manifest_list",
                "summary"
            ]
        );
        assert!(matches!(
            snapshots.column(5).data_type(),
            DataType::Map(_, false)
        ));

        let history = history::batch(&metadata)?;
        let ancestors = history
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean ancestor column");
        assert_eq!(
            (0..ancestors.len())
                .map(|index| ancestors.value(index))
                .collect::<Vec<_>>(),
            vec![true, false, true]
        );

        let refs = refs::batch(&metadata)?;
        let names = refs
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string ref name column");
        let types = refs
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string ref type column");
        assert_eq!(names.value(0), "main");
        assert_eq!(types.value(0), "BRANCH");
        assert_eq!(names.value(1), "release");
        assert_eq!(types.value(1), "TAG");

        let metadata_log =
            metadata_log_entries::batch(&metadata, "file:///table/metadata/v2.metadata.json")?;
        assert_eq!(metadata_log.num_rows(), 2);
        let latest_snapshot_ids = metadata_log
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("snapshot id column");
        assert!(latest_snapshot_ids.is_null(0));
        assert_eq!(latest_snapshot_ids.value(1), 20);
        Ok(())
    }

    #[test]
    fn builds_spark_compatible_manifest_rows() -> Result<()> {
        let metadata = table_metadata();
        let data_manifest = ManifestFile::builder()
            .with_manifest_path("file:///table/metadata/data.avro")
            .with_manifest_length(100)
            .with_partition_spec_id(0)
            .with_content(ManifestContentType::Data)
            .with_sequence_number(3)
            .with_min_sequence_number(1)
            .with_added_snapshot_id(20)
            .with_file_counts(2, 3, 1)
            .build()
            .expect("valid data manifest");
        let delete_manifest = ManifestFile::builder()
            .with_manifest_path("file:///table/metadata/deletes.avro")
            .with_manifest_length(80)
            .with_partition_spec_id(0)
            .with_content(ManifestContentType::Deletes)
            .with_sequence_number(3)
            .with_min_sequence_number(3)
            .with_added_snapshot_id(20)
            .with_file_counts(4, 0, 0)
            .build()
            .expect("valid delete manifest");

        let batch =
            manifests::batch_from_manifest_files(&metadata, &[data_manifest, delete_manifest])?;

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec![
                "content",
                "path",
                "length",
                "partition_spec_id",
                "added_snapshot_id",
                "added_data_files_count",
                "existing_data_files_count",
                "deleted_data_files_count",
                "added_delete_files_count",
                "existing_delete_files_count",
                "deleted_delete_files_count",
                "partition_summaries",
            ]
        );
        let added_data = batch
            .column(5)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .expect("added data count column");
        let added_deletes = batch
            .column(8)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .expect("added delete count column");
        assert_eq!((added_data.value(0), added_data.value(1)), (2, 0));
        assert_eq!((added_deletes.value(0), added_deletes.value(1)), (0, 4));
        Ok(())
    }
}
