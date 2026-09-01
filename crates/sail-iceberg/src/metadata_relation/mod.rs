//! Iceberg metadata relations.

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
        for name in ["files", "position_deletes"] {
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
