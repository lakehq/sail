use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::builder::{MapBuilder, MapFieldNames, StringBuilder};
use datafusion::arrow::array::{
    Array, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use datafusion::catalog::{MemTable, TableProvider};
use datafusion::common::{DataFusionError, Result};

use crate::spec::{SnapshotRetention, TableMetadata};
use crate::table::Table;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IcebergMetadataTableType {
    Entries,
    Files,
    DataFiles,
    DeleteFiles,
    History,
    MetadataLogEntries,
    Snapshots,
    Refs,
    Manifests,
    Partitions,
    AllDataFiles,
    AllDeleteFiles,
    AllFiles,
    AllManifests,
    AllEntries,
    PositionDeletes,
}

impl IcebergMetadataTableType {
    pub(crate) fn parse(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "entries" => Some(Self::Entries),
            "files" => Some(Self::Files),
            "data_files" => Some(Self::DataFiles),
            "delete_files" => Some(Self::DeleteFiles),
            "history" => Some(Self::History),
            "metadata_log_entries" => Some(Self::MetadataLogEntries),
            "snapshots" => Some(Self::Snapshots),
            "refs" => Some(Self::Refs),
            "manifests" => Some(Self::Manifests),
            "partitions" => Some(Self::Partitions),
            "all_data_files" => Some(Self::AllDataFiles),
            "all_delete_files" => Some(Self::AllDeleteFiles),
            "all_files" => Some(Self::AllFiles),
            "all_manifests" => Some(Self::AllManifests),
            "all_entries" => Some(Self::AllEntries),
            "position_deletes" => Some(Self::PositionDeletes),
            _ => None,
        }
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Entries => "entries",
            Self::Files => "files",
            Self::DataFiles => "data_files",
            Self::DeleteFiles => "delete_files",
            Self::History => "history",
            Self::MetadataLogEntries => "metadata_log_entries",
            Self::Snapshots => "snapshots",
            Self::Refs => "refs",
            Self::Manifests => "manifests",
            Self::Partitions => "partitions",
            Self::AllDataFiles => "all_data_files",
            Self::AllDeleteFiles => "all_delete_files",
            Self::AllFiles => "all_files",
            Self::AllManifests => "all_manifests",
            Self::AllEntries => "all_entries",
            Self::PositionDeletes => "position_deletes",
        }
    }

    pub(crate) fn is_supported(self) -> bool {
        matches!(
            self,
            Self::History | Self::MetadataLogEntries | Self::Snapshots | Self::Refs
        )
    }

    pub(crate) fn unsupported_reason(self) -> String {
        format!(
            "Iceberg metadata table '{}' is recognized but not implemented",
            self.name()
        )
    }

    fn record_batch(self, table: &Table) -> Result<RecordBatch> {
        match self {
            Self::History => history_batch(table.metadata()),
            Self::MetadataLogEntries => {
                metadata_log_entries_batch(table.metadata(), table.metadata_location())
            }
            Self::Refs => refs_batch(table.metadata()),
            Self::Snapshots => snapshots_batch(table.metadata()),
            unsupported => Err(DataFusionError::NotImplemented(
                unsupported.unsupported_reason(),
            )),
        }
    }
}

pub(crate) fn metadata_table_provider(
    table: &Table,
    table_type: IcebergMetadataTableType,
) -> Result<Arc<dyn TableProvider>> {
    let batch = table_type.record_batch(table)?;
    let schema = batch.schema();
    Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
}

fn timestamp_micros(timestamp_ms: i64) -> Result<i64> {
    timestamp_ms.checked_mul(1_000).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "Iceberg metadata timestamp is outside microsecond range: {timestamp_ms}"
        ))
    })
}

fn timestamp_field(name: &str) -> Field {
    Field::new(
        name,
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        false,
    )
}

fn snapshots_batch(metadata: &TableMetadata) -> Result<RecordBatch> {
    let committed_at = metadata
        .snapshots
        .iter()
        .map(|snapshot| timestamp_micros(snapshot.timestamp_ms()))
        .collect::<Result<Vec<_>>>()?;
    let snapshot_id = metadata
        .snapshots
        .iter()
        .map(|snapshot| snapshot.snapshot_id())
        .collect::<Vec<_>>();
    let parent_id = metadata
        .snapshots
        .iter()
        .map(|snapshot| snapshot.parent_snapshot_id())
        .collect::<Vec<_>>();
    let operation = metadata
        .snapshots
        .iter()
        .map(|snapshot| Some(snapshot.summary.operation.as_str()))
        .collect::<Vec<_>>();
    let manifest_list = metadata
        .snapshots
        .iter()
        .map(|snapshot| {
            let location = snapshot.manifest_list();
            (!location.is_empty()).then_some(location)
        })
        .collect::<Vec<_>>();

    let field_names = MapFieldNames {
        entry: "entries".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    };
    let mut summary_builder = MapBuilder::new(
        Some(field_names),
        StringBuilder::new(),
        StringBuilder::new(),
    )
    .with_keys_field(Field::new("key", DataType::Utf8, false))
    .with_values_field(Field::new("value", DataType::Utf8, false));
    for snapshot in &metadata.snapshots {
        let mut entries = snapshot
            .summary
            .additional_properties
            .iter()
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.0.cmp(right.0));
        for (key, value) in entries {
            summary_builder.keys().append_value(key);
            summary_builder.values().append_value(value);
        }
        summary_builder.append(true)?;
    }
    let summary = summary_builder.finish();
    let schema = Arc::new(ArrowSchema::new(vec![
        timestamp_field("committed_at"),
        Field::new("snapshot_id", DataType::Int64, false),
        Field::new("parent_id", DataType::Int64, true),
        Field::new("operation", DataType::Utf8, true),
        Field::new("manifest_list", DataType::Utf8, true),
        Field::new("summary", summary.data_type().clone(), true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMicrosecondArray::from(committed_at).with_timezone("UTC")),
            Arc::new(Int64Array::from(snapshot_id)),
            Arc::new(Int64Array::from(parent_id)),
            Arc::new(StringArray::from(operation)),
            Arc::new(StringArray::from(manifest_list)),
            Arc::new(summary),
        ],
    )
    .map_err(Into::into)
}

fn current_ancestor_ids(metadata: &TableMetadata) -> HashSet<i64> {
    let snapshots_by_id = metadata
        .snapshots
        .iter()
        .map(|snapshot| (snapshot.snapshot_id(), snapshot))
        .collect::<HashMap<_, _>>();
    let mut ancestors = HashSet::new();
    let mut next_id = metadata
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id());
    while let Some(snapshot_id) = next_id {
        if !ancestors.insert(snapshot_id) {
            break;
        }
        next_id = snapshots_by_id
            .get(&snapshot_id)
            .and_then(|snapshot| snapshot.parent_snapshot_id());
    }
    ancestors
}

fn history_batch(metadata: &TableMetadata) -> Result<RecordBatch> {
    let snapshots_by_id = metadata
        .snapshots
        .iter()
        .map(|snapshot| (snapshot.snapshot_id(), snapshot))
        .collect::<HashMap<_, _>>();
    let ancestors = current_ancestor_ids(metadata);
    let made_current_at = metadata
        .snapshot_log
        .iter()
        .map(|entry| timestamp_micros(entry.timestamp_ms))
        .collect::<Result<Vec<_>>>()?;
    let snapshot_id = metadata
        .snapshot_log
        .iter()
        .map(|entry| entry.snapshot_id)
        .collect::<Vec<_>>();
    let parent_id = metadata
        .snapshot_log
        .iter()
        .map(|entry| {
            snapshots_by_id
                .get(&entry.snapshot_id)
                .and_then(|snapshot| snapshot.parent_snapshot_id())
        })
        .collect::<Vec<_>>();
    let is_current_ancestor = metadata
        .snapshot_log
        .iter()
        .map(|entry| ancestors.contains(&entry.snapshot_id))
        .collect::<Vec<_>>();
    let schema = Arc::new(ArrowSchema::new(vec![
        timestamp_field("made_current_at"),
        Field::new("snapshot_id", DataType::Int64, false),
        Field::new("parent_id", DataType::Int64, true),
        Field::new("is_current_ancestor", DataType::Boolean, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMicrosecondArray::from(made_current_at).with_timezone("UTC")),
            Arc::new(Int64Array::from(snapshot_id)),
            Arc::new(Int64Array::from(parent_id)),
            Arc::new(BooleanArray::from(is_current_ancestor)),
        ],
    )
    .map_err(Into::into)
}

fn refs_batch(metadata: &TableMetadata) -> Result<RecordBatch> {
    let mut references = metadata.refs.iter().collect::<Vec<_>>();
    references.sort_by(|left, right| left.0.cmp(right.0));

    let mut names = Vec::with_capacity(references.len());
    let mut types = Vec::with_capacity(references.len());
    let mut snapshot_ids = Vec::with_capacity(references.len());
    let mut max_reference_ages = Vec::with_capacity(references.len());
    let mut min_snapshots_to_keep = Vec::with_capacity(references.len());
    let mut max_snapshot_ages = Vec::with_capacity(references.len());
    for (name, reference) in references {
        names.push(name.as_str());
        snapshot_ids.push(reference.snapshot_id);
        match &reference.retention {
            SnapshotRetention::Branch {
                min_snapshots_to_keep: minimum,
                max_snapshot_age_ms: snapshot_age,
                max_ref_age_ms: reference_age,
            } => {
                types.push("BRANCH");
                max_reference_ages.push(*reference_age);
                min_snapshots_to_keep.push(*minimum);
                max_snapshot_ages.push(*snapshot_age);
            }
            SnapshotRetention::Tag { max_ref_age_ms } => {
                types.push("TAG");
                max_reference_ages.push(*max_ref_age_ms);
                min_snapshots_to_keep.push(None);
                max_snapshot_ages.push(None);
            }
        }
    }
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("snapshot_id", DataType::Int64, false),
        Field::new("max_reference_age_in_ms", DataType::Int64, true),
        Field::new("min_snapshots_to_keep", DataType::Int32, true),
        Field::new("max_snapshot_age_in_ms", DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(types)),
            Arc::new(Int64Array::from(snapshot_ids)),
            Arc::new(Int64Array::from(max_reference_ages)),
            Arc::new(Int32Array::from(min_snapshots_to_keep)),
            Arc::new(Int64Array::from(max_snapshot_ages)),
        ],
    )
    .map_err(Into::into)
}

fn latest_snapshot_at(metadata: &TableMetadata, timestamp_ms: i64) -> Option<i64> {
    metadata
        .snapshot_log
        .iter()
        .filter(|entry| entry.timestamp_ms <= timestamp_ms)
        .map(|entry| entry.snapshot_id)
        .next_back()
}

fn metadata_log_entries_batch(
    metadata: &TableMetadata,
    current_metadata_location: &str,
) -> Result<RecordBatch> {
    let snapshots_by_id = metadata
        .snapshots
        .iter()
        .map(|snapshot| (snapshot.snapshot_id(), snapshot))
        .collect::<HashMap<_, _>>();
    let mut entries = metadata
        .metadata_log
        .iter()
        .map(|entry| (entry.timestamp_ms, entry.metadata_file.as_str()))
        .collect::<Vec<_>>();
    entries.push((metadata.last_updated_ms, current_metadata_location));

    let timestamps = entries
        .iter()
        .map(|(timestamp_ms, _)| timestamp_micros(*timestamp_ms))
        .collect::<Result<Vec<_>>>()?;
    let files = entries.iter().map(|(_, file)| *file).collect::<Vec<_>>();
    let latest_snapshot_ids = entries
        .iter()
        .map(|(timestamp_ms, _)| latest_snapshot_at(metadata, *timestamp_ms))
        .collect::<Vec<_>>();
    let latest_schema_ids = latest_snapshot_ids
        .iter()
        .map(|snapshot_id| {
            snapshot_id
                .and_then(|snapshot_id| snapshots_by_id.get(&snapshot_id))
                .and_then(|snapshot| snapshot.schema_id())
        })
        .collect::<Vec<_>>();
    let latest_sequence_numbers = latest_snapshot_ids
        .iter()
        .map(|snapshot_id| {
            snapshot_id
                .and_then(|snapshot_id| snapshots_by_id.get(&snapshot_id))
                .map(|snapshot| snapshot.sequence_number())
        })
        .collect::<Vec<_>>();
    let schema = Arc::new(ArrowSchema::new(vec![
        timestamp_field("timestamp"),
        Field::new("file", DataType::Utf8, false),
        Field::new("latest_snapshot_id", DataType::Int64, true),
        Field::new("latest_schema_id", DataType::Int32, true),
        Field::new("latest_sequence_number", DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMicrosecondArray::from(timestamps).with_timezone("UTC")),
            Arc::new(StringArray::from(files)),
            Arc::new(Int64Array::from(latest_snapshot_ids)),
            Arc::new(Int32Array::from(latest_schema_ids)),
            Arc::new(Int64Array::from(latest_sequence_numbers)),
        ],
    )
    .map_err(Into::into)
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::array::{Array, BooleanArray, Int64Array, StringArray};

    use super::*;
    use crate::spec::{
        FormatVersion, MetadataLog, Operation, PartitionSpec, Schema, Snapshot, SnapshotLog,
        SnapshotReference, Summary,
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
        for name in names {
            let table_type = IcebergMetadataTableType::parse(&name.to_ascii_uppercase())
                .expect("recognized Iceberg metadata table");
            assert_eq!(table_type.name(), name);
        }
        assert_eq!(IcebergMetadataTableType::parse("unknown_relation"), None);
    }

    #[test]
    fn only_static_metadata_tables_are_supported() {
        for name in ["history", "metadata_log_entries", "snapshots", "refs"] {
            assert!(
                IcebergMetadataTableType::parse(name)
                    .expect("recognized static metadata table")
                    .is_supported()
            );
        }
        for name in ["manifests", "files", "position_deletes"] {
            assert!(
                !IcebergMetadataTableType::parse(name)
                    .expect("recognized deferred metadata table")
                    .is_supported()
            );
        }
    }

    #[test]
    fn builds_spark_compatible_static_metadata_rows() -> Result<()> {
        let metadata = table_metadata();

        let snapshots = snapshots_batch(&metadata)?;
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

        let history = history_batch(&metadata)?;
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

        let refs = refs_batch(&metadata)?;
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
            metadata_log_entries_batch(&metadata, "file:///table/metadata/v2.metadata.json")?;
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
}
