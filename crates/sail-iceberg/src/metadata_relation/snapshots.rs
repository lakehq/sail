use std::sync::Arc;

use datafusion::arrow::array::builder::{MapBuilder, MapFieldNames, StringBuilder};
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::common::Result;

use super::time::{timestamp_field, timestamp_micros};
use crate::spec::TableMetadata;

pub(super) fn schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        timestamp_field("committed_at"),
        Field::new("snapshot_id", DataType::Int64, false),
        Field::new("parent_id", DataType::Int64, true),
        Field::new("operation", DataType::Utf8, true),
        Field::new("manifest_list", DataType::Utf8, true),
        Field::new("summary", summary_data_type(), true),
    ]))
}

pub(super) fn batch(metadata: &TableMetadata) -> Result<RecordBatch> {
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
    RecordBatch::try_new(
        schema(),
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

fn summary_data_type() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("key", DataType::Utf8, false)),
                    Arc::new(Field::new("value", DataType::Utf8, false)),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}
