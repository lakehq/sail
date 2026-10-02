use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Int32Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::common::Result;

use super::time::{timestamp_field, timestamp_micros};
use crate::spec::TableMetadata;

pub(super) fn schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        timestamp_field("timestamp"),
        Field::new("file", DataType::Utf8, false),
        Field::new("latest_snapshot_id", DataType::Int64, true),
        Field::new("latest_schema_id", DataType::Int32, true),
        Field::new("latest_sequence_number", DataType::Int64, true),
    ]))
}

pub(super) fn batch(
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
    RecordBatch::try_new(
        schema(),
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

fn latest_snapshot_at(metadata: &TableMetadata, timestamp_ms: i64) -> Option<i64> {
    metadata
        .snapshot_log
        .iter()
        .filter(|entry| entry.timestamp_ms <= timestamp_ms)
        .map(|entry| entry.snapshot_id)
        .next_back()
}
