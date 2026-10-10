use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{BooleanArray, Int64Array, RecordBatch, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::common::Result;

use super::time::{timestamp_field, timestamp_micros};
use crate::spec::TableMetadata;

pub(super) fn schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        timestamp_field("made_current_at"),
        Field::new("snapshot_id", DataType::Int64, false),
        Field::new("parent_id", DataType::Int64, true),
        Field::new("is_current_ancestor", DataType::Boolean, false),
    ]))
}

pub(super) fn batch(metadata: &TableMetadata) -> Result<RecordBatch> {
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
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(TimestampMicrosecondArray::from(made_current_at).with_timezone("UTC")),
            Arc::new(Int64Array::from(snapshot_id)),
            Arc::new(Int64Array::from(parent_id)),
            Arc::new(BooleanArray::from(is_current_ancestor)),
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
