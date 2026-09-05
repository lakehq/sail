use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::common::Result;

use crate::spec::{SnapshotRetention, TableMetadata};

pub(super) fn schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("snapshot_id", DataType::Int64, false),
        Field::new("max_reference_age_in_ms", DataType::Int64, true),
        Field::new("min_snapshots_to_keep", DataType::Int32, true),
        Field::new("max_snapshot_age_in_ms", DataType::Int64, true),
    ]))
}

pub(super) fn batch(metadata: &TableMetadata) -> Result<RecordBatch> {
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
    RecordBatch::try_new(
        schema(),
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
