use datafusion::arrow::record_batch::RecordBatch;

use crate::spec::partition::UnboundPartitionSpec as PartitionSpec;
use crate::spec::types::values::Literal;

pub struct PartitionBatchResult {
    pub record_batch: RecordBatch,
    pub partition_values: Vec<Option<Literal>>, // aligned with PartitionSpec fields
    pub partition_dir: String, // formatted path segment like key=value/... or empty
    pub spec_id: i32,
}

pub fn compute_partition_values(
    _batch: &RecordBatch,
    spec: &PartitionSpec,
) -> Result<(Vec<Option<Literal>>, String), String> {
    // Placeholder: leave actual transforms for later, return empty partitioning
    let values = vec![None; spec.fields.len()];
    Ok((values, String::new()))
}
