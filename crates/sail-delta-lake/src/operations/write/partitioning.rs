use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::row::{RowConverter, SortField};
use delta_kernel::expressions::Scalar;
use indexmap::IndexMap;

use crate::kernel::models::ScalarExt;
use crate::kernel::DeltaTableError;

/// A contiguous range of rows that share the same partition values.
#[derive(Debug)]
pub struct PartitionRange {
    pub start: usize,
    pub end: usize,
    pub partition_values: IndexMap<String, Scalar>,
}

/// Detect contiguous partition ranges from an input batch.
///
/// The input is expected to be grouped by `physical_partition_columns` (typically guaranteed by
/// the planner via `SortExec` on partition columns). This function does **not** sort; it only
/// detects boundaries where the partition key changes.
pub(crate) fn partition_ranges(
    logical_partition_columns: &[String],
    physical_partition_columns: &[String],
    values: &RecordBatch,
) -> Result<Vec<PartitionRange>, DeltaTableError> {
    let mut partitions = Vec::new();

    if values.num_rows() == 0 {
        return Ok(partitions);
    }

    if logical_partition_columns.is_empty() {
        partitions.push(PartitionRange {
            start: 0,
            end: values.num_rows(),
            partition_values: IndexMap::new(),
        });
        return Ok(partitions);
    }

    let schema = values.schema();
    let partition_indices: Vec<usize> = physical_partition_columns
        .iter()
        .map(|name| {
            schema.index_of(name).map_err(|_| {
                DeltaTableError::schema(format!("Partition column '{name}' not found in batch"))
            })
        })
        .collect::<Result<_, _>>()?;

    // Build comparable rows for partition columns (no sorting, just detect contiguous groups).
    let partition_arrays: Vec<ArrayRef> = partition_indices
        .iter()
        .map(|&idx| values.column(idx).clone())
        .collect();
    let sort_fields = partition_arrays
        .iter()
        .map(|a| SortField::new(a.data_type().clone()))
        .collect();
    let converter = RowConverter::new(sort_fields)
        .map_err(|e| DeltaTableError::generic(format!("failed to create RowConverter: {e}")))?;
    let rows = converter.convert_columns(&partition_arrays).map_err(|e| {
        DeltaTableError::generic(format!("failed to convert partition columns: {e}"))
    })?;

    let mut start = 0usize;
    let mut prev = rows.row(0);
    for i in 1..rows.num_rows() {
        let cur = rows.row(i);
        if cur != prev {
            push_partition_range(
                &mut partitions,
                values,
                logical_partition_columns,
                &partition_indices,
                start,
                i,
            )?;
            start = i;
            prev = cur;
        }
    }
    push_partition_range(
        &mut partitions,
        values,
        logical_partition_columns,
        &partition_indices,
        start,
        rows.num_rows(),
    )?;

    Ok(partitions)
}

fn push_partition_range(
    out: &mut Vec<PartitionRange>,
    values: &RecordBatch,
    logical_partition_columns: &[String],
    partition_indices: &[usize],
    start: usize,
    end: usize,
) -> Result<(), DeltaTableError> {
    let len = end.saturating_sub(start);
    if len == 0 {
        return Ok(());
    }

    // Extract partition key values from the first row in the range.
    let partition_key_iter = partition_indices
        .iter()
        .map(|&idx| {
            let col = values.column(idx);
            Scalar::from_array(&col.slice(start, 1), 0)
                .ok_or_else(|| DeltaTableError::generic("failed to parse partition value"))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let partition_values = logical_partition_columns
        .iter()
        .cloned()
        .zip(partition_key_iter)
        .collect();

    out.push(PartitionRange {
        start,
        end,
        partition_values,
    });

    Ok(())
}
