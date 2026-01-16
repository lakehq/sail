use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::arrow::row::{RowConverter, SortField};
use delta_kernel::expressions::Scalar;
use indexmap::IndexMap;

use crate::kernel::models::ScalarExt;
use crate::kernel::DeltaTableError;

/// Result of partitioning a record batch.
#[derive(Debug)]
pub struct PartitionResult {
    pub record_batch: RecordBatch,
    pub partition_values: IndexMap<String, Scalar>,
}

/// Partition a RecordBatch along partition columns.
pub(crate) fn divide_by_partition_values(
    arrow_schema: ArrowSchemaRef,
    logical_partition_columns: Vec<String>,
    physical_partition_columns: Vec<String>,
    values: &RecordBatch,
) -> Result<Vec<PartitionResult>, DeltaTableError> {
    let mut partitions = Vec::new();

    if values.num_rows() == 0 {
        return Ok(partitions);
    }

    if logical_partition_columns.is_empty() {
        partitions.push(PartitionResult {
            partition_values: IndexMap::new(),
            record_batch: values.clone(),
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

    // Pre-compute indices for non-partition columns, as described by `arrow_schema`.
    let data_indices: Vec<usize> = arrow_schema
        .fields()
        .iter()
        .map(|f| {
            schema.index_of(f.name()).map_err(|_| {
                DeltaTableError::schema(format!("Column {} not found in batch", f.name()))
            })
        })
        .collect::<Result<_, _>>()?;

    let mut start = 0usize;
    let mut prev = rows.row(0);
    for i in 1..rows.num_rows() {
        let cur = rows.row(i);
        if cur != prev {
            push_partition_range(
                &mut partitions,
                values,
                &logical_partition_columns,
                &partition_indices,
                &data_indices,
                arrow_schema.clone(),
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
        &logical_partition_columns,
        &partition_indices,
        &data_indices,
        arrow_schema,
        start,
        rows.num_rows(),
    )?;

    Ok(partitions)
}

fn push_partition_range(
    out: &mut Vec<PartitionResult>,
    values: &RecordBatch,
    logical_partition_columns: &[String],
    partition_indices: &[usize],
    data_indices: &[usize],
    arrow_schema: ArrowSchemaRef,
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

    // Slice is zero-copy; projection preserves order and avoids per-row take/sort.
    let slice = values.slice(start, len);
    let projected = slice
        .project(data_indices)
        .map_err(|e| DeltaTableError::generic(format!("Failed to project record batch: {e}")))?;

    let record_batch = RecordBatch::try_new(arrow_schema, projected.columns().to_vec())
        .map_err(|e| DeltaTableError::generic(format!("Failed to build record batch: {e}")))?;

    out.push(PartitionResult {
        partition_values,
        record_batch,
    });

    Ok(())
}
