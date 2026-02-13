// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, RecordBatch, StructArray, UInt64Builder};
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::arrow::row::{RowConverter, SortField};
use delta_kernel::expressions::Scalar;
use indexmap::IndexMap;

use crate::conversion::scalar::ScalarExt as _;
use crate::kernel::DeltaTableError;

/// A single partitioned batch.
#[derive(Debug)]
pub struct PartitionedBatch {
    pub record_batch: RecordBatch,
    /// Partition values keyed by *logical* partition column names.
    pub partition_values: IndexMap<String, Scalar>,
}

/// Split/Group a RecordBatch by partition columns.
pub trait Partitioner: Send + Sync {
    fn partition(
        &self,
        output_schema: ArrowSchemaRef,
        logical_partition_columns: &[String],
        physical_partition_columns: &[String],
        input: &RecordBatch,
    ) -> Result<Vec<PartitionedBatch>, DeltaTableError>;
}

/// Current behavior: compute comparable rows for partition columns and split contiguous runs.
#[derive(Debug, Default)]
pub struct ContiguousPartitioner;

impl Partitioner for ContiguousPartitioner {
    fn partition(
        &self,
        output_schema: ArrowSchemaRef,
        logical_partition_columns: &[String],
        physical_partition_columns: &[String],
        input: &RecordBatch,
    ) -> Result<Vec<PartitionedBatch>, DeltaTableError> {
        divide_by_partition_values_contiguous(
            output_schema,
            logical_partition_columns.to_vec(),
            physical_partition_columns.to_vec(),
            input,
        )
    }
}

/// Group each batch by partition key using take-indices (adapts DataFusion's demux approach).
#[derive(Debug, Default)]
pub struct HashPartitioner;

impl Partitioner for HashPartitioner {
    fn partition(
        &self,
        output_schema: ArrowSchemaRef,
        logical_partition_columns: &[String],
        physical_partition_columns: &[String],
        input: &RecordBatch,
    ) -> Result<Vec<PartitionedBatch>, DeltaTableError> {
        divide_by_partition_values_hash(
            output_schema,
            logical_partition_columns,
            physical_partition_columns,
            input,
        )
    }
}

/// A lightweight partitioner that chooses a strategy based on a heuristic.
#[derive(Debug, Default)]
pub struct AutoPartitioner {
    contiguous: ContiguousPartitioner,
    hash: HashPartitioner,
}

impl Partitioner for AutoPartitioner {
    fn partition(
        &self,
        output_schema: ArrowSchemaRef,
        logical_partition_columns: &[String],
        physical_partition_columns: &[String],
        input: &RecordBatch,
    ) -> Result<Vec<PartitionedBatch>, DeltaTableError> {
        // Fast path: small batch or no partitions.
        if input.num_rows() <= 1024 || logical_partition_columns.is_empty() {
            return self.contiguous.partition(
                output_schema,
                logical_partition_columns,
                physical_partition_columns,
                input,
            );
        }

        // Try contiguous split first; if it produces "too many" partitions, regroup with hash.
        let contiguous = self.contiguous.partition(
            Arc::clone(&output_schema),
            logical_partition_columns,
            physical_partition_columns,
            input,
        )?;

        // Heuristic: contiguous partitions approaching per-row indicates unsorted / high churn.
        if contiguous.len() > 256 && contiguous.len() > input.num_rows() / 2 {
            return self.hash.partition(
                output_schema,
                logical_partition_columns,
                physical_partition_columns,
                input,
            );
        }

        Ok(contiguous)
    }
}

fn divide_by_partition_values_contiguous(
    output_schema: ArrowSchemaRef,
    logical_partition_columns: Vec<String>,
    physical_partition_columns: Vec<String>,
    values: &RecordBatch,
) -> Result<Vec<PartitionedBatch>, DeltaTableError> {
    let mut partitions = Vec::new();

    if values.num_rows() == 0 {
        return Ok(partitions);
    }

    if logical_partition_columns.is_empty() {
        partitions.push(PartitionedBatch {
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

    // Pre-compute indices for non-partition columns, as described by `output_schema`.
    let data_indices: Vec<usize> = output_schema
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
                Arc::clone(&output_schema),
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
        output_schema,
        start,
        rows.num_rows(),
    )?;

    Ok(partitions)
}

fn push_partition_range(
    out: &mut Vec<PartitionedBatch>,
    values: &RecordBatch,
    logical_partition_columns: &[String],
    partition_indices: &[usize],
    data_indices: &[usize],
    output_schema: ArrowSchemaRef,
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

    // Slice is zero-copy; projection preserves order.
    let slice = values.slice(start, len);
    let projected = slice
        .project(data_indices)
        .map_err(|e| DeltaTableError::generic(format!("Failed to project record batch: {e}")))?;

    let record_batch = RecordBatch::try_new(output_schema, projected.columns().to_vec())
        .map_err(|e| DeltaTableError::generic(format!("Failed to build record batch: {e}")))?;

    out.push(PartitionedBatch {
        partition_values,
        record_batch,
    });

    Ok(())
}

fn divide_by_partition_values_hash(
    output_schema: ArrowSchemaRef,
    logical_partition_columns: &[String],
    physical_partition_columns: &[String],
    input: &RecordBatch,
) -> Result<Vec<PartitionedBatch>, DeltaTableError> {
    if input.num_rows() == 0 {
        return Ok(vec![]);
    }

    if logical_partition_columns.is_empty() {
        return Ok(vec![PartitionedBatch {
            record_batch: input.clone(),
            partition_values: IndexMap::new(),
        }]);
    }

    let schema = input.schema();
    let partition_arrays: Vec<ArrayRef> = physical_partition_columns
        .iter()
        .map(|name| {
            let idx = schema.index_of(name).map_err(|_| {
                DeltaTableError::schema(format!("Partition column '{name}' not found in batch"))
            })?;
            Ok::<_, DeltaTableError>(input.column(idx).clone())
        })
        .collect::<Result<_, _>>()?;

    // For non-string partition types, we must encode according to ScalarExt::serialize_encoded.
    // Compute per-row partition key strings (encoded) + also keep the typed Scalars for Add.partitionValues.
    let mut key_strings: Vec<Vec<String>> = Vec::with_capacity(input.num_rows());
    let mut key_values: Vec<Vec<Scalar>> = Vec::with_capacity(input.num_rows());

    for row in 0..input.num_rows() {
        let mut parts = Vec::with_capacity(partition_arrays.len());
        let mut vals = Vec::with_capacity(partition_arrays.len());
        for arr in &partition_arrays {
            let scalar = Scalar::from_array(arr.as_ref(), row)
                .ok_or_else(|| DeltaTableError::generic("failed to parse partition value"))?;
            parts.push(scalar.serialize_encoded());
            vals.push(scalar);
        }
        key_strings.push(parts);
        key_values.push(vals);
    }

    // Build take indices grouped by encoded key.
    let mut take_map: HashMap<Vec<String>, UInt64Builder> = HashMap::new();
    let mut first_row_for_key: HashMap<Vec<String>, usize> = HashMap::new();
    for i in 0..input.num_rows() {
        let key = key_strings[i].clone();
        let b = take_map.entry(key.clone()).or_insert_with(UInt64Builder::new);
        b.append_value(i as u64);
        first_row_for_key.entry(key).or_insert(i);
    }

    // Pre-compute indices for data columns (non-partition) based on output_schema.
    let data_indices: Vec<usize> = output_schema
        .fields()
        .iter()
        .map(|f| {
            schema.index_of(f.name()).map_err(|_| {
                DeltaTableError::schema(format!("Column {} not found in batch", f.name()))
            })
        })
        .collect::<Result<_, _>>()?;

    let struct_array: StructArray = input.clone().into();
    let mut out = Vec::with_capacity(take_map.len());
    for (encoded_key, mut builder) in take_map.into_iter() {
        let take_indices = builder.finish();
        let taken = datafusion::arrow::compute::take(&struct_array, &take_indices, None)
            .map_err(|e| DeltaTableError::generic(format!("take failed: {e}")))?;
        let taken_rb = RecordBatch::from(taken.as_struct());

        // Project to non-partition columns.
        let projected = taken_rb
            .project(&data_indices)
            .map_err(|e| DeltaTableError::generic(format!("Failed to project record batch: {e}")))?;
        let record_batch = RecordBatch::try_new(Arc::clone(&output_schema), projected.columns().to_vec())
            .map_err(|e| DeltaTableError::generic(format!("Failed to build record batch: {e}")))?;

        // Reconstruct typed partition values for Add (use first row for that key).
        let first_row = *first_row_for_key
            .get(&encoded_key)
            .ok_or_else(|| DeltaTableError::generic("missing first-row mapping for partition key"))?;
        let typed_vals = key_values[first_row].clone();
        let partition_values: IndexMap<String, Scalar> = logical_partition_columns
            .iter()
            .cloned()
            .zip(typed_vals.into_iter())
            .collect();

        out.push(PartitionedBatch {
            record_batch,
            partition_values,
        });
    }

    Ok(out)
}

