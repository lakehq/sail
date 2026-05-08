// https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
//
// Copyright (2020) QP Hou and a number of other contributors.
// Portions Copyright 2025-2026 LakeSail, Inc.
// Modified in 2026 by LakeSail, Inc.
//
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

// [Credit]: <https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/crates/core/src/kernel/snapshot/log_data.rs>

use std::collections::HashSet;
use std::sync::Arc;

use ::datafusion::common::stats::{ColumnStatistics, Precision, Statistics};
use arrow_schema::DataType as ArrowDataType;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray, StructArray, UInt64Array,
};
use datafusion::arrow::compute::sum;
use datafusion::common::scalar::ScalarValue;
use datafusion::common::{Column, DataFusionError};
use datafusion::functions_aggregate::min_max::{MaxAccumulator, MinAccumulator};
use datafusion::physical_optimizer::pruning::PruningStatistics;
use datafusion::physical_plan::Accumulator;
use log::warn;

use super::DeltaSnapshot;
use crate::schema::arrow_field_physical_name;
use crate::spec::fields::{
    FIELD_NAME_PARTITION_VALUES_PARSED, FIELD_NAME_SIZE, FIELD_NAME_STATS_PARSED,
    STATS_FIELD_MAX_VALUES, STATS_FIELD_MIN_VALUES, STATS_FIELD_NULL_COUNT,
    STATS_FIELD_NUM_RECORDS,
};
use crate::spec::{DeltaError as DeltaTableError, DeltaResult};

#[derive(Debug, Clone)]
enum AccumulatorType {
    Min,
    Max,
}

// TODO validate this works with "wide and narrow" builds / stats

/// Pruning/statistics view over a materialized snapshot file batch.
#[derive(Clone)]
pub struct SnapshotPruningStats<'a> {
    data: &'a RecordBatch,
    snapshot: &'a DeltaSnapshot,
    sizes: &'a Int64Array,
    stats: &'a StructArray,
}

impl<'a> SnapshotPruningStats<'a> {
    pub(crate) fn try_new(data: &'a RecordBatch, snapshot: &'a DeltaSnapshot) -> DeltaResult<Self> {
        let sizes = batch_column::<Int64Array>(data, FIELD_NAME_SIZE)?;
        let stats = batch_column::<StructArray>(data, FIELD_NAME_STATS_PARSED)?;
        Ok(Self {
            data,
            snapshot,
            sizes,
            stats,
        })
    }

    /// The number of files in the log data.
    pub fn num_files(&self) -> usize {
        self.data.num_rows()
    }

    fn collect_count(&self, name: &str) -> Precision<usize> {
        let num_records = nested_struct_column_exact_or_path(self.stats, name)
            .and_then(|col| col.as_any().downcast_ref::<Int64Array>());
        if let Some(num_records) = num_records {
            if num_records.is_empty() {
                Precision::Exact(0)
            } else if let Some(null_count_mulls) = num_records.nulls() {
                if null_count_mulls.null_count() > 0 {
                    Precision::Absent
                } else {
                    sum(num_records)
                        .map(|s| Precision::Exact(s as usize))
                        .unwrap_or(Precision::Absent)
                }
            } else {
                sum(num_records)
                    .map(|s| Precision::Exact(s as usize))
                    .unwrap_or(Precision::Absent)
            }
        } else {
            Precision::Absent
        }
    }

    fn column_bounds(
        &self,
        path_step: &str,
        name: &str,
        fun_type: AccumulatorType,
    ) -> Precision<ScalarValue> {
        let array = match nested_column(self.stats, path_step, name) {
            Ok(array) => array,
            Err(_) => return Precision::Absent,
        };
        let array_ref = array.as_ref();

        if array_ref.data_type().is_primitive() {
            let accumulator: Option<Box<dyn Accumulator>> = match fun_type {
                AccumulatorType::Min => MinAccumulator::try_new(array_ref.data_type())
                    .map_or(None, |a| Some(Box::new(a))),
                AccumulatorType::Max => MaxAccumulator::try_new(array_ref.data_type())
                    .map_or(None, |a| Some(Box::new(a))),
            };

            if let Some(mut accumulator) = accumulator {
                return accumulator
                    .update_batch(std::slice::from_ref(array))
                    .ok()
                    .and_then(|_| accumulator.evaluate().ok())
                    .map(Precision::Exact)
                    .unwrap_or(Precision::Absent);
            }

            return Precision::Absent;
        }

        match array_ref.data_type() {
            ArrowDataType::Struct(fields) => fields
                .iter()
                .map(|f| {
                    self.column_bounds(path_step, &format!("{name}.{}", f.name()), fun_type.clone())
                })
                .map(|s| match s {
                    Precision::Exact(s) => Some(s),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()
                .map(|o| {
                    let arrays = match o
                        .into_iter()
                        .map(|sv| sv.to_array())
                        .collect::<Result<Vec<_>, DataFusionError>>()
                    {
                        Ok(arrays) => arrays,
                        Err(_) => return Precision::Absent,
                    };
                    let sa = StructArray::new(fields.clone(), arrays, None);
                    Precision::Exact(ScalarValue::Struct(Arc::new(sa)))
                })
                .unwrap_or(Precision::Absent),
            _ => Precision::Absent,
        }
    }

    fn num_records(&self) -> Precision<usize> {
        self.collect_count(STATS_FIELD_NUM_RECORDS)
    }

    fn total_size_files(&self) -> Precision<usize> {
        let size = self
            .sizes
            .iter()
            .flat_map(|s| s.map(|s| s as usize))
            .sum::<usize>();
        Precision::Inexact(size)
    }

    fn build_column_stats(&self, name: impl AsRef<str>) -> DeltaResult<ColumnStatistics> {
        let null_count_col = format!("{STATS_FIELD_NULL_COUNT}.{}", name.as_ref());
        let null_count = self.collect_count(&null_count_col);

        let min_value =
            self.column_bounds(STATS_FIELD_MIN_VALUES, name.as_ref(), AccumulatorType::Min);
        let min_value = match &min_value {
            Precision::Exact(value) if value.is_null() => Precision::Absent,
            // TODO this is a hack, we should not be casting here but rather when we read the checkpoint data.
            // it seems sometimes the min/max values are stored as nanoseconds and sometimes as microseconds?
            Precision::Exact(ScalarValue::TimestampNanosecond(a, b)) => Precision::Exact(
                ScalarValue::TimestampMicrosecond(a.map(|v| v / 1000), b.clone()),
            ),
            _ => min_value,
        };

        let max_value =
            self.column_bounds(STATS_FIELD_MAX_VALUES, name.as_ref(), AccumulatorType::Max);
        let max_value = match &max_value {
            Precision::Exact(value) if value.is_null() => Precision::Absent,
            Precision::Exact(ScalarValue::TimestampNanosecond(a, b)) => Precision::Exact(
                ScalarValue::TimestampMicrosecond(a.map(|v| v / 1000), b.clone()),
            ),
            _ => max_value,
        };

        Ok(ColumnStatistics {
            null_count,
            max_value,
            min_value,
            sum_value: Precision::Absent,
            distinct_count: Precision::Absent,
            byte_size: Precision::Absent,
        })
    }

    pub(crate) fn column_stats(&self, name: impl AsRef<str>) -> Option<ColumnStatistics> {
        self.build_column_stats(name).ok()
    }

    pub(crate) fn statistics(&self) -> Option<Statistics> {
        let num_rows = self.num_records();
        let total_byte_size = self.total_size_files();
        let column_statistics = self
            .snapshot
            .schema()
            .fields()
            .iter()
            .map(|field| self.column_stats(field.name()))
            .collect::<Option<Vec<_>>>()?;
        Some(Statistics {
            num_rows,
            total_byte_size,
            column_statistics,
        })
    }

    fn pick_stats(&self, column: &Column, stats_field: &'static str) -> Option<ArrayRef> {
        let schema = self.snapshot.schema();
        let field = schema.field_with_name(&column.name).ok()?;
        let physical_name =
            arrow_field_physical_name(field, self.snapshot.effective_column_mapping_mode());
        // See issue #1214. Binary type does not support natural order which is required for Datafusion to prune
        if matches!(
            field.data_type(),
            ArrowDataType::Binary | ArrowDataType::LargeBinary | ArrowDataType::BinaryView
        ) {
            return None;
        }
        if self
            .snapshot
            .metadata()
            .partition_columns()
            .contains(&column.name)
        {
            let partition_values =
                match batch_column::<StructArray>(self.data, FIELD_NAME_PARTITION_VALUES_PARSED) {
                    Ok(values) => values,
                    Err(err) => {
                        warn!(
                            "Failed to access partitionValues_parsed for column {}: {err}",
                            column.name()
                        );
                        return None;
                    }
                };
            return nested_struct_column_exact_or_path(partition_values, physical_name).cloned();
        }

        nested_column(self.stats, stats_field, physical_name)
            .ok()
            .cloned()
    }
}

fn batch_column<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> DeltaResult<&'a T> {
    batch
        .column_by_name(name)
        .and_then(|col| col.as_any().downcast_ref::<T>())
        .ok_or_else(|| DeltaTableError::schema(format!("column {name} not found in log data")))
}

fn nested_column<'a>(
    array: &'a StructArray,
    root: &str,
    name: &str,
) -> Result<&'a Arc<dyn Array>, DeltaTableError> {
    let current = array.column_by_name(root).ok_or_else(|| {
        DeltaTableError::schema(format!("{root} column not found in stats struct"))
    })?;
    let struct_array = current
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            DeltaTableError::schema(format!("Expected struct column for {root} in stats struct"))
        })?;
    nested_struct_column_exact_or_path(struct_array, name)
        .ok_or_else(|| DeltaTableError::schema(format!("{name} column not found in stats struct")))
}

fn nested_struct_column_exact_or_path<'a>(
    array: &'a StructArray,
    name: &str,
) -> Option<&'a Arc<dyn Array>> {
    if let Some(current) = array.column_by_name(name) {
        return Some(current);
    }

    let mut path_iter = name.split('.');
    let first = path_iter.next()?;
    let mut current = array.column_by_name(first)?;
    for segment in path_iter {
        let struct_array = current.as_any().downcast_ref::<StructArray>()?;
        current = struct_array.column_by_name(segment)?;
    }
    Some(current)
}

impl PruningStatistics for SnapshotPruningStats<'_> {
    /// return the minimum values for the named column, if known.
    /// Note: the returned array must contain `num_containers()` rows
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.pick_stats(column, STATS_FIELD_MIN_VALUES)
    }

    /// return the maximum values for the named column, if known.
    /// Note: the returned array must contain `num_containers()` rows.
    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.pick_stats(column, STATS_FIELD_MAX_VALUES)
    }

    /// return the number of containers (e.g. row groups) being
    /// pruned with these statistics
    fn num_containers(&self) -> usize {
        self.data.num_rows()
    }

    /// return the number of null values for the named column as an
    /// `Option<UInt64Array>`.
    ///
    /// Note: the returned array must contain `num_containers()` rows.
    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        if !self
            .snapshot
            .metadata()
            .partition_columns()
            .contains(&column.name)
        {
            let counts = self.pick_stats(column, STATS_FIELD_NULL_COUNT)?;
            return ::datafusion::arrow::compute::cast(counts.as_ref(), &ArrowDataType::UInt64)
                .ok();
        }
        let partition_values = self.pick_stats(column, "__dummy__")?;
        let row_counts = self.row_counts(column)?;
        let row_counts = row_counts.as_any().downcast_ref::<UInt64Array>()?;
        let mut null_counts = Vec::with_capacity(partition_values.len());
        for i in 0..partition_values.len() {
            let null_count = if partition_values.is_null(i) {
                row_counts.value(i)
            } else {
                0
            };
            null_counts.push(null_count);
        }
        Some(Arc::new(UInt64Array::from(null_counts)))
    }

    /// return the number of rows for the named column in each container
    /// as an `Option<UInt64Array>`.
    ///
    /// Note: the returned array must contain `num_containers()` rows
    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        let row_counts =
            nested_struct_column_exact_or_path(self.stats, STATS_FIELD_NUM_RECORDS)?.clone();
        ::datafusion::arrow::compute::cast(row_counts.as_ref(), &ArrowDataType::UInt64).ok()
    }

    // This function is optional but will optimize partition column pruning
    fn contained(&self, column: &Column, value: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        if value.is_empty()
            || !self
                .snapshot
                .metadata()
                .partition_columns()
                .contains(&column.name)
        {
            return None;
        }

        // Retrieve the partition values for the column
        let partition_values = self.pick_stats(column, "__dummy__")?;

        let partition_values = partition_values
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(DeltaTableError::generic(
                "failed to downcast string result to StringArray.",
            ))
            .ok()?;

        let mut contains = Vec::with_capacity(partition_values.len());

        // TODO: this was inspired by parquet's BloomFilter pruning, decide if we should
        //  just convert to Vec<String> for a subset of column types and use .contains
        fn check_scalar(pv: &str, value: &ScalarValue) -> bool {
            match value {
                ScalarValue::Utf8(Some(v))
                | ScalarValue::Utf8View(Some(v))
                | ScalarValue::LargeUtf8(Some(v)) => pv == v,

                ScalarValue::Dictionary(_, inner) => check_scalar(pv, inner),
                // FIXME: is this a good enough default or should we sync this with
                //  expr_applicable_for_cols and bail out with None
                _ => value.to_string() == pv,
            }
        }

        for i in 0..partition_values.len() {
            if partition_values.is_null(i) {
                // For IS NULL predicates, we want to include NULL partitions
                let contains_null = value.iter().any(|scalar| scalar.is_null());
                contains.push(contains_null);
            } else {
                contains.push(
                    value
                        .iter()
                        .any(|scalar| check_scalar(partition_values.value(i), scalar)),
                );
            }
        }

        Some(BooleanArray::from(contains))
    }
}
