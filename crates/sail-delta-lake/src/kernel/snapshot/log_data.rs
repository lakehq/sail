// https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
//
// Copyright (2020) QP Hou and a number of other contributors.
// Portions Copyright (2025) LakeSail, Inc.
// Modified in 2025 by LakeSail, Inc.
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

// [Credit]: <https://github.com/delta-io/delta-rs/blob/1f0b4d0965a85400c1effc6e9b4c7ebbb6795978/crates/core/src/kernel/snapshot/log_data.rs>

use ::datafusion::arrow::array::{Array, RecordBatch, StringArray, StructArray};
use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::scan::scan_row_schema;
use delta_kernel::table_configuration::TableConfiguration;
use delta_kernel::table_properties::TableProperties;
use log::warn;

use crate::kernel::snapshot::iterators::LogicalFileView;
use crate::kernel::{DeltaResult, DeltaTableError};

const COL_NUM_RECORDS: &str = "numRecords";
const COL_MIN_VALUES: &str = "minValues";
const COL_MAX_VALUES: &str = "maxValues";
const COL_NULL_COUNT: &str = "nullCount";

/// Provides semanitc access to the log data.
///
/// This is a helper struct that provides access to the log data in a more semantic way
/// to avid the necessiity of knowing the exact layout of the underlying log data.
#[derive(Clone)]
pub struct LogDataHandler<'a> {
    data: &'a RecordBatch,
    config: &'a TableConfiguration,
}

impl<'a> LogDataHandler<'a> {
    pub(crate) fn new(data: &'a RecordBatch, config: &'a TableConfiguration) -> Self {
        Self { data, config }
    }

    #[allow(dead_code)]
    pub(crate) fn table_configuration(&self) -> &TableConfiguration {
        self.config
    }

    #[allow(dead_code)]
    pub(crate) fn table_properties(&self) -> &TableProperties {
        self.config.table_properties()
    }

    #[allow(dead_code)]
    pub(crate) fn protocol(&self) -> &Protocol {
        self.config.protocol()
    }

    #[allow(dead_code)]
    pub(crate) fn metadata(&self) -> &Metadata {
        self.config.metadata()
    }

    /// The number of files in the log data.
    pub fn num_files(&self) -> usize {
        self.data.num_rows()
    }

    pub fn iter(&self) -> impl Iterator<Item = LogicalFileView> {
        let batch = self.data.clone();
        (0..batch.num_rows()).map(move |idx| LogicalFileView::new(batch.clone(), idx))
    }
}

impl IntoIterator for LogDataHandler<'_> {
    type Item = LogicalFileView;
    type IntoIter = Box<dyn Iterator<Item = Self::Item>>;

    fn into_iter(self) -> Self::IntoIter {
        let batch = self.data.clone();
        Box::new((0..self.data.num_rows()).map(move |idx| LogicalFileView::new(batch.clone(), idx)))
    }
}

mod datafusion {
    use std::collections::HashSet;
    use std::sync::{Arc, LazyLock};

    use ::datafusion::arrow::array::{ArrayRef, BooleanArray, Int64Array, UInt64Array};
    use ::datafusion::arrow::compute::sum;
    use ::datafusion::common::scalar::ScalarValue;
    use ::datafusion::common::stats::{ColumnStatistics, Precision, Statistics};
    use ::datafusion::common::{Column, DataFusionError};
    use ::datafusion::functions_aggregate::min_max::{MaxAccumulator, MinAccumulator};
    use ::datafusion::physical_optimizer::pruning::PruningStatistics;
    use ::datafusion::physical_plan::Accumulator;
    use arrow_schema::DataType as ArrowDataType;
    use delta_kernel::expressions::Expression;
    use delta_kernel::schema::{DataType, PrimitiveType};
    use delta_kernel::{EvaluationHandler, ExpressionEvaluator};

    use super::*;
    use crate::kernel::arrow::engine_ext::ExpressionEvaluatorExt as _;
    use crate::kernel::ARROW_HANDLER;

    #[derive(Debug, Default, Clone)]
    enum AccumulatorType {
        Min,
        Max,
        #[default]
        Unused,
    }
    // TODO validate this works with "wide and narrow" builds / stats

    /// Helper for processing data from the materialized Delta log.
    struct FileStatsAccessor<'a> {
        sizes: &'a Int64Array,
        stats: &'a StructArray,
    }

    impl<'a> FileStatsAccessor<'a> {
        pub(crate) fn try_new(data: &'a RecordBatch) -> DeltaResult<Self> {
            let sizes = batch_column::<Int64Array>(data, "size")?;
            let stats = batch_column::<StructArray>(data, "stats_parsed")?;
            Ok(Self { sizes, stats })
        }
    }

    impl FileStatsAccessor<'_> {
        fn collect_count(&self, name: &str) -> Precision<usize> {
            let num_records = struct_column_opt::<Int64Array>(self.stats, name);
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
            let mut path = name.split('.');
            let array = match nested_column(self.stats, path_step, &mut path) {
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
                    _ => None,
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
                        self.column_bounds(
                            path_step,
                            &format!("{name}.{}", f.name()),
                            fun_type.clone(),
                        )
                    })
                    .map(|s| match s {
                        Precision::Exact(s) => Some(s),
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>()
                    .map(|o| {
                        let arrays = match o.into_iter().map(|sv| sv.to_array()).collect::<Result<
                            Vec<_>,
                            DataFusionError,
                        >>(
                        ) {
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
            self.collect_count(COL_NUM_RECORDS)
        }

        fn total_size_files(&self) -> Precision<usize> {
            let size = self
                .sizes
                .iter()
                .flat_map(|s| s.map(|s| s as usize))
                .sum::<usize>();
            Precision::Inexact(size)
        }

        fn column_stats(&self, name: impl AsRef<str>) -> DeltaResult<ColumnStatistics> {
            let null_count_col = format!("{COL_NULL_COUNT}.{}", name.as_ref());
            let null_count = self.collect_count(&null_count_col);

            let min_value = self.column_bounds(COL_MIN_VALUES, name.as_ref(), AccumulatorType::Min);
            let min_value = match &min_value {
                Precision::Exact(value) if value.is_null() => Precision::Absent,
                // TODO this is a hack, we should not be casting here but rather when we read the checkpoint data.
                // it seems sometimes the min/max values are stored as nanoseconds and sometimes as microseconds?
                Precision::Exact(ScalarValue::TimestampNanosecond(a, b)) => Precision::Exact(
                    ScalarValue::TimestampMicrosecond(a.map(|v| v / 1000), b.clone()),
                ),
                _ => min_value,
            };

            let max_value = self.column_bounds(COL_MAX_VALUES, name.as_ref(), AccumulatorType::Max);
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
            })
        }
    }

    trait StatsExt {
        fn add(&self, other: &Self) -> Self;
    }

    impl StatsExt for ColumnStatistics {
        fn add(&self, other: &Self) -> Self {
            Self {
                null_count: self.null_count.add(&other.null_count),
                max_value: self.max_value.max(&other.max_value),
                min_value: self.min_value.min(&other.min_value),
                sum_value: Precision::Absent,
                distinct_count: self.distinct_count.add(&other.distinct_count),
            }
        }
    }

    impl LogDataHandler<'_> {
        fn num_records(&self) -> Precision<usize> {
            FileStatsAccessor::try_new(self.data)
                .map(|a| a.num_records())
                .into_iter()
                .reduce(|acc, num_records| acc.add(&num_records))
                .unwrap_or(Precision::Absent)
        }

        fn total_size_files(&self) -> Precision<usize> {
            FileStatsAccessor::try_new(self.data)
                .map(|a| a.total_size_files())
                .into_iter()
                .reduce(|acc, size| acc.add(&size))
                .unwrap_or(Precision::Absent)
        }

        pub(crate) fn column_stats(&self, name: impl AsRef<str>) -> Option<ColumnStatistics> {
            FileStatsAccessor::try_new(self.data)
                .map(|a| a.column_stats(name.as_ref()))
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .ok()?
                .iter()
                .fold(None::<ColumnStatistics>, |acc, stats| match (acc, stats) {
                    (None, stats) => Some(stats.clone()),
                    (Some(acc), stats) => Some(acc.add(stats)),
                })
        }

        pub(crate) fn statistics(&self) -> Option<Statistics> {
            let num_rows = self.num_records();
            let total_byte_size = self.total_size_files();
            let column_statistics = self
                .config
                .schema()
                .fields()
                .map(|f| self.column_stats(f.name()))
                .collect::<Option<Vec<_>>>()?;
            Some(Statistics {
                num_rows,
                total_byte_size,
                column_statistics,
            })
        }

        fn pick_stats(&self, column: &Column, stats_field: &'static str) -> Option<ArrayRef> {
            let schema = self.config.schema();
            let field = schema.field(&column.name)?;
            // See issue #1214. Binary type does not support natural order which is required for Datafusion to prune
            if field.data_type() == &DataType::Primitive(PrimitiveType::Binary) {
                return None;
            }
            let expression = if self
                .config
                .metadata()
                .partition_columns()
                .contains(&column.name)
            {
                Expression::column(["partitionValues_parsed", &column.name])
            } else {
                Expression::column(["stats_parsed", stats_field, &column.name])
            };
            // `nullCount` is always a Long/Int64 count in stats (independent of column type).
            let output_type = match stats_field {
                COL_NULL_COUNT => DataType::Primitive(PrimitiveType::Long),
                _ => field.data_type().clone(),
            };
            let evaluator = match ARROW_HANDLER.new_expression_evaluator(
                scan_row_schema(),
                Arc::new(expression),
                output_type,
            ) {
                Ok(value) => value,
                Err(err) => {
                    warn!(
                        "Failed to construct stats evaluator for column {} (field {stats_field}): {err}",
                        column.name()
                    );
                    return None;
                }
            };
            let batch = match evaluator.evaluate_arrow(self.data.clone()) {
                Ok(batch) => batch,
                Err(err) => {
                    warn!(
                        "Failed to evaluate stats expression for column {} (field {stats_field}): {err}",
                        column.name()
                    );
                    return None;
                }
            };
            batch.column_by_name("output").cloned()
        }
    }

    fn batch_column<'a, T: Array + 'static>(
        batch: &'a RecordBatch,
        name: &str,
    ) -> DeltaResult<&'a T> {
        batch
            .column_by_name(name)
            .and_then(|col| col.as_any().downcast_ref::<T>())
            .ok_or_else(|| DeltaTableError::schema(format!("column {name} not found in log data")))
    }

    fn struct_column_opt<'a, T: Array + 'static>(
        array: &'a StructArray,
        name: &str,
    ) -> Option<&'a T> {
        array
            .column_by_name(name)
            .and_then(|col| col.as_any().downcast_ref::<T>())
    }

    fn nested_column<'a>(
        array: &'a StructArray,
        root: &str,
        path: &mut impl Iterator<Item = &'a str>,
    ) -> Result<&'a Arc<dyn Array>, DeltaTableError> {
        let mut current = array.column_by_name(root).ok_or_else(|| {
            DeltaTableError::schema(format!("{root} column not found in stats struct"))
        })?;
        for segment in path {
            let struct_array = current
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    DeltaTableError::schema(format!(
                        "Expected struct while accessing {segment} in stats"
                    ))
                })?;
            current = struct_array.column_by_name(segment).ok_or_else(|| {
                DeltaTableError::schema(format!("{segment} column not found in stats struct"))
            })?;
        }
        Ok(current)
    }

    impl PruningStatistics for LogDataHandler<'_> {
        /// return the minimum values for the named column, if known.
        /// Note: the returned array must contain `num_containers()` rows
        fn min_values(&self, column: &Column) -> Option<ArrayRef> {
            self.pick_stats(column, "minValues")
        }

        /// return the maximum values for the named column, if known.
        /// Note: the returned array must contain `num_containers()` rows.
        fn max_values(&self, column: &Column) -> Option<ArrayRef> {
            self.pick_stats(column, "maxValues")
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
                .config
                .metadata()
                .partition_columns()
                .contains(&column.name)
            {
                let counts = self.pick_stats(column, "nullCount")?;
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
            static ROW_COUNTS_EVAL: LazyLock<Option<Arc<dyn ExpressionEvaluator>>> =
                LazyLock::new(|| {
                    ARROW_HANDLER
                        .new_expression_evaluator(
                            scan_row_schema(),
                            Arc::new(Expression::column(["stats_parsed", "numRecords"])),
                            DataType::Primitive(PrimitiveType::Long),
                        )
                        .ok()
                });

            let evaluator = ROW_COUNTS_EVAL.as_ref()?;
            let batch = evaluator.evaluate_arrow(self.data.clone()).ok()?;
            ::datafusion::arrow::compute::cast(
                batch.column_by_name("output")?,
                &ArrowDataType::UInt64,
            )
            .ok()
        }

        // This function is optional but will optimize partition column pruning
        fn contained(&self, column: &Column, value: &HashSet<ScalarValue>) -> Option<BooleanArray> {
            if value.is_empty()
                || !self
                    .config
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
}
