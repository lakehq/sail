use std::collections::HashSet;

use datafusion::arrow::array::{ArrayRef, BooleanArray};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, SchemaRef as ArrowSchemaRef};
use datafusion::common::scalar::ScalarValue;
use datafusion::common::stats::{ColumnStatistics, Precision, Statistics};
use datafusion::common::{Column, ToDFSchema};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_optimizer::pruning::{PruningPredicate, PruningStatistics};
use deltalake::errors::DeltaResult;
use deltalake::kernel::Add;

use super::{get_null_of_arrow_type, to_correct_scalar_value};

pub struct AddContainer<'a> {
    inner: &'a Vec<Add>,
    partition_columns: &'a Vec<String>,
    schema: ArrowSchemaRef,
}

impl<'a> AddContainer<'a> {
    /// Create a new instance of [`AddContainer`]
    pub fn new(
        adds: &'a Vec<Add>,
        partition_columns: &'a Vec<String>,
        schema: ArrowSchemaRef,
    ) -> Self {
        Self {
            inner: adds,
            partition_columns,
            schema,
        }
    }

    pub fn get_prune_stats(&self, column: &Column, get_max: bool) -> Option<ArrayRef> {
        let (_, field) = self.schema.column_with_name(&column.name)?;

        // See issue 1214. Binary type does not support natural order which is required for Datafusion to prune
        if field.data_type() == &ArrowDataType::Binary {
            return None;
        }

        let data_type = field.data_type();

        let values = self.inner.iter().map(|add| {
            if self.partition_columns.contains(&column.name) {
                let value = add.partition_values.get(&column.name).unwrap();
                let value = match value {
                    Some(v) => serde_json::Value::String(v.to_string()),
                    None => serde_json::Value::Null,
                };
                to_correct_scalar_value(&value, data_type)
                    .ok()
                    .flatten()
                    .unwrap_or(
                        get_null_of_arrow_type(data_type).expect("Could not determine null type"),
                    )
            } else if let Ok(Some(statistics)) = add.get_stats() {
                let values = if get_max {
                    &statistics.max_values
                } else {
                    &statistics.min_values
                };

                values
                    .get(&column.name)
                    .and_then(|f| {
                        to_correct_scalar_value(f.as_value()?, data_type)
                            .ok()
                            .flatten()
                    })
                    .unwrap_or(
                        get_null_of_arrow_type(data_type).expect("Could not determine null type"),
                    )
            } else {
                get_null_of_arrow_type(data_type).expect("Could not determine null type")
            }
        });
        ScalarValue::iter_to_array(values).ok()
    }

    /// Get an iterator of add actions / files, that MAY contain data matching the predicate.
    ///
    /// Expressions are evaluated for file statistics, essentially column-wise min max bounds,
    /// so evaluating expressions is inexact. However, excluded files are guaranteed (for a correct log)
    /// to not contain matches by the predicate expression.
    pub fn predicate_matches(&self, predicate: Expr) -> DeltaResult<impl Iterator<Item = &Add>> {
        //let expr = logical_expr_to_physical_expr(predicate, &self.schema);
        let expr = SessionContext::new()
            .create_physical_expr(predicate, &self.schema.clone().to_dfschema().map_err(crate::delta_datafusion::datafusion_to_delta_error)?)
            .map_err(crate::delta_datafusion::datafusion_to_delta_error)?;
        let pruning_predicate = PruningPredicate::try_new(expr, self.schema.clone()).map_err(crate::delta_datafusion::datafusion_to_delta_error)?;
        Ok(self
            .inner
            .iter()
            .zip(pruning_predicate.prune(self).map_err(crate::delta_datafusion::datafusion_to_delta_error)?)
            .filter_map(
                |(action, keep_file)| {
                    if keep_file {
                        Some(action)
                    } else {
                        None
                    }
                },
            ))
    }

    /// Calculate statistics for this set of Add actions
    pub fn statistics(&self) -> Option<Statistics> {
        let mut total_rows = 0;
        let mut total_byte_size = 0;
        let mut column_stats_map: std::collections::HashMap<String, Vec<ColumnStatistics>> =
            std::collections::HashMap::new();

        // Collect statistics from all Add actions
        for add in self.inner {
            if let Ok(Some(stats)) = add.get_stats() {
                total_rows += stats.num_records;
                total_byte_size += add.size;

                // Process each column in the schema
                for field in self.schema.fields() {
                    let column_name = field.name();

                    let column_stat = if self.partition_columns.contains(column_name) {
                        // For partition columns, we need to handle them specially
                        let null_count = if let Some(partition_value) = add.partition_values.get(column_name) {
                            if partition_value.is_some() {
                                Precision::Exact(0)
                            } else {
                                Precision::Exact(stats.num_records as usize)
                            }
                        } else {
                            Precision::Absent
                        };

                        ColumnStatistics {
                            null_count,
                            max_value: Precision::Absent,
                            min_value: Precision::Absent,
                            sum_value: Precision::Absent,
                            distinct_count: Precision::Absent,
                        }
                    } else {
                        // For data columns, extract statistics from the stats
                        let null_count = stats.null_count.get(column_name)
                            .and_then(|nc| nc.as_value())
                            .map(|v| Precision::Exact(v as usize))
                            .unwrap_or(Precision::Absent);

                        let min_value = stats.min_values.get(column_name)
                            .and_then(|mv| mv.as_value())
                            .and_then(|v| to_correct_scalar_value(v, field.data_type()).ok().flatten())
                            .map(Precision::Exact)
                            .unwrap_or(Precision::Absent);

                        let max_value = stats.max_values.get(column_name)
                            .and_then(|mv| mv.as_value())
                            .and_then(|v| to_correct_scalar_value(v, field.data_type()).ok().flatten())
                            .map(Precision::Exact)
                            .unwrap_or(Precision::Absent);

                        ColumnStatistics {
                            null_count,
                            max_value,
                            min_value,
                            sum_value: Precision::Absent,
                            distinct_count: Precision::Absent,
                        }
                    };

                    column_stats_map.entry(column_name.clone())
                        .or_insert_with(Vec::new)
                        .push(column_stat);
                }
            } else {
                // If no statistics available, just add the file size
                total_byte_size += add.size;
            }
        }

        // Aggregate column statistics
        let column_statistics = self.schema.fields()
            .iter()
            .map(|field| {
                let column_name = field.name();
                if let Some(stats_vec) = column_stats_map.get(column_name) {
                    // Aggregate the statistics for this column
                    stats_vec.iter().fold(
                        ColumnStatistics {
                            null_count: Precision::Exact(0),
                            max_value: Precision::Absent,
                            min_value: Precision::Absent,
                            sum_value: Precision::Absent,
                            distinct_count: Precision::Absent,
                        },
                        |acc, stat| ColumnStatistics {
                            null_count: acc.null_count.add(&stat.null_count),
                            max_value: acc.max_value.max(&stat.max_value),
                            min_value: acc.min_value.min(&stat.min_value),
                            sum_value: Precision::Absent,
                            distinct_count: Precision::Absent,
                        }
                    )
                } else {
                    // No statistics available for this column
                    ColumnStatistics {
                        null_count: Precision::Absent,
                        max_value: Precision::Absent,
                        min_value: Precision::Absent,
                        sum_value: Precision::Absent,
                        distinct_count: Precision::Absent,
                    }
                }
            })
            .collect();

        Some(Statistics {
            num_rows: if total_rows > 0 {
                Precision::Exact(total_rows as usize)
            } else {
                Precision::Absent
            },
            total_byte_size: if total_byte_size > 0 {
                Precision::Inexact(total_byte_size as usize)
            } else {
                Precision::Absent
            },
            column_statistics,
        })
    }
}

impl PruningStatistics for AddContainer<'_> {
    /// return the minimum values for the named column, if known.
    /// Note: the returned array must contain `num_containers()` rows
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.get_prune_stats(column, false)
    }

    /// return the maximum values for the named column, if known.
    /// Note: the returned array must contain `num_containers()` rows.
    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.get_prune_stats(column, true)
    }

    /// return the number of containers (e.g. row groups) being
    /// pruned with these statistics
    fn num_containers(&self) -> usize {
        self.inner.len()
    }

    /// return the number of null values for the named column as an
    /// `Option<UInt64Array>`.
    ///
    /// Note: the returned array must contain `num_containers()` rows.
    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let values = self.inner.iter().map(|add| {
            if let Ok(Some(statistics)) = add.get_stats() {
                if self.partition_columns.contains(&column.name) {
                    let value = add.partition_values.get(&column.name).unwrap();
                    match value {
                        Some(_) => ScalarValue::UInt64(Some(0)),
                        None => ScalarValue::UInt64(Some(statistics.num_records as u64)),
                    }
                } else {
                    statistics
                        .null_count
                        .get(&column.name)
                        .map(|f| ScalarValue::UInt64(f.as_value().map(|val| val as u64)))
                        .unwrap_or(ScalarValue::UInt64(None))
                }
            } else if self.partition_columns.contains(&column.name) {
                let value = add.partition_values.get(&column.name).unwrap();
                match value {
                    Some(_) => ScalarValue::UInt64(Some(0)),
                    None => ScalarValue::UInt64(None),
                }
            } else {
                ScalarValue::UInt64(None)
            }
        });
        ScalarValue::iter_to_array(values).ok()
    }

    /// return the number of rows for the named column in each container
    /// as an `Option<UInt64Array>`.
    ///
    /// Note: the returned array must contain `num_containers()` rows
    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        let values = self.inner.iter().map(|add| {
            if let Ok(Some(statistics)) = add.get_stats() {
                ScalarValue::UInt64(Some(statistics.num_records as u64))
            } else {
                ScalarValue::UInt64(None)
            }
        });
        ScalarValue::iter_to_array(values).ok()
    }

    // This function is required since DataFusion 35.0, but is implemented as a no-op
    // https://github.com/apache/arrow-datafusion/blob/ec6abece2dcfa68007b87c69eefa6b0d7333f628/datafusion/core/src/datasource/physical_plan/parquet/page_filter.rs#L550
    fn contained(&self, _column: &Column, _value: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        None
    }
}
