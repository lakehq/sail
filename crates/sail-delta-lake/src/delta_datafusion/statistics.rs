use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray, UInt64Array};

use datafusion::common::scalar::ScalarValue;
use datafusion::common::Column;
use datafusion::physical_optimizer::pruning::PruningStatistics;
use deltalake::errors::DeltaResult;
use deltalake::kernel::{Add, EagerSnapshot};
use deltalake::NULL_PARTITION_VALUE_DATA_PATH;

use serde_json::Value;

use crate::operations::write::stats::{ColumnCountStat, ColumnValueStat, Stats};

/// Implementation of PruningStatistics for Delta Lake files
pub struct DeltaFileStatistics {
    files: Vec<Add>,
    partition_columns: Vec<String>,
}

impl DeltaFileStatistics {
    pub fn new(files: Vec<Add>, partition_columns: Vec<String>) -> Self {
        Self {
            files,
            partition_columns,
        }
    }

    /// Extract statistics from an Add action
    fn extract_stats_from_add(add: &Add) -> Option<Stats> {
        if let Some(stats_str) = &add.stats {
            serde_json::from_str(stats_str).ok()
        } else {
            None
        }
    }

    /// Get scalar value for a column from either partition values or statistics
    fn get_column_scalar(
        &self,
        add: &Add,
        column_name: &str,
        get_max: bool,
    ) -> Option<ScalarValue> {
        // Check if it's a partition column first
        if self.partition_columns.contains(&column_name.to_string()) {
            if let Some(partition_value) = add.partition_values.get(column_name) {
                return match partition_value {
                    Some(value) => {
                        // Check if this is the special Hive null partition value
                        if value == NULL_PARTITION_VALUE_DATA_PATH {
                            Some(ScalarValue::Null)
                        } else {
                            Self::parse_partition_value(value)
                        }
                    }
                    None => Some(ScalarValue::Null),
                };
            }
            // If partition column is not found in partition values, return Null
            return Some(ScalarValue::Null);
        }

        // Otherwise, look in file statistics
        if let Some(stats) = Self::extract_stats_from_add(add) {
            let stat_map = if get_max {
                &stats.max_values
            } else {
                &stats.min_values
            };

            if let Some(column_stat) = stat_map.get(column_name) {
                return Self::extract_scalar_from_column_stat(column_stat);
            }
        }

        None
    }

    /// Parse partition value string to ScalarValue
    fn parse_partition_value(value: &str) -> Option<ScalarValue> {
        // Check for special Hive null partition value first
        if value == NULL_PARTITION_VALUE_DATA_PATH {
            return Some(ScalarValue::Null);
        }

        // Try to parse as different types
        if let Ok(bool_val) = value.parse::<bool>() {
            return Some(ScalarValue::Boolean(Some(bool_val)));
        }
        if let Ok(int_val) = value.parse::<i64>() {
            return Some(ScalarValue::Int64(Some(int_val)));
        }
        if let Ok(float_val) = value.parse::<f64>() {
            return Some(ScalarValue::Float64(Some(float_val)));
        }
        // Default to string
        Some(ScalarValue::Utf8(Some(value.to_string())))
    }

    /// Extract ScalarValue from ColumnValueStat
    fn extract_scalar_from_column_stat(column_stat: &ColumnValueStat) -> Option<ScalarValue> {
        match column_stat {
            ColumnValueStat::Value(value) => Self::json_value_to_scalar_value(value),
            ColumnValueStat::Column(_) => None, // Nested columns not supported for now
        }
    }

    /// Convert JSON Value to ScalarValue
    fn json_value_to_scalar_value(value: &Value) -> Option<ScalarValue> {
        match value {
            Value::Null => Some(ScalarValue::Null),
            Value::Bool(b) => Some(ScalarValue::Boolean(Some(*b))),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Some(ScalarValue::Int64(Some(i)))
                } else if let Some(f) = n.as_f64() {
                    Some(ScalarValue::Float64(Some(f)))
                } else {
                    None
                }
            }
            Value::String(s) => Some(ScalarValue::Utf8(Some(s.clone()))),
            _ => None,
        }
    }

    /// Get null count for a column from statistics
    fn get_null_count(&self, add: &Add, column_name: &str) -> Option<u64> {
        if self.partition_columns.contains(&column_name.to_string()) {
            // For partition columns, null count is based on whether the partition value is null
            // If partition value is None (null), then all rows in this file are null for this column
            // If partition value is Some(value), then no rows are null for this column
            if let Some(partition_value) = add.partition_values.get(column_name) {
                return Some(match partition_value {
                    Some(value) if value == NULL_PARTITION_VALUE_DATA_PATH => {
                        // This file has __HIVE_DEFAULT_PARTITION__ for this partition column
                        self.get_row_count(add).unwrap_or(0)
                    }
                    Some(_) => {
                        // This file has a non-null value for this partition column
                        0
                    }
                    None => {
                        // This file has null for this partition column
                        self.get_row_count(add).unwrap_or(0)
                    }
                });
            }
            // If partition column is not found, assume no nulls
            return Some(0);
        }

        // For non-partition columns, get from statistics
        if let Some(stats) = Self::extract_stats_from_add(add) {
            if let Some(null_stat) = stats.null_count.get(column_name) {
                return match null_stat {
                    ColumnCountStat::Value(count) => Some(*count as u64),
                    ColumnCountStat::Column(_) => None, // Nested not supported
                };
            }
        }

        None
    }

    /// Get row count for a file
    fn get_row_count(&self, add: &Add) -> Option<u64> {
        if let Some(stats) = Self::extract_stats_from_add(add) {
            Some(stats.num_records as u64)
        } else {
            None
        }
    }
}

impl PruningStatistics for DeltaFileStatistics {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let values: Vec<ScalarValue> = self
            .files
            .iter()
            .map(|add| {
                self.get_column_scalar(add, &column.name, false)
                    .unwrap_or(ScalarValue::Null)
            })
            .collect();

        ScalarValue::iter_to_array(values).ok()
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let values: Vec<ScalarValue> = self
            .files
            .iter()
            .map(|add| {
                self.get_column_scalar(add, &column.name, true)
                    .unwrap_or(ScalarValue::Null)
            })
            .collect();

        ScalarValue::iter_to_array(values).ok()
    }

    fn num_containers(&self) -> usize {
        self.files.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let counts: Vec<u64> = self
            .files
            .iter()
            .map(|add| self.get_null_count(add, &column.name).unwrap_or(0))
            .collect();

        Some(Arc::new(UInt64Array::from(counts)))
    }

    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        let counts: Vec<u64> = self
            .files
            .iter()
            .map(|add| self.get_row_count(add).unwrap_or(0))
            .collect();

        Some(Arc::new(UInt64Array::from(counts)))
    }

    fn contained(&self, _column: &Column, _values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        // Not implemented for now - this is for more advanced pruning
        None
    }
}

/// Create DeltaFileStatistics from a snapshot and optional file list
pub fn create_file_statistics(
    snapshot: &EagerSnapshot,
    files: Option<&[Add]>,
) -> DeltaResult<DeltaFileStatistics> {
    let partition_columns = snapshot.metadata().partition_columns().clone();
    let files = match files {
        Some(file_list) => file_list.to_vec(),
        None => snapshot.file_actions()?.collect(),
    };

    Ok(DeltaFileStatistics::new(files, partition_columns))
}
