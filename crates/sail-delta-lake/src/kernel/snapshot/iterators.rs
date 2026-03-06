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

// [Credit]: <https://github.com/delta-io/delta-rs/blob/1f0b4d0965a85400c1effc6e9b4c7ebbb6795978/crates/core/src/kernel/snapshot/iterators.rs>

use std::borrow::Cow;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::arrow::array::cast::AsArray;
use datafusion::arrow::array::types::Int64Type;
use datafusion::arrow::array::{Array, RecordBatch, StructArray};
use datafusion::arrow::datatypes::DataType as ArrowDataType;
use datafusion::common::scalar::ScalarValue;
use percent_encoding::percent_decode_str;

use crate::spec::fields::{
    FIELD_NAME_MODIFICATION_TIME, FIELD_NAME_PARTITION_VALUES_PARSED, FIELD_NAME_PATH,
    FIELD_NAME_SIZE, FIELD_NAME_STATS, FIELD_NAME_STATS_PARSED, STATS_FIELD_MAX_VALUES,
    STATS_FIELD_MIN_VALUES, STATS_FIELD_NULL_COUNT, STATS_FIELD_NUM_RECORDS,
};
use crate::spec::{DeltaError as DeltaTableError, DeltaResult};

/// Provides semantic, typed access to file metadata from Delta log replay.
///
/// This struct wraps a RecordBatch containing file data and provides zero-copy
/// access to individual file entries through an index. It serves as a view into
/// the kernel's log replay results, offering convenient methods to extract
/// file properties without unnecessary data copies.
#[derive(Clone)]
pub struct LogicalFileView {
    files: RecordBatch,
    index: usize,
}

impl LogicalFileView {
    /// Creates a new view into the specified file entry.
    pub(crate) fn new(files: RecordBatch, index: usize) -> Self {
        Self { files, index }
    }

    pub(super) fn record_batch(&self) -> &RecordBatch {
        &self.files
    }

    pub(super) fn row_index(&self) -> usize {
        self.index
    }

    /// Returns the file path with URL decoding applied.
    pub fn path(&self) -> Cow<'_, str> {
        if let Some(raw) = self
            .files
            .column_by_name(FIELD_NAME_PATH)
            .and_then(|col| get_string_value(col.as_ref(), self.index))
        {
            return percent_decode_str(raw).decode_utf8_lossy();
        }
        Cow::Borrowed("")
    }

    /// Returns the file size in bytes.
    pub fn size(&self) -> i64 {
        self.files
            .column_by_name(FIELD_NAME_SIZE)
            .map(|c| c.as_primitive::<Int64Type>())
            .filter(|c| c.is_valid(self.index))
            .map(|c| c.value(self.index))
            .unwrap_or(0)
    }

    /// Returns the file modification time in milliseconds since Unix epoch.
    pub fn modification_time(&self) -> i64 {
        self.files
            .column_by_name(FIELD_NAME_MODIFICATION_TIME)
            .map(|c| c.as_primitive::<Int64Type>())
            .filter(|c| c.is_valid(self.index))
            .map(|c| c.value(self.index))
            .unwrap_or(0)
    }

    /// Returns the file modification time as a UTC DateTime.
    pub fn modification_datetime(&self) -> DeltaResult<chrono::DateTime<Utc>> {
        DateTime::from_timestamp_millis(self.modification_time()).ok_or_else(|| {
            DeltaTableError::generic(format!(
                "invalid modification_time: {:?}",
                self.modification_time()
            ))
        })
    }

    /// Returns the raw JSON statistics string for this file, if available.
    pub fn stats(&self) -> Option<&str> {
        self.files
            .column_by_name(FIELD_NAME_STATS)
            .and_then(|col| get_string_value(col.as_ref(), self.index))
    }

    /// Returns the parsed partition values as a `ScalarValue::Struct`, if available.
    pub fn partition_values(&self) -> Option<ScalarValue> {
        self.files
            .column_by_name(FIELD_NAME_PARTITION_VALUES_PARSED)
            .and_then(|col| col.as_struct_opt())
            .and_then(|arr| {
                arr.is_valid(self.index)
                    .then(|| ScalarValue::try_from_array(arr, self.index).ok())
                    .flatten()
            })
    }

    /// Returns the parsed statistics as a StructArray, if available.
    fn stats_parsed(&self) -> Option<&StructArray> {
        self.files
            .column_by_name(FIELD_NAME_STATS_PARSED)
            .and_then(|col| col.as_struct_opt())
    }

    /// Returns the number of records in this file.
    pub fn num_records(&self) -> Option<usize> {
        self.stats_parsed()
            .and_then(|stats| stats.column_by_name(STATS_FIELD_NUM_RECORDS))
            .and_then(|col| col.as_primitive_opt::<Int64Type>())
            .map(|a| a.value(self.index) as usize)
    }

    /// Returns null counts for all columns in this file as a `ScalarValue`.
    pub fn null_counts(&self) -> Option<ScalarValue> {
        self.stats_parsed()
            .and_then(|stats| stats.column_by_name(STATS_FIELD_NULL_COUNT))
            .and_then(|c| ScalarValue::try_from_array(c.as_ref(), self.index).ok())
    }

    /// Returns minimum values for all columns with statistics in this file as a `ScalarValue`.
    pub fn min_values(&self) -> Option<ScalarValue> {
        self.stats_parsed()
            .and_then(|stats| stats.column_by_name(STATS_FIELD_MIN_VALUES))
            .and_then(|c| ScalarValue::try_from_array(c.as_ref(), self.index).ok())
    }

    /// Returns maximum values for all columns in this file as a `ScalarValue`.
    ///
    /// For timestamp columns, values are rounded up to handle microsecond truncation
    /// in checkpoint statistics.
    pub fn max_values(&self) -> Option<ScalarValue> {
        self.stats_parsed()
            .and_then(|stats| stats.column_by_name(STATS_FIELD_MAX_VALUES))
            .and_then(|c| ScalarValue::try_from_array(c.as_ref(), self.index).ok())
            .map(|s| round_ms_datetimes(s, &ceil_datetime))
    }
}

/// Rounds up timestamp values to handle microsecond truncation in checkpoint statistics.
///
/// When delta.checkpoint.writeStatsAsStruct is enabled, microsecond timestamps are
/// truncated to milliseconds. This function rounds up by 1ms to ensure correct
/// range queries when stats are parsed on-the-fly.
fn ceil_datetime(v: i64) -> i64 {
    let remainder = v % 1000;
    if remainder == 0 {
        // if nanoseconds precision remainder is 0, we assume it was truncated
        // else we use the exact stats
        ((v as f64 / 1000.0).floor() as i64 + 1) * 1000
    } else {
        v
    }
}

/// Recursively applies a rounding function to timestamp values in a `ScalarValue`.
fn round_ms_datetimes<F>(value: ScalarValue, func: &F) -> ScalarValue
where
    F: Fn(i64) -> i64,
{
    match value {
        ScalarValue::TimestampMicrosecond(Some(v), tz) => {
            ScalarValue::TimestampMicrosecond(Some(func(v)), tz)
        }
        ScalarValue::Struct(struct_array) => {
            let fields = struct_array.fields().clone();
            let new_columns: Vec<Arc<dyn Array>> = fields
                .iter()
                .enumerate()
                .map(|(i, _field)| {
                    let col = struct_array.column(i);
                    let sv =
                        ScalarValue::try_from_array(col.as_ref(), 0).unwrap_or(ScalarValue::Null);
                    let rounded = round_ms_datetimes(sv, func);
                    rounded.to_array_of_size(1).unwrap_or_else(|_| col.clone())
                })
                .collect();
            let new_struct = StructArray::new(fields, new_columns, None);
            ScalarValue::Struct(Arc::new(new_struct))
        }
        other => other,
    }
}

/// Extracts a string value from an Arrow array at the specified index.
///
/// Handles different string array types (Utf8, LargeUtf8, Utf8View) and
/// returns None for null values or unsupported types.
pub(super) fn get_string_value(data: &dyn Array, index: usize) -> Option<&str> {
    match data.data_type() {
        ArrowDataType::Utf8 => {
            let arr = data.as_string::<i32>();
            arr.is_valid(index).then(|| arr.value(index))
        }
        ArrowDataType::LargeUtf8 => {
            let arr = data.as_string::<i64>();
            arr.is_valid(index).then(|| arr.value(index))
        }
        ArrowDataType::Utf8View => {
            let arr = data.as_string_view();
            arr.is_valid(index).then(|| arr.value(index))
        }
        _ => None,
    }
}
