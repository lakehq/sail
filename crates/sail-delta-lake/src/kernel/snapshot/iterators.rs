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
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::arrow::array::cast::AsArray;
use datafusion::arrow::array::types::Int64Type;
use datafusion::arrow::array::{Array, RecordBatch, StructArray};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Int32Type};
use datafusion::common::scalar::ScalarValue;
use percent_encoding::percent_decode_str;

use crate::conversion::ScalarExt;
use crate::kernel::{DeltaResult, DeltaTableError};
use crate::spec::{Add, DeletionVectorDescriptor, Remove, StorageType};

const FIELD_NAME_PATH: &str = "path";
const FIELD_NAME_SIZE: &str = "size";
const FIELD_NAME_MODIFICATION_TIME: &str = "modificationTime";
const FIELD_NAME_STATS: &str = "stats";
const FIELD_NAME_STATS_PARSED: &str = "stats_parsed";
#[expect(dead_code)]
const FIELD_NAME_FILE_CONSTANT_VALUES: &str = "fileConstantValues";
#[expect(dead_code)]
const FIELD_NAME_PARTITION_VALUES: &str = "partitionValues";
const FIELD_NAME_PARTITION_VALUES_PARSED: &str = "partitionValues_parsed";
const FIELD_NAME_DELETION_VECTOR: &str = "deletionVector";

const STATS_FIELD_NUM_RECORDS: &str = "numRecords";
const STATS_FIELD_MIN_VALUES: &str = "minValues";
const STATS_FIELD_MAX_VALUES: &str = "maxValues";
const STATS_FIELD_NULL_COUNT: &str = "nullCount";

const DV_FIELD_STORAGE_TYPE: &str = "storageType";
const DV_FIELD_PATH_OR_INLINE_DV: &str = "pathOrInlineDv";
const DV_FIELD_SIZE_IN_BYTES: &str = "sizeInBytes";
const DV_FIELD_CARDINALITY: &str = "cardinality";
const DV_FIELD_OFFSET: &str = "offset";

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

    /// Converts partition values to a map of column names to serialized values.
    fn partition_values_map(&self) -> HashMap<String, Option<String>> {
        let Some(pv) = self.partition_values() else {
            return HashMap::new();
        };
        let ScalarValue::Struct(struct_array) = &pv else {
            return HashMap::new();
        };
        let fields = struct_array.fields();
        fields
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let col = struct_array.column(i);
                let sv = ScalarValue::try_from_array(col.as_ref(), 0).ok();
                let serialized = sv.as_ref().and_then(|v| {
                    if v.is_null() {
                        None
                    } else {
                        Some(v.serialize().into_owned())
                    }
                });
                (field.name().clone(), serialized)
            })
            .collect()
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

    /// Returns a view into the deletion vector for this file, if present.
    fn deletion_vector(&self) -> Option<DeletionVectorView<'_>> {
        let dv_col = self
            .files
            .column_by_name(FIELD_NAME_DELETION_VECTOR)
            .and_then(|col| col.as_struct_opt())?;
        if dv_col.null_count() == dv_col.len() {
            return None;
        }
        dv_col
            .is_valid(self.index)
            .then(|| {
                dv_col
                    .column_by_name(DV_FIELD_STORAGE_TYPE)
                    .filter(|storage_col| storage_col.is_valid(self.index))
                    .map(|_| DeletionVectorView {
                        data: dv_col,
                        index: self.index,
                    })
            })
            .flatten()
    }

    /// Converts this file view into an Add action for log operations.
    pub(crate) fn add_action(&self) -> Add {
        Add {
            path: self.path().to_string(),
            partition_values: self.partition_values_map(),
            size: self.size(),
            modification_time: self.modification_time(),
            data_change: true,
            stats: self.stats().map(|v| v.to_string()),
            tags: None,
            deletion_vector: self.deletion_vector().map(|dv| dv.descriptor()),
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
            commit_version: None,
            commit_timestamp: None,
        }
    }

    /// Converts this file view into a Remove action for log operations.
    pub fn remove_action(&self, data_change: bool) -> Remove {
        Remove {
            // TODO use the raw (still encoded) path here once we reconciled serde ...
            path: self.path().to_string(),
            data_change,
            deletion_timestamp: Some(Utc::now().timestamp_millis()),
            extended_file_metadata: Some(true),
            size: Some(self.size()),
            partition_values: Some(self.partition_values_map()),
            deletion_vector: self.deletion_vector().map(|dv| dv.descriptor()),
            tags: None,
            base_row_id: None,
            default_row_commit_version: None,
        }
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

/// Provides typed access to deletion vector metadata from log data.
///
/// This struct wraps a StructArray containing deletion vector information
/// and provides zero-copy access to individual fields through an index.
#[derive(Debug)]
struct DeletionVectorView<'a> {
    data: &'a StructArray,
    /// Index into the deletion vector data array.
    index: usize,
}

impl DeletionVectorView<'_> {
    /// Converts this view into a DeletionVectorDescriptor.
    fn descriptor(&self) -> DeletionVectorDescriptor {
        let storage_type =
            StorageType::from_str(self.storage_type()).unwrap_or(StorageType::UuidRelativePath);
        DeletionVectorDescriptor {
            storage_type,
            path_or_inline_dv: self.path_or_inline_dv().to_string(),
            size_in_bytes: self.size_in_bytes(),
            cardinality: self.cardinality(),
            offset: self.offset(),
        }
    }

    /// Returns the storage type of the deletion vector.
    fn storage_type(&self) -> &str {
        self.data
            .column_by_name(DV_FIELD_STORAGE_TYPE)
            .and_then(|col| get_string_value(col.as_ref(), self.index))
            .unwrap_or("")
    }

    /// Returns the path or inline data for the deletion vector.
    fn path_or_inline_dv(&self) -> &str {
        self.data
            .column_by_name(DV_FIELD_PATH_OR_INLINE_DV)
            .and_then(|col| get_string_value(col.as_ref(), self.index))
            .unwrap_or("")
    }

    /// Returns the size of the deletion vector in bytes.
    fn size_in_bytes(&self) -> i32 {
        self.data
            .column_by_name(DV_FIELD_SIZE_IN_BYTES)
            .map(|c| c.as_primitive::<Int32Type>())
            .filter(|c| c.is_valid(self.index))
            .map(|c| c.value(self.index))
            .unwrap_or(0)
    }

    /// Returns the number of deleted rows represented by this deletion vector.
    fn cardinality(&self) -> i64 {
        self.data
            .column_by_name(DV_FIELD_CARDINALITY)
            .map(|c| c.as_primitive::<Int64Type>())
            .filter(|c| c.is_valid(self.index))
            .map(|c| c.value(self.index))
            .unwrap_or(0)
    }

    /// Returns the offset within the deletion vector file, if applicable.
    fn offset(&self) -> Option<i32> {
        let col = self
            .data
            .column_by_name(DV_FIELD_OFFSET)
            .map(|c| c.as_primitive::<Int32Type>())?;
        col.is_valid(self.index).then(|| col.value(self.index))
    }
}

/// Extracts a string value from an Arrow array at the specified index.
///
/// Handles different string array types (Utf8, LargeUtf8, Utf8View) and
/// returns None for null values or unsupported types.
fn get_string_value(data: &dyn Array, index: usize) -> Option<&str> {
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
