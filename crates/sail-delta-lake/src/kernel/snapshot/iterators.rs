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
use std::sync::LazyLock;

// TODO: Stop depending on delta-rs StorageType.
use chrono::{DateTime, Utc};
use datafusion::arrow::array::cast::AsArray;
use datafusion::arrow::array::types::Int64Type;
use datafusion::arrow::array::{Array, RecordBatch, StructArray};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Int32Type};
use delta_kernel::expressions::{Scalar, StructData};
use delta_kernel::scan::scan_row_schema;
use delta_kernel::schema::DataType;
use percent_encoding::percent_decode_str;

use crate::kernel::models::{Add, DeletionVectorDescriptor, Remove, ScalarExt, StorageType};
use crate::kernel::{DeltaResult, DeltaTableError};

const FIELD_NAME_PATH: &str = "path";
const FIELD_NAME_SIZE: &str = "size";
const FIELD_NAME_MODIFICATION_TIME: &str = "modificationTime";
const FIELD_NAME_STATS: &str = "stats";
const FIELD_NAME_STATS_PARSED: &str = "stats_parsed";
#[allow(dead_code)]
const FIELD_NAME_FILE_CONSTANT_VALUES: &str = "fileConstantValues";
#[allow(dead_code)]
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

static FIELD_INDICES: LazyLock<HashMap<&'static str, usize>> = LazyLock::new(|| {
    let schema = scan_row_schema();
    let mut indices = HashMap::new();

    if let Some(path_idx) = schema.index_of(FIELD_NAME_PATH) {
        indices.insert(FIELD_NAME_PATH, path_idx);
    }

    if let Some(size_idx) = schema.index_of(FIELD_NAME_SIZE) {
        indices.insert(FIELD_NAME_SIZE, size_idx);
    }

    if let Some(modification_time_idx) = schema.index_of(FIELD_NAME_MODIFICATION_TIME) {
        indices.insert(FIELD_NAME_MODIFICATION_TIME, modification_time_idx);
    }

    if let Some(stats_idx) = schema.index_of(FIELD_NAME_STATS) {
        indices.insert(FIELD_NAME_STATS, stats_idx);
    }

    indices
});

static DV_FIELD_INDICES: LazyLock<HashMap<&'static str, usize>> = LazyLock::new(|| {
    let schema = scan_row_schema();
    let mut indices = HashMap::new();

    if let Some(dv_field) = schema.field(FIELD_NAME_DELETION_VECTOR) {
        if let DataType::Struct(dv_type) = dv_field.data_type() {
            if let Some(storage_type_idx) = dv_type.index_of(DV_FIELD_STORAGE_TYPE) {
                indices.insert(DV_FIELD_STORAGE_TYPE, storage_type_idx);
            }

            if let Some(path_or_inline_dv_idx) = dv_type.index_of(DV_FIELD_PATH_OR_INLINE_DV) {
                indices.insert(DV_FIELD_PATH_OR_INLINE_DV, path_or_inline_dv_idx);
            }

            if let Some(size_in_bytes_idx) = dv_type.index_of(DV_FIELD_SIZE_IN_BYTES) {
                indices.insert(DV_FIELD_SIZE_IN_BYTES, size_in_bytes_idx);
            }

            if let Some(cardinality_idx) = dv_type.index_of(DV_FIELD_CARDINALITY) {
                indices.insert(DV_FIELD_CARDINALITY, cardinality_idx);
            }
        }
    }

    indices
});

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
        if let Some(&path_idx) = FIELD_INDICES.get(FIELD_NAME_PATH) {
            if let Some(raw) = get_string_value(self.files.column(path_idx), self.index) {
                return percent_decode_str(raw).decode_utf8_lossy();
            }
        }
        Cow::Borrowed("")
    }

    /// Returns the file size in bytes.
    pub fn size(&self) -> i64 {
        if let Some(&size_idx) = FIELD_INDICES.get(FIELD_NAME_SIZE) {
            self.files
                .column(size_idx)
                .as_primitive::<Int64Type>()
                .value(self.index)
        } else {
            0
        }
    }

    /// Returns the file modification time in milliseconds since Unix epoch.
    pub fn modification_time(&self) -> i64 {
        if let Some(&mod_time_idx) = FIELD_INDICES.get(FIELD_NAME_MODIFICATION_TIME) {
            self.files
                .column(mod_time_idx)
                .as_primitive::<Int64Type>()
                .value(self.index)
        } else {
            0
        }
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
        FIELD_INDICES
            .get(FIELD_NAME_STATS)
            .and_then(|&stats_idx| get_string_value(self.files.column(stats_idx), self.index))
    }

    /// Returns the parsed partition values as structured data.
    pub fn partition_values(&self) -> Option<StructData> {
        self.files
            .column_by_name(FIELD_NAME_PARTITION_VALUES_PARSED)
            .and_then(|col| col.as_struct_opt())
            .and_then(|arr| {
                arr.is_valid(self.index)
                    .then(|| match Scalar::from_array(arr, self.index) {
                        Some(Scalar::Struct(s)) => Some(s),
                        _ => None,
                    })
                    .flatten()
            })
    }

    /// Converts partition values to a map of column names to serialized values.
    fn partition_values_map(&self) -> HashMap<String, Option<String>> {
        self.partition_values()
            .map(|data| {
                data.fields()
                    .iter()
                    .zip(data.values().iter())
                    .map(|(k, v)| {
                        (
                            k.name().to_string(),
                            if v.is_null() {
                                None
                            } else {
                                Some(v.serialize().into_owned())
                            },
                        )
                    })
                    .collect()
            })
            .unwrap_or_default()
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

    /// Returns null counts for all columns in this file as structured data.
    pub fn null_counts(&self) -> Option<Scalar> {
        self.stats_parsed()
            .and_then(|stats| stats.column_by_name(STATS_FIELD_NULL_COUNT))
            .and_then(|c| Scalar::from_array(c.as_ref(), self.index))
    }

    /// Returns minimum values for all columns with statics in this file as structured data.
    pub fn min_values(&self) -> Option<Scalar> {
        self.stats_parsed()
            .and_then(|stats| stats.column_by_name(STATS_FIELD_MIN_VALUES))
            .and_then(|c| Scalar::from_array(c.as_ref(), self.index))
    }

    /// Returns maximum values for all columns in this file as structured data.
    ///
    /// For timestamp columns, values are rounded up to handle microsecond truncation
    /// in checkpoint statistics.
    pub fn max_values(&self) -> Option<Scalar> {
        self.stats_parsed()
            .and_then(|stats| stats.column_by_name(STATS_FIELD_MAX_VALUES))
            .and_then(|c| Scalar::from_array(c.as_ref(), self.index))
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
                DV_FIELD_INDICES
                    .get(DV_FIELD_STORAGE_TYPE)
                    .and_then(|&storage_idx| {
                        let storage_col = dv_col.column(storage_idx);
                        storage_col
                            .is_valid(self.index)
                            .then_some(DeletionVectorView {
                                data: dv_col,
                                index: self.index,
                            })
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

/// Recursively applies a rounding function to timestamp values in scalar data.
fn round_ms_datetimes<F>(value: Scalar, func: &F) -> Scalar
where
    F: Fn(i64) -> i64,
{
    match value {
        Scalar::Timestamp(v) => Scalar::Timestamp(func(v)),
        Scalar::TimestampNtz(v) => Scalar::TimestampNtz(func(v)),
        Scalar::Struct(ref struct_data) => {
            let mut fields = Vec::new();
            let mut scalars = Vec::new();

            for (field, scalar_value) in
                struct_data.fields().iter().zip(struct_data.values().iter())
            {
                fields.push(field.clone());
                scalars.push(round_ms_datetimes(scalar_value.clone(), func));
            }
            match StructData::try_new(fields, scalars) {
                Ok(data) => Scalar::Struct(data),
                Err(_) => value, // Return original value if struct creation fails
            }
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
        DV_FIELD_INDICES
            .get(DV_FIELD_STORAGE_TYPE)
            .and_then(|&idx| get_string_value(self.data.column(idx), self.index))
            .unwrap_or("")
    }

    /// Returns the path or inline data for the deletion vector.
    fn path_or_inline_dv(&self) -> &str {
        DV_FIELD_INDICES
            .get(DV_FIELD_PATH_OR_INLINE_DV)
            .and_then(|&idx| get_string_value(self.data.column(idx), self.index))
            .unwrap_or("")
    }

    /// Returns the size of the deletion vector in bytes.
    fn size_in_bytes(&self) -> i32 {
        DV_FIELD_INDICES
            .get(DV_FIELD_SIZE_IN_BYTES)
            .map(|&idx| {
                self.data
                    .column(idx)
                    .as_primitive::<Int32Type>()
                    .value(self.index)
            })
            .unwrap_or(0)
    }

    /// Returns the number of deleted rows represented by this deletion vector.
    fn cardinality(&self) -> i64 {
        DV_FIELD_INDICES
            .get(DV_FIELD_CARDINALITY)
            .map(|&idx| {
                self.data
                    .column(idx)
                    .as_primitive::<Int64Type>()
                    .value(self.index)
            })
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
