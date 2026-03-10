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

// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/kernel/scalars.rs>

use std::borrow::Cow;
use std::fmt::Write;
use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, LargeStringArray, RecordBatch, StringArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use datafusion::arrow::compute::{cast, cast_with_options, CastOptions};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use datafusion::common::scalar::ScalarValue;
use datafusion::common::Result as DataFusionResult;
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use serde_json::Value;

use crate::spec::{DeltaError as DeltaTableError, DeltaResult as DeltaResultLocal, StatValue};

pub const NULL_PARTITION_VALUE_DATA_PATH: &str = "__HIVE_DEFAULT_PARTITION__";

const RFC3986_PART: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

#[derive(Debug)]
pub struct ScalarConverter;

impl ScalarConverter {
    pub fn stat_values_to_array(
        values: &[Option<&StatValue>],
        field_dt: &ArrowDataType,
    ) -> DataFusionResult<Option<ArrayRef>> {
        macro_rules! typed_array {
            ($array_ty:ty, $extract:expr) => {{
                let mut out = Vec::with_capacity(values.len());
                for value in values {
                    match value {
                        None => out.push(None),
                        Some(StatValue::Null) => out.push(None),
                        Some(value) => {
                            let Some(converted) = $extract(value) else {
                                return Ok(None);
                            };
                            out.push(Some(converted));
                        }
                    }
                }
                Ok(Some(Arc::new(<$array_ty>::from(out)) as ArrayRef))
            }};
        }

        match field_dt {
            ArrowDataType::Boolean => typed_array!(BooleanArray, |value: &StatValue| match value {
                StatValue::Boolean(value) => Some(*value),
                _ => None,
            }),
            ArrowDataType::Int8 => {
                typed_array!(Int8Array, |value: &StatValue| match value {
                    StatValue::Number(value) => value.as_i64().and_then(|v| i8::try_from(v).ok()),
                    _ => None,
                })
            }
            ArrowDataType::Int16 => {
                typed_array!(Int16Array, |value: &StatValue| match value {
                    StatValue::Number(value) => value.as_i64().and_then(|v| i16::try_from(v).ok()),
                    _ => None,
                })
            }
            ArrowDataType::Int32 => {
                typed_array!(Int32Array, |value: &StatValue| match value {
                    StatValue::Number(value) => value.as_i64().and_then(|v| i32::try_from(v).ok()),
                    _ => None,
                })
            }
            ArrowDataType::Int64 => typed_array!(Int64Array, |value: &StatValue| match value {
                StatValue::Number(value) => value.as_i64(),
                _ => None,
            }),
            ArrowDataType::UInt8 => {
                typed_array!(UInt8Array, |value: &StatValue| match value {
                    StatValue::Number(value) => value.as_u64().and_then(|v| u8::try_from(v).ok()),
                    _ => None,
                })
            }
            ArrowDataType::UInt16 => {
                typed_array!(UInt16Array, |value: &StatValue| match value {
                    StatValue::Number(value) => value.as_u64().and_then(|v| u16::try_from(v).ok()),
                    _ => None,
                })
            }
            ArrowDataType::UInt32 => {
                typed_array!(UInt32Array, |value: &StatValue| match value {
                    StatValue::Number(value) => value.as_u64().and_then(|v| u32::try_from(v).ok()),
                    _ => None,
                })
            }
            ArrowDataType::UInt64 => typed_array!(UInt64Array, |value: &StatValue| match value {
                StatValue::Number(value) => value.as_u64(),
                _ => None,
            }),
            ArrowDataType::Float32 => {
                typed_array!(Float32Array, |value: &StatValue| match value {
                    StatValue::Number(value) => value.as_f64().map(|v| v as f32),
                    _ => None,
                })
            }
            ArrowDataType::Float64 => typed_array!(Float64Array, |value: &StatValue| match value {
                StatValue::Number(value) => value.as_f64(),
                _ => None,
            }),
            ArrowDataType::Utf8 => typed_array!(StringArray, |value: &StatValue| match value {
                StatValue::String(value) => Some(value.clone()),
                _ => None,
            }),
            ArrowDataType::LargeUtf8 => {
                typed_array!(LargeStringArray, |value: &StatValue| match value {
                    StatValue::String(value) => Some(value.clone()),
                    _ => None,
                })
            }
            _ => Ok(None),
        }
    }

    pub fn stat_value_to_arrow_scalar_value(
        stat_val: &StatValue,
        field_dt: &ArrowDataType,
    ) -> DataFusionResult<Option<ScalarValue>> {
        match stat_val {
            StatValue::Null => Ok(Some(ScalarValue::try_new_null(field_dt)?)),
            StatValue::Boolean(value) => Self::bool_to_arrow_scalar_value(*value, field_dt),
            StatValue::Number(value) => Self::number_to_arrow_scalar_value(value, field_dt),
            StatValue::String(value) => Self::string_json_to_arrow_scalar_value(value, field_dt),
        }
    }

    pub fn string_values_to_array(
        values: &[Option<&str>],
        field_dt: &ArrowDataType,
    ) -> DataFusionResult<ArrayRef> {
        let utf8_array: ArrayRef = Arc::new(StringArray::from(
            values
                .iter()
                .map(|value| value.map(ToOwned::to_owned))
                .collect::<Vec<_>>(),
        ));

        match field_dt {
            ArrowDataType::Utf8 => Ok(utf8_array),
            ArrowDataType::LargeUtf8 => Ok(Arc::new(LargeStringArray::from(
                values
                    .iter()
                    .map(|value| value.map(ToOwned::to_owned))
                    .collect::<Vec<_>>(),
            ))),
            _ => Ok(cast(&utf8_array, field_dt)?),
        }
    }

    pub fn json_to_arrow_scalar_value(
        stat_val: &serde_json::Value,
        field_dt: &ArrowDataType,
    ) -> DataFusionResult<Option<ScalarValue>> {
        let Some(stat_val) = (match stat_val {
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => None,
            serde_json::Value::Null => Some(StatValue::Null),
            serde_json::Value::Bool(value) => Some(StatValue::Boolean(*value)),
            serde_json::Value::Number(value) => Some(StatValue::Number(value.clone())),
            serde_json::Value::String(value) => Some(StatValue::String(value.clone())),
        }) else {
            return Ok(None);
        };
        Self::stat_value_to_arrow_scalar_value(&stat_val, field_dt)
    }

    fn string_json_to_arrow_scalar_value(
        value: &str,
        field_dt: &ArrowDataType,
    ) -> DataFusionResult<Option<ScalarValue>> {
        match field_dt {
            ArrowDataType::Utf8 => Ok(Some(ScalarValue::Utf8(Some(value.to_string())))),
            ArrowDataType::LargeUtf8 => Ok(Some(ScalarValue::LargeUtf8(Some(value.to_string())))),
            ArrowDataType::Utf8View => Ok(Some(ScalarValue::Utf8View(Some(value.to_string())))),
            _ => Ok(Some(Self::string_to_arrow_scalar_value(value, field_dt)?)),
        }
    }

    fn bool_to_arrow_scalar_value(
        value: bool,
        field_dt: &ArrowDataType,
    ) -> DataFusionResult<Option<ScalarValue>> {
        match field_dt {
            ArrowDataType::Boolean => Ok(Some(ScalarValue::Boolean(Some(value)))),
            _ => Ok(Some(Self::string_to_arrow_scalar_value(
                if value { "true" } else { "false" },
                field_dt,
            )?)),
        }
    }

    fn number_to_arrow_scalar_value(
        value: &serde_json::Number,
        field_dt: &ArrowDataType,
    ) -> DataFusionResult<Option<ScalarValue>> {
        let scalar = match field_dt {
            ArrowDataType::Int8 => value
                .as_i64()
                .and_then(|v| i8::try_from(v).ok())
                .map(|v| ScalarValue::Int8(Some(v))),
            ArrowDataType::Int16 => value
                .as_i64()
                .and_then(|v| i16::try_from(v).ok())
                .map(|v| ScalarValue::Int16(Some(v))),
            ArrowDataType::Int32 => value
                .as_i64()
                .and_then(|v| i32::try_from(v).ok())
                .map(|v| ScalarValue::Int32(Some(v))),
            ArrowDataType::Int64 => value.as_i64().map(|v| ScalarValue::Int64(Some(v))),
            ArrowDataType::UInt8 => value
                .as_u64()
                .and_then(|v| u8::try_from(v).ok())
                .map(|v| ScalarValue::UInt8(Some(v))),
            ArrowDataType::UInt16 => value
                .as_u64()
                .and_then(|v| u16::try_from(v).ok())
                .map(|v| ScalarValue::UInt16(Some(v))),
            ArrowDataType::UInt32 => value
                .as_u64()
                .and_then(|v| u32::try_from(v).ok())
                .map(|v| ScalarValue::UInt32(Some(v))),
            ArrowDataType::UInt64 => value.as_u64().map(|v| ScalarValue::UInt64(Some(v))),
            ArrowDataType::Float32 => value.as_f64().map(|v| ScalarValue::Float32(Some(v as f32))),
            ArrowDataType::Float64 => value.as_f64().map(|v| ScalarValue::Float64(Some(v))),
            _ => None,
        };

        match scalar {
            Some(scalar) => Ok(Some(scalar)),
            None => Ok(Some(Self::string_to_arrow_scalar_value(
                &value.to_string(),
                field_dt,
            )?)),
        }
    }

    pub fn string_to_arrow_scalar_value(
        value: &str,
        field_dt: &ArrowDataType,
    ) -> DataFusionResult<ScalarValue> {
        match field_dt {
            ArrowDataType::Timestamp(_, _) => Self::parse_timestamp_str(value, field_dt),
            ArrowDataType::Date32 => Self::parse_date_str(value, field_dt),
            _ => ScalarValue::try_from_string(value.to_string(), field_dt),
        }
    }

    /// Convert a column from a `RecordBatch` into a `Vec<ScalarValue>` for partition value use.
    pub fn column_to_scalar_values(
        batch: &RecordBatch,
        col_idx: usize,
    ) -> DeltaResultLocal<Vec<ScalarValue>> {
        let col = batch.column(col_idx);
        (0..col.len())
            .map(|i| {
                ScalarValue::try_from_array(col.as_ref(), i)
                    .map_err(|e| DeltaTableError::generic(format!("Failed to read scalar: {e}")))
            })
            .collect()
    }

    fn parse_date_str(date_str: &str, field_dt: &ArrowDataType) -> DataFusionResult<ScalarValue> {
        let time_micro =
            ScalarValue::try_from_string(date_str.to_string(), &ArrowDataType::Date32)?;
        let cast_arr = cast_with_options(
            &time_micro.to_array()?,
            field_dt,
            &CastOptions {
                safe: false,
                ..Default::default()
            },
        )?;
        ScalarValue::try_from_array(&cast_arr, 0)
    }

    fn parse_timestamp_str(
        timestamp_str: &str,
        field_dt: &ArrowDataType,
    ) -> DataFusionResult<ScalarValue> {
        let time_micro = ScalarValue::try_from_string(
            timestamp_str.to_string(),
            &ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
        )?;
        let cast_arr = cast_with_options(
            &time_micro.to_array()?,
            field_dt,
            &CastOptions {
                safe: false,
                ..Default::default()
            },
        )?;
        ScalarValue::try_from_array(&cast_arr, 0)
    }
}

fn encode_partition_value(value: &str) -> String {
    utf8_percent_encode(value, RFC3986_PART).to_string()
}

/// Extension trait providing Delta-specific serialization and extraction for `ScalarValue`.
pub trait ScalarExt: Sized {
    /// Serialize to a partition value string (Delta log format).
    fn serialize(&self) -> Cow<'_, str>;
    /// Serialize with percent-encoding for use in Hive partition paths.
    fn serialize_encoded(&self) -> String;
    /// Extract a scalar from an Arrow array at the given index.
    fn from_array(arr: &dyn Array, index: usize) -> Option<Self>;
    /// Convert to a `serde_json::Value`.
    fn to_json(&self) -> Value;
}

impl ScalarExt for ScalarValue {
    fn serialize(&self) -> Cow<'_, str> {
        match self {
            ScalarValue::Utf8(Some(v))
            | ScalarValue::LargeUtf8(Some(v))
            | ScalarValue::Utf8View(Some(v)) => Cow::Borrowed(v.as_str()),
            ScalarValue::Int8(Some(v)) => Cow::Owned(v.to_string()),
            ScalarValue::Int16(Some(v)) => Cow::Owned(v.to_string()),
            ScalarValue::Int32(Some(v)) => Cow::Owned(v.to_string()),
            ScalarValue::Int64(Some(v)) => Cow::Owned(v.to_string()),
            ScalarValue::UInt8(Some(v)) => Cow::Owned(v.to_string()),
            ScalarValue::UInt16(Some(v)) => Cow::Owned(v.to_string()),
            ScalarValue::UInt32(Some(v)) => Cow::Owned(v.to_string()),
            ScalarValue::UInt64(Some(v)) => Cow::Owned(v.to_string()),
            ScalarValue::Float32(Some(v)) => Cow::Owned(v.to_string()),
            ScalarValue::Float64(Some(v)) => Cow::Owned(v.to_string()),
            ScalarValue::Boolean(Some(v)) => Cow::Owned(v.to_string()),
            ScalarValue::TimestampMicrosecond(Some(ts), _) => Cow::Owned(format_timestamp(*ts)),
            ScalarValue::Date32(Some(days)) => Cow::Owned(format_date(*days)),
            ScalarValue::Decimal128(Some(bits), _, scale) => {
                Cow::Owned(serialize_decimal(*bits, *scale))
            }
            ScalarValue::Binary(Some(bytes))
            | ScalarValue::LargeBinary(Some(bytes))
            | ScalarValue::BinaryView(Some(bytes)) => {
                Cow::Owned(create_escaped_binary_string(bytes.as_slice()))
            }
            ScalarValue::FixedSizeBinary(_, Some(bytes)) => {
                Cow::Owned(create_escaped_binary_string(bytes.as_slice()))
            }
            _ if self.is_null() => Cow::Borrowed("null"),
            other => Cow::Owned(other.to_string()),
        }
    }

    fn serialize_encoded(&self) -> String {
        if self.is_null() {
            return NULL_PARTITION_VALUE_DATA_PATH.to_string();
        }
        encode_partition_value(self.serialize().as_ref())
    }

    fn from_array(arr: &dyn Array, index: usize) -> Option<Self> {
        if arr.len() <= index {
            return None;
        }
        ScalarValue::try_from_array(arr, index).ok()
    }

    fn to_json(&self) -> Value {
        match self {
            ScalarValue::Utf8(Some(v))
            | ScalarValue::LargeUtf8(Some(v))
            | ScalarValue::Utf8View(Some(v)) => Value::String(v.clone()),
            ScalarValue::Int8(Some(v)) => Value::Number((*v).into()),
            ScalarValue::Int16(Some(v)) => Value::Number((*v).into()),
            ScalarValue::Int32(Some(v)) => Value::Number((*v).into()),
            ScalarValue::Int64(Some(v)) => Value::Number((*v).into()),
            ScalarValue::UInt8(Some(v)) => Value::Number((*v).into()),
            ScalarValue::UInt16(Some(v)) => Value::Number((*v).into()),
            ScalarValue::UInt32(Some(v)) => Value::Number((*v).into()),
            ScalarValue::UInt64(Some(v)) => Value::Number((*v).into()),
            ScalarValue::Float32(Some(v)) => number_from_f64(*v as f64),
            ScalarValue::Float64(Some(v)) => number_from_f64(*v),
            ScalarValue::Boolean(Some(v)) => Value::Bool(*v),
            ScalarValue::TimestampMicrosecond(Some(ts), _) => Value::String(format_timestamp(*ts)),
            ScalarValue::Date32(Some(days)) => Value::String(format_date(*days)),
            ScalarValue::Decimal128(Some(bits), _, scale) => {
                Value::String(serialize_decimal(*bits, *scale))
            }
            ScalarValue::Binary(Some(bytes))
            | ScalarValue::LargeBinary(Some(bytes))
            | ScalarValue::BinaryView(Some(bytes)) => {
                Value::String(create_escaped_binary_string(bytes.as_slice()))
            }
            ScalarValue::FixedSizeBinary(_, Some(bytes)) => {
                Value::String(create_escaped_binary_string(bytes.as_slice()))
            }
            ScalarValue::Struct(struct_array) => {
                let fields = struct_array.fields();
                let map: serde_json::Map<String, Value> = fields
                    .iter()
                    .enumerate()
                    .map(|(i, field)| {
                        let col = struct_array.column(i);
                        let sv = ScalarValue::try_from_array(col.as_ref(), 0)
                            .unwrap_or(ScalarValue::Null);
                        (field.name().clone(), sv.to_json())
                    })
                    .collect();
                Value::Object(map)
            }
            ScalarValue::List(list_array) => {
                let values: Vec<Value> = (0..list_array.len())
                    .map(|i| {
                        ScalarValue::try_from_array(list_array.as_ref(), i)
                            .map(|sv| sv.to_json())
                            .unwrap_or(Value::Null)
                    })
                    .collect();
                Value::Array(values)
            }
            _ if self.is_null() => Value::Null,
            other => Value::String(other.to_string()),
        }
    }
}

fn format_timestamp(value: i64) -> String {
    Utc.timestamp_micros(value)
        .single()
        .map(|ts| ts.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
        .unwrap_or_else(|| value.to_string())
}

fn format_date(days: i32) -> String {
    DateTime::from_timestamp(days as i64 * 24 * 3600, 0)
        .map(|date| date.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| days.to_string())
}

fn serialize_decimal(bits: i128, scale: i8) -> String {
    use std::cmp::Ordering;

    match scale.cmp(&0) {
        Ordering::Equal => bits.to_string(),
        Ordering::Greater => {
            let value = bits;
            let scale_u32 = scale as u32;
            let scalar_multiple = 10_i128.pow(scale_u32);
            let mut string = String::new();
            string.push_str((value / scalar_multiple).to_string().as_str());
            string.push('.');
            string.push_str(&format!(
                "{:0>scale$}",
                value % scalar_multiple,
                scale = scale as usize
            ));
            string
        }
        Ordering::Less => {
            let mut string = bits.to_string();
            let zeros = (-scale) as u8;
            for _ in 0..zeros {
                string.push('0');
            }
            string
        }
    }
}

fn create_escaped_binary_string(bytes: &[u8]) -> String {
    let mut escaped = String::with_capacity(bytes.len() * 6);
    for byte in bytes {
        let _ = write!(escaped, "\\u{:04X}", byte);
    }
    escaped
}

fn number_from_f64(value: f64) -> Value {
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .unwrap_or_else(|| Value::String(value.to_string()))
}

/// Parse a partition value string into a `ScalarValue` for the given Arrow data type.
///
/// This implements Delta-specific parsing rules for partition values stored in the log.
pub fn parse_partition_value(raw: &str, field_dt: &ArrowDataType) -> DeltaResultLocal<ScalarValue> {
    if raw.is_empty() || raw == NULL_PARTITION_VALUE_DATA_PATH {
        return ScalarValue::try_new_null(field_dt)
            .map_err(|e| DeltaTableError::generic(format!("Failed to create null scalar: {e}")));
    }
    ScalarConverter::string_to_arrow_scalar_value(raw, field_dt)
        .map_err(|e| DeltaTableError::generic(format!("Failed to parse partition value: {e}")))
}

pub fn parse_optional_partition_value(
    raw: Option<&str>,
    field_dt: &ArrowDataType,
) -> DeltaResultLocal<ScalarValue> {
    match raw {
        Some(raw) => parse_partition_value(raw, field_dt),
        None => ScalarValue::try_new_null(field_dt)
            .map_err(|e| DeltaTableError::generic(format!("Failed to create null scalar: {e}"))),
    }
}

/// Build a `ScalarValue` from an Arrow array at the given index, returning `None` for nulls.
///
/// Returns `None` if the value is null or if extraction fails.
pub fn scalar_from_array_opt(arr: &dyn Array, index: usize) -> Option<ScalarValue> {
    if arr.len() <= index || arr.is_null(index) {
        return None;
    }
    ScalarValue::try_from_array(arr, index).ok()
}

/// Convert a `ScalarValue` to an `Arc<dyn Array>` suitable for use as a partition column.
pub fn scalar_value_to_array(
    value: &ScalarValue,
    len: usize,
) -> DeltaResultLocal<Arc<dyn datafusion::arrow::array::Array>> {
    value
        .to_array_of_size(len)
        .map_err(|e| DeltaTableError::generic(format!("Failed to convert scalar to array: {e}")))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType as ArrowDataType;
    use datafusion::common::ScalarValue;

    use super::{
        parse_optional_partition_value, parse_partition_value, NULL_PARTITION_VALUE_DATA_PATH,
    };

    #[test]
    fn test_parse_partition_value_treats_hive_default_partition_as_null_for_strings() {
        #[expect(clippy::expect_used)]
        let value = parse_partition_value(NULL_PARTITION_VALUE_DATA_PATH, &ArrowDataType::Utf8)
            .expect("partition value should parse");

        assert_eq!(value, ScalarValue::Utf8(None));
    }

    #[test]
    fn test_parse_optional_partition_value_none_returns_typed_null() {
        #[expect(clippy::expect_used)]
        let value = parse_optional_partition_value(None, &ArrowDataType::Utf8)
            .expect("partition value should parse");

        assert_eq!(value, ScalarValue::Utf8(None));
    }
}
