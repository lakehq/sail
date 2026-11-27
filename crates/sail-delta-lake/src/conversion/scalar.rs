// SPDX-License-Identifier: Apache-2.0
// SPDX-FileComment: Credit: https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/kernel/scalars.rs
// SPDX-FileAttributionText: License: https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
// SPDX-FileCopyrightText: Copyright (2020) QP Hou and a number of other contributors.
// SPDX-FileCopyrightText: Portions Copyright (2025) LakeSail, Inc.
// SPDX-FileContributor: Modified by LakeSail, Inc. in 2025

use std::borrow::Cow;
use std::fmt::Write;
use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeStringArray, RecordBatch, StringArray, StructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use datafusion::arrow::compute::{CastOptions, cast, cast_with_options};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use datafusion::common::scalar::ScalarValue;
use datafusion::common::{Result as DataFusionResult, exec_datafusion_err};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use sail_common_datafusion::utils::datetime::SparkZone;
use sail_function::scalar::datetime::timestamp::parse_unformatted_timestamp;
use serde_json::Value;

use crate::spec::{
    ColumnValueStat, DeltaError as DeltaTableError, DeltaResult as DeltaResultLocal, StatValue,
};

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
            ($array_ty:ty, $extract:expr_2021) => {{
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
            StatValue::ExactNumber(value) => {
                Self::string_json_to_arrow_scalar_value(value.get(), field_dt)
            }
            StatValue::String(value) => Self::string_json_to_arrow_scalar_value(value, field_dt),
        }
    }

    pub(crate) fn column_value_stat_to_arrow_scalar_value(
        column_stat: &ColumnValueStat,
        field_dt: &ArrowDataType,
    ) -> DataFusionResult<Option<ScalarValue>> {
        match (column_stat, field_dt) {
            (ColumnValueStat::Value(value), _) => {
                Self::stat_value_to_arrow_scalar_value(value, field_dt)
            }
            (ColumnValueStat::Column(values), ArrowDataType::Struct(fields)) => {
                let arrays = fields
                    .iter()
                    .map(|field| {
                        let scalar = match values.get(field.name()) {
                            Some(value) => match Self::column_value_stat_to_arrow_scalar_value(
                                value,
                                field.data_type(),
                            )? {
                                Some(scalar) => scalar,
                                None => ScalarValue::try_new_null(field.data_type())?,
                            },
                            None => ScalarValue::try_new_null(field.data_type())?,
                        };
                        scalar.to_array_of_size(1)
                    })
                    .collect::<DataFusionResult<Vec<_>>>()?;
                // Missing statistics may violate a physical child's non-null constraint.
                // Return an error so scan planning can discard this unusable bound.
                Ok(Some(ScalarValue::Struct(Arc::new(StructArray::try_new(
                    fields.clone(),
                    arrays,
                    None,
                )?))))
            }
            _ => Ok(None),
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
        ScalarValue::try_from_string(timestamp_str.to_string(), field_dt)
    }
}

fn encode_partition_value(value: &str) -> String {
    utf8_percent_encode(value, RFC3986_PART).to_string()
}

/// Extension trait providing Delta-specific serialization and extraction for `ScalarValue`.
pub trait ScalarExt: Sized {
    /// Serialize to a partition value string (Delta log format).
    fn serialize(&self) -> Cow<'_, str>;
    /// Serialize with percent-encoding for use in Hive partition paths; timestamps with a time
    /// zone keep their UTC wall clock without an offset, unlike the log value.
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
            ScalarValue::TimestampMicrosecond(Some(ts), Some(_)) => {
                Cow::Owned(format_utc_timestamp(*ts))
            }
            ScalarValue::TimestampMicrosecond(Some(ts), None) => Cow::Owned(format_timestamp(*ts)),
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
        match self {
            // The directory keeps the format without an offset, which the log value had before it
            // named the offset. Readers use the log value.
            ScalarValue::TimestampMicrosecond(Some(ts), Some(_)) => {
                encode_partition_value(&format_timestamp(*ts))
            }
            _ => encode_partition_value(self.serialize().as_ref()),
        }
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

fn format_micros(value: i64, format: &str) -> String {
    Utc.timestamp_micros(value)
        .single()
        .map(|ts| ts.format(format).to_string())
        .unwrap_or_else(|| value.to_string())
}

fn format_timestamp(value: i64) -> String {
    format_micros(value, "%Y-%m-%d %H:%M:%S%.6f")
}

/// Formats a timestamp partition value as UTC with an explicit offset, as Delta writes it with
/// `spark.databricks.delta.write.utcTimestampPartitionValues` (the default), so that every reader
/// gets the same instant whatever its session time zone.
fn format_utc_timestamp(value: i64) -> String {
    // chrono's `%Y` signs years outside 0000-9999, as Delta-Spark's `uuuu` pattern does.
    format_micros(value, "%Y-%m-%dT%H:%M:%S%.6fZ")
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

/// Parses the partition values of one column. The log uses empty strings for NULL; directory
/// markers remain literal values.
///
/// A TIMESTAMP value (a type with a time zone) is parsed as `CAST(value AS TIMESTAMP)` in the
/// session time zone, as Delta-Spark reads partition values: a value without an offset is a wall
/// clock in that zone, whatever the label of the type. Other values are parsed by Arrow.
#[derive(Debug)]
pub struct PartitionValueParser {
    field_dt: ArrowDataType,
    /// The session time zone, for a TIMESTAMP type.
    session_zone: Option<SparkZone>,
}

impl PartitionValueParser {
    pub fn try_new(field_dt: &ArrowDataType, time_zone: &str) -> DeltaResultLocal<Self> {
        let session_zone = match field_dt {
            ArrowDataType::Timestamp(_, Some(_)) => Some(SparkZone::parse(time_zone)?),
            _ => None,
        };
        Ok(Self {
            field_dt: field_dt.clone(),
            session_zone,
        })
    }

    pub fn data_type(&self) -> &ArrowDataType {
        &self.field_dt
    }

    pub fn parse(&self, raw: Option<&str>) -> DeltaResultLocal<ScalarValue> {
        let raw = match raw {
            Some(raw) if !raw.is_empty() => raw,
            _ => {
                return ScalarValue::try_new_null(&self.field_dt).map_err(|e| {
                    DeltaTableError::generic(format!("Failed to create null scalar: {e}"))
                });
            }
        };
        match (&self.session_zone, &self.field_dt) {
            (Some(zone), ArrowDataType::Timestamp(unit, label)) => {
                parse_timestamp(raw, zone, *unit).map(|value| timestamp_scalar(value, *unit, label))
            }
            _ => ScalarConverter::string_to_arrow_scalar_value(raw, &self.field_dt),
        }
        .map_err(|e| DeltaTableError::generic(format!("Failed to parse partition value: {e}")))
    }

    /// Parses values that are not empty, where a value that does not parse is null.
    pub fn parse_array(&self, values: &[Option<&str>]) -> DataFusionResult<ArrayRef> {
        if let (Some(zone), ArrowDataType::Timestamp(unit, label)) =
            (&self.session_zone, &self.field_dt)
        {
            let values = values
                .iter()
                .map(|value| value.and_then(|raw| parse_timestamp(raw, zone, *unit).ok()));
            let label = label.clone();
            return Ok(match unit {
                TimeUnit::Second => {
                    Arc::new(TimestampSecondArray::from_iter(values).with_timezone_opt(label))
                }
                TimeUnit::Millisecond => {
                    Arc::new(TimestampMillisecondArray::from_iter(values).with_timezone_opt(label))
                }
                TimeUnit::Microsecond => {
                    Arc::new(TimestampMicrosecondArray::from_iter(values).with_timezone_opt(label))
                }
                TimeUnit::Nanosecond => {
                    Arc::new(TimestampNanosecondArray::from_iter(values).with_timezone_opt(label))
                }
            });
        }
        let utf8_array: ArrayRef = Arc::new(StringArray::from(
            values
                .iter()
                .map(|value| value.map(ToOwned::to_owned))
                .collect::<Vec<_>>(),
        ));
        match &self.field_dt {
            ArrowDataType::Utf8 => Ok(utf8_array),
            ArrowDataType::LargeUtf8 => Ok(Arc::new(LargeStringArray::from(
                values
                    .iter()
                    .map(|value| value.map(ToOwned::to_owned))
                    .collect::<Vec<_>>(),
            ))),
            field_dt => Ok(cast(&utf8_array, field_dt)?),
        }
    }
}

/// Parses a TIMESTAMP partition value in the session time zone, in `unit` since the epoch.
fn parse_timestamp(raw: &str, zone: &SparkZone, unit: TimeUnit) -> DataFusionResult<i64> {
    let micros = parse_unformatted_timestamp(raw, Some(zone))?;
    match unit {
        TimeUnit::Second => Ok(micros.div_euclid(1_000_000)),
        TimeUnit::Millisecond => Ok(micros.div_euclid(1_000)),
        TimeUnit::Microsecond => Ok(micros),
        TimeUnit::Nanosecond => micros.checked_mul(1_000).ok_or_else(|| {
            exec_datafusion_err!("timestamp is outside the nanosecond range: {raw}")
        }),
    }
}

fn timestamp_scalar(value: i64, unit: TimeUnit, label: &Option<Arc<str>>) -> ScalarValue {
    let (value, label) = (Some(value), label.clone());
    match unit {
        TimeUnit::Second => ScalarValue::TimestampSecond(value, label),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(value, label),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(value, label),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(value, label),
    }
}

/// Parse a partition value string into a `ScalarValue` for the given Arrow data type, as
/// [`PartitionValueParser`] does.
pub fn parse_partition_value(
    raw: &str,
    field_dt: &ArrowDataType,
    time_zone: &str,
) -> DeltaResultLocal<ScalarValue> {
    PartitionValueParser::try_new(field_dt, time_zone)?.parse(Some(raw))
}

pub fn parse_optional_partition_value(
    raw: Option<&str>,
    field_dt: &ArrowDataType,
    time_zone: &str,
) -> DeltaResultLocal<ScalarValue> {
    PartitionValueParser::try_new(field_dt, time_zone)?.parse(raw)
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
    use datafusion::arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
    use datafusion::common::ScalarValue;

    use super::{
        NULL_PARTITION_VALUE_DATA_PATH, PartitionValueParser, ScalarExt,
        parse_optional_partition_value, parse_partition_value,
    };

    fn timestamp(micros: i64, time_zone: Option<&str>) -> ScalarValue {
        ScalarValue::TimestampMicrosecond(Some(micros), time_zone.map(Into::into))
    }

    #[test]
    fn timestamp_partition_values_are_utc_with_an_offset() {
        for time_zone in ["UTC", "America/Los_Angeles"] {
            let value = timestamp(1_704_099_600_000_000, Some(time_zone));
            assert_eq!(value.serialize(), "2024-01-01T09:00:00.000000Z");
        }
        assert_eq!(
            timestamp(1_705_314_600_123_456, Some("UTC")).serialize(),
            "2024-01-15T10:30:00.123456Z"
        );
        assert_eq!(
            timestamp(-2_083_172_400_000_000, Some("UTC")).serialize(),
            "1903-12-28T05:00:00.000000Z"
        );
        assert_eq!(
            timestamp(-1, Some("UTC")).serialize(),
            "1969-12-31T23:59:59.999999Z"
        );
    }

    /// Years outside 0000-9999 are signed, as Delta-Spark's `uuuu` pattern writes them.
    #[test]
    fn timestamp_partition_values_of_extreme_years() {
        for (year, expected) in [
            (0, "0000-01-01T00:00:00.000000Z"),
            (-1, "-0001-01-01T00:00:00.000000Z"),
            (10000, "+10000-01-01T00:00:00.000000Z"),
        ] {
            let micros = chrono::NaiveDate::from_ymd_opt(year, 1, 1)
                .and_then(|date| date.and_hms_opt(0, 0, 0))
                .map(|datetime| datetime.and_utc().timestamp_micros());
            assert_eq!(
                micros.map(|micros| timestamp(micros, Some("UTC")).serialize().into_owned()),
                Some(expected.to_string())
            );
        }
    }

    #[test]
    fn timestamp_ntz_partition_values_are_unchanged() {
        assert_eq!(
            timestamp(1_704_099_600_000_000, None).serialize(),
            "2024-01-01 09:00:00.000000"
        );
    }

    #[test]
    fn timestamp_partition_directories_are_unchanged() {
        assert_eq!(
            timestamp(1_704_099_600_000_000, Some("America/Los_Angeles")).serialize_encoded(),
            "2024-01-01%2009%3A00%3A00.000000"
        );
    }

    /// A TIMESTAMP value without an offset is a wall clock in the session time zone, whatever the
    /// label of the type, read as Spark reads it: the first instant when the wall clock occurs
    /// twice, and the instant after a skipped hour. A value with an offset is that instant.
    /// TIMESTAMP_NTZ values are wall clocks. The scalar and the array parse agree.
    #[test]
    fn timestamp_partition_values_parse_in_the_session_time_zone()
    -> Result<(), Box<dyn std::error::Error>> {
        let sessions = ["UTC", "America/Los_Angeles", "+05:30"];
        // Each value in seconds since the epoch in each session time zone.
        let cases = [
            (
                "2024-01-01 09:00:00.000000",
                [1_704_099_600, 1_704_128_400, 1_704_079_800],
            ),
            ("2024-01-01T09:00:00.000000Z", [1_704_099_600; 3]),
            // 02:30 does not occur in Los Angeles on 2024-03-10 and reads as 03:30 PDT.
            (
                "2024-03-10 02:30:00",
                [1_710_037_800, 1_710_066_600, 1_710_018_000],
            ),
            // 01:30 occurs twice in Los Angeles on 2024-11-03 and reads as the first, in PDT.
            (
                "2024-11-03 01:30:00",
                [1_730_597_400, 1_730_622_600, 1_730_577_600],
            ),
        ];
        let units = [
            (TimeUnit::Second, 1),
            (TimeUnit::Millisecond, 1_000),
            (TimeUnit::Microsecond, 1_000_000),
            (TimeUnit::Nanosecond, 1_000_000_000),
        ];
        let raw = cases.map(|(raw, _)| Some(raw));
        for (session_index, session) in sessions.into_iter().enumerate() {
            for (unit, per_second) in units {
                for label in [None, Some("UTC"), Some("America/Los_Angeles")] {
                    let data_type = ArrowDataType::Timestamp(unit, label.map(Into::into));
                    let parser = PartitionValueParser::try_new(&data_type, session)?;
                    let array = parser.parse_array(&raw)?;
                    assert_eq!(array.data_type(), &data_type);
                    for (index, (raw, seconds)) in cases.into_iter().enumerate() {
                        // A wall clock without a time zone is its UTC reading.
                        let seconds = seconds[if label.is_some() { session_index } else { 0 }];
                        let expected =
                            ScalarValue::Int64(Some(seconds * per_second)).cast_to(&data_type)?;
                        let case = format!("{raw} as {data_type} in {session}");
                        assert_eq!(
                            parse_partition_value(raw, &data_type, session)?,
                            expected,
                            "{case}"
                        );
                        assert_eq!(
                            ScalarValue::try_from_array(&array, index)?,
                            expected,
                            "{case}"
                        );
                    }
                }
            }
        }
        // Dates do not depend on the session time zone.
        assert_eq!(
            parse_partition_value("2024-01-01", &ArrowDataType::Date32, "America/Los_Angeles")?,
            ScalarValue::Date32(Some(19_723))
        );
        Ok(())
    }

    #[test]
    fn incomplete_required_struct_statistics_return_error() {
        let data_type = ArrowDataType::Struct(
            vec![datafusion::arrow::datatypes::Field::new(
                "metadata",
                ArrowDataType::Binary,
                false,
            )]
            .into(),
        );
        let stats = crate::spec::ColumnValueStat::Column(Default::default());
        assert!(
            super::ScalarConverter::column_value_stat_to_arrow_scalar_value(&stats, &data_type)
                .is_err()
        );
    }

    #[test]
    fn test_parse_partition_value_preserves_hive_default_partition_string() {
        #[expect(clippy::expect_used)]
        let value =
            parse_partition_value(NULL_PARTITION_VALUE_DATA_PATH, &ArrowDataType::Utf8, "UTC")
                .expect("partition value should parse");

        assert_eq!(
            value,
            ScalarValue::Utf8(Some(NULL_PARTITION_VALUE_DATA_PATH.to_string()))
        );
    }

    #[test]
    fn test_parse_optional_partition_value_none_returns_typed_null() {
        #[expect(clippy::expect_used)]
        let value = parse_optional_partition_value(None, &ArrowDataType::Utf8, "UTC")
            .expect("partition value should parse");

        assert_eq!(value, ScalarValue::Utf8(None));
    }
}
