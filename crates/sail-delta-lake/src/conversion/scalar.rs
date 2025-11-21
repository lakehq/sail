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
    self, Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
    TimestampMicrosecondArray,
};
use datafusion::arrow::compute::{cast_with_options, CastOptions};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use datafusion::common::scalar::ScalarValue;
use datafusion::common::Result as DataFusionResult;
use delta_kernel::engine::arrow_conversion::TryIntoKernel as _;
use delta_kernel::expressions::{Scalar, StructData};
use delta_kernel::schema::{DataType, PrimitiveType, StructField};
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use serde_json::Value;

use crate::kernel::{DeltaResult as DeltaResultLocal, DeltaTableError};

pub const NULL_PARTITION_VALUE_DATA_PATH: &str = "__HIVE_DEFAULT_PARTITION__";

const RFC3986_PART: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

#[derive(Debug)]
pub struct ScalarConverter;

impl ScalarConverter {
    pub fn json_to_arrow_scalar_value(
        stat_val: &serde_json::Value,
        field_dt: &ArrowDataType,
    ) -> DataFusionResult<Option<ScalarValue>> {
        match stat_val {
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => Ok(None),
            serde_json::Value::Null => Ok(Some(ScalarValue::try_new_null(field_dt)?)),
            serde_json::Value::String(value) => {
                Ok(Some(Self::string_to_arrow_scalar_value(value, field_dt)?))
            }
            other => {
                let owned = other.to_string();
                Ok(Some(Self::string_to_arrow_scalar_value(&owned, field_dt)?))
            }
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

    pub fn scalars_to_arrow_array(
        field: &StructField,
        values: &[Scalar],
    ) -> DeltaResultLocal<Arc<dyn Array>> {
        let array: Arc<dyn Array> = match field.data_type() {
            DataType::Primitive(PrimitiveType::String) => {
                Arc::new(StringArray::from_iter(values.iter().map(|v| match v {
                    Scalar::String(s) => Some(s.clone()),
                    Scalar::Null(_) => None,
                    _ => None,
                })))
            }
            DataType::Primitive(PrimitiveType::Long) => {
                Arc::new(Int64Array::from_iter(values.iter().map(|v| match v {
                    Scalar::Long(i) => Some(*i),
                    Scalar::Null(_) => None,
                    _ => None,
                })))
            }
            DataType::Primitive(PrimitiveType::Integer) => {
                Arc::new(Int32Array::from_iter(values.iter().map(|v| match v {
                    Scalar::Integer(i) => Some(*i),
                    Scalar::Null(_) => None,
                    _ => None,
                })))
            }
            DataType::Primitive(PrimitiveType::Short) => {
                Arc::new(Int16Array::from_iter(values.iter().map(|v| match v {
                    Scalar::Short(i) => Some(*i),
                    Scalar::Null(_) => None,
                    _ => None,
                })))
            }
            DataType::Primitive(PrimitiveType::Byte) => {
                Arc::new(Int8Array::from_iter(values.iter().map(|v| match v {
                    Scalar::Byte(i) => Some(*i),
                    Scalar::Null(_) => None,
                    _ => None,
                })))
            }
            DataType::Primitive(PrimitiveType::Float) => {
                Arc::new(Float32Array::from_iter(values.iter().map(|v| match v {
                    Scalar::Float(f) => Some(*f),
                    Scalar::Null(_) => None,
                    _ => None,
                })))
            }
            DataType::Primitive(PrimitiveType::Double) => {
                Arc::new(Float64Array::from_iter(values.iter().map(|v| match v {
                    Scalar::Double(f) => Some(*f),
                    Scalar::Null(_) => None,
                    _ => None,
                })))
            }
            DataType::Primitive(PrimitiveType::Boolean) => {
                Arc::new(BooleanArray::from_iter(values.iter().map(|v| match v {
                    Scalar::Boolean(b) => Some(*b),
                    Scalar::Null(_) => None,
                    _ => None,
                })))
            }
            DataType::Primitive(PrimitiveType::Binary) => {
                Arc::new(BinaryArray::from_iter(values.iter().map(|v| match v {
                    Scalar::Binary(b) => Some(b.clone()),
                    Scalar::Null(_) => None,
                    _ => None,
                })))
            }
            DataType::Primitive(PrimitiveType::Date) => {
                Arc::new(Date32Array::from_iter(values.iter().map(|v| match v {
                    Scalar::Date(d) => Some(*d),
                    Scalar::Null(_) => None,
                    _ => None,
                })))
            }
            DataType::Primitive(PrimitiveType::Timestamp) => Arc::new(
                TimestampMicrosecondArray::from_iter(values.iter().map(|v| match v {
                    Scalar::Timestamp(ts) => Some(*ts),
                    Scalar::Null(_) => None,
                    _ => None,
                }))
                .with_timezone("UTC"),
            ),
            DataType::Primitive(PrimitiveType::TimestampNtz) => Arc::new(
                TimestampMicrosecondArray::from_iter(values.iter().map(|v| match v {
                    Scalar::TimestampNtz(ts) => Some(*ts),
                    Scalar::Null(_) => None,
                    _ => None,
                })),
            ),
            DataType::Primitive(PrimitiveType::Decimal(decimal)) => {
                let array = Decimal128Array::from_iter(values.iter().map(|v| match v {
                    Scalar::Decimal(d) => Some(d.bits()),
                    Scalar::Null(_) => None,
                    _ => None,
                }));
                let array = array
                    .with_precision_and_scale(decimal.precision(), decimal.scale() as i8)
                    .map_err(|e| {
                        DeltaTableError::generic(format!("Decimal precision error: {e}"))
                    })?;
                Arc::new(array)
            }
            _ => {
                return Err(DeltaTableError::generic(
                    "complex partition values are not supported",
                ))
            }
        };

        Ok(array)
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

pub trait ScalarExt: Sized {
    fn serialize(&self) -> Cow<'_, str>;
    fn serialize_encoded(&self) -> String;
    fn from_array(arr: &dyn Array, index: usize) -> Option<Self>;
    fn to_json(&self) -> Value;
}

impl ScalarExt for Scalar {
    fn serialize(&self) -> Cow<'_, str> {
        match self {
            Self::String(value) => Cow::Borrowed(value),
            Self::Byte(value) => Cow::Owned(value.to_string()),
            Self::Short(value) => Cow::Owned(value.to_string()),
            Self::Integer(value) => Cow::Owned(value.to_string()),
            Self::Long(value) => Cow::Owned(value.to_string()),
            Self::Float(value) => Cow::Owned(value.to_string()),
            Self::Double(value) => Cow::Owned(value.to_string()),
            Self::Boolean(value) => Cow::Owned(value.to_string()),
            Self::TimestampNtz(ts) | Self::Timestamp(ts) => Cow::Owned(format_timestamp(*ts)),
            Self::Date(days) => Cow::Owned(format_date(*days)),
            Self::Decimal(decimal) => {
                Cow::Owned(serialize_decimal(decimal.bits(), decimal.scale() as i8))
            }
            Self::Binary(bytes) => Cow::Owned(create_escaped_binary_string(bytes.as_slice())),
            Self::Null(_) => Cow::Borrowed("null"),
            Self::Struct(_) | Self::Array(_) | Self::Map(_) => Cow::Owned(self.to_string()),
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
        if arr.is_null(index) {
            return Some(Self::Null(arr.data_type().try_into_kernel().ok()?));
        }

        ScalarValue::try_from_array(arr, index)
            .ok()
            .and_then(kernel_scalar_from_datafusion)
    }

    fn to_json(&self) -> Value {
        match self {
            Self::String(value) => Value::String(value.to_owned()),
            Self::Byte(value) => Value::Number((*value).into()),
            Self::Short(value) => Value::Number((*value).into()),
            Self::Integer(value) => Value::Number((*value).into()),
            Self::Long(value) => Value::Number((*value).into()),
            Self::Float(value) => number_from_f64(*value as f64),
            Self::Double(value) => number_from_f64(*value),
            Self::Boolean(value) => Value::Bool(*value),
            Self::TimestampNtz(ts) | Self::Timestamp(ts) => Value::String(format_timestamp(*ts)),
            Self::Date(days) => Value::String(format_date(*days)),
            Self::Decimal(decimal) => {
                Value::String(serialize_decimal(decimal.bits(), decimal.scale() as i8))
            }
            Self::Binary(bytes) => Value::String(create_escaped_binary_string(bytes.as_slice())),
            Self::Null(_) => Value::Null,
            Self::Struct(data) => {
                let map: serde_json::Map<String, Value> = data
                    .fields()
                    .iter()
                    .zip(data.values().iter())
                    .map(|(field, value)| (field.name.clone(), value.to_json()))
                    .collect();
                Value::Object(map)
            }
            Self::Array(array_data) => {
                #[allow(deprecated)]
                let values: Vec<Value> = array_data
                    .array_elements()
                    .iter()
                    .map(|value| value.to_json())
                    .collect();
                Value::Array(values)
            }
            Self::Map(map_data) => {
                let map: serde_json::Map<String, Value> = map_data
                    .pairs()
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_json()))
                    .collect();
                Value::Object(map)
            }
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

fn kernel_scalar_from_datafusion(value: ScalarValue) -> Option<Scalar> {
    match value {
        ScalarValue::Utf8(Some(v))
        | ScalarValue::LargeUtf8(Some(v))
        | ScalarValue::Utf8View(Some(v)) => Some(Scalar::String(v)),
        ScalarValue::Boolean(Some(v)) => Some(Scalar::Boolean(v)),
        ScalarValue::Binary(Some(bytes))
        | ScalarValue::LargeBinary(Some(bytes))
        | ScalarValue::BinaryView(Some(bytes))
        | ScalarValue::FixedSizeBinary(_, Some(bytes)) => Some(Scalar::Binary(bytes)),
        ScalarValue::Int8(Some(v)) => Some(Scalar::Byte(v)),
        ScalarValue::Int16(Some(v)) => Some(Scalar::Short(v)),
        ScalarValue::Int32(Some(v)) => Some(Scalar::Integer(v)),
        ScalarValue::Int64(Some(v)) => Some(Scalar::Long(v)),
        ScalarValue::UInt8(Some(v)) => Some(Scalar::Byte(v as i8)),
        ScalarValue::UInt16(Some(v)) => Some(Scalar::Short(v as i16)),
        ScalarValue::UInt32(Some(v)) => Some(Scalar::Integer(v as i32)),
        ScalarValue::UInt64(Some(v)) => Some(Scalar::Long(v as i64)),
        ScalarValue::Float32(Some(v)) => Some(Scalar::Float(v)),
        ScalarValue::Float64(Some(v)) => Some(Scalar::Double(v)),
        ScalarValue::Decimal128(Some(bits), precision, scale) => {
            let scale = u8::try_from(scale).ok()?;
            Scalar::decimal(bits, precision, scale).ok()
        }
        ScalarValue::Date32(Some(days)) => Some(Scalar::Date(days)),
        ScalarValue::TimestampMicrosecond(Some(value), None) => Some(Scalar::TimestampNtz(value)),
        ScalarValue::TimestampMicrosecond(Some(value), Some(tz))
            if tz.eq_ignore_ascii_case("utc") =>
        {
            Some(Scalar::Timestamp(value))
        }
        ScalarValue::Struct(struct_array) => {
            struct_data_from_array(struct_array.as_ref()).map(Scalar::Struct)
        }
        _ => None,
    }
}

fn struct_data_from_array(struct_array: &array::StructArray) -> Option<StructData> {
    let fields = struct_array.fields();
    let columns = struct_array.columns();

    if fields.len() != columns.len() {
        return None;
    }

    let mut struct_fields = Vec::with_capacity(fields.len());
    let mut values = Vec::with_capacity(columns.len());

    for (field, column) in fields.iter().zip(columns.iter()) {
        let kernel_field = field.as_ref().try_into_kernel().ok()?;
        let value = Scalar::from_array(column.as_ref(), 0)?;
        struct_fields.push(kernel_field);
        values.push(value);
    }

    StructData::try_new(struct_fields, values).ok()
}
