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

// [Credit]: <https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/crates/core/src/kernel/scalars.rs>
#![allow(dead_code)]

use std::cmp::Ordering;

use chrono::{DateTime, TimeZone, Utc};
use datafusion::arrow::array::{self, Array};
use datafusion::common::scalar::ScalarValue;
use delta_kernel::engine::arrow_conversion::TryIntoKernel as _;
use delta_kernel::expressions::{Scalar, StructData};
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use serde_json::Value;

pub const NULL_PARTITION_VALUE_DATA_PATH: &str = "__HIVE_DEFAULT_PARTITION__";

const RFC3986_PART: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

fn encode_partition_value(value: &str) -> String {
    utf8_percent_encode(value, RFC3986_PART).to_string()
}

pub trait ScalarExt: Sized {
    fn serialize(&self) -> String;
    fn serialize_encoded(&self) -> String;
    fn from_array(arr: &dyn Array, index: usize) -> Option<Self>;
    fn to_json(&self) -> Value;
}

impl ScalarExt for Scalar {
    fn serialize(&self) -> String {
        match self {
            Self::String(value) => value.to_owned(),
            Self::Byte(value) => value.to_string(),
            Self::Short(value) => value.to_string(),
            Self::Integer(value) => value.to_string(),
            Self::Long(value) => value.to_string(),
            Self::Float(value) => value.to_string(),
            Self::Double(value) => value.to_string(),
            Self::Boolean(value) => value.to_string(),
            Self::TimestampNtz(ts) | Self::Timestamp(ts) => format_timestamp(*ts),
            Self::Date(days) => format_date(*days),
            Self::Decimal(decimal) => serialize_decimal(decimal.bits(), decimal.scale() as i8),
            Self::Binary(bytes) => create_escaped_binary_string(bytes.as_slice()),
            Self::Null(_) => "null".to_string(),
            Self::Struct(_) | Self::Array(_) | Self::Map(_) => self.to_string(),
        }
    }

    fn serialize_encoded(&self) -> String {
        if self.is_null() {
            return NULL_PARTITION_VALUE_DATA_PATH.to_string();
        }
        encode_partition_value(self.serialize().as_str())
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
                let mut result = serde_json::Map::new();
                for (field, value) in data.fields().iter().zip(data.values().iter()) {
                    result.insert(field.name.clone(), value.to_json());
                }
                Value::Object(result)
            }
            Self::Array(array_data) => {
                let mut result = Vec::new();
                #[allow(deprecated)]
                for value in array_data.array_elements() {
                    result.push(value.to_json());
                }
                Value::Array(result)
            }
            Self::Map(map_data) => {
                let mut result = serde_json::Map::new();
                for (key, value) in map_data.pairs() {
                    result.insert(key.to_string(), value.to_json());
                }
                Value::Object(result)
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
    let mut escaped = String::new();
    for byte in bytes {
        escaped.push_str("\\u");
        escaped.push_str(&format!("{byte:04X}"));
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
