#![allow(dead_code)]

use std::cmp::Ordering;

use chrono::{DateTime, TimeZone, Utc};
use datafusion::arrow::array::{self, Array};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use delta_kernel::engine::arrow_conversion::TryIntoKernel as _;
use delta_kernel::expressions::{Scalar, StructData};
use delta_kernel::schema::StructField;
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use serde_json::Value;

pub const NULL_PARTITION_VALUE_DATA_PATH: &str = "__HIVE_DEFAULT_PARTITION__";

const RFC3986_PART: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'!')
    .add(b'"')
    .add(b'#')
    .add(b'$')
    .add(b'%')
    .add(b'&')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b'+')
    .add(b',')
    .add(b'/')
    .add(b':')
    .add(b';')
    .add(b'<')
    .add(b'=')
    .add(b'>')
    .add(b'?')
    .add(b'@')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

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

        match arr.data_type() {
            ArrowDataType::Utf8 => arr
                .as_any()
                .downcast_ref::<array::StringArray>()
                .map(|values| Self::String(values.value(index).to_string())),
            ArrowDataType::LargeUtf8 => arr
                .as_any()
                .downcast_ref::<array::LargeStringArray>()
                .map(|values| Self::String(values.value(index).to_string())),
            ArrowDataType::Utf8View => arr
                .as_any()
                .downcast_ref::<array::StringViewArray>()
                .map(|values| Self::String(values.value(index).to_string())),
            ArrowDataType::Boolean => arr
                .as_any()
                .downcast_ref::<array::BooleanArray>()
                .map(|values| Self::Boolean(values.value(index))),
            ArrowDataType::Binary => arr
                .as_any()
                .downcast_ref::<array::BinaryArray>()
                .map(|values| Self::Binary(values.value(index).to_vec())),
            ArrowDataType::LargeBinary => arr
                .as_any()
                .downcast_ref::<array::LargeBinaryArray>()
                .map(|values| Self::Binary(values.value(index).to_vec())),
            ArrowDataType::FixedSizeBinary(_) => arr
                .as_any()
                .downcast_ref::<array::FixedSizeBinaryArray>()
                .map(|values| Self::Binary(values.value(index).to_vec())),
            ArrowDataType::BinaryView => arr
                .as_any()
                .downcast_ref::<array::BinaryViewArray>()
                .map(|values| Self::Binary(values.value(index).to_vec())),
            ArrowDataType::Int8 => arr
                .as_any()
                .downcast_ref::<array::Int8Array>()
                .map(|values| Self::Byte(values.value(index))),
            ArrowDataType::Int16 => arr
                .as_any()
                .downcast_ref::<array::Int16Array>()
                .map(|values| Self::Short(values.value(index))),
            ArrowDataType::Int32 => arr
                .as_any()
                .downcast_ref::<array::Int32Array>()
                .map(|values| Self::Integer(values.value(index))),
            ArrowDataType::Int64 => arr
                .as_any()
                .downcast_ref::<array::Int64Array>()
                .map(|values| Self::Long(values.value(index))),
            ArrowDataType::UInt8 => arr
                .as_any()
                .downcast_ref::<array::UInt8Array>()
                .map(|values| Self::Byte(values.value(index) as i8)),
            ArrowDataType::UInt16 => arr
                .as_any()
                .downcast_ref::<array::UInt16Array>()
                .map(|values| Self::Short(values.value(index) as i16)),
            ArrowDataType::UInt32 => arr
                .as_any()
                .downcast_ref::<array::UInt32Array>()
                .map(|values| Self::Integer(values.value(index) as i32)),
            ArrowDataType::UInt64 => arr
                .as_any()
                .downcast_ref::<array::UInt64Array>()
                .map(|values| Self::Long(values.value(index) as i64)),
            ArrowDataType::Float32 => arr
                .as_any()
                .downcast_ref::<array::Float32Array>()
                .map(|values| Self::Float(values.value(index))),
            ArrowDataType::Float64 => arr
                .as_any()
                .downcast_ref::<array::Float64Array>()
                .map(|values| Self::Double(values.value(index))),
            ArrowDataType::Decimal128(precision, scale) => arr
                .as_any()
                .downcast_ref::<array::Decimal128Array>()
                .and_then(|values| {
                    let value = values.value(index);
                    Self::decimal(value, *precision, *scale as u8).ok()
                }),
            ArrowDataType::Date32 => arr
                .as_any()
                .downcast_ref::<array::Date32Array>()
                .map(|values| Self::Date(values.value(index))),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None) => arr
                .as_any()
                .downcast_ref::<array::TimestampMicrosecondArray>()
                .map(|values| Self::TimestampNtz(values.value(index))),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some(tz))
                if tz.eq_ignore_ascii_case("utc") =>
            {
                arr.as_any()
                    .downcast_ref::<array::TimestampMicrosecondArray>()
                    .map(|values| Self::Timestamp(values.value(index)))
            }
            ArrowDataType::Struct(fields) => {
                let struct_fields = fields
                    .iter()
                    .flat_map(|field| field.as_ref().try_into_kernel())
                    .collect::<Vec<_>>();
                let values =
                    arr.as_any()
                        .downcast_ref::<array::StructArray>()
                        .and_then(|struct_array| {
                            struct_fields
                                .iter()
                                .map(|field: &StructField| {
                                    struct_array
                                        .column_by_name(field.name())
                                        .and_then(|column| Self::from_array(column.as_ref(), index))
                                })
                                .collect::<Option<Vec<_>>>()
                        })?;
                Some(Self::Struct(
                    StructData::try_new(struct_fields, values).ok()?,
                ))
            }
            ArrowDataType::Float16
            | ArrowDataType::Decimal32(_, _)
            | ArrowDataType::Decimal64(_, _)
            | ArrowDataType::Decimal256(_, _)
            | ArrowDataType::List(_)
            | ArrowDataType::LargeList(_)
            | ArrowDataType::FixedSizeList(_, _)
            | ArrowDataType::Map(_, _)
            | ArrowDataType::Date64
            | ArrowDataType::Timestamp(_, _)
            | ArrowDataType::Time32(_)
            | ArrowDataType::Time64(_)
            | ArrowDataType::Duration(_)
            | ArrowDataType::Interval(_)
            | ArrowDataType::Dictionary(_, _)
            | ArrowDataType::RunEndEncoded(_, _)
            | ArrowDataType::Union(_, _)
            | ArrowDataType::ListView(_)
            | ArrowDataType::LargeListView(_)
            | ArrowDataType::Null => None,
        }
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
