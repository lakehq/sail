use std::sync::Arc;

use arrow::datatypes::i256;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::spec::DataType;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum Literal {
    Null,
    Binary(Vec<u8>),
    Boolean(bool),
    Byte(i8),
    Short(i16),
    Integer(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Decimal128(Decimal128),
    Decimal256(Decimal256),
    String(String),
    Date {
        days: i32,
    },
    // TODO: We need to implement the remaining Timestamp variants:
    //  TimestampSecond, TimestampMillisecond, TimestampMicrosecond, TimestampNanosecond
    TimestampMicrosecond {
        microseconds: i64,
        timezone: Option<Arc<str>>,
    },
    TimestampNtz {
        microseconds: i64,
    },
    CalendarInterval {
        months: i32,
        days: i32,
        microseconds: i64,
    },
    YearMonthInterval {
        months: i32,
    },
    DayTimeInterval {
        microseconds: i64,
    },
    Array {
        element_type: DataType,
        elements: Vec<Literal>,
    },
    Map {
        key_type: DataType,
        value_type: DataType,
        keys: Vec<Literal>,
        values: Vec<Literal>,
    },
    Struct {
        struct_type: DataType,
        elements: Vec<Literal>,
    },
}

#[derive(Debug)]
pub enum DecimalLiteral {
    Decimal128(Decimal128),
    Decimal256(Decimal256),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Decimal128 {
    pub value: i128,
    pub precision: u8,
    pub scale: i8,
}

impl Decimal128 {
    pub fn new(value: i128, precision: u8, scale: i8) -> Self {
        Self {
            value,
            precision,
            scale,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Decimal256 {
    #[serde(
        serialize_with = "serialize_i256",
        deserialize_with = "deserialize_i256"
    )]
    pub value: i256,
    pub precision: u8,
    pub scale: i8,
}

impl Decimal256 {
    pub fn new(value: i256, precision: u8, scale: i8) -> Self {
        Self {
            value,
            precision,
            scale,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Int256 {
    low: u128,
    high: i128,
}

fn serialize_i256<S>(value: &i256, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let (low, high) = value.to_parts();
    Int256 { low, high }.serialize(serializer)
}

fn deserialize_i256<'de, D>(deserializer: D) -> Result<i256, D::Error>
where
    D: Deserializer<'de>,
{
    let parts = Int256::deserialize(deserializer)?;
    Ok(i256::from_parts(parts.low, parts.high))
}
