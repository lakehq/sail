use serde::{Deserialize, Serialize};

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
    Decimal128(DecimalType),
    Decimal256(DecimalType),
    String(String),
    Date {
        days: i32,
    },
    Timestamp {
        microseconds: i64,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DecimalType {
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

#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct i256 {
    low: u128,
    high: i128,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Decimal256 {
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
