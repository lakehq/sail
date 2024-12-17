use datafusion::arrow::datatypes::i256;
use half::f16;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::sync::Arc;

use crate::spec;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum Literal {
    Null,
    Boolean {
        value: Option<bool>,
    },
    Int8 {
        value: Option<i8>,
    },
    Int16 {
        value: Option<i16>,
    },
    Int32 {
        value: Option<i32>,
    },
    Int64 {
        value: Option<i64>,
    },
    UInt8 {
        value: Option<u8>,
    },
    UInt16 {
        value: Option<u16>,
    },
    UInt32 {
        value: Option<u32>,
    },
    UInt64 {
        value: Option<u64>,
    },
    Float16 {
        value: Option<f16>,
    },
    Float32 {
        value: Option<f32>,
    },
    Float64 {
        value: Option<f64>,
    },
    TimestampSecond {
        seconds: Option<i64>,
        timezone: Option<Arc<str>>,
    },
    TimestampMillisecond {
        milliseconds: Option<i64>,
        timezone: Option<Arc<str>>,
    },
    TimestampMicrosecond {
        microseconds: Option<i64>,
        timezone: Option<Arc<str>>,
    },
    TimestampNanosecond {
        nanoseconds: Option<i64>,
        timezone: Option<Arc<str>>,
    },
    TimestampNtzSecond {
        seconds: Option<i64>,
    },
    TimestampNtzMillisecond {
        milliseconds: Option<i64>,
    },
    TimestampNtzMicrosecond {
        microseconds: Option<i64>,
    },
    TimestampNtzNanosecond {
        nanoseconds: Option<i64>,
    },
    Date32 {
        days: Option<i32>,
    },
    Date64 {
        milliseconds: Option<i64>,
    },
    Time32Second {
        seconds: Option<i32>,
    },
    Time32Millisecond {
        milliseconds: Option<i32>,
    },
    Time64Microsecond {
        microseconds: Option<i64>,
    },
    Time64Nanosecond {
        nanoseconds: Option<i64>,
    },
    DurationSecond {
        seconds: Option<i64>,
    },
    DurationMillisecond {
        milliseconds: Option<i64>,
    },
    DurationMicrosecond {
        microseconds: Option<i64>,
    },
    DurationNanosecond {
        nanoseconds: Option<i64>,
    },
    IntervalYearMonth {
        months: Option<i32>,
    },
    IntervalDayTime {
        days: Option<i32>,
        milliseconds: Option<i32>,
    },
    IntervalMonthDayNano {
        months: Option<i32>,
        days: Option<i32>,
        nanoseconds: Option<i64>,
    },
    Binary {
        value: Option<Vec<u8>>,
    },
    FixedSizeBinary {
        size: i32,
        value: Option<Vec<u8>>,
    },
    LargeBinary {
        value: Option<Vec<u8>>,
    },
    BinaryView {
        value: Option<Vec<u8>>,
    },
    Utf8 {
        value: Option<String>,
    },
    LargeUtf8 {
        value: Option<String>,
    },
    Utf8View {
        value: Option<String>,
    },
    List {
        data_type: spec::DataType,
        values: Arc<Vec<Literal>>,
    },
    FixedSizeList {
        data_type: spec::DataType,
        size: i32,
        values: Arc<Vec<Literal>>,
    },
    LargeList {
        data_type: spec::DataType,
        values: Arc<Vec<Literal>>,
    },
    Struct {
        data_type: spec::DataType,
        values: Arc<Vec<Literal>>,
    },
    Union {
        union_fields: spec::UnionFields,
        union_mode: spec::UnionMode,
    },
    Dictionary {
        key_type: Box<spec::DataType>,
        value_type: Box<spec::DataType>,
    },
    Decimal128 {
        precision: u8,
        scale: i8,
    },
    Decimal256 {
        precision: u8,
        scale: i8,
    },
    Map {
        key_type: Box<spec::DataType>,
        value_type: Box<spec::DataType>,
        value_type_nullable: bool,
        keys_sorted: bool,
    },
    UserDefined {
        jvm_class: Option<String>,
        python_class: Option<String>,
        serialized_python_class: Option<String>,
        sql_type: Box<spec::DataType>,
    },
    ConfiguredUtf8 {
        utf8_type: spec::Utf8Type,
    },
    ConfiguredBinary,
    // Null,
    // Binary(Vec<u8>),
    // Boolean(bool),
    // Byte(i8),
    // Short(i16),
    // Integer(i32),
    // Long(i64),
    // Float(f32),
    // Double(f64),
    // Decimal128(Decimal128),
    // Decimal256(Decimal256),
    // String(String),
    // Date {
    //     days: i32,
    // },
    // // TODO: We need to implement the remaining Timestamp variants:
    // //  TimestampSecond, TimestampMillisecond, TimestampMicrosecond, TimestampNanosecond
    // TimestampMicrosecond {
    //     microseconds: i64,
    //     timezone: Option<Arc<str>>,
    // },
    // TimestampNtz {
    //     microseconds: i64,
    // },
    // CalendarInterval {
    //     months: i32,
    //     days: i32,
    //     microseconds: i64,
    // },
    // YearMonthInterval {
    //     months: i32,
    // },
    // DayTimeInterval {
    //     microseconds: i64,
    // },
    // Array {
    //     element_type: DataType,
    //     elements: Vec<Literal>,
    // },
    // Map {
    //     key_type: DataType,
    //     value_type: DataType,
    //     keys: Vec<Literal>,
    //     values: Vec<Literal>,
    // },
    // Struct {
    //     struct_type: DataType,
    //     elements: Vec<Literal>,
    // },
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
