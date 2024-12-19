use datafusion::arrow::datatypes::i256;
use half::f16;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::spec;

/// See [`spec::DataType`] for details on datatypes
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
        timezone_info: spec::TimeZoneInfo,
    },
    TimestampMillisecond {
        milliseconds: Option<i64>,
        timezone_info: spec::TimeZoneInfo,
    },
    TimestampMicrosecond {
        microseconds: Option<i64>,
        timezone_info: spec::TimeZoneInfo,
    },
    TimestampNanosecond {
        nanoseconds: Option<i64>,
        timezone_info: spec::TimeZoneInfo,
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
        values: Option<Vec<Literal>>,
    },
    FixedSizeList {
        size: i32,
        data_type: spec::DataType,
        values: Option<Vec<Literal>>,
    },
    LargeList {
        data_type: spec::DataType,
        values: Option<Vec<Literal>>,
    },
    Struct {
        data_type: spec::DataType,
        values: Option<Vec<Literal>>,
    },
    Union {
        union_fields: spec::UnionFields,
        union_mode: spec::UnionMode,
        value: Option<(i8, Box<Literal>)>,
    },
    Dictionary {
        key_type: spec::DataType,
        value_type: spec::DataType,
        value: Option<Box<Literal>>,
    },
    Decimal128 {
        precision: u8,
        scale: i8,
        value: Option<i128>,
    },
    Decimal256 {
        precision: u8,
        scale: i8,
        #[serde(
            serialize_with = "serialize_i256",
            deserialize_with = "deserialize_i256"
        )]
        value: Option<i256>,
    },
    Map {
        key_type: spec::DataType,
        value_type: spec::DataType,
        keys: Option<Vec<Literal>>,
        values: Option<Vec<Literal>>,
    },
}

#[derive(Serialize, Deserialize)]
struct Int256 {
    low: u128,
    high: i128,
}

fn serialize_i256<S>(value: &Option<i256>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(num) => {
            let (low, high) = num.to_parts();
            serializer.serialize_some(&Int256 { low, high })
        }
        None => serializer.serialize_none(),
    }
}

fn deserialize_i256<'de, D>(deserializer: D) -> Result<Option<i256>, D::Error>
where
    D: Deserializer<'de>,
{
    let parts = Int256::deserialize(deserializer)?;
    Ok(Some(i256::from_parts(parts.low, parts.high)))
}
