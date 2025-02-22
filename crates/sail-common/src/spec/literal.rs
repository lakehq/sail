use std::str::FromStr;

pub use arrow_buffer::i256;
use half::f16;
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::{CommonError, CommonResult};
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
        value: Option<IntervalDayTime>,
    },
    IntervalMonthDayNano {
        value: Option<IntervalMonthDayNano>,
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
        length: i32,
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
        #[serde(
            serialize_with = "serialize_optional",
            deserialize_with = "deserialize_optional"
        )]
        value: Option<i128>,
    },
    Decimal256 {
        precision: u8,
        scale: i8,
        #[serde(
            serialize_with = "serialize_optional",
            deserialize_with = "deserialize_optional"
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IntervalDayTime {
    pub days: i32,
    pub milliseconds: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IntervalMonthDayNano {
    pub months: i32,
    pub days: i32,
    pub nanoseconds: i64,
}

fn serialize_optional<T, S>(value: &Option<T>, serializer: S) -> Result<S::Ok, S::Error>
where
    T: ToString,
    S: Serializer,
{
    match value {
        Some(num) => serializer.serialize_some(&num.to_string()),
        None => serializer.serialize_none(),
    }
}

fn deserialize_optional<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
where
    T: FromStr,
    T::Err: std::fmt::Display,
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Ok(Some(
        T::from_str(&s).map_err(|e| Error::custom(e.to_string()))?,
    ))
}

pub fn data_type_to_null_literal(data_type: spec::DataType) -> CommonResult<Literal> {
    match data_type {
        spec::DataType::Null => Ok(Literal::Null),
        spec::DataType::Boolean => Ok(Literal::Boolean { value: None }),
        spec::DataType::Int8 => Ok(Literal::Int8 { value: None }),
        spec::DataType::Int16 => Ok(Literal::Int16 { value: None }),
        spec::DataType::Int32 => Ok(Literal::Int32 { value: None }),
        spec::DataType::Int64 => Ok(Literal::Int64 { value: None }),
        spec::DataType::UInt8 => Ok(Literal::UInt8 { value: None }),
        spec::DataType::UInt16 => Ok(Literal::UInt16 { value: None }),
        spec::DataType::UInt32 => Ok(Literal::UInt32 { value: None }),
        spec::DataType::UInt64 => Ok(Literal::UInt64 { value: None }),
        spec::DataType::Float16 => Ok(Literal::Float16 { value: None }),
        spec::DataType::Float32 => Ok(Literal::Float32 { value: None }),
        spec::DataType::Float64 => Ok(Literal::Float64 { value: None }),
        spec::DataType::Timestamp {
            time_unit,
            timezone_info,
        } => Ok({
            match time_unit {
                spec::TimeUnit::Second => Literal::TimestampSecond {
                    seconds: None,
                    timezone_info,
                },
                spec::TimeUnit::Millisecond => Literal::TimestampMillisecond {
                    milliseconds: None,
                    timezone_info,
                },
                spec::TimeUnit::Microsecond => Literal::TimestampMicrosecond {
                    microseconds: None,
                    timezone_info,
                },
                spec::TimeUnit::Nanosecond => Literal::TimestampNanosecond {
                    nanoseconds: None,
                    timezone_info,
                },
            }
        }),
        spec::DataType::Date32 => Ok(Literal::Date32 { days: None }),
        spec::DataType::Date64 => Ok(Literal::Date64 { milliseconds: None }),
        spec::DataType::Time32 { time_unit } => match time_unit {
            spec::TimeUnit::Second => Ok(Literal::Time32Second { seconds: None }),
            spec::TimeUnit::Millisecond => Ok(Literal::Time32Millisecond { milliseconds: None }),
            spec::TimeUnit::Microsecond => Err(CommonError::invalid(
                "DataType::Time32 to Literal::Time32 with TimeUnit::Microsecond",
            )),
            spec::TimeUnit::Nanosecond => Err(CommonError::invalid(
                "DataType::Time32 to Literal::Time32 with TimeUnit::Nanosecond",
            )),
        },
        spec::DataType::Time64 { time_unit } => match time_unit {
            spec::TimeUnit::Second => Err(CommonError::invalid(
                "DataType::Time64 to Literal::Time64 with TimeUnit::Second",
            )),
            spec::TimeUnit::Millisecond => Err(CommonError::invalid(
                "DataType::Time64 to Literal::Time64 with TimeUnit::Millisecond",
            )),
            spec::TimeUnit::Microsecond => Ok(Literal::Time64Microsecond { microseconds: None }),
            spec::TimeUnit::Nanosecond => Ok(Literal::Time64Nanosecond { nanoseconds: None }),
        },
        spec::DataType::Duration { time_unit } => match time_unit {
            spec::TimeUnit::Second => Ok(Literal::DurationSecond { seconds: None }),
            spec::TimeUnit::Millisecond => Ok(Literal::DurationMillisecond { milliseconds: None }),
            spec::TimeUnit::Microsecond => Ok(Literal::DurationMicrosecond { microseconds: None }),
            spec::TimeUnit::Nanosecond => Ok(Literal::DurationNanosecond { nanoseconds: None }),
        },
        spec::DataType::Interval {
            interval_unit,
            start_field: _,
            end_field: _,
        } => match interval_unit {
            spec::IntervalUnit::YearMonth => Ok(Literal::IntervalYearMonth { months: None }),
            spec::IntervalUnit::DayTime => Ok(Literal::IntervalDayTime { value: None }),
            spec::IntervalUnit::MonthDayNano => Ok(Literal::IntervalMonthDayNano { value: None }),
        },
        spec::DataType::Binary => Ok(Literal::Binary { value: None }),
        spec::DataType::FixedSizeBinary { size } => {
            Ok(Literal::FixedSizeBinary { size, value: None })
        }
        spec::DataType::LargeBinary => Ok(Literal::LargeBinary { value: None }),
        spec::DataType::BinaryView => Ok(Literal::BinaryView { value: None }),
        spec::DataType::Utf8 => Ok(Literal::Utf8 { value: None }),
        spec::DataType::LargeUtf8 => Ok(Literal::LargeUtf8 { value: None }),
        spec::DataType::Utf8View => Ok(Literal::Utf8View { value: None }),
        spec::DataType::List {
            data_type,
            nullable: _,
        } => Ok(Literal::List {
            data_type: *data_type,
            values: None,
        }),
        spec::DataType::FixedSizeList {
            data_type,
            nullable: _,
            length,
        } => Ok(Literal::FixedSizeList {
            length,
            data_type: *data_type,
            values: None,
        }),
        spec::DataType::LargeList {
            data_type,
            nullable: _,
        } => Ok(Literal::LargeList {
            data_type: *data_type,
            values: None,
        }),
        spec::DataType::Struct { fields } => Ok(Literal::Struct {
            data_type: spec::DataType::Struct { fields },
            values: None,
        }),
        spec::DataType::Union {
            union_fields,
            union_mode,
        } => Ok(Literal::Union {
            union_fields,
            union_mode,
            value: None,
        }),
        spec::DataType::Dictionary {
            key_type,
            value_type,
        } => Ok(Literal::Dictionary {
            key_type: *key_type,
            value_type: *value_type,
            value: None,
        }),
        spec::DataType::Decimal128 { precision, scale } => Ok(Literal::Decimal128 {
            precision,
            scale,
            value: None,
        }),
        spec::DataType::Decimal256 { precision, scale } => Ok(Literal::Decimal256 {
            precision,
            scale,
            value: None,
        }),
        spec::DataType::Map {
            key_type,
            value_type,
            value_type_nullable: _,
            keys_sorted: _,
        } => Ok(Literal::Map {
            key_type: *key_type,
            value_type: *value_type,
            keys: None,
            values: None,
        }),
        spec::DataType::ConfiguredUtf8 { .. } => Ok(Literal::Utf8 { value: None }),
        spec::DataType::ConfiguredBinary => Ok(Literal::Binary { value: None }),
        spec::DataType::UserDefined { .. } => Err(CommonError::NotSupported(
            "DataType::UserDefined to Literal".to_string(),
        )),
    }
}
