use sail_common::spec;
use sail_sql::literal::{microseconds_to_interval, parse_decimal_string};

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::spark::connect::expression::literal::{Array, Decimal, LiteralType, Map, Struct};
use crate::spark::connect::expression::Literal;

impl TryFrom<Literal> for spec::Literal {
    type Error = SparkError;

    fn try_from(literal: Literal) -> SparkResult<spec::Literal> {
        let Literal { literal_type } = literal;
        let literal_type = literal_type.required("literal type")?;
        let literal = match literal_type {
            LiteralType::Null(data_type) => {
                if data_type.kind.is_some() {
                    let data_type: spec::DataType = data_type.try_into()?;
                    match data_type {
                        spec::DataType::Null => Ok(spec::Literal::Null),
                        spec::DataType::Boolean => Ok(spec::Literal::Boolean { value: None }),
                        spec::DataType::Int8 => Ok(spec::Literal::Int8 { value: None }),
                        spec::DataType::Int16 => Ok(spec::Literal::Int16 { value: None }),
                        spec::DataType::Int32 => Ok(spec::Literal::Int32 { value: None }),
                        spec::DataType::Int64 => Ok(spec::Literal::Int64 { value: None }),
                        spec::DataType::UInt8 => Ok(spec::Literal::UInt8 { value: None }),
                        spec::DataType::UInt16 => Ok(spec::Literal::UInt16 { value: None }),
                        spec::DataType::UInt32 => Ok(spec::Literal::UInt32 { value: None }),
                        spec::DataType::UInt64 => Ok(spec::Literal::UInt64 { value: None }),
                        spec::DataType::Float16 => Ok(spec::Literal::Float16 { value: None }),
                        spec::DataType::Float32 => Ok(spec::Literal::Float32 { value: None }),
                        spec::DataType::Float64 => Ok(spec::Literal::Float64 { value: None }),
                        spec::DataType::Timestamp { time_unit, timezone_info } => Ok({
                            match time_unit {
                                spec::TimeUnit::Second => spec::Literal::TimestampSecond {
                                    seconds: None,
                                    timezone_info,
                                },
                                spec::TimeUnit::Millisecond => spec::Literal::TimestampMillisecond {
                                    milliseconds: None,
                                    timezone_info,
                                },
                                spec::TimeUnit::Microsecond => spec::Literal::TimestampMicrosecond {
                                    microseconds: None,
                                    timezone_info,
                                },
                                spec::TimeUnit::Nanosecond => spec::Literal::TimestampNanosecond {
                                    nanoseconds: None,
                                    timezone_info,
                                },
                            }
                        }),
                        spec::DataType::Date32 => Ok(spec::Literal::Date32 { days: None }),
                        spec::DataType::Date64 => Ok(spec::Literal::Date64 { milliseconds: None }),
                        spec::DataType::Time32 { time_unit } => {
                            match time_unit {
                                spec::TimeUnit::Second => Ok(spec::Literal::Time32Second {
                                    seconds: None,
                                }),
                                spec::TimeUnit::Millisecond => Ok(spec::Literal::Time32Millisecond {
                                    milliseconds: None,
                                }),
                                spec::TimeUnit::Microsecond => Err(SparkError::todo("TryFrom Spark Literal to Sail Literal Time32 with TimeUnit::Microsecond")),
                                spec::TimeUnit::Nanosecond => Err(SparkError::todo("TryFrom Spark Literal to Sail Literal Time32 with TimeUnit::Nanosecond")),
                            }
                        }
                        spec::DataType::Time64 { time_unit } => {
                            match time_unit {
                                spec::TimeUnit::Second => Err(SparkError::todo("TryFrom Spark Literal to Sail Literal Time64 with TimeUnit::Second")),
                                spec::TimeUnit::Millisecond => Err(SparkError::todo("TryFrom Spark Literal to Sail Literal Time64 with TimeUnit::Millisecond")),
                                spec::TimeUnit::Microsecond => Ok(spec::Literal::Time64Microsecond {
                                    microseconds: None,
                                }),
                                spec::TimeUnit::Nanosecond => Ok(spec::Literal::Time64Nanosecond {
                                    nanoseconds: None,
                                }),
                            }
                        }
                        spec::DataType::Duration { time_unit } => {
                            match time_unit {
                                spec::TimeUnit::Second => Ok(spec::Literal::DurationSecond {
                                    seconds: None,
                                }),
                                spec::TimeUnit::Millisecond => Ok(spec::Literal::DurationMillisecond{
                                    milliseconds: None,
                                }),
                                spec::TimeUnit::Microsecond => Ok(spec::Literal::DurationMicrosecond {
                                    microseconds: None,
                                }),
                                spec::TimeUnit::Nanosecond => Ok(spec::Literal::DurationNanosecond {
                                    nanoseconds: None,
                                }),
                            }
                        }
                        spec::DataType::Interval { interval_unit, start_field: _, end_field: _ } => {
                            match interval_unit {
                                spec::IntervalUnit::YearMonth => Ok(spec::Literal::IntervalYearMonth {
                                    months: None,
                                }),
                                spec::IntervalUnit::DayTime => Ok(spec::Literal::IntervalDayTime {
                                    days: None,
                                    milliseconds: None,
                                }),
                                spec::IntervalUnit::MonthDayNano => Ok(spec::Literal::IntervalMonthDayNano {
                                    months: None,
                                    days: None,
                                    nanoseconds: None,
                                }),
                            }
                        },
                        spec::DataType::Binary => Ok(spec::Literal::Binary { value: None }),
                        spec::DataType::FixedSizeBinary { size } => Ok(spec::Literal::FixedSizeBinary { size, value: None }),
                        spec::DataType::LargeBinary => Ok(spec::Literal::LargeBinary { value: None }),
                        spec::DataType::BinaryView => Ok(spec::Literal::BinaryView { value: None }),
                        spec::DataType::Utf8 => Ok(spec::Literal::Utf8 { value: None }),
                        spec::DataType::LargeUtf8 => Ok(spec::Literal::LargeUtf8 { value: None }),
                        spec::DataType::Utf8View => Ok(spec::Literal::Utf8View { value: None }),
                        spec::DataType::List { data_type, nullable: _ } => Ok(spec::Literal::List { data_type: *data_type, values: None }),
                        spec::DataType::FixedSizeList { data_type, nullable: _, length } => Ok(spec::Literal::FixedSizeList { length, data_type: *data_type, values: None }),
                        spec::DataType::LargeList { data_type, nullable: _ } => Ok(spec::Literal::LargeList { data_type: *data_type, values: None }),
                        spec::DataType::Struct { fields } => Ok(spec::Literal::Struct { data_type: spec::DataType::Struct { fields }, values: None }),
                        spec::DataType::Union { union_fields, union_mode } => Ok(spec::Literal::Union { union_fields, union_mode, value: None }),
                        spec::DataType::Dictionary { key_type, value_type } => Ok(spec::Literal::Dictionary { key_type: *key_type, value_type: *value_type, value: None }),
                        spec::DataType::Decimal128 { precision, scale } => Ok(spec::Literal::Decimal128 { precision, scale, value: None }),
                        spec::DataType::Decimal256 { precision, scale } => Ok(spec::Literal::Decimal256 { precision, scale, value: None }),
                        spec::DataType::Map { key_type, value_type, value_type_nullable: _, keys_sorted: _ } => Ok(spec::Literal::Map { key_type: *key_type, value_type: *value_type, keys: None, values: None }),
                        spec::DataType::ConfiguredUtf8 { .. } => Ok(spec::Literal::Utf8 { value: None }),
                        spec::DataType::ConfiguredBinary => Ok(spec::Literal::Binary { value: None }),
                        spec::DataType::UserDefined { .. } => Err(SparkError::todo("TryFrom Spark Literal to Sail Literal UserDefined")),
                    }?
                } else {
                    spec::Literal::Null
                }
            }
            LiteralType::Binary(x) => spec::Literal::Binary { value: Some(x) },
            LiteralType::Boolean(x) => spec::Literal::Boolean { value: Some(x) },
            LiteralType::Byte(x) => spec::Literal::Int8 {
                value: Some(x as i8),
            },
            LiteralType::Short(x) => spec::Literal::Int16 {
                value: Some(x as i16),
            },
            LiteralType::Integer(x) => spec::Literal::Int32 { value: Some(x) },
            LiteralType::Long(x) => spec::Literal::Int64 { value: Some(x) },
            LiteralType::Float(x) => spec::Literal::Float32 { value: Some(x) },
            LiteralType::Double(x) => spec::Literal::Float64 { value: Some(x) },
            LiteralType::Decimal(Decimal {
                value,
                precision,
                scale,
            }) => {
                if precision.is_some() || scale.is_some() {
                    return Err(SparkError::todo("decimal literal with precision or scale"));
                }
                parse_decimal_string(value.as_str())?
            }
            LiteralType::String(x) => spec::Literal::Utf8 { value: Some(x) },
            LiteralType::Date(x) => spec::Literal::Date32 { days: Some(x) },
            LiteralType::Timestamp(x) => spec::Literal::TimestampMicrosecond {
                microseconds: Some(x),
                timezone_info: spec::TimeZoneInfo::Configured,
            },
            LiteralType::TimestampNtz(x) => spec::Literal::TimestampMicrosecond {
                microseconds: Some(x),
                timezone_info: spec::TimeZoneInfo::NoTimeZone,
            },
            LiteralType::CalendarInterval(x) => {
                let nanoseconds = x.microseconds * 1_000;
                spec::Literal::IntervalMonthDayNano {
                    months: Some(x.months),
                    days: Some(x.days),
                    nanoseconds: Some(nanoseconds),
                }
            }
            LiteralType::YearMonthInterval(x) => {
                spec::Literal::IntervalYearMonth { months: Some(x) }
            }
            LiteralType::DayTimeInterval(x) => microseconds_to_interval(x),
            LiteralType::Array(Array {
                element_type,
                elements,
            }) => {
                let element_type = element_type.required("element type")?;
                spec::Literal::List {
                    data_type: element_type.try_into()?,
                    values: Some(
                        elements
                            .into_iter()
                            .map(|x| x.try_into())
                            .collect::<SparkResult<_>>()?,
                    ),
                }
            }
            LiteralType::Map(Map {
                key_type,
                value_type,
                keys,
                values,
            }) => {
                let key_type = key_type.required("key type")?;
                let value_type = value_type.required("value type")?;
                spec::Literal::Map {
                    key_type: key_type.try_into()?,
                    value_type: value_type.try_into()?,
                    keys: Some(
                        keys.into_iter()
                            .map(|x| x.try_into())
                            .collect::<SparkResult<_>>()?,
                    ),
                    values: Some(
                        values
                            .into_iter()
                            .map(|x| x.try_into())
                            .collect::<SparkResult<_>>()?,
                    ),
                }
            }
            LiteralType::Struct(Struct {
                struct_type,
                elements,
            }) => {
                let struct_type = struct_type.required("struct type")?;
                spec::Literal::Struct {
                    data_type: struct_type.try_into()?,
                    values: Some(
                        elements
                            .into_iter()
                            .map(|x| x.try_into())
                            .collect::<SparkResult<_>>()?,
                    ),
                }
            }
        };
        Ok(literal)
    }
}
