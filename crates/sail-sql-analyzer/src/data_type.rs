use sail_common::spec;
use sail_common::spec::ARROW_DECIMAL128_MAX_PRECISION;
use sail_sql_parser::ast::data_type::{
    DataType, IntervalDayTimeUnit, IntervalType, IntervalYearMonthUnit, StructField, TimezoneType,
};
use sail_sql_parser::ast::literal::IntegerLiteral;
use sail_sql_parser::ast::operator::{LeftParenthesis, RightParenthesis};

use crate::error::{SqlError, SqlResult};
use crate::value::from_ast_string;

pub const SQL_DECIMAL_DEFAULT_PRECISION: u8 = 10;
pub const SQL_DECIMAL_DEFAULT_SCALE: i8 = 0;

fn from_ast_char_length(length: Option<&IntegerLiteral>) -> SqlResult<u32> {
    let Some(length) = length else {
        return Err(SqlError::invalid("missing character length"));
    };
    length
        .value
        .try_into()
        .map_err(|_| SqlError::invalid("char length"))
}

fn from_ast_year_month_interval_field(
    field: &IntervalYearMonthUnit,
) -> SqlResult<spec::IntervalFieldType> {
    match field {
        IntervalYearMonthUnit::Year(_) => Ok(spec::IntervalFieldType::Year),
        IntervalYearMonthUnit::Month(_) => Ok(spec::IntervalFieldType::Month),
    }
}

fn from_ast_day_time_interval_field(
    field: &IntervalDayTimeUnit,
) -> SqlResult<spec::IntervalFieldType> {
    match field {
        IntervalDayTimeUnit::Day(_) => Ok(spec::IntervalFieldType::Day),
        IntervalDayTimeUnit::Hour(_) => Ok(spec::IntervalFieldType::Hour),
        IntervalDayTimeUnit::Minute(_) => Ok(spec::IntervalFieldType::Minute),
        IntervalDayTimeUnit::Second(_) => Ok(spec::IntervalFieldType::Second),
    }
}

fn from_ast_timestamp_precision(
    precision: Option<(LeftParenthesis, IntegerLiteral, RightParenthesis)>,
) -> SqlResult<spec::TimeUnit> {
    let precision = precision.as_ref().map(|(_, p, _)| p.value);
    match precision {
        Some(0) => Ok(spec::TimeUnit::Second),
        Some(3) => Ok(spec::TimeUnit::Millisecond),
        None | Some(6) => Ok(spec::TimeUnit::Microsecond),
        Some(9) => Ok(spec::TimeUnit::Nanosecond),
        _ => Err(SqlError::invalid("timestamp precision"))?,
    }
}

pub fn from_ast_data_type(sql_type: DataType) -> SqlResult<spec::DataType> {
    match sql_type {
        DataType::Null(_) | DataType::Void(_) => Ok(spec::DataType::Null),
        DataType::Boolean(_) | DataType::Bool(_) => Ok(spec::DataType::Boolean),
        DataType::TinyInt(None, _) | DataType::Byte(None, _) | DataType::Int8(_) => {
            Ok(spec::DataType::Int8)
        }
        DataType::SmallInt(None, _) | DataType::Short(None, _) | DataType::Int16(_) => {
            Ok(spec::DataType::Int16)
        }
        DataType::Int(None, _) | DataType::Integer(None, _) | DataType::Int32(_) => {
            Ok(spec::DataType::Int32)
        }
        DataType::BigInt(None, _) | DataType::Long(None, _) | DataType::Int64(_) => {
            Ok(spec::DataType::Int64)
        }
        DataType::TinyInt(Some(_), _) | DataType::Byte(Some(_), _) | DataType::UInt8(_) => {
            Ok(spec::DataType::UInt8)
        }
        DataType::SmallInt(Some(_), _) | DataType::Short(Some(_), _) | DataType::UInt16(_) => {
            Ok(spec::DataType::UInt16)
        }
        DataType::Int(Some(_), _) | DataType::Integer(Some(_), _) | DataType::UInt32(_) => {
            Ok(spec::DataType::UInt32)
        }
        DataType::BigInt(Some(_), _) | DataType::Long(Some(_), _) | DataType::UInt64(_) => {
            Ok(spec::DataType::UInt64)
        }
        DataType::Binary(_) | DataType::Bytea(_) => Ok(spec::DataType::ConfiguredBinary),
        DataType::Float(_) | DataType::Float32(_) => Ok(spec::DataType::Float32),
        DataType::Double(_) | DataType::Float64(_) => Ok(spec::DataType::Float64),
        DataType::Decimal(_, info) => {
            let (precision, scale) = match info {
                None => (SQL_DECIMAL_DEFAULT_PRECISION, SQL_DECIMAL_DEFAULT_SCALE),
                Some((_, precision, None, _)) => {
                    let precision = precision
                        .value
                        .try_into()
                        .map_err(|_| SqlError::invalid("precision"))?;
                    (precision, SQL_DECIMAL_DEFAULT_SCALE)
                }
                Some((_, precision, Some((_, scale)), _)) => {
                    let precision = precision
                        .value
                        .try_into()
                        .map_err(|_| SqlError::invalid("precision"))?;
                    let scale = scale
                        .value
                        .try_into()
                        .map_err(|_| SqlError::invalid("scale"))?;
                    (precision, scale)
                }
            };
            if precision > ARROW_DECIMAL128_MAX_PRECISION {
                Ok(spec::DataType::Decimal256 { precision, scale })
            } else {
                Ok(spec::DataType::Decimal128 { precision, scale })
            }
        }
        DataType::Char(_, ref n) | DataType::Character(_, ref n) => {
            let n = n.as_ref().map(|(_, n, _)| n);
            Ok(spec::DataType::ConfiguredUtf8 {
                utf8_type: spec::Utf8Type::Char {
                    length: from_ast_char_length(n)?,
                },
            })
        }
        DataType::Varchar(_, _, ref n, _) => Ok(spec::DataType::ConfiguredUtf8 {
            utf8_type: spec::Utf8Type::VarChar {
                length: from_ast_char_length(Some(n))?,
            },
        }),
        DataType::String(_) => Ok(spec::DataType::ConfiguredUtf8 {
            utf8_type: spec::Utf8Type::Configured,
        }),
        DataType::Text(_) => Ok(spec::DataType::LargeUtf8),
        DataType::Timestamp(_, precision, tz) => {
            let timezone_info = match tz {
                Some(TimezoneType::WithoutTimeZone(_, _, _)) => spec::TimeZoneInfo::NoTimeZone,
                Some(TimezoneType::WithLocalTimeZone(_, _, _, _))
                | Some(TimezoneType::WithTimeZone(_, _, _)) => spec::TimeZoneInfo::LocalTimeZone,
                None => spec::TimeZoneInfo::SQLConfigured,
            };
            let time_unit = from_ast_timestamp_precision(precision)?;
            Ok(spec::DataType::Timestamp {
                time_unit,
                timezone_info,
            })
        }
        DataType::TimestampNtz(_, precision) => {
            let time_unit = from_ast_timestamp_precision(precision)?;
            Ok(spec::DataType::Timestamp {
                time_unit,
                timezone_info: spec::TimeZoneInfo::NoTimeZone,
            })
        }
        DataType::TimestampLtz(_, precision) => {
            let time_unit = from_ast_timestamp_precision(precision)?;
            Ok(spec::DataType::Timestamp {
                time_unit,
                timezone_info: spec::TimeZoneInfo::LocalTimeZone,
            })
        }
        DataType::Date(_) | DataType::Date32(_) => Ok(spec::DataType::Date32),
        DataType::Date64(_) => Ok(spec::DataType::Date64),
        DataType::Interval(ref interval) => match interval {
            IntervalType::YearMonth(_, start, end) => {
                let start = from_ast_year_month_interval_field(start)?;
                let end = end
                    .as_ref()
                    .map(|(_, end)| from_ast_year_month_interval_field(end))
                    .transpose()?;
                if end.is_some_and(|end| end <= start) {
                    return Err(SqlError::invalid(format!(
                        "interval end field: {interval:?}"
                    )));
                }
                Ok(spec::DataType::Interval {
                    interval_unit: spec::IntervalUnit::YearMonth,
                    start_field: Some(start),
                    end_field: end,
                })
            }
            IntervalType::DayTime(_, start, end) => {
                let start = from_ast_day_time_interval_field(start)?;
                let end = end
                    .as_ref()
                    .map(|(_, end)| from_ast_day_time_interval_field(end))
                    .transpose()?;
                if end.is_some_and(|end| end <= start) {
                    return Err(SqlError::invalid(format!(
                        "interval end field: {interval:?}"
                    )));
                }
                // FIXME: Currently `start_field` and `end_field` are lost in translation.
                //  This does not impact computation accuracy.
                //  This may affect the display string in the `data_type_to_simple_string` function.
                Ok(spec::DataType::Duration {
                    time_unit: spec::TimeUnit::Microsecond,
                })
            }
            IntervalType::Default(_) => Ok(spec::DataType::Interval {
                interval_unit: spec::IntervalUnit::MonthDayNano,
                start_field: None,
                end_field: None,
            }),
        },
        DataType::Array(_, _, inner, _) => {
            let inner = from_ast_data_type(*inner)?;
            Ok(spec::DataType::List {
                data_type: Box::new(inner),
                nullable: true,
            })
        }
        DataType::Struct(_, _, fields, _) => {
            let fields = fields
                .map(|x| {
                    x.into_items()
                        .map(|f| {
                            let StructField {
                                identifier,
                                colon: _,
                                data_type,
                                not_null,
                                comment,
                            } = f;
                            let name = identifier.value.clone();
                            let data_type = from_ast_data_type(data_type)?;
                            let mut metadata = vec![];
                            if let Some((_, comment)) = comment {
                                metadata.push(("comment".to_string(), from_ast_string(comment)?));
                            };
                            Ok(spec::Field {
                                name,
                                data_type,
                                nullable: not_null.is_none(),
                                metadata,
                            })
                        })
                        .collect::<SqlResult<Vec<_>>>()
                })
                .transpose()?
                .unwrap_or_default();
            Ok(spec::DataType::Struct {
                fields: spec::Fields::from(fields),
            })
        }
        DataType::Map(_, _, key, _, value, _) => {
            let key = from_ast_data_type(*key)?;
            let value = from_ast_data_type(*value)?;
            Ok(spec::DataType::Map {
                key_type: Box::new(key),
                value_type: Box::new(value),
                value_type_nullable: true,
                keys_sorted: false,
            })
        }
    }
}
