use std::sync::Arc;

use datafusion::arrow::datatypes::DECIMAL128_MAX_PRECISION as ARROW_DECIMAL128_MAX_PRECISION;
use sail_common::spec;
use sqlparser::ast;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use crate::error::{SqlError, SqlResult};
use crate::parser::{fail_on_extra_token, SparkDialect};
pub const SQL_DECIMAL_DEFAULT_PRECISION: u8 = 10;
pub const SQL_DECIMAL_DEFAULT_SCALE: i8 = 0;
pub const SQL_DECIMAL_MAX_PRECISION: u8 = 38;
pub const SQL_DECIMAL_MAX_SCALE: i8 = 38;

pub fn parse_data_type(sql: &str) -> SqlResult<spec::DataType> {
    let mut parser = Parser::new(&SparkDialect {}).try_with_sql(sql)?;
    if parser.peek_token().token == Token::EOF {
        return Err(SqlError::invalid("empty data type"));
    }
    let data_type = parser.parse_data_type()?;
    fail_on_extra_token(&mut parser, "data type")?;
    from_ast_data_type(&data_type)
}

fn from_ast_char_length(length: &Option<ast::CharacterLength>) -> SqlResult<u32> {
    let length = length.ok_or_else(|| SqlError::invalid("missing character length"))?;
    match length {
        ast::CharacterLength::IntegerLength { length, unit } => {
            if unit.is_some() {
                return Err(SqlError::unsupported("char length unit"));
            }
            let length = length
                .try_into()
                .map_err(|_| SqlError::invalid("char length"))?;
            Ok(length)
        }
        ast::CharacterLength::Max => Err(SqlError::unsupported("char length max")),
    }
}

fn from_ast_year_month_interval_field(field: &ast::DateTimeField) -> SqlResult<i32> {
    match field {
        ast::DateTimeField::Year => Ok(0),
        ast::DateTimeField::Month => Ok(1),
        _ => Err(SqlError::unsupported(format!(
            "year month interval field: {field:?}"
        ))),
    }
}

fn from_ast_day_time_interval_field(field: &ast::DateTimeField) -> SqlResult<i32> {
    match field {
        ast::DateTimeField::Day => Ok(0),
        ast::DateTimeField::Hour => Ok(1),
        ast::DateTimeField::Minute => Ok(2),
        ast::DateTimeField::Second => Ok(3),
        _ => Err(SqlError::unsupported(format!(
            "date time interval field: {field:?}"
        ))),
    }
}

pub fn from_ast_data_type(sql_type: &ast::DataType) -> SqlResult<spec::DataType> {
    match sql_type {
        ast::DataType::Null | ast::DataType::Void => Ok(spec::DataType::Null),
        ast::DataType::Boolean | ast::DataType::Bool => Ok(spec::DataType::Boolean),
        ast::DataType::TinyInt(_) => Ok(spec::DataType::Int8),
        ast::DataType::SmallInt(_) | ast::DataType::Int16 => Ok(spec::DataType::Int16),
        ast::DataType::Int(_) | ast::DataType::Integer(_) | ast::DataType::Int32 => {
            Ok(spec::DataType::Int32)
        }
        ast::DataType::BigInt(_) | ast::DataType::Long(_) | ast::DataType::Int64 => {
            Ok(spec::DataType::Int64)
        }
        ast::DataType::UnsignedTinyInt(_) | ast::DataType::UInt8 => Ok(spec::DataType::UInt8),
        ast::DataType::UnsignedSmallInt(_) | ast::DataType::UInt16 => Ok(spec::DataType::UInt16),
        ast::DataType::UnsignedInt(_)
        | ast::DataType::UnsignedInteger(_)
        | ast::DataType::UInt32 => Ok(spec::DataType::UInt32),
        ast::DataType::UnsignedBigInt(_)
        | ast::DataType::UnsignedLong(_)
        | ast::DataType::UInt64 => Ok(spec::DataType::UInt64),
        ast::DataType::Binary(_) | ast::DataType::Bytea => Ok(spec::DataType::Binary),
        ast::DataType::Float(_) | ast::DataType::Real | ast::DataType::Float32 => {
            Ok(spec::DataType::Float32)
        }
        ast::DataType::Double | ast::DataType::DoublePrecision | ast::DataType::Float64 => {
            Ok(spec::DataType::Float64)
        }
        ast::DataType::Decimal(info) | ast::DataType::Dec(info) | ast::DataType::Numeric(info) => {
            use ast::ExactNumberInfo;

            let (precision, scale) = match *info {
                ExactNumberInfo::None => (SQL_DECIMAL_DEFAULT_PRECISION, SQL_DECIMAL_DEFAULT_SCALE),
                ExactNumberInfo::Precision(precision) => {
                    let precision = precision
                        .try_into()
                        .map_err(|_| SqlError::invalid("precision"))?;
                    (precision, SQL_DECIMAL_DEFAULT_SCALE)
                }
                ExactNumberInfo::PrecisionAndScale(precision, scale) => {
                    let precision = precision
                        .try_into()
                        .map_err(|_| SqlError::invalid("precision"))?;
                    let scale = scale.try_into().map_err(|_| SqlError::invalid("scale"))?;
                    (precision, scale)
                }
            };
            if precision > ARROW_DECIMAL128_MAX_PRECISION {
                Ok(spec::DataType::Decimal256(precision, scale))
            } else {
                Ok(spec::DataType::Decimal128(precision, scale))
            }
        }
        ast::DataType::Char(n) | ast::DataType::Character(n) => Ok(spec::DataType::ConfiguredUtf8(
            Some(from_ast_char_length(n)?),
            Some(spec::ConfiguredUtf8Type::Char),
        )),
        ast::DataType::Varchar(n)
        | ast::DataType::CharVarying(n)
        | ast::DataType::CharacterVarying(n) => Ok(spec::DataType::ConfiguredUtf8(
            Some(from_ast_char_length(n)?),
            Some(spec::ConfiguredUtf8Type::VarChar),
        )),
        ast::DataType::String(_) => Ok(spec::DataType::Utf8),
        ast::DataType::Text => Ok(spec::DataType::LargeUtf8),
        ast::DataType::Timestamp(precision, tz_info) => {
            use ast::TimezoneInfo;

            let tz = match tz_info {
                TimezoneInfo::None | TimezoneInfo::WithoutTimeZone => None,
                TimezoneInfo::WithLocalTimeZone | TimezoneInfo::WithTimeZone | TimezoneInfo::Tz => {
                    Some(Arc::<str>::from("ltz"))
                }
            };
            let precision = match precision {
                Some(0) => spec::TimeUnit::Second,
                Some(3) => spec::TimeUnit::Millisecond,
                None | Some(6) => spec::TimeUnit::Microsecond,
                Some(9) => spec::TimeUnit::Nanosecond,
                _ => Err(SqlError::invalid("from_ast_data_type timestamp precision"))?,
            };
            Ok(spec::DataType::Timestamp(precision, tz))
        }
        ast::DataType::Date | ast::DataType::Date32 => Ok(spec::DataType::Date32),
        ast::DataType::Interval(unit) => match unit {
            ast::IntervalUnit {
                leading_field: None,
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None,
            } => Ok(spec::DataType::Interval(spec::IntervalUnit::MonthDayNano)),
            ast::IntervalUnit {
                leading_field: Some(start),
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None,
            } => {
                // TODO: [CHECK HERE] BEFORE MERGING IN. If tests fail because of no start_field/end_field then need to adjust tests.
                if let Ok(_start) = from_ast_year_month_interval_field(start) {
                    Ok(spec::DataType::Interval(spec::IntervalUnit::MonthDayNano))
                    // Ok(spec::DataType::YearMonthInterval {
                    //     start_field: Some(start.try_into()?),
                    //     end_field: None,
                    // })
                } else if let Ok(_start) = from_ast_day_time_interval_field(start) {
                    // Ok(spec::DataType::YearMonthInterval {
                    //     start_field: Some(start.try_into()?),
                    //     end_field: None,
                    // })
                    Ok(spec::DataType::Duration(spec::TimeUnit::Microsecond))
                } else {
                    Err(SqlError::invalid(format!("interval start field: {unit:?}")))
                }
            }
            ast::IntervalUnit {
                leading_field: Some(start),
                leading_precision: None,
                last_field: Some(end),
                fractional_seconds_precision: None,
            } => {
                // TODO: [CHECK HERE] BEFORE MERGING IN. If tests fail because of no start_field/end_field then need to adjust tests.
                if let Ok(start) = from_ast_year_month_interval_field(start) {
                    let end = from_ast_year_month_interval_field(end)?;
                    if end <= start {
                        return Err(SqlError::invalid(format!("interval end field: {unit:?}")));
                    }
                    // Ok(spec::DataType::YearMonthInterval {
                    //     start_field: Some(start.try_into()?),
                    //     end_field: Some(end.try_into()?),
                    // })
                    Ok(spec::DataType::Interval(spec::IntervalUnit::MonthDayNano))
                } else if let Ok(start) = from_ast_day_time_interval_field(start) {
                    let end = from_ast_day_time_interval_field(end)?;
                    if end <= start {
                        return Err(SqlError::invalid(format!("interval end field: {unit:?}")));
                    }
                    // Ok(spec::DataType::DayTimeInterval {
                    //     start_field: Some(start.try_into()?),
                    //     end_field: Some(end.try_into()?),
                    // })
                    Ok(spec::DataType::Duration(spec::TimeUnit::Microsecond))
                } else {
                    return Err(SqlError::invalid(format!("interval start field: {unit:?}")));
                }
            }
            _ => Err(SqlError::invalid(format!("interval: {unit:?}"))),
        },
        ast::DataType::Array(def) => {
            use ast::ArrayElemTypeDef;

            match def {
                ArrayElemTypeDef::AngleBracket(inner) => {
                    let inner = from_ast_data_type(inner)?;
                    let field = spec::Field {
                        name: "item".to_string(),
                        data_type: inner,
                        nullable: true,
                        metadata: vec![],
                    };
                    Ok(spec::DataType::List(Arc::new(field)))
                }
                ArrayElemTypeDef::SquareBracket(_, _)
                | ArrayElemTypeDef::Parenthesis(_)
                | ArrayElemTypeDef::None => Err(SqlError::unsupported("array data type")),
            }
        }
        ast::DataType::Struct(fields, _bracket_kind) => {
            let fields = fields
                .iter()
                .map(|f| {
                    let name = match f.field_name {
                        Some(ref name) => name.value.clone(),
                        None => return Err(SqlError::invalid("missing field name")),
                    };
                    let data_type = from_ast_data_type(&f.field_type)?;
                    let mut metadata = vec![];
                    if let Some(comment) = &f.comment {
                        metadata.push(("comment".to_string(), comment.clone()));
                    };
                    Ok(spec::Field {
                        name,
                        data_type,
                        nullable: !f.not_null,
                        metadata,
                    })
                })
                .collect::<SqlResult<Vec<_>>>()?;
            Ok(spec::DataType::Struct(spec::Fields::from(fields)))
        }
        ast::DataType::Map(key, value) => {
            let key = from_ast_data_type(key)?;
            let value = from_ast_data_type(value)?;
            let fields = spec::Fields::from(vec![
                spec::Field {
                    name: "key".to_string(),
                    data_type: key,
                    nullable: false,
                    metadata: vec![],
                },
                spec::Field {
                    name: "value".to_string(),
                    data_type: value,
                    nullable: true,
                    metadata: vec![],
                },
            ]);
            let keys_are_sorted = false;
            Ok(spec::DataType::Map(
                Arc::new(spec::Field {
                    name: "entries".to_string(),
                    data_type: spec::DataType::Struct(fields),
                    nullable: false,
                    metadata: vec![],
                }),
                keys_are_sorted,
            ))
        }
        ast::DataType::Int2(_)
        | ast::DataType::Int4(_)
        | ast::DataType::Int8(_)
        | ast::DataType::Int128
        | ast::DataType::Int256
        | ast::DataType::MediumInt(_)
        | ast::DataType::UnsignedMediumInt(_)
        | ast::DataType::UnsignedInt2(_)
        | ast::DataType::UnsignedInt4(_)
        | ast::DataType::UnsignedInt8(_)
        | ast::DataType::Float4
        | ast::DataType::Float8
        | ast::DataType::Nvarchar(_)
        | ast::DataType::JSON
        | ast::DataType::Uuid
        | ast::DataType::Varbinary(_)
        | ast::DataType::Blob(_)
        | ast::DataType::Datetime(_)
        | ast::DataType::Regclass
        | ast::DataType::Enum(_)
        | ast::DataType::Set(_)
        | ast::DataType::CharacterLargeObject(_)
        | ast::DataType::CharLargeObject(_)
        | ast::DataType::Time(_, _)
        | ast::DataType::BigNumeric(_)
        | ast::DataType::BigDecimal(_)
        | ast::DataType::Clob(_)
        | ast::DataType::Bytes(_)
        | ast::DataType::JSONB
        | ast::DataType::Custom(_, _)
        | ast::DataType::Unspecified
        | ast::DataType::UInt128
        | ast::DataType::UInt256
        | ast::DataType::Datetime64(_, _)
        | ast::DataType::FixedString(_)
        | ast::DataType::Tuple(_)
        | ast::DataType::Nested(_)
        | ast::DataType::Union(_)
        | ast::DataType::Nullable(_)
        | ast::DataType::Trigger
        | ast::DataType::LowCardinality(_) => {
            Err(SqlError::unsupported(format!("SQL type {sql_type:?}")))
        }
    }
}
