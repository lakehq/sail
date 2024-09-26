use std::sync::Arc;

use datafusion::arrow::datatypes::DECIMAL128_MAX_PRECISION as ARROW_DECIMAL128_MAX_PRECISION;
use sail_common::spec;
use sqlparser::ast;
use sqlparser::ast::DateTimeField;
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
        DateTimeField::Year => Ok(0),
        DateTimeField::Month => Ok(1),
        _ => Err(SqlError::unsupported(format!(
            "year month interval field: {field:?}"
        ))),
    }
}

fn from_ast_date_time_interval_field(field: &ast::DateTimeField) -> SqlResult<i32> {
    match field {
        DateTimeField::Day => Ok(0),
        DateTimeField::Hour => Ok(1),
        DateTimeField::Minute => Ok(2),
        DateTimeField::Second => Ok(3),
        _ => Err(SqlError::unsupported(format!(
            "date time interval field: {field:?}"
        ))),
    }
}

pub fn from_ast_data_type(sql_type: &ast::DataType) -> SqlResult<spec::DataType> {
    match sql_type {
        ast::DataType::Null | ast::DataType::Void => Ok(spec::DataType::Null),
        ast::DataType::Boolean | ast::DataType::Bool => Ok(spec::DataType::Boolean),
        ast::DataType::TinyInt(_) => Ok(spec::DataType::Byte),
        ast::DataType::SmallInt(_) => Ok(spec::DataType::Short),
        ast::DataType::Int(_) | ast::DataType::Integer(_) => Ok(spec::DataType::Integer),
        ast::DataType::BigInt(_) | ast::DataType::Long(_) => Ok(spec::DataType::Long),
        ast::DataType::Binary(_) | ast::DataType::Bytea => Ok(spec::DataType::Binary),
        ast::DataType::Float(_) | ast::DataType::Real => Ok(spec::DataType::Float),
        ast::DataType::Double | ast::DataType::DoublePrecision => Ok(spec::DataType::Double),
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
                Ok(spec::DataType::Decimal256 { precision, scale })
            } else {
                Ok(spec::DataType::Decimal128 { precision, scale })
            }
        }
        ast::DataType::Char(n) | ast::DataType::Character(n) => Ok(spec::DataType::Char {
            length: from_ast_char_length(n)?,
        }),
        ast::DataType::Varchar(n)
        | ast::DataType::CharVarying(n)
        | ast::DataType::CharacterVarying(n) => Ok(spec::DataType::VarChar {
            length: from_ast_char_length(n)?,
        }),
        ast::DataType::String(_) | ast::DataType::Text => Ok(spec::DataType::String),
        ast::DataType::Timestamp(None, tz_info) => {
            use ast::TimezoneInfo;

            match tz_info {
                // FIXME: `timestamp` can either be `timestamp_ltz` (default) or `timestamp_ntz`,
                //  We need to consider the `spark.sql.timestampType` configuration.
                TimezoneInfo::WithoutTimeZone => Ok(spec::DataType::TimestampNtz),
                TimezoneInfo::None => Ok(spec::DataType::Timestamp(
                    Some(spec::TimeUnit::Microsecond),
                    None,
                )),
                TimezoneInfo::WithLocalTimeZone => Ok(spec::DataType::Timestamp(
                    Some(spec::TimeUnit::Microsecond),
                    Some(Arc::<str>::from("ltz")),
                )),
                TimezoneInfo::WithTimeZone | TimezoneInfo::Tz => {
                    Err(SqlError::unsupported("timestamp with time zone"))
                }
            }
        }
        ast::DataType::Date => Ok(spec::DataType::Date),
        ast::DataType::Interval(unit) => match unit {
            ast::IntervalUnit {
                leading_field: None,
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None,
            } => Ok(spec::DataType::CalendarInterval),
            ast::IntervalUnit {
                leading_field: Some(start),
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None,
            } => {
                if let Ok(start) = from_ast_year_month_interval_field(start) {
                    Ok(spec::DataType::YearMonthInterval {
                        start_field: Some(start.try_into()?),
                        end_field: None,
                    })
                } else if let Ok(start) = from_ast_date_time_interval_field(start) {
                    Ok(spec::DataType::YearMonthInterval {
                        start_field: Some(start.try_into()?),
                        end_field: None,
                    })
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
                if let Ok(start) = from_ast_year_month_interval_field(start) {
                    let end = from_ast_year_month_interval_field(end)?;
                    if end <= start {
                        return Err(SqlError::invalid(format!("interval end field: {unit:?}")));
                    }
                    Ok(spec::DataType::YearMonthInterval {
                        start_field: Some(start.try_into()?),
                        end_field: Some(end.try_into()?),
                    })
                } else if let Ok(start) = from_ast_date_time_interval_field(start) {
                    let end = from_ast_date_time_interval_field(end)?;
                    if end <= start {
                        return Err(SqlError::invalid(format!("interval end field: {unit:?}")));
                    }
                    Ok(spec::DataType::DayTimeInterval {
                        start_field: Some(start.try_into()?),
                        end_field: Some(end.try_into()?),
                    })
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
                    Ok(spec::DataType::Array {
                        element_type: Box::new(inner),
                        contains_null: true,
                    })
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
            Ok(spec::DataType::Struct {
                fields: spec::Fields::new(fields),
            })
        }
        ast::DataType::Map(key, value) => {
            let key = from_ast_data_type(key)?;
            let value = from_ast_data_type(value)?;
            Ok(spec::DataType::Map {
                key_type: Box::new(key),
                value_type: Box::new(value),
                value_contains_null: true,
            })
        }
        ast::DataType::Int2(_)
        | ast::DataType::Int4(_)
        | ast::DataType::Int8(_)
        | ast::DataType::Int16
        | ast::DataType::Int32
        | ast::DataType::Int64
        | ast::DataType::Int128
        | ast::DataType::Int256
        | ast::DataType::MediumInt(_)
        | ast::DataType::UnsignedTinyInt(_)
        | ast::DataType::UnsignedSmallInt(_)
        | ast::DataType::UnsignedMediumInt(_)
        | ast::DataType::UnsignedInt2(_)
        | ast::DataType::UnsignedInt(_)
        | ast::DataType::UnsignedInteger(_)
        | ast::DataType::UnsignedLong(_)
        | ast::DataType::UnsignedInt4(_)
        | ast::DataType::UnsignedBigInt(_)
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
        | ast::DataType::Timestamp(Some(_), _)
        | ast::DataType::Time(_, _)
        | ast::DataType::BigNumeric(_)
        | ast::DataType::BigDecimal(_)
        | ast::DataType::Clob(_)
        | ast::DataType::Bytes(_)
        | ast::DataType::Float64
        | ast::DataType::JSONB
        | ast::DataType::Custom(_, _)
        | ast::DataType::Unspecified
        | ast::DataType::UInt8
        | ast::DataType::UInt16
        | ast::DataType::UInt32
        | ast::DataType::UInt64
        | ast::DataType::UInt128
        | ast::DataType::UInt256
        | ast::DataType::Float32
        | ast::DataType::Date32
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
