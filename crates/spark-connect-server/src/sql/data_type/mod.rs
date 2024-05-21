use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::schema::{from_spark_field, from_spark_fields};
use crate::spark::connect as sc;
use crate::sql::fail_on_extra_token;
use crate::sql::parser::SparkDialect;
use arrow::datatypes as adt;
use arrow::datatypes::{Fields, SchemaRef};
use sc::data_type as sdt;
use sqlparser::ast;
use sqlparser::ast::DateTimeField;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) mod json;

const DEFAULT_FIELD_NAME: &str = "value";

pub(crate) const SPARK_DECIMAL_MAX_PRECISION: i32 = 38;
pub(crate) const SPARK_DECIMAL_MAX_SCALE: i32 = 38;
pub(crate) const SPARK_DECIMAL_USER_DEFAULT_PRECISION: i32 = 10;
pub(crate) const SPARK_DECIMAL_USER_DEFAULT_SCALE: i32 = 0;
#[allow(dead_code)]
pub(crate) const SPARK_DECIMAL_SYSTEM_DEFAULT_PRECISION: i32 = 38;
#[allow(dead_code)]
pub(crate) const SPARK_DECIMAL_SYSTEM_DEFAULT_SCALE: i32 = 18;

pub(crate) fn parse_spark_schema(schema: &str) -> SparkResult<SchemaRef> {
    let data_type = if let Ok(dt) = parse_spark_data_type(schema) {
        dt
    } else if let Ok(dt) = parse_spark_data_type(format!("struct<{schema}>").as_str()) {
        dt
    } else {
        json::parse_spark_json_data_type(schema)?
    };
    let sc::DataType { ref kind } = data_type;
    let kind = kind.as_ref().required("data type kind")?;
    let fields = match kind {
        sdt::Kind::Struct(r#struct) => from_spark_fields(&r#struct.fields)?,
        _ => Fields::from(vec![from_spark_field(
            DEFAULT_FIELD_NAME,
            &data_type,
            true,
            None,
        )?]),
    };
    Ok(Arc::new(adt::Schema::new(fields)))
}

pub(crate) fn parse_spark_data_type(sql: &str) -> SparkResult<sc::DataType> {
    let mut parser = Parser::new(&SparkDialect {}).try_with_sql(sql)?;
    if parser.peek_token() == Token::EOF {
        return Err(SparkError::invalid("empty data type"));
    }
    let data_type = parser.parse_data_type()?;
    fail_on_extra_token(&mut parser, "data type")?;
    Ok(from_ast_data_type(&data_type)?)
}

fn from_ast_char_length(length: &Option<ast::CharacterLength>) -> SparkResult<i32> {
    let length = length.required("char length")?;
    match length {
        ast::CharacterLength::IntegerLength { length, unit } => {
            if unit.is_some() {
                return Err(SparkError::unsupported("char length unit"));
            }
            // check if length can be converted to i32
            if length <= 0 || length > i32::MAX as u64 {
                return Err(SparkError::invalid("char length"));
            }
            Ok(length as i32)
        }
        ast::CharacterLength::Max => return Err(SparkError::unsupported("char length max")),
    }
}

fn from_ast_year_month_interval_field(field: &ast::DateTimeField) -> SparkResult<i32> {
    match field {
        DateTimeField::Year => Ok(0),
        DateTimeField::Month => Ok(1),
        _ => {
            return Err(SparkError::unsupported(format!(
                "year month interval field: {field:?}"
            )))
        }
    }
}

fn from_ast_date_time_interval_field(field: &ast::DateTimeField) -> SparkResult<i32> {
    match field {
        DateTimeField::Day => Ok(0),
        DateTimeField::Hour => Ok(1),
        DateTimeField::Minute => Ok(2),
        DateTimeField::Second => Ok(3),
        _ => {
            return Err(SparkError::unsupported(format!(
                "date time interval field: {field:?}"
            )))
        }
    }
}

pub(crate) fn from_ast_data_type(sql_type: &ast::DataType) -> SparkResult<sc::DataType> {
    use sc::data_type::Kind;

    match sql_type {
        ast::DataType::Null | ast::DataType::Void => Ok(sc::DataType {
            kind: Some(Kind::Null(sdt::Null::default())),
        }),
        ast::DataType::Boolean | ast::DataType::Bool => Ok(sc::DataType {
            kind: Some(Kind::Boolean(sdt::Boolean::default())),
        }),
        ast::DataType::TinyInt(_) => Ok(sc::DataType {
            kind: Some(Kind::Byte(sdt::Byte::default())),
        }),
        ast::DataType::SmallInt(_) => Ok(sc::DataType {
            kind: Some(Kind::Short(sdt::Short::default())),
        }),
        ast::DataType::Int(_) | ast::DataType::Integer(_) => Ok(sc::DataType {
            kind: Some(Kind::Integer(sdt::Integer::default())),
        }),
        ast::DataType::BigInt(_) | ast::DataType::Long(_) => Ok(sc::DataType {
            kind: Some(Kind::Long(sdt::Long::default())),
        }),
        ast::DataType::Binary(_) | ast::DataType::Bytea => Ok(sc::DataType {
            kind: Some(Kind::Binary(sdt::Binary::default())),
        }),
        ast::DataType::Float(_) | ast::DataType::Real => Ok(sc::DataType {
            kind: Some(Kind::Float(sdt::Float::default())),
        }),
        ast::DataType::Double | ast::DataType::DoublePrecision => Ok(sc::DataType {
            kind: Some(Kind::Double(sdt::Double::default())),
        }),
        ast::DataType::Decimal(info) | ast::DataType::Dec(info) | ast::DataType::Numeric(info) => {
            use ast::ExactNumberInfo;

            let (precision, scale) = match *info {
                ExactNumberInfo::None => (
                    Some(SPARK_DECIMAL_USER_DEFAULT_PRECISION),
                    Some(SPARK_DECIMAL_USER_DEFAULT_SCALE),
                ),
                ExactNumberInfo::Precision(precision) => (
                    Some(precision as i32),
                    Some(SPARK_DECIMAL_USER_DEFAULT_SCALE),
                ),
                ExactNumberInfo::PrecisionAndScale(precision, scale) => {
                    (Some(precision as i32), Some(scale as i32))
                }
            };
            Ok(sc::DataType {
                kind: Some(Kind::Decimal(sdt::Decimal {
                    precision,
                    scale,
                    type_variation_reference: 0,
                })),
            })
        }
        ast::DataType::Char(n) | ast::DataType::Character(n) => Ok(sc::DataType {
            kind: Some(Kind::Char(sdt::Char {
                length: from_ast_char_length(n)?,
                type_variation_reference: 0,
            })),
        }),
        ast::DataType::Varchar(n)
        | ast::DataType::CharVarying(n)
        | ast::DataType::CharacterVarying(n) => Ok(sc::DataType {
            kind: Some(Kind::VarChar(sdt::VarChar {
                length: from_ast_char_length(n)?,
                type_variation_reference: 0,
            })),
        }),
        ast::DataType::String(_) | ast::DataType::Text => Ok(sc::DataType {
            kind: Some(Kind::String(sdt::String::default())),
        }),
        ast::DataType::Timestamp(None, tz_info) => {
            use ast::TimezoneInfo;

            match tz_info {
                // FIXME: `timestamp` can either be `timestamp_ltz` (default) or `timestamp_ntz`,
                // We need to consider the `spark.sql.timestampType` configuration.
                TimezoneInfo::None => Ok(sc::DataType {
                    kind: Some(Kind::Timestamp(sdt::Timestamp::default())),
                }),
                TimezoneInfo::WithoutTimeZone => Ok(sc::DataType {
                    kind: Some(Kind::TimestampNtz(sdt::TimestampNtz::default())),
                }),
                TimezoneInfo::WithLocalTimeZone => Ok(sc::DataType {
                    kind: Some(Kind::Timestamp(sdt::Timestamp::default())),
                }),
                TimezoneInfo::WithTimeZone | TimezoneInfo::Tz => {
                    Err(SparkError::unsupported("timestamp with time zone"))
                }
            }
        }
        ast::DataType::Date => Ok(sc::DataType {
            kind: Some(Kind::Date(sdt::Date::default())),
        }),
        ast::DataType::Interval(unit) => match unit {
            ast::IntervalUnit {
                leading_field: None,
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None,
            } => Ok(sc::DataType {
                kind: Some(Kind::CalendarInterval(sdt::CalendarInterval::default())),
            }),
            ast::IntervalUnit {
                leading_field: Some(start),
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None,
            } => {
                if let Ok(start) = from_ast_year_month_interval_field(start) {
                    Ok(sc::DataType {
                        kind: Some(Kind::YearMonthInterval(sdt::YearMonthInterval {
                            start_field: Some(start),
                            end_field: None,
                            type_variation_reference: 0,
                        })),
                    })
                } else if let Ok(start) = from_ast_date_time_interval_field(start) {
                    Ok(sc::DataType {
                        kind: Some(Kind::DayTimeInterval(sdt::DayTimeInterval {
                            start_field: Some(start),
                            end_field: None,
                            type_variation_reference: 0,
                        })),
                    })
                } else {
                    Err(SparkError::invalid(format!(
                        "interval start field: {unit:?}"
                    )))
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
                        return Err(SparkError::invalid(format!("interval end field: {unit:?}")));
                    }
                    Ok(sc::DataType {
                        kind: Some(Kind::YearMonthInterval(sdt::YearMonthInterval {
                            start_field: Some(start),
                            end_field: Some(end),
                            type_variation_reference: 0,
                        })),
                    })
                } else if let Ok(start) = from_ast_date_time_interval_field(start) {
                    let end = from_ast_date_time_interval_field(end)?;
                    if end <= start {
                        return Err(SparkError::invalid(format!("interval end field: {unit:?}")));
                    }
                    Ok(sc::DataType {
                        kind: Some(Kind::DayTimeInterval(sdt::DayTimeInterval {
                            start_field: Some(start),
                            end_field: Some(end),
                            type_variation_reference: 0,
                        })),
                    })
                } else {
                    return Err(SparkError::invalid(format!(
                        "interval start field: {unit:?}"
                    )));
                }
            }
            _ => Err(SparkError::invalid(format!("interval: {unit:?}"))),
        },
        ast::DataType::Array(def) => {
            use ast::ArrayElemTypeDef;

            match def {
                ArrayElemTypeDef::AngleBracket(inner) => {
                    let inner = from_ast_data_type(inner)?;
                    Ok(sc::DataType {
                        kind: Some(Kind::Array(Box::new(sdt::Array {
                            element_type: Some(Box::new(inner)),
                            contains_null: true,
                            type_variation_reference: 0,
                        }))),
                    })
                }
                ArrayElemTypeDef::SquareBracket(_, _) | ArrayElemTypeDef::None => {
                    Err(SparkError::unsupported("array data type"))
                }
            }
        }
        ast::DataType::Struct(fields) => {
            let fields = fields
                .iter()
                .map(|f| {
                    let name = match f.field_name {
                        Some(ref name) => name.value.clone(),
                        None => return Err(SparkError::invalid("missing field name")),
                    };
                    let data_type = from_ast_data_type(&f.field_type)?;
                    let metadata = if let Some(comment) = &f.comment {
                        let mut metadata = HashMap::new();
                        metadata.insert("comment".to_string(), comment.clone());
                        Some(serde_json::to_string(&metadata)?)
                    } else {
                        None
                    };
                    Ok(sdt::StructField {
                        name,
                        data_type: Some(data_type),
                        nullable: !f.not_null,
                        metadata,
                    })
                })
                .collect::<SparkResult<Vec<_>>>()?;
            Ok(sc::DataType {
                kind: Some(Kind::Struct(sdt::Struct {
                    fields,
                    type_variation_reference: 0,
                })),
            })
        }
        ast::DataType::Map(key, value) => {
            let key = from_ast_data_type(key)?;
            let value = from_ast_data_type(value)?;
            Ok(sc::DataType {
                kind: Some(Kind::Map(Box::new(sdt::Map {
                    key_type: Some(Box::new(key)),
                    value_type: Some(Box::new(value)),
                    value_contains_null: true,
                    type_variation_reference: 0,
                }))),
            })
        }
        ast::DataType::Int2(_)
        | ast::DataType::Int4(_)
        | ast::DataType::Int8(_)
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
        | ast::DataType::Int64
        | ast::DataType::Float64
        | ast::DataType::JSONB
        | ast::DataType::Custom(_, _)
        | ast::DataType::Unspecified => {
            Err(SparkError::unsupported(format!("SQL type {sql_type:?}")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_spark_data_type, parse_spark_schema};
    use crate::tests::test_gold_set;

    #[test]
    fn test_parse_spark_data_type_gold_set() -> Result<(), Box<dyn std::error::Error>> {
        Ok(test_gold_set(
            "tests/gold_data/data_type.json",
            |s: String| Ok(parse_spark_data_type(&s)?),
        )?)
    }

    #[test]
    fn test_parse_spark_schema_gold_set() -> Result<(), Box<dyn std::error::Error>> {
        Ok(test_gold_set(
            "tests/gold_data/table_schema.json",
            |s: String| Ok(parse_spark_schema(&s)?),
        )?)
    }
}
