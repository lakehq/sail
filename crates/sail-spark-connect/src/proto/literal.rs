use sail_common::spec;
use sail_sql::literal::parse_decimal_string;

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::spark::connect::expression::literal::{Array, Decimal, LiteralType, Map, Struct};
use crate::spark::connect::expression::Literal;

impl TryFrom<Literal> for spec::Literal {
    type Error = SparkError;

    fn try_from(literal: Literal) -> SparkResult<spec::Literal> {
        let Literal { literal_type } = literal;
        let literal_type = literal_type.required("literal type")?;
        let literal = match literal_type {
            LiteralType::Null(_) => spec::Literal::Null,
            LiteralType::Binary(x) => spec::Literal::Binary(x),
            LiteralType::Boolean(x) => spec::Literal::Boolean(x),
            LiteralType::Byte(x) => spec::Literal::Byte(x as i8),
            LiteralType::Short(x) => spec::Literal::Short(x as i16),
            LiteralType::Integer(x) => spec::Literal::Integer(x),
            LiteralType::Long(x) => spec::Literal::Long(x),
            LiteralType::Float(x) => spec::Literal::Float(x),
            LiteralType::Double(x) => spec::Literal::Double(x),
            LiteralType::Decimal(Decimal {
                value,
                precision,
                scale,
            }) => {
                if precision.is_some() || scale.is_some() {
                    return Err(SparkError::todo("decimal literal with precision or scale"));
                }
                let decimal = parse_decimal_string(value.as_str())?;
                spec::Literal::Decimal(decimal)
            }
            LiteralType::String(x) => spec::Literal::String(x),
            LiteralType::Date(x) => spec::Literal::Date { days: x },
            LiteralType::Timestamp(x) => spec::Literal::Timestamp { microseconds: x },
            LiteralType::TimestampNtz(x) => spec::Literal::TimestampNtz { microseconds: x },
            LiteralType::CalendarInterval(x) => spec::Literal::CalendarInterval {
                months: x.months,
                days: x.days,
                microseconds: x.microseconds,
            },
            LiteralType::YearMonthInterval(x) => spec::Literal::YearMonthInterval { months: x },
            LiteralType::DayTimeInterval(x) => spec::Literal::DayTimeInterval { microseconds: x },
            LiteralType::Array(Array {
                element_type,
                elements,
            }) => {
                let element_type = element_type.required("element type")?;
                spec::Literal::Array {
                    element_type: element_type.try_into()?,
                    elements: elements
                        .into_iter()
                        .map(|x| x.try_into())
                        .collect::<SparkResult<_>>()?,
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
                    keys: keys
                        .into_iter()
                        .map(|x| x.try_into())
                        .collect::<SparkResult<_>>()?,
                    values: values
                        .into_iter()
                        .map(|x| x.try_into())
                        .collect::<SparkResult<_>>()?,
                }
            }
            LiteralType::Struct(Struct {
                struct_type,
                elements,
            }) => {
                let struct_type = struct_type.required("struct type")?;
                spec::Literal::Struct {
                    struct_type: struct_type.try_into()?,
                    elements: elements
                        .into_iter()
                        .map(|x| x.try_into())
                        .collect::<SparkResult<_>>()?,
                }
            }
        };
        Ok(literal)
    }
}
