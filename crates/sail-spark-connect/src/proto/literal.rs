use sail_common::spec;
use sail_common::spec::data_type_to_null_literal;
use sail_sql_analyzer::literal::numeric::parse_decimal_string;

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::spark::connect::expression::literal::{Array, Decimal, LiteralType, Map, Struct};
use crate::spark::connect::expression::Literal;
use crate::spark::connect::{data_type as sdt, DataType};

impl TryFrom<Literal> for spec::Literal {
    type Error = SparkError;

    fn try_from(literal: Literal) -> SparkResult<spec::Literal> {
        let Literal {
            data_type,
            literal_type,
        } = literal;
        let literal_type = literal_type.required("literal type")?;
        let literal = match literal_type {
            LiteralType::Null(data_type) => {
                if data_type.kind.is_some() {
                    let data_type: spec::DataType = data_type.try_into()?;
                    data_type_to_null_literal(data_type)?
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
                precision: provided_precision,
                scale: provided_scale,
            }) => {
                let decimal = parse_decimal_string(value.as_str())?;
                if provided_precision.is_none() && provided_scale.is_none() {
                    decimal
                } else {
                    match decimal {
                        spec::Literal::Decimal128 {
                            precision,
                            scale,
                            value,
                        } => {
                            let computed_precision =
                                if let Some(provided_precision) = provided_precision {
                                    precision.max(provided_precision as u8)
                                } else {
                                    precision
                                };
                            let computed_scale = if let Some(provided_scale) = provided_scale {
                                scale.max(provided_scale as i8)
                            } else {
                                scale
                            };
                            spec::Literal::Decimal128 {
                                precision: computed_precision.max(computed_scale as u8),
                                scale: computed_scale,
                                value,
                            }
                        }
                        spec::Literal::Decimal256 {
                            precision,
                            scale,
                            value,
                        } => {
                            let computed_precision =
                                if let Some(provided_precision) = provided_precision {
                                    precision.max(provided_precision as u8)
                                } else {
                                    precision
                                };
                            let computed_scale = if let Some(provided_scale) = provided_scale {
                                scale.max(provided_scale as i8)
                            } else {
                                scale
                            };
                            spec::Literal::Decimal256 {
                                precision: computed_precision.max(computed_scale as u8),
                                scale: computed_scale,
                                value,
                            }
                        }
                        other => {
                            return Err(SparkError::invalid(format!(
                                "Unexpected Literal type for Decimal: {other:?}"
                            )))
                        }
                    }
                }
            }
            LiteralType::String(x) => spec::Literal::Utf8 { value: Some(x) },
            LiteralType::Date(x) => spec::Literal::Date32 { days: Some(x) },
            LiteralType::Timestamp(x) => spec::Literal::TimestampMicrosecond {
                microseconds: Some(x),
                timestamp_type: spec::TimestampType::WithLocalTimeZone,
            },
            LiteralType::TimestampNtz(x) => spec::Literal::TimestampMicrosecond {
                microseconds: Some(x),
                timestamp_type: spec::TimestampType::WithoutTimeZone,
            },
            LiteralType::CalendarInterval(x) => spec::Literal::IntervalMonthDayNano {
                value: Some(spec::IntervalMonthDayNano {
                    months: x.months,
                    days: x.days,
                    nanoseconds: x.microseconds * 1_000,
                }),
            },
            LiteralType::YearMonthInterval(x) => {
                spec::Literal::IntervalYearMonth { months: Some(x) }
            }
            LiteralType::DayTimeInterval(x) => spec::Literal::DurationMicrosecond {
                microseconds: Some(x),
            },
            #[expect(deprecated)]
            LiteralType::Array(Array {
                element_type,
                elements,
            }) => {
                let (element_type, nullable) = if let Some(data_type) = data_type {
                    let DataType {
                        kind: Some(sdt::Kind::Array(array)),
                    } = data_type
                    else {
                        return Err(SparkError::invalid(
                            "expected array data type for array literal",
                        ));
                    };
                    let element_type = *array.element_type.required("element type")?;
                    (element_type, array.contains_null)
                } else {
                    let element_type = element_type.required("element type")?;
                    (element_type, true)
                };
                spec::Literal::List {
                    data_type: element_type.try_into()?,
                    nullable,
                    values: Some(
                        elements
                            .into_iter()
                            .map(|x| x.try_into())
                            .collect::<SparkResult<_>>()?,
                    ),
                }
            }
            #[expect(deprecated)]
            LiteralType::Map(Map {
                key_type,
                value_type,
                keys,
                values,
            }) => {
                let (key_type, value_type, value_type_nullable) = if let Some(data_type) = data_type
                {
                    let DataType {
                        kind: Some(sdt::Kind::Map(map)),
                    } = data_type
                    else {
                        return Err(SparkError::invalid(
                            "expected map data type for map literal",
                        ));
                    };
                    let key_type = map.key_type.required("key type")?;
                    let value_type = map.value_type.required("value type")?;
                    (*key_type, *value_type, map.value_contains_null)
                } else {
                    let key_type = key_type.required("key type")?;
                    let value_type = value_type.required("value type")?;
                    (key_type, value_type, true)
                };
                spec::Literal::Map {
                    key_type: key_type.try_into()?,
                    value_type: value_type.try_into()?,
                    value_type_nullable,
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
            #[expect(deprecated)]
            LiteralType::Struct(Struct {
                struct_type,
                elements,
            }) => {
                let struct_type = data_type.or(struct_type).required("struct type")?;
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
            LiteralType::SpecializedArray(_) => {
                return Err(SparkError::todo("specialized array literal"))
            }
            LiteralType::Time(_) => return Err(SparkError::todo("time literal")),
        };
        Ok(literal)
    }
}
