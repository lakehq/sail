use sail_common::spec::{self, data_type_to_null_literal, i256};
use sail_sql_analyzer::literal::numeric::parse_decimal_string;

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::spark::connect::expression::literal::{Array, Decimal, LiteralType, Map, Struct, Time};
use crate::spark::connect::expression::Literal;
use crate::spark::connect::{data_type as sdt, DataType};

fn decimal_rescale_error() -> SparkError {
    SparkError::invalid("decimal rescale overflow")
}

fn pow10_i128(scale: u8) -> SparkResult<i128> {
    10_i128
        .checked_pow(scale as u32)
        .ok_or_else(decimal_rescale_error)
}

fn pow10_i256(scale: u8) -> SparkResult<i256> {
    let mut value = i256::ONE;
    for _ in 0..scale {
        value = value
            .checked_mul(i256::from(10))
            .ok_or_else(decimal_rescale_error)?;
    }
    Ok(value)
}

fn ceil_div_i128(value: i128, divisor: i128) -> SparkResult<i128> {
    let quotient = value
        .checked_div(divisor)
        .ok_or_else(decimal_rescale_error)?;
    let remainder = value
        .checked_rem(divisor)
        .ok_or_else(decimal_rescale_error)?;
    if value > 0 && remainder != 0 {
        quotient.checked_add(1).ok_or_else(decimal_rescale_error)
    } else {
        Ok(quotient)
    }
}

fn ceil_div_i256(value: i256, divisor: i256) -> SparkResult<i256> {
    let quotient = value
        .checked_div(divisor)
        .ok_or_else(decimal_rescale_error)?;
    let remainder = value
        .checked_rem(divisor)
        .ok_or_else(decimal_rescale_error)?;
    if value > i256::ZERO && remainder != i256::ZERO {
        quotient
            .checked_add(i256::ONE)
            .ok_or_else(decimal_rescale_error)
    } else {
        Ok(quotient)
    }
}

fn rescale_decimal128(
    value: Option<i128>,
    scale: i8,
    target_scale: i8,
) -> SparkResult<Option<i128>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let scale_diff = i16::from(target_scale) - i16::from(scale);
    match scale_diff.cmp(&0) {
        std::cmp::Ordering::Greater => {
            let factor = pow10_i128(scale_diff as u8)?;
            value
                .checked_mul(factor)
                .map(Some)
                .ok_or_else(decimal_rescale_error)
        }
        std::cmp::Ordering::Equal => Ok(Some(value)),
        std::cmp::Ordering::Less => {
            let factor = pow10_i128((-scale_diff) as u8)?;
            ceil_div_i128(value, factor).map(Some)
        }
    }
}

fn rescale_decimal256(
    value: Option<i256>,
    scale: i8,
    target_scale: i8,
) -> SparkResult<Option<i256>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let scale_diff = i16::from(target_scale) - i16::from(scale);
    match scale_diff.cmp(&0) {
        std::cmp::Ordering::Greater => {
            let factor = pow10_i256(scale_diff as u8)?;
            value
                .checked_mul(factor)
                .map(Some)
                .ok_or_else(decimal_rescale_error)
        }
        std::cmp::Ordering::Equal => Ok(Some(value)),
        std::cmp::Ordering::Less => {
            let factor = pow10_i256((-scale_diff) as u8)?;
            ceil_div_i256(value, factor).map(Some)
        }
    }
}

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
                                provided_scale
                                    .try_into()
                                    .map_err(|_| SparkError::invalid("decimal scale"))?
                            } else {
                                scale
                            };
                            spec::Literal::Decimal128 {
                                precision: computed_precision.max(computed_scale.unsigned_abs()),
                                scale: computed_scale,
                                value: rescale_decimal128(value, scale, computed_scale)?,
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
                                provided_scale
                                    .try_into()
                                    .map_err(|_| SparkError::invalid("decimal scale"))?
                            } else {
                                scale
                            };
                            spec::Literal::Decimal256 {
                                precision: computed_precision.max(computed_scale.unsigned_abs()),
                                scale: computed_scale,
                                value: rescale_decimal256(value, scale, computed_scale)?,
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
            LiteralType::YearMonthInterval(x) => spec::Literal::IntervalYearMonth {
                months: Some(x),
                start_field: None,
                end_field: None,
            },
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
            LiteralType::Time(Time { nano, precision }) => {
                // Spark TIME literals carry nanoseconds since midnight.
                // Precision values: 0 = seconds, 3 = milliseconds, 6 = microseconds (default), 9 = nanoseconds.
                match precision.unwrap_or(6) {
                    0 => spec::Literal::Time32Second {
                        seconds: Some((nano / 1_000_000_000) as i32),
                    },
                    3 => spec::Literal::Time32Millisecond {
                        milliseconds: Some((nano / 1_000_000) as i32),
                    },
                    6 => spec::Literal::Time64Microsecond {
                        microseconds: Some(nano / 1_000),
                    },
                    9 => spec::Literal::Time64Nanosecond {
                        nanoseconds: Some(nano),
                    },
                    p => return Err(SparkError::invalid(format!("invalid TIME precision: {p}"))),
                }
            }
        };
        Ok(literal)
    }
}
