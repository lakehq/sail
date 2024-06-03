use crate::error::{CommonError, CommonResult};
use crate::spec::DataType;
use arrow::datatypes::IntervalMonthDayNanoType;
use datafusion::common::scalar::ScalarStructBuilder;
use datafusion::common::ScalarValue;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum Literal {
    Null,
    Binary(Vec<u8>),
    Boolean(bool),
    Byte(i8),
    Short(i16),
    Integer(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Decimal(Decimal),
    String(String),
    Date {
        days: i32,
    },
    Timestamp {
        microseconds: i64,
    },
    TimestampNtz {
        microseconds: i64,
    },
    CalendarInterval {
        months: i32,
        days: i32,
        microseconds: i64,
    },
    YearMonthInterval {
        months: i32,
    },
    DayTimeInterval {
        microseconds: i64,
    },
    Array {
        element_type: DataType,
        elements: Vec<Literal>,
    },
    Map {
        key_type: DataType,
        value_type: DataType,
        keys: Vec<Literal>,
        values: Vec<Literal>,
    },
    Struct {
        struct_type: DataType,
        elements: Vec<Literal>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Decimal {
    pub value: i128,
    pub precision: u8,
    pub scale: i8,
}

impl Decimal {
    pub fn new(value: i128, precision: u8, scale: i8) -> Self {
        Self {
            value,
            precision,
            scale,
        }
    }
}

impl TryFrom<Literal> for ScalarValue {
    type Error = CommonError;

    fn try_from(literal: Literal) -> CommonResult<ScalarValue> {
        match literal {
            Literal::Null => Ok(ScalarValue::Null),
            Literal::Binary(x) => Ok(ScalarValue::Binary(Some(x))),
            Literal::Boolean(x) => Ok(ScalarValue::Boolean(Some(x))),
            Literal::Byte(x) => Ok(ScalarValue::Int8(Some(x))),
            Literal::Short(x) => Ok(ScalarValue::Int16(Some(x))),
            Literal::Integer(x) => Ok(ScalarValue::Int32(Some(x))),
            Literal::Long(x) => Ok(ScalarValue::Int64(Some(x))),
            Literal::Float(x) => Ok(ScalarValue::Float32(Some(x))),
            Literal::Double(x) => Ok(ScalarValue::Float64(Some(x))),
            Literal::Decimal(decimal) => {
                let Decimal {
                    value,
                    precision,
                    scale,
                } = decimal;
                Ok(ScalarValue::Decimal128(Some(value), precision, scale))
            }
            Literal::String(x) => Ok(ScalarValue::Utf8(Some(x))),
            Literal::Date { days } => Ok(ScalarValue::Date32(Some(days))),
            Literal::Timestamp { microseconds } => {
                // TODO: should we use "spark.sql.session.timeZone"?
                let timezone: Arc<str> = Arc::from("UTC");
                Ok(ScalarValue::TimestampMicrosecond(
                    Some(microseconds),
                    Some(timezone),
                ))
            }
            Literal::TimestampNtz { microseconds } => {
                Ok(ScalarValue::TimestampMicrosecond(Some(microseconds), None))
            }
            Literal::CalendarInterval {
                months,
                days,
                microseconds,
            } => {
                let nanos = microseconds
                    .checked_mul(1000)
                    .ok_or(CommonError::invalid("calendar interval microseconds"))?;
                Ok(ScalarValue::IntervalMonthDayNano(Some(
                    IntervalMonthDayNanoType::make_value(months, days, nanos),
                )))
            }
            Literal::YearMonthInterval { months } => {
                Ok(ScalarValue::IntervalYearMonth(Some(months)))
            }
            Literal::DayTimeInterval { microseconds } => {
                Ok(ScalarValue::DurationMicrosecond(Some(microseconds)))
            }
            Literal::Array {
                element_type,
                elements,
            } => {
                let element_type: arrow::datatypes::DataType = element_type.try_into()?;
                let scalars: Vec<ScalarValue> = elements
                    .into_iter()
                    .map(|literal| literal.try_into())
                    .collect::<CommonResult<Vec<_>>>()?;
                Ok(ScalarValue::List(ScalarValue::new_list_from_iter(
                    scalars.into_iter(),
                    &element_type,
                )))
            }
            Literal::Map { .. } => Err(CommonError::invalid("map literal")),
            Literal::Struct {
                struct_type,
                elements,
            } => {
                let struct_type: arrow::datatypes::DataType = struct_type.try_into()?;
                let fields = match &struct_type {
                    arrow::datatypes::DataType::Struct(fields) => fields.clone(),
                    _ => return Err(CommonError::invalid("expected struct type")),
                };

                let mut builder: ScalarStructBuilder = ScalarStructBuilder::new();
                for (literal, field) in elements.into_iter().zip(fields.into_iter()) {
                    let scalar = literal.try_into()?;
                    builder = builder.with_scalar(field, scalar);
                }
                Ok(builder.build()?)
            }
        }
    }
}
