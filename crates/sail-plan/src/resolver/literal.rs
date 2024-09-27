use arrow::datatypes::{IntervalDayTime, IntervalMonthDayNanoType};
use datafusion_common::scalar::ScalarStructBuilder;
use datafusion_common::ScalarValue;
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub fn resolve_literal(&self, literal: spec::Literal) -> PlanResult<ScalarValue> {
        use spec::{Decimal128, Decimal256, Literal};

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
            Literal::Decimal128(decimal128) => {
                let Decimal128 {
                    value,
                    precision,
                    scale,
                } = decimal128;
                Ok(ScalarValue::Decimal128(Some(value), precision, scale))
            }
            Literal::Decimal256(decimal256) => {
                let Decimal256 {
                    value,
                    precision,
                    scale,
                } = decimal256;
                Ok(ScalarValue::Decimal256(Some(value), precision, scale))
            }
            Literal::String(x) => Ok(ScalarValue::Utf8(Some(x))),
            Literal::Date { days } => Ok(ScalarValue::Date32(Some(days))),
            Literal::TimestampMicrosecond {
                microseconds,
                timezone,
            } => Ok(ScalarValue::TimestampMicrosecond(
                Some(microseconds),
                self.resolve_timezone(timezone)?,
            )),
            Literal::TimestampNtz { microseconds } => {
                Ok(ScalarValue::TimestampMicrosecond(Some(microseconds), None))
            }
            Literal::CalendarInterval {
                months,
                days,
                microseconds,
            } => {
                let nanoseconds = microseconds
                    .checked_mul(1000)
                    .ok_or(PlanError::invalid("calendar interval microseconds"))?;
                Ok(ScalarValue::IntervalMonthDayNano(Some(
                    IntervalMonthDayNanoType::make_value(months, days, nanoseconds),
                )))
            }
            Literal::YearMonthInterval { months } => {
                Ok(ScalarValue::IntervalYearMonth(Some(months)))
            }
            // TODO: add tests for negative values etc.
            Literal::DayTimeInterval { microseconds } => {
                let days = microseconds / (24 * 60 * 60 * 1_000_000);
                let microseconds = microseconds % (24 * 60 * 60 * 1_000_000);
                if microseconds % 1_000 == 0 {
                    let milliseconds = microseconds / 1_000;
                    Ok(ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(
                        days as i32,
                        milliseconds as i32,
                    ))))
                } else {
                    let nanoseconds = microseconds * 1_000;
                    Ok(ScalarValue::IntervalMonthDayNano(Some(
                        IntervalMonthDayNanoType::make_value(0, days as i32, nanoseconds),
                    )))
                }
            }
            Literal::Array {
                element_type,
                elements,
            } => {
                let element_type = self.resolve_data_type(element_type)?;
                let scalars: Vec<ScalarValue> = elements
                    .into_iter()
                    .map(|literal| self.resolve_literal(literal))
                    .collect::<PlanResult<Vec<_>>>()?;
                Ok(ScalarValue::List(ScalarValue::new_list_from_iter(
                    scalars.into_iter(),
                    &element_type,
                    true,
                )))
            }
            Literal::Map { .. } => Err(PlanError::invalid("map literal")),
            Literal::Struct {
                struct_type,
                elements,
            } => {
                let struct_type = self.resolve_data_type(struct_type)?;
                let fields = match &struct_type {
                    arrow::datatypes::DataType::Struct(fields) => fields.clone(),
                    _ => return Err(PlanError::invalid("expected struct type")),
                };

                let mut builder: ScalarStructBuilder = ScalarStructBuilder::new();
                for (literal, field) in elements.into_iter().zip(fields.into_iter()) {
                    let scalar = self.resolve_literal(literal)?;
                    builder = builder.with_scalar(field, scalar);
                }
                Ok(builder.build()?)
            }
        }
    }
}
