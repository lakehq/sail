use std::sync::Arc;

use arrow::datatypes::IntervalMonthDayNanoType;
use datafusion_common::scalar::ScalarStructBuilder;
use datafusion_common::ScalarValue;
use framework_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub fn resolve_literal(&self, literal: spec::Literal) -> PlanResult<ScalarValue> {
        use spec::{Decimal, Literal};

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
                let timezone: Arc<str> = Arc::from(self.config.time_zone.as_str());
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
                    .ok_or(PlanError::invalid("calendar interval microseconds"))?;
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
                let element_type = self.resolve_data_type(element_type)?;
                let scalars: Vec<ScalarValue> = elements
                    .into_iter()
                    .map(|literal| self.resolve_literal(literal))
                    .collect::<PlanResult<Vec<_>>>()?;
                Ok(ScalarValue::List(ScalarValue::new_list_from_iter(
                    scalars.into_iter(),
                    &element_type,
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
