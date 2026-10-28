use std::sync::Arc;

use chrono::{Datelike, Duration, NaiveDate, Offset, TimeZone};
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{AsArray, PrimitiveArray};
use datafusion::arrow::datatypes::{DataType, FieldRef, TimeUnit, TimestampMicrosecondType};
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, internal_err};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_functions::datetime::date_trunc::DateTruncFunc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDateTrunc {
    inner: DateTruncFunc,
}

impl Default for SparkDateTrunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDateTrunc {
    pub fn new() -> Self {
        Self {
            inner: DateTruncFunc::new(),
        }
    }
}

impl ScalarUDFImpl for SparkDateTrunc {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `TruncInstant.nullable = true`, unconditional.
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/datetimeExpressions.scala#L2287>
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let field = self.inner.return_field_from_args(args)?;
        Ok(Arc::new(field.as_ref().clone().with_nullable(true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // DataFusion truncates a value with a time zone to an hour or longer in nanoseconds, which
        // fail out of 1677-2262. Time zones have constant offsets there in the zone tables (local
        // mean time before 1677, the last offset after 2099), with which those values truncate.
        // TODO: Java applies DST rules after 2099, where the zone tables have none.
        let in_range = |value: i64| value.checked_mul(1000).is_some();
        if let [ColumnarValue::Scalar(part), value] = args.args.as_slice()
            && let Some(Some(part)) = part.try_as_str()
            && let DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) = value.data_type()
            && let Some(unit) = CoarseUnit::parse(part)
            && match value {
                ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(value, _)) => {
                    value.is_some_and(|value| !in_range(value))
                }
                ColumnarValue::Scalar(_) => false,
                ColumnarValue::Array(array) => array
                    .as_primitive::<TimestampMicrosecondType>()
                    .iter()
                    .flatten()
                    .any(|value| !in_range(value)),
            }
        {
            let tz = tz.parse::<Tz>()?;
            let array = value.to_array(1)?;
            let micros = array.as_primitive::<TimestampMicrosecondType>();
            // The rows in range truncate as before, in the time zone of the label.
            let in_range_micros = micros
                .unary_opt::<_, TimestampMicrosecondType>(|value| in_range(value).then_some(value))
                .with_timezone_opt(micros.timezone());
            let inner_args = ScalarFunctionArgs {
                args: vec![
                    args.args[0].clone(),
                    ColumnarValue::Array(Arc::new(in_range_micros)),
                ],
                ..args
            };
            let truncated = self
                .inner
                .invoke_with_args(inner_args)?
                .to_array(micros.len())?;
            let truncated = truncated.as_primitive::<TimestampMicrosecondType>();
            let result = micros
                .iter()
                .zip(truncated.iter())
                .map(|(value, truncated)| match value {
                    Some(value) if !in_range(value) => unit
                        .truncate(value, &tz)
                        .map(Some)
                        .ok_or_else(|| exec_datafusion_err!("Timestamp {value} out of range")),
                    _ => Ok(truncated),
                })
                .collect::<Result<PrimitiveArray<TimestampMicrosecondType>>>()?
                .with_timezone_opt(micros.timezone());
            return Ok(match value {
                ColumnarValue::Scalar(_) => {
                    ColumnarValue::Scalar(ScalarValue::try_from_array(&result, 0)?)
                }
                ColumnarValue::Array(_) => ColumnarValue::Array(Arc::new(result)),
            });
        }
        self.inner.invoke_with_args(args)
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        self.inner.output_ordering(input)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}

/// The units of `date_trunc` that DataFusion truncates in nanoseconds for a value with a time zone.
#[derive(Debug, Clone, Copy)]
enum CoarseUnit {
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl CoarseUnit {
    fn parse(unit: &str) -> Option<Self> {
        match unit.to_lowercase().as_str() {
            "hour" => Some(Self::Hour),
            "day" => Some(Self::Day),
            "week" => Some(Self::Week),
            "month" => Some(Self::Month),
            "quarter" => Some(Self::Quarter),
            "year" => Some(Self::Year),
            _ => None,
        }
    }

    /// Truncates `micros` in `tz`, whose offset is constant around it.
    fn truncate(self, micros: i64, tz: &Tz) -> Option<i64> {
        const MICROS_PER_HOUR: i64 = 3_600_000_000;
        const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;
        let utc = chrono::DateTime::from_timestamp_micros(micros)?.naive_utc();
        let offset =
            i64::from(tz.offset_from_utc_datetime(&utc).fix().local_minus_utc()) * 1_000_000;
        let local = micros.checked_add(offset)?;
        let truncated = match self {
            Self::Hour => local - local.rem_euclid(MICROS_PER_HOUR),
            Self::Day => local - local.rem_euclid(MICROS_PER_DAY),
            Self::Week | Self::Month | Self::Quarter | Self::Year => {
                let date = chrono::DateTime::from_timestamp_micros(local)?.date_naive();
                let date = match self {
                    Self::Week => {
                        date - Duration::days(i64::from(date.weekday().num_days_from_monday()))
                    }
                    Self::Month => date.with_day(1)?,
                    Self::Quarter => {
                        NaiveDate::from_ymd_opt(date.year(), (date.month0() / 3) * 3 + 1, 1)?
                    }
                    _ => NaiveDate::from_ymd_opt(date.year(), 1, 1)?,
                };
                date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1)?)
                    .num_days()
                    .checked_mul(MICROS_PER_DAY)?
            }
        };
        truncated.checked_sub(offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coarse_unit_truncates_out_of_nanosecond_range() {
        let utc = "UTC".parse::<Tz>().unwrap_or_else(|_| unreachable!());
        let los_angeles = "America/Los_Angeles"
            .parse::<Tz>()
            .unwrap_or_else(|_| unreachable!());
        // 1582-10-15 05:30:00 UTC
        let micros = -12_219_273_000_000_000;
        assert_eq!(
            CoarseUnit::Day.truncate(micros, &utc),
            Some(-12_219_292_800_000_000)
        );
        assert_eq!(
            CoarseUnit::Hour.truncate(micros, &utc),
            Some(-12_219_274_800_000_000)
        );
        assert_eq!(
            CoarseUnit::Month.truncate(micros, &utc),
            Some(-12_220_502_400_000_000)
        );
        // 1582-10-14 21:37:02 in local mean time (-07:52:58), whose day starts at 07:52:58 UTC.
        assert_eq!(
            CoarseUnit::Day.truncate(micros, &los_angeles),
            Some(-12_219_350_822_000_000)
        );
    }
}
