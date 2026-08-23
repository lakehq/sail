use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, DurationMicrosecondType, IntervalMonthDayNano, IntervalUnit,
    IntervalYearMonthType, TimeUnit,
};
use datafusion_common::arrow::array::{AsArray, PrimitiveArray};
use datafusion_common::arrow::datatypes::IntervalMonthDayNanoType;
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::types::logical_string;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_sql_analyzer::literal::interval::{IntervalValue, parse_calendar_interval_string};
use sail_sql_analyzer::parser::parse_interval;

use crate::functions_utils::StrMemo;

/// Parses interval strings with per-batch memoization of distinct values.
///
/// `parse_interval` builds the full recursive SQL expression parser on every
/// call, which costs on the order of a millisecond and a nontrivial amount of
/// memory per call. Per-row interval columns (e.g. a dynamic `session_window`
/// gap fed by a `CASE` expression) typically hold only a few distinct strings,
/// so parsing each distinct value once per batch is the difference between a
/// query finishing and it exhausting all memory.
fn parse_memoized<'a, P, F>(
    values: impl Iterator<Item = Option<&'a str>>,
    parse: F,
) -> Result<PrimitiveArray<P>>
where
    P: ArrowPrimitiveType,
    F: Fn(&str) -> Result<P::Native>,
{
    let mut memo: StrMemo<'a, P::Native> = StrMemo::new();
    values
        .map(|value| {
            value
                .map(|s| memo.get_or_try_insert_ref(s, &parse).copied())
                .transpose()
        })
        .collect()
}

macro_rules! define_interval_udf {
    ($udf:ident, $name:expr_2021, $return_type:expr_2021, $primitive_type:ty, $func:expr_2021, $scalar:expr_2021 $(,)?) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        pub struct $udf {
            signature: Signature,
        }

        impl Default for $udf {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $udf {
            pub fn new() -> Self {
                Self {
                    signature: Signature::coercible(
                        vec![Coercion::new_exact(TypeSignatureClass::Native(
                            logical_string(),
                        ))],
                        Volatility::Immutable,
                    ),
                }
            }
        }

        impl ScalarUDFImpl for $udf {
            fn name(&self) -> &str {
                $name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok($return_type)
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                let ScalarFunctionArgs { args, .. } = args;
                let arg = args.one()?;
                match arg {
                    ColumnarValue::Array(array) => {
                        let array: PrimitiveArray<$primitive_type> = match array.data_type() {
                            DataType::Utf8 => {
                                parse_memoized(as_string_array(&array)?.iter(), $func)?
                            }
                            DataType::LargeUtf8 => {
                                parse_memoized(as_large_string_array(&array)?.iter(), $func)?
                            }
                            DataType::Utf8View => {
                                parse_memoized(as_string_view_array(&array)?.iter(), $func)?
                            }
                            _ => return exec_err!("expected string array for intervals"),
                        };
                        Ok(ColumnarValue::Array(Arc::new(array)))
                    }
                    ColumnarValue::Scalar(scalar) => {
                        let value = match scalar.try_as_str() {
                            Some(x) => x.map(|x| $func(x)).transpose()?,
                            _ => return exec_err!("expected string scalar for intervals"),
                        };
                        Ok(ColumnarValue::Scalar($scalar(value)))
                    }
                }
            }
        }
    };
}

define_interval_udf!(
    SparkYearMonthInterval,
    "spark_year_month_interval",
    DataType::Interval(IntervalUnit::YearMonth),
    IntervalYearMonthType,
    string_to_year_month_interval,
    ScalarValue::IntervalYearMonth,
);

define_interval_udf!(
    SparkDayTimeInterval,
    "spark_day_time_interval",
    DataType::Duration(TimeUnit::Microsecond),
    DurationMicrosecondType,
    string_to_day_time_interval,
    ScalarValue::DurationMicrosecond,
);

define_interval_udf!(
    SparkCalendarInterval,
    "spark_calendar_interval",
    DataType::Interval(IntervalUnit::MonthDayNano),
    IntervalMonthDayNanoType,
    string_to_calendar_interval,
    ScalarValue::IntervalMonthDayNano,
);

/// Lenient variant of [`SparkCalendarInterval`] for the `session_window` gap:
/// Spark casts the gap with `safeStringToInterval`, which yields NULL for an
/// invalid string (even under ANSI), and the desugar's `end > time` filter
/// then drops the row — an invalid gap must not fail the query.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTryCalendarInterval {
    signature: Signature,
}

impl Default for SparkTryCalendarInterval {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTryCalendarInterval {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_exact(TypeSignatureClass::Native(
                    logical_string(),
                ))],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkTryCalendarInterval {
    fn name(&self) -> &str {
        "spark_try_calendar_interval"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Interval(IntervalUnit::MonthDayNano))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        fn parse(s: &str) -> Option<IntervalMonthDayNano> {
            string_to_calendar_interval(s).ok()
        }
        fn parse_all<'a>(
            values: impl Iterator<Item = Option<&'a str>>,
        ) -> PrimitiveArray<IntervalMonthDayNanoType> {
            // NULL results are memoized too: an invalid string is
            // deterministically NULL, unlike a transient error.
            let mut memo: StrMemo<'a, Option<IntervalMonthDayNano>> = StrMemo::new();
            values
                .map(|value| {
                    value.and_then(|s| {
                        memo.get_or_try_insert_ref(s, |s| Ok(parse(s)))
                            .map_or(None, |v| *v)
                    })
                })
                .collect()
        }
        match args.one()? {
            ColumnarValue::Array(array) => {
                let array = match array.data_type() {
                    DataType::Utf8 => parse_all(as_string_array(&array)?.iter()),
                    DataType::LargeUtf8 => parse_all(as_large_string_array(&array)?.iter()),
                    DataType::Utf8View => parse_all(as_string_view_array(&array)?.iter()),
                    _ => return exec_err!("expected string array for intervals"),
                };
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            ColumnarValue::Scalar(scalar) => {
                let value = match scalar.try_as_str() {
                    Some(x) => x.and_then(parse),
                    _ => return exec_err!("expected string scalar for intervals"),
                };
                Ok(ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(
                    value,
                )))
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDayTimeIntervalToCalendarInterval {
    signature: Signature,
}

impl Default for SparkDayTimeIntervalToCalendarInterval {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDayTimeIntervalToCalendarInterval {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Duration(TimeUnit::Microsecond)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkDayTimeIntervalToCalendarInterval {
    fn name(&self) -> &str {
        "spark_day_time_interval_to_calendar_interval"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Interval(IntervalUnit::MonthDayNano))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let arg = args.one()?;
        match arg {
            ColumnarValue::Array(array) => {
                let array = match array.data_type() {
                    DataType::Duration(TimeUnit::Microsecond) => array
                        .as_primitive::<DurationMicrosecondType>()
                        .iter()
                        .map(|value| {
                            value
                                .map(day_time_interval_to_calendar_interval)
                                .transpose()
                        })
                        .collect::<Result<PrimitiveArray<IntervalMonthDayNanoType>>>()?,
                    data_type => {
                        return exec_err!(
                            "expected microsecond day-time interval, got {data_type}"
                        );
                    }
                };
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            ColumnarValue::Scalar(ScalarValue::DurationMicrosecond(value)) => {
                let value = value
                    .map(day_time_interval_to_calendar_interval)
                    .transpose()?;
                Ok(ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(
                    value,
                )))
            }
            value => exec_err!("expected microsecond day-time interval, got {value:?}"),
        }
    }
}

// TODO: support alternative form of interval strings
//   In Spark, interval strings can be specified in two forms.
//   For example, the `INTERVAL HOUR` type can have the following string representations.
//   1. `[+|-]h`
//   2. `INTERVAL [+|-]'[+|-]h' HOUR`
//   The first form cannot be parsed since the start and end field information is lost in
//   Arrow types. Types such as `INTERVAL DAY` and `INTERVAL HOUR` has the same physical type
//   in Arrow, and we cannot distinguish `[+|-]d` from `[+|-]h`.

fn string_to_year_month_interval(value: &str) -> Result<i32> {
    let interval = parse_interval(value).map_err(|e| exec_datafusion_err!("{e}"))?;
    match interval {
        IntervalValue::YearMonth { months } => Ok(months),
        IntervalValue::Microsecond { .. } | IntervalValue::MonthDayNanosecond { .. } => {
            exec_err!("expected year month interval, but got: {value}")
        }
    }
}

fn string_to_day_time_interval(value: &str) -> Result<i64> {
    let interval = parse_interval(value).map_err(|e| exec_datafusion_err!("{e}"))?;
    match interval {
        IntervalValue::Microsecond { microseconds } => Ok(microseconds),
        IntervalValue::YearMonth { .. } | IntervalValue::MonthDayNanosecond { .. } => {
            exec_err!("expected day time interval, but got: {value}")
        }
    }
}

fn string_to_calendar_interval(value: &str) -> Result<IntervalMonthDayNano> {
    // Spark bucketing: the unit the user wrote decides the bucket; sub-day
    // amounts stay absolute microseconds and are never rebucketed into days.
    let interval =
        parse_calendar_interval_string(value).map_err(|e| exec_datafusion_err!("{e}"))?;
    let (days, nanoseconds) = interval
        .days_and_nanoseconds()
        .ok_or_else(|| exec_datafusion_err!("interval out of range: {value:?}"))?;
    Ok(IntervalMonthDayNano {
        months: interval.months,
        days,
        nanoseconds,
    })
}

fn day_time_interval_to_calendar_interval(microseconds: i64) -> Result<IntervalMonthDayNano> {
    const MICROSECONDS_PER_DAY: i64 = 24 * 60 * 60 * 1_000_000;

    let days = i32::try_from(microseconds / MICROSECONDS_PER_DAY).map_err(|_| {
        exec_datafusion_err!("microseconds overflow for calendar interval: {microseconds}")
    })?;
    Ok(IntervalMonthDayNano {
        months: 0,
        days,
        nanoseconds: microseconds % MICROSECONDS_PER_DAY * 1_000,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_memoized_computes_distinct_values_and_propagates_errors() -> Result<()> {
        use datafusion_common::arrow::array::Array;

        let values = [Some("5 minutes"), None, Some("5 minutes"), Some("1 month")];
        let array: PrimitiveArray<IntervalMonthDayNanoType> =
            parse_memoized(values.into_iter(), string_to_calendar_interval)?;
        assert_eq!(array.len(), 4);
        assert_eq!(
            array.value(0),
            IntervalMonthDayNano::new(0, 0, 300_000_000_000)
        );
        assert!(array.is_null(1));
        assert_eq!(array.value(2), array.value(0));
        assert_eq!(array.value(3), IntervalMonthDayNano::new(1, 0, 0));

        let invalid: Result<PrimitiveArray<IntervalMonthDayNanoType>> = parse_memoized(
            [Some("### nonsense")].into_iter(),
            string_to_calendar_interval,
        );
        assert!(invalid.is_err());
        Ok(())
    }

    /// A sub-day amount too large for i64 nanoseconds splits whole days out
    /// (Spark's microsecond-based CalendarInterval still represents it);
    /// below that bound the absolute bucket is preserved exactly.
    #[test]
    fn calendar_interval_nanosecond_overflow_splits_days() -> Result<()> {
        // 3000000 hours = 125000 days; fits i64 µs but not ns.
        let v = string_to_calendar_interval("3000000 hours")?;
        assert_eq!((v.months, v.days, v.nanoseconds), (0, 125_000, 0));
        // 2000000 hours fits ns: stays absolute, no day splitting.
        let v = string_to_calendar_interval("2000000 hours")?;
        assert_eq!(
            (v.months, v.days, v.nanoseconds),
            (0, 0, 2_000_000i64 * 3_600 * 1_000_000_000)
        );
        Ok(())
    }

    #[test]
    fn string_parsers_map_interval_kinds() -> Result<()> {
        assert_eq!(string_to_year_month_interval("2 years")?, 24);
        assert!(string_to_year_month_interval("5 minutes").is_err());
        assert_eq!(string_to_day_time_interval("5 minutes")?, 300_000_000);
        assert!(string_to_day_time_interval("1 month").is_err());
        assert_eq!(
            string_to_calendar_interval("1 month 2 days")?,
            IntervalMonthDayNano::new(1, 2, 0)
        );
        Ok(())
    }

    #[test]
    fn day_time_interval_preserves_calendar_days_and_microsecond_remainder() -> Result<()> {
        const MICROSECONDS_PER_DAY: i64 = 24 * 60 * 60 * 1_000_000;

        assert_eq!(
            day_time_interval_to_calendar_interval(MICROSECONDS_PER_DAY + 5)?,
            IntervalMonthDayNano::new(0, 1, 5_000)
        );
        assert_eq!(
            day_time_interval_to_calendar_interval(-MICROSECONDS_PER_DAY - 5)?,
            IntervalMonthDayNano::new(0, -1, -5_000)
        );
        Ok(())
    }
}
