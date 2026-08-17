use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DataType, DurationMicrosecondType, IntervalMonthDayNano, IntervalUnit, IntervalYearMonthType,
    TimeUnit,
};
use datafusion_common::arrow::array::{AsArray, PrimitiveArray};
use datafusion_common::arrow::datatypes::IntervalMonthDayNanoType;
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::types::logical_string;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_sql_analyzer::literal::interval::IntervalValue;
use sail_sql_analyzer::parser::parse_interval;

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
                            DataType::Utf8 => as_string_array(&array)?
                                .iter()
                                .map(|x| x.map(|x| $func(x)).transpose())
                                .collect::<Result<_>>()?,
                            DataType::LargeUtf8 => as_large_string_array(&array)?
                                .iter()
                                .map(|x| x.map(|x| $func(x)).transpose())
                                .collect::<Result<_>>()?,
                            DataType::Utf8View => as_string_view_array(&array)?
                                .iter()
                                .map(|x| x.map(|x| $func(x)).transpose())
                                .collect::<Result<_>>()?,
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
    let interval = parse_interval(value).map_err(|e| exec_datafusion_err!("{e}"))?;
    match interval {
        IntervalValue::YearMonth { months } => Ok(IntervalMonthDayNano {
            months,
            days: 0,
            nanoseconds: 0,
        }),
        IntervalValue::Microsecond { microseconds } => {
            day_time_interval_to_calendar_interval(microseconds)
        }
        IntervalValue::MonthDayNanosecond {
            months,
            days,
            nanoseconds,
        } => Ok(IntervalMonthDayNano {
            months,
            days,
            nanoseconds,
        }),
    }
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
