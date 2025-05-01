use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DataType, DurationMicrosecondType, IntervalMonthDayNano, IntervalUnit, IntervalYearMonthType,
    TimeUnit,
};
use datafusion_common::arrow::array::PrimitiveArray;
use datafusion_common::arrow::datatypes::IntervalMonthDayNanoType;
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::types::logical_string;
use datafusion_common::{exec_datafusion_err, exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};
use sail_sql_analyzer::literal::interval::IntervalValue;
use sail_sql_analyzer::parser::parse_interval;

use crate::utils::ItemTaker;

macro_rules! define_interval_udf {
    ($udf:ident, $name:expr, $return_type:expr, $primitive_type:ty, $func:expr, $scalar:expr $(,)?) => {
        #[derive(Debug)]
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
            fn as_any(&self) -> &dyn Any {
                self
            }

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
            // We do not take into account daylight saving days here.
            let days = i32::try_from(microseconds / (24 * 60 * 60 * 1_000_000)).map_err(|_| {
                exec_datafusion_err!("microseconds overflow for calendar interval: {value}")
            })?;
            Ok(IntervalMonthDayNano {
                months: 0,
                days,
                nanoseconds: microseconds % (24 * 60 * 60 * 1_000_000) * 1_000,
            })
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
