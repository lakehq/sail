use std::any::Any;
use std::sync::Arc;

use chrono::{Duration, NaiveDate};
use datafusion::arrow::array::{Array, PrimitiveArray, PrimitiveBuilder};
use datafusion::arrow::datatypes::{DataType, TimeUnit, TimestampMicrosecondType};
use datafusion_common::types::NativeType;
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::scalar::datetime::utils::{
    to_date32_array, to_float64_array, to_int32_array, to_time64_array, to_uint32_array,
};

const MICROS_PER_DAY: i64 = 86_400_000_000; // 24 * 60 * 60 * 1_000_000

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMakeTimestampNtz {
    signature: Signature,
}

impl Default for SparkMakeTimestampNtz {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMakeTimestampNtz {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkMakeTimestampNtz {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_make_timestamp_ntz"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;
        if args.len() == 2 {
            let contains_scalar_null = args.iter().any(|arg| {
                matches!(arg, ColumnarValue::Scalar(ScalarValue::Date32(None)))
                    || matches!(
                        arg,
                        ColumnarValue::Scalar(ScalarValue::Time64Microsecond(None))
                    )
            });

            if contains_scalar_null {
                // TODO: If the configuration spark.sql.ansi.enabled is false,
                //  the function returns NULL on invalid inputs. Otherwise, it will throw an error.
                return Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    None, None,
                )));
            }

            let dates = to_date32_array(&args[0], "date", "make_timestamp_ntz", number_rows)?;
            let times = to_time64_array(&args[1], "time", "make_timestamp_ntz", number_rows)?;

            let mut builder =
                PrimitiveBuilder::<TimestampMicrosecondType>::with_capacity(number_rows);
            for i in 0..number_rows {
                if dates.is_null(i) || times.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let date_val = dates.value(i);
                let time_val = times.value(i);

                match make_timestamp_from_date_time(date_val, time_val) {
                    Some(micros) => builder.append_value(micros),
                    None => {
                        // TODO: If the configuration spark.sql.ansi.enabled is false,
                        //  the function returns NULL on invalid inputs. Otherwise, it will throw an error.
                        builder.append_null()
                    }
                }
            }
            return Ok(ColumnarValue::Array(Arc::new(builder.finish())));
        }

        if args.len() != 6 {
            return exec_err!(
                "Spark `make_timestamp_ntz` function requires 6 arguments, got {}",
                args.len()
            );
        }

        let contains_scalar_null = args.iter().any(|arg| {
            matches!(arg, ColumnarValue::Scalar(ScalarValue::Int64(None)))
                || matches!(arg, ColumnarValue::Scalar(ScalarValue::Float64(None)))
        });

        if contains_scalar_null {
            // TODO: If the configuration spark.sql.ansi.enabled is false,
            //  the function returns NULL on invalid inputs. Otherwise, it will throw an error.
            return Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                None, None,
            )));
        }

        // Convert arguments to arrays using shared utilities
        let (years, months, days, hours, mins, secs) = (
            to_int32_array(&args[0], "years", "make_timestamp_ntz", number_rows),
            to_uint32_array(&args[1], "months", "make_timestamp_ntz", number_rows),
            to_uint32_array(&args[2], "days", "make_timestamp_ntz", number_rows),
            to_uint32_array(&args[3], "hours", "make_timestamp_ntz", number_rows),
            to_uint32_array(&args[4], "mins", "make_timestamp_ntz", number_rows),
            to_float64_array(&args[5], "secs", "make_timestamp_ntz", number_rows),
        );
        let years = match years {
            Ok(years) => years,
            Err(_) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    None, None,
                )))
            }
        };
        let months = match months {
            Ok(months) => months,
            Err(_) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    None, None,
                )))
            }
        };
        let days = match days {
            Ok(days) => days,
            Err(_) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    None, None,
                )))
            }
        };
        let hours = match hours {
            Ok(hours) => hours,
            Err(_) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    None, None,
                )))
            }
        };
        let mins = match mins {
            Ok(mins) => mins,
            Err(_) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    None, None,
                )))
            }
        };
        let secs = match secs {
            Ok(secs) => secs,
            Err(_) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    None, None,
                )))
            }
        };

        let mut builder: PrimitiveBuilder<TimestampMicrosecondType> =
            PrimitiveArray::builder(number_rows);
        for i in 0..number_rows {
            let (year, month, day, hour, min, sec) = (
                years.value(i),
                months.value(i),
                days.value(i),
                hours.value(i),
                mins.value(i),
                secs.value(i),
            );
            match make_timestamp_ntz(year, month, day, hour, min, sec) {
                Some(micros) => builder.append_value(micros),
                None => {
                    // TODO: If the configuration spark.sql.ansi.enabled is false,
                    //  the function returns NULL on invalid inputs. Otherwise, it will throw an error.
                    builder.append_null()
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() == 2 {
            return Ok(vec![
                DataType::Date32,
                DataType::Time64(TimeUnit::Microsecond),
            ]);
        }

        if arg_types.len() != 6 {
            return exec_err!(
                "Spark `make_timestamp_ntz` function requires 2 or 6 arguments, got {}",
                arg_types.len()
            );
        }

        let (years, months, days, hours, mins, secs): (
            NativeType,
            NativeType,
            NativeType,
            NativeType,
            NativeType,
            NativeType,
        ) = (
            (&arg_types[0]).into(),
            (&arg_types[1]).into(),
            (&arg_types[2]).into(),
            (&arg_types[3]).into(),
            (&arg_types[4]).into(),
            (&arg_types[5]).into(),
        );
        if (years.is_integer()
            || matches!(years, NativeType::String)
            || matches!(years, NativeType::Null))
            && (months.is_integer()
                || matches!(months, NativeType::String)
                || matches!(months, NativeType::Null))
            && (days.is_integer()
                || matches!(days, NativeType::String)
                || matches!(days, NativeType::Null))
            && (hours.is_integer()
                || matches!(hours, NativeType::String)
                || matches!(hours, NativeType::Null))
            && (mins.is_integer()
                || matches!(mins, NativeType::String)
                || matches!(mins, NativeType::Null))
            && (secs.is_numeric()
                || matches!(secs, NativeType::String)
                || matches!(secs, NativeType::Null))
        {
            Ok(vec![
                DataType::Int32,
                DataType::UInt32,
                DataType::UInt32,
                DataType::UInt32,
                DataType::UInt32,
                DataType::Float64,
            ])
        } else {
            plan_err!(
                "The arguments of Spark `make_timestamp_ntz` must be integer or string or null"
            )
        }
    }
}

/// Helper function to create timestamp from Date32 (days since epoch) and Time64 (microseconds since midnight)
pub(crate) fn make_timestamp_from_date_time(date: i32, time: i64) -> Option<i64> {
    (date as i64)
        .checked_mul(MICROS_PER_DAY)
        .and_then(|v| v.checked_add(time))
}

/// Helper function to create timestamp from date/time components
///
/// Matches Spark's `MakeTimestamp` semantics:
///   - year: 1 to 9999
///   - month: 1 to 12
///   - day: 1 to 31 (validated by chrono for the given month/year)
///   - hour: 0 to 23
///   - min: 0 to 59
///   - sec: 0 to 60 (as f64, representing seconds + microsecond fraction)
///
/// Special handling for sec=60 (Spark/PostgreSQL compatibility):
///   - If the integer part of sec == 60 and the fractional part (microseconds) == 0,
///     the seconds field is set to 0 and 1 minute is added to the final timestamp.
///   - If the integer part of sec == 60 and the fractional part != 0,
///     returns None (invalid: fractional seconds with sec=60).
pub(crate) fn make_timestamp_ntz(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    min: u32,
    sec: f64,
) -> Option<i64> {
    // Spark restricts year to 1-9999 (chrono accepts wider range)
    if !(1..=9999).contains(&year) {
        return None;
    }
    // Second must be in [0, 60] range
    if !(0.0..=60.0).contains(&sec) || sec.is_nan() {
        return None;
    }

    // Convert sec (f64) to integer seconds + microseconds, matching Spark's Decimal(16,6) approach.
    // Spark does: unscaledSecFrac = decimal.toUnscaledLong (with 6 decimal places)
    //   seconds = floorDiv(unscaledSecFrac, 1_000_000)
    //   nanos = floorMod(unscaledSecFrac, 1_000_000) * 1000
    let micros_total = (sec * 1_000_000.0).round() as i64;
    let seconds = (micros_total / 1_000_000) as i64;
    let micro_frac = (micros_total % 1_000_000) as u32;

    if seconds == 60 {
        // Spark: sec=60 with zero fractional part → add 1 minute
        // Spark: sec=60 with non-zero fractional part → error (NULL in non-ANSI mode)
        if micro_frac != 0 {
            return None;
        }
        let naive_date = NaiveDate::from_ymd_opt(year, month, day)?;
        let base_time = naive_date.and_hms_micro_opt(hour, min, 0, 0)?;
        return base_time
            .checked_add_signed(Duration::minutes(1))
            .map(|dt| dt.and_utc().timestamp_micros());
    }

    // Regular case: seconds in 0..59 with optional microsecond fraction
    let naive_date = NaiveDate::from_ymd_opt(year, month, day)?;
    naive_date
        .and_hms_micro_opt(hour, min, seconds as u32, micro_frac)
        .map(|dt| dt.and_utc().timestamp_micros())
}
