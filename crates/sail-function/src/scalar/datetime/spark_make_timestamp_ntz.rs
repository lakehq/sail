use std::sync::Arc;

use chrono::{Duration, NaiveDate};
use datafusion::arrow::array::{Array, PrimitiveArray, PrimitiveBuilder};
use datafusion::arrow::datatypes::{DataType, TimeUnit, TimestampMicrosecondType};
use datafusion_common::cast::{
    as_date32_array, as_float64_array, as_int32_array, as_time64_microsecond_array, as_uint32_array,
};
use datafusion_common::types::NativeType;
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

const MICROS_PER_DAY: i64 = 86_400_000_000; // 24 * 60 * 60 * 1_000_000

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMakeTimestampNtz {
    signature: Signature,
    is_try: bool,
}

impl SparkMakeTimestampNtz {
    pub fn new(is_try: bool) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            is_try,
        }
    }

    pub fn is_try(&self) -> bool {
        self.is_try
    }
}

impl ScalarUDFImpl for SparkMakeTimestampNtz {
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

        if let [dates, times] = args.as_slice() {
            if contains_scalar_null(&args) {
                return Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    None, None,
                )));
            }

            let dates = dates.to_array(number_rows)?;
            let times = times.to_array(number_rows)?;

            let dates = as_date32_array(&dates)?;
            let times = as_time64_microsecond_array(&times)?;

            let mut builder =
                PrimitiveBuilder::<TimestampMicrosecondType>::with_capacity(number_rows);
            for i in 0..number_rows {
                if dates.is_null(i) || times.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let date = dates.value(i);
                let time = times.value(i);

                match make_timestamp_from_date_time(date, time) {
                    Some(micros) => builder.append_value(micros),
                    None => {
                        if self.is_try {
                            builder.append_null()
                        } else {
                            return exec_err!(
                                "invalid input for Spark `make_timestamp_ntz`: date={}, time={}",
                                date,
                                time
                            );
                        }
                    }
                }
            }
            return Ok(ColumnarValue::Array(Arc::new(builder.finish())));
        }

        if let [years, months, days, hours, minutes, seconds] = args.as_slice() {
            if contains_scalar_null(&args) {
                return Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    None, None,
                )));
            }
            let years = years.to_array(number_rows)?;
            let months = months.to_array(number_rows)?;
            let days = days.to_array(number_rows)?;
            let hours = hours.to_array(number_rows)?;
            let minutes = minutes.to_array(number_rows)?;
            let seconds = seconds.to_array(number_rows)?;

            let years = as_int32_array(&years)?;
            let months = as_uint32_array(&months)?;
            let days = as_uint32_array(&days)?;
            let hours = as_uint32_array(&hours)?;
            let minutes = as_uint32_array(&minutes)?;
            let seconds = as_float64_array(&seconds)?;

            let mut builder: PrimitiveBuilder<TimestampMicrosecondType> =
                PrimitiveArray::builder(number_rows);
            for i in 0..number_rows {
                if years.is_null(i)
                    || months.is_null(i)
                    || days.is_null(i)
                    || hours.is_null(i)
                    || minutes.is_null(i)
                    || seconds.is_null(i)
                {
                    builder.append_null();
                    continue;
                }
                let year = years.value(i);
                let month = months.value(i);
                let day = days.value(i);
                let hour = hours.value(i);
                let minute = minutes.value(i);
                let second = seconds.value(i);
                match make_timestamp_ntz(year, month, day, hour, minute, second) {
                    Some(micros) => builder.append_value(micros),
                    None => {
                        if self.is_try {
                            builder.append_null()
                        } else {
                            return exec_err!(
                                "invalid input for Spark `make_timestamp_ntz`: year={}, month={}, day={}, hour={}, minute={}, second={}",
                                year, month, day, hour, minute, second
                            );
                        }
                    }
                }
            }
            return Ok(ColumnarValue::Array(Arc::new(builder.finish())));
        }

        exec_err!(
            "Spark `make_timestamp_ntz` function requires 2 or 6 arguments, got {}",
            args.len()
        )
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() == 2 {
            return Ok(vec![
                DataType::Date32,
                DataType::Time64(TimeUnit::Microsecond),
            ]);
        }

        if let [years, months, days, hours, mins, secs] = arg_types {
            let years: NativeType = years.into();
            let months: NativeType = months.into();
            let days: NativeType = days.into();
            let hours: NativeType = hours.into();
            let mins: NativeType = mins.into();
            let secs: NativeType = secs.into();
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
                return Ok(vec![
                    DataType::Int32,
                    DataType::UInt32,
                    DataType::UInt32,
                    DataType::UInt32,
                    DataType::UInt32,
                    DataType::Float64,
                ]);
            } else {
                return plan_err!(
                    "The arguments of Spark `make_timestamp_ntz` must be integer or string or null"
                );
            }
        }

        exec_err!(
            "Spark `make_timestamp_ntz` function requires 2 or 6 arguments, got {}",
            arg_types.len()
        )
    }
}

fn contains_scalar_null(args: &[ColumnarValue]) -> bool {
    args.iter().any(|arg| {
        if let ColumnarValue::Scalar(scalar) = arg {
            scalar.is_null()
        } else {
            false
        }
    })
}

/// Helper function to create timestamp from Date32 (days since epoch) and Time64 (microseconds since midnight)
fn make_timestamp_from_date_time(date: i32, time: i64) -> Option<i64> {
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
fn make_timestamp_ntz(
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
    let seconds = micros_total / 1_000_000;
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
