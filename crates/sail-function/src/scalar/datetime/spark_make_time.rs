use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, PrimitiveBuilder};
use datafusion::arrow::datatypes::{DataType, Time64MicrosecondType, TimeUnit};
use datafusion_common::types::NativeType;
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::scalar::datetime::utils::{to_decimal128_array, to_int32_array};

// Seconds argument is coerced to Decimal128(16, 6) — matching Spark's DecimalType(16, 6).
// The unscaled i128 value is therefore already in microseconds (value * 10^-6 gives seconds,
// so unscaled directly equals whole_seconds * 1_000_000 + microsecond_fraction).
const SECONDS_PRECISION: u8 = 16;
const SECONDS_SCALE: i8 = 6;

// Scale factor: 10^6, the number of microseconds per whole second.
const SCALE_FACTOR: i128 = 1_000_000;

const MICROS_PER_MINUTE: i64 = 60 * 1_000_000;
const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMakeTime {
    signature: Signature,
}

impl Default for SparkMakeTime {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMakeTime {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkMakeTime {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_make_time"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Time64(TimeUnit::Microsecond))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;

        if args.len() != 3 {
            return exec_err!(
                "Spark `make_time` function requires 3 arguments, got {}",
                args.len()
            );
        }

        // Handle NULL propagation for scalar inputs
        let contains_scalar_null = args.iter().any(|arg| {
            matches!(
                arg,
                ColumnarValue::Scalar(ScalarValue::Int32(None))
                    | ColumnarValue::Scalar(ScalarValue::Decimal128(None, _, _))
                    | ColumnarValue::Scalar(ScalarValue::Null)
            )
        });

        if contains_scalar_null {
            return Ok(ColumnarValue::Scalar(ScalarValue::Time64Microsecond(None)));
        }

        let hours = to_int32_array(&args[0], "hour", "make_time", number_rows)?;
        let minutes = to_int32_array(&args[1], "minute", "make_time", number_rows)?;
        let seconds = to_decimal128_array(&args[2], "second", "make_time", number_rows)?;

        let mut builder = PrimitiveBuilder::<Time64MicrosecondType>::with_capacity(number_rows);

        for i in 0..number_rows {
            if hours.is_null(i) || minutes.is_null(i) || seconds.is_null(i) {
                builder.append_null();
                continue;
            }

            let h = hours.value(i);
            let m = minutes.value(i);
            let sec_unscaled = seconds.value(i);

            builder.append_value(make_time(h, m, sec_unscaled)?);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 3 {
            return exec_err!(
                "Spark `make_time` function requires 3 arguments, got {}",
                arg_types.len()
            );
        }

        let hour: NativeType = (&arg_types[0]).into();
        let minute: NativeType = (&arg_types[1]).into();
        let second: NativeType = (&arg_types[2]).into();

        if (hour.is_integer() || matches!(hour, NativeType::String | NativeType::Null))
            && (minute.is_integer() || matches!(minute, NativeType::String | NativeType::Null))
            && (second.is_numeric() || matches!(second, NativeType::String | NativeType::Null))
        {
            // Use Decimal128(16, 6) for seconds, matching Spark's DecimalType(16, 6).
            // This avoids f64 rounding errors (e.g. 59.9999999 rounding up to 60 seconds).
            Ok(vec![
                DataType::Int32,
                DataType::Int32,
                DataType::Decimal128(SECONDS_PRECISION, SECONDS_SCALE),
            ])
        } else {
            plan_err!("The arguments of Spark `make_time` must be (integer, integer, numeric)")
        }
    }
}

/// Create a TIME value (microseconds since midnight) from hour, minute, and second components.
///
/// Matches Spark 4.1 `MakeTime` / `DateTimeUtils.makeTime` semantics:
///   - hour:   0 to 23
///   - minute: 0 to 59
///   - sec_unscaled: the unscaled i128 of a `Decimal128(16, 6)` value,
///     where actual_seconds = sec_unscaled / 1_000_000.
///     Valid range: [0, 60_000_000) — i.e., [0.000000, 59.999999].
///
/// `sec_unscaled` is the raw integer from `Decimal128(16, 6)`, matching Spark's
/// `DecimalType(16, 6)`. Using the unscaled integer directly avoids the f64 rounding
/// trap where values like 59.9999999 would round up to 60_000_000 µs (= 60 s) and
/// silently overflow. Instead, 59.9999999 as Decimal(16,6) has unscaled = 59_999_999,
/// which is within range; an input of exactly 60.0 (unscaled = 60_000_000) correctly errors.
///
/// Errors on invalid inputs, matching Spark's `DATETIME_FIELD_OUT_OF_BOUNDS` behaviour.
fn make_time(hour: i32, minute: i32, sec_unscaled: i128) -> Result<i64> {
    if !(0..=23).contains(&hour) {
        return exec_err!("make_time: Invalid value for HourOfDay (valid values 0 - 23): {hour}");
    }
    if !(0..=59).contains(&minute) {
        return exec_err!(
            "make_time: Invalid value for MinuteOfHour (valid values 0 - 59): {minute}"
        );
    }
    // Unscaled range for valid seconds [0.000000, 59.999999] is [0, 59_999_999].
    if !(0..60 * SCALE_FACTOR).contains(&sec_unscaled) {
        return exec_err!(
            "make_time: Invalid value for SecondOfMinute (valid values 0 - 59): {}",
            sec_unscaled / SCALE_FACTOR
        );
    }

    // sec_unscaled is already in microseconds because Decimal(16,6) has scale 6,
    // so no further conversion is needed — just cast to i64.
    let sec_micros = sec_unscaled as i64;

    Ok(hour as i64 * MICROS_PER_HOUR + minute as i64 * MICROS_PER_MINUTE + sec_micros)
}
