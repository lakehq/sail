use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, IntervalMonthDayNanoArray, PrimitiveArray};
use datafusion::arrow::compute::try_binary;
use datafusion::arrow::datatypes::{
    DataType, Decimal256Type, DurationMicrosecondType, Field, FieldRef, Float64Type, Int64Type,
    IntervalMonthDayNano, IntervalMonthDayNanoType, IntervalUnit, IntervalYearMonthType, TimeUnit,
    i256,
};
use datafusion::arrow::error::ArrowError;
use datafusion_common::types::NativeType;
use datafusion_common::{DataFusionError, Result, plan_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use sail_common::spec;

use crate::error::invalid_arg_count_exec_err;

/// Spark scales an ANSI interval by operating on its single stored number -- the MONTHS of a
/// year-month interval or the MICROS of a day-time one -- and rounding HALF_UP, away from zero on
/// a tie (`intervalExpressions.scala:610-623,660-676,690-706`). None of the four expressions reads
/// `spark.sql.ansi.enabled`: they use exact operations, and raise on overflow and on a zero
/// divisor in both modes. DataFusion cannot express either half of that -- its `/` truncates and
/// returns NULL for a zero divisor with ANSI off -- which is why these are UDFs and not a rewrite.
macro_rules! interval_scale_udf {
    ($name:ident, $udf:literal, $spark:literal, $arrow:ty, $result:expr, $integral:expr, $fractional:expr, $decimal:expr) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        pub struct $name {
            signature: Signature,
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $name {
            pub fn new() -> Self {
                Self {
                    signature: Signature::user_defined(Volatility::Immutable),
                }
            }
        }

        impl ScalarUDFImpl for $name {
            fn name(&self) -> &str {
                $udf
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok($result)
            }

            fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
                // Scaling returns the full interval qualifier, independently of the input:
                // intervalExpressions.scala:606,659,746,829 in Spark 4.2.0.
                let metadata = match $result {
                    DataType::Interval(IntervalUnit::YearMonth) => {
                        spec::SparkIntervalMetadata::YearMonth {
                            start_field: spec::YearMonthIntervalField::Year,
                            end_field: spec::YearMonthIntervalField::Month,
                        }
                    }
                    DataType::Duration(TimeUnit::Microsecond) => {
                        spec::SparkIntervalMetadata::DayTime {
                            start_field: spec::DayTimeIntervalField::Day,
                            end_field: spec::DayTimeIntervalField::Second,
                        }
                    }
                    other => return plan_err!("unexpected scaled interval type: {other}"),
                };
                let metadata = metadata
                    .to_json()
                    .map_err(|error| DataFusionError::Plan(error.to_string()))?;
                Ok(Arc::new(
                    Field::new(
                        self.name(),
                        $result,
                        args.arg_fields.iter().any(|field| field.is_nullable()),
                    )
                    .with_metadata(
                        [(spec::SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(), metadata)].into(),
                    ),
                ))
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                let ScalarFunctionArgs {
                    args, number_rows, ..
                } = args;
                if args.len() != 2 {
                    return Err(invalid_arg_count_exec_err($spark, (2, 2), args.len()));
                }
                let interval = args[0].to_array(number_rows)?;
                let number = args[1].to_array(number_rows)?;
                let interval = interval.as_primitive::<$arrow>();
                let scaled: PrimitiveArray<$arrow> = match number.data_type() {
                    DataType::Int64 => {
                        try_binary(interval, number.as_primitive::<Int64Type>(), $integral)?
                    }
                    DataType::Float64 => {
                        try_binary(interval, number.as_primitive::<Float64Type>(), $fractional)?
                    }
                    DataType::Decimal256(_, scale) => {
                        let scale = *scale;
                        try_binary(
                            interval,
                            number.as_primitive::<Decimal256Type>(),
                            |interval, unscaled| $decimal(interval, unscaled, scale),
                        )?
                    }
                    other => return plan_err!("Spark `{}` cannot scale by {other}", $spark),
                };
                let scaled: ArrayRef = Arc::new(scaled.with_data_type($result));
                Ok(ColumnarValue::Array(scaled))
            }

            /// The interval keeps its type; the number collapses to the three shapes Spark
            /// distinguishes -- exact integral arithmetic, exact DECIMAL arithmetic rounded
            /// HALF_UP, or a `Double` rounded HALF_UP.
            fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
                let [interval, number] = arg_types else {
                    return Err(invalid_arg_count_exec_err($spark, (2, 2), arg_types.len()));
                };
                if interval != &$result {
                    return plan_err!("Spark `{}` expects {}", $spark, $result);
                }
                // `ImplicitCastInputTypes` with `NumericType`, so a STRING scales the interval
                // too: `NumericType.defaultConcreteType` is DOUBLE (`TypeCoercion.scala:212`).
                let native: NativeType = number.into();
                let number = if native.is_integer() {
                    DataType::Int64
                } else if let DataType::Decimal32(precision, scale)
                | DataType::Decimal64(precision, scale)
                | DataType::Decimal128(precision, scale)
                | DataType::Decimal256(precision, scale) = number
                {
                    DataType::Decimal256(*precision, *scale)
                } else if native.is_numeric()
                    || matches!(native, NativeType::Null | NativeType::String)
                {
                    DataType::Float64
                } else {
                    return plan_err!("Spark `{}` cannot scale by {number}", $spark);
                };
                Ok(vec![interval.clone(), number])
            }
        }
    };
}

interval_scale_udf!(
    SparkMultiplyYmInterval,
    "spark_multiply_ym_interval",
    "MultiplyYMInterval",
    IntervalYearMonthType,
    DataType::Interval(IntervalUnit::YearMonth),
    multiply_integral_i32,
    multiply_fractional_i32,
    multiply_decimal_i32
);
interval_scale_udf!(
    SparkDivideYmInterval,
    "spark_divide_ym_interval",
    "DivideYMInterval",
    IntervalYearMonthType,
    DataType::Interval(IntervalUnit::YearMonth),
    divide_integral_i32,
    divide_fractional_i32,
    divide_decimal_i32
);
interval_scale_udf!(
    SparkMultiplyDtInterval,
    "spark_multiply_dt_interval",
    "MultiplyDTInterval",
    DurationMicrosecondType,
    DataType::Duration(TimeUnit::Microsecond),
    multiply_integral_i64,
    multiply_fractional_i64,
    multiply_decimal_i64
);
interval_scale_udf!(
    SparkDivideDtInterval,
    "spark_divide_dt_interval",
    "DivideDTInterval",
    DurationMicrosecondType,
    DataType::Duration(TimeUnit::Microsecond),
    divide_integral_i64,
    divide_fractional_i64,
    divide_decimal_i64
);

fn overflow() -> ArrowError {
    ArrowError::ComputeError("integer overflow".to_string())
}

fn divided_by_zero() -> ArrowError {
    ArrowError::ComputeError(
        "[INTERVAL_DIVIDED_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead.".to_string(),
    )
}

/// Rounds HALF_UP and checks the range. `f64::round` breaks ties away from zero, which is what
/// Spark's HALF_UP means for a signed count. The upper bound is EXCLUSIVE: `i64::MAX` is not a
/// DOUBLE, it reads as 2^63, and `DoubleMath.roundToLong` refuses 2^63.
fn round_half_up(
    scaled: f64,
    input: f64,
    min: f64,
    max_exclusive: f64,
) -> std::result::Result<f64, ArrowError> {
    if !scaled.is_finite() {
        return Err(ArrowError::ComputeError(
            "input is infinite or NaN".to_string(),
        ));
    }
    let rounded = scaled.round();
    if rounded < min || rounded >= max_exclusive {
        return Err(ArrowError::ComputeError(format!(
            "rounded value is out of range for input {input} and rounding mode HALF_UP"
        )));
    }
    Ok(rounded)
}

/// Divides exactly and rounds HALF_UP on the remainder, the way Spark's `IntMath`/`LongMath` do.
/// A float would round the wrong way on a tie, so the remainder decides, not the quotient.
fn divide_half_up(numerator: i256, denominator: i256) -> std::result::Result<i256, ArrowError> {
    let quotient = numerator
        .checked_div(denominator)
        .ok_or_else(divided_by_zero)?;
    let remainder = numerator
        .checked_rem(denominator)
        .ok_or_else(divided_by_zero)?;
    let twice_remainder = remainder
        .wrapping_abs()
        .checked_mul(i256::from_i128(2))
        .ok_or_else(overflow)?;
    if twice_remainder >= denominator.wrapping_abs() {
        let away = if numerator.is_negative() == denominator.is_negative() {
            i256::ONE
        } else {
            i256::MINUS_ONE
        };
        quotient.checked_add(away).ok_or_else(overflow)
    } else {
        Ok(quotient)
    }
}

/// Scales an interval by a DECIMAL exactly: `Decimal` multiplication or division, then
/// `setScale(0, HALF_UP)` (`intervalExpressions.scala:616-618,665-667,756-758,835-837`). Spark
/// truncates the exact result at 39 digits first, which never moves a HALF_UP decision, so the
/// exact quotient rounds the same way.
fn scale_decimal(
    value: i64,
    unscaled: i256,
    scale: i8,
    divide: bool,
) -> std::result::Result<i128, ArrowError> {
    let value = i256::from_i128(i128::from(value));
    let power = i256::from_i128(10)
        .checked_pow(u32::from(scale.unsigned_abs()))
        .ok_or_else(overflow)?;
    let (numerator, denominator) = match (divide, scale >= 0) {
        (false, true) => (value.checked_mul(unscaled), Some(power)),
        (false, false) => (
            value
                .checked_mul(unscaled)
                .and_then(|v| v.checked_mul(power)),
            Some(i256::ONE),
        ),
        (true, true) => (value.checked_mul(power), Some(unscaled)),
        (true, false) => (Some(value), unscaled.checked_mul(power)),
    };
    let (Some(numerator), Some(denominator)) = (numerator, denominator) else {
        return Err(overflow());
    };
    if denominator == i256::ZERO {
        return Err(divided_by_zero());
    }
    divide_half_up(numerator, denominator)?
        .to_i128()
        .ok_or_else(overflow)
}

fn multiply_decimal_i32(
    months: i32,
    unscaled: i256,
    scale: i8,
) -> std::result::Result<i32, ArrowError> {
    let scaled = scale_decimal(i64::from(months), unscaled, scale, false)?;
    i32::try_from(scaled).map_err(|_| overflow())
}

fn divide_decimal_i32(
    months: i32,
    unscaled: i256,
    scale: i8,
) -> std::result::Result<i32, ArrowError> {
    let scaled = scale_decimal(i64::from(months), unscaled, scale, true)?;
    i32::try_from(scaled).map_err(|_| overflow())
}

fn multiply_decimal_i64(
    micros: i64,
    unscaled: i256,
    scale: i8,
) -> std::result::Result<i64, ArrowError> {
    let scaled = scale_decimal(micros, unscaled, scale, false)?;
    i64::try_from(scaled).map_err(|_| overflow())
}

fn divide_decimal_i64(
    micros: i64,
    unscaled: i256,
    scale: i8,
) -> std::result::Result<i64, ArrowError> {
    let scaled = scale_decimal(micros, unscaled, scale, true)?;
    i64::try_from(scaled).map_err(|_| overflow())
}

fn multiply_integral_i32(months: i32, number: i64) -> std::result::Result<i32, ArrowError> {
    i64::from(months)
        .checked_mul(number)
        .and_then(|scaled| i32::try_from(scaled).ok())
        .ok_or_else(overflow)
}

fn multiply_fractional_i32(months: i32, number: f64) -> std::result::Result<i32, ArrowError> {
    let scaled = round_half_up(
        f64::from(months) * number,
        number,
        f64::from(i32::MIN),
        f64::from(i32::MAX) + 1.0,
    )?;
    Ok(scaled as i32)
}

fn divide_integral_i32(months: i32, number: i64) -> std::result::Result<i32, ArrowError> {
    let scaled = divide_half_up(
        i256::from_i128(i128::from(months)),
        i256::from_i128(i128::from(number)),
    )?;
    scaled
        .to_i128()
        .and_then(|scaled| i32::try_from(scaled).ok())
        .ok_or_else(overflow)
}

fn divide_fractional_i32(months: i32, number: f64) -> std::result::Result<i32, ArrowError> {
    if number == 0.0 {
        return Err(divided_by_zero());
    }
    let scaled = round_half_up(
        f64::from(months) / number,
        number,
        f64::from(i32::MIN),
        f64::from(i32::MAX) + 1.0,
    )?;
    Ok(scaled as i32)
}

fn multiply_integral_i64(micros: i64, number: i64) -> std::result::Result<i64, ArrowError> {
    micros.checked_mul(number).ok_or_else(overflow)
}

fn multiply_fractional_i64(micros: i64, number: f64) -> std::result::Result<i64, ArrowError> {
    let scaled = round_half_up(
        micros as f64 * number,
        number,
        i64::MIN as f64,
        -(i64::MIN as f64),
    )?;
    Ok(scaled as i64)
}

fn divide_integral_i64(micros: i64, number: i64) -> std::result::Result<i64, ArrowError> {
    let scaled = divide_half_up(
        i256::from_i128(i128::from(micros)),
        i256::from_i128(i128::from(number)),
    )?;
    scaled
        .to_i128()
        .and_then(|scaled| i64::try_from(scaled).ok())
        .ok_or_else(overflow)
}

fn divide_fractional_i64(micros: i64, number: f64) -> std::result::Result<i64, ArrowError> {
    if number == 0.0 {
        return Err(divided_by_zero());
    }
    let scaled = round_half_up(
        micros as f64 / number,
        number,
        i64::MIN as f64,
        -(i64::MIN as f64),
    )?;
    Ok(scaled as i64)
}

/// Microseconds in a day, the unit Spark folds a fractional day into, and the step from Spark's
/// storage to Sail's.
const MICROS_PER_DAY: f64 = 24.0 * 60.0 * 60.0 * 1_000_000.0;
const NANOS_PER_MICRO: i64 = 1_000;

/// Spark scales the LEGACY calendar interval field by field, and it does NOT round the way the
/// ANSI intervals do: months and days are TRUNCATED toward zero (`monthsWithFraction.toInt`),
/// the fraction of a day left over is folded into the time part, and only that is rounded --
/// with `Math.round`, which rounds a tie toward POSITIVE INFINITY (`IntervalUtils.scala:634-658`).
/// Measured, not assumed: `1 microsecond * -0.5` is `0` in Spark, where the ANSI rule would give
/// `-1`.
///
/// This one DOES read `spark.sql.ansi.enabled` (`MultiplyInterval`'s `failOnError`,
/// `intervalExpressions.scala:597-601`): with it on, a field that leaves `Int32` raises and a zero
/// divisor raises; with it off the field saturates and a zero divisor yields NULL.
macro_rules! calendar_scale_udf {
    ($name:ident, $udf:literal, $spark:literal, $divide:expr) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        pub struct $name {
            signature: Signature,
            ansi_mode: bool,
        }

        impl $name {
            pub fn new(ansi_mode: bool) -> Self {
                Self {
                    signature: Signature::user_defined(Volatility::Immutable),
                    ansi_mode,
                }
            }

            pub fn ansi_mode(&self) -> bool {
                self.ansi_mode
            }
        }

        impl ScalarUDFImpl for $name {
            fn name(&self) -> &str {
                $udf
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok(DataType::Interval(IntervalUnit::MonthDayNano))
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                let ScalarFunctionArgs {
                    args, number_rows, ..
                } = args;
                if args.len() != 2 {
                    return Err(invalid_arg_count_exec_err($spark, (2, 2), args.len()));
                }
                let interval = args[0].to_array(number_rows)?;
                let number = args[1].to_array(number_rows)?;
                let interval = interval.as_primitive::<IntervalMonthDayNanoType>();
                let number = number.as_primitive::<Float64Type>();
                let ansi_mode = self.ansi_mode;
                // Not `try_binary`: with ANSI off a zero divisor is NULL, which only a
                // null-producing kernel can return.
                let scaled = interval
                    .iter()
                    .zip(number.iter())
                    .map(|pair| match pair {
                        (Some(interval), Some(number)) => {
                            scale_calendar(interval, number, $divide, ansi_mode)
                        }
                        _ => Ok(None),
                    })
                    .collect::<std::result::Result<IntervalMonthDayNanoArray, ArrowError>>()?;
                Ok(ColumnarValue::Array(Arc::new(scaled) as ArrayRef))
            }

            /// `IntervalNumOperation.inputTypes` is `(CalendarIntervalType, DoubleType)`
            /// (`intervalExpressions.scala:181`), so every number -- integral, decimal or string --
            /// reaches the operation as a DOUBLE.
            fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
                let [interval, number] = arg_types else {
                    return Err(invalid_arg_count_exec_err($spark, (2, 2), arg_types.len()));
                };
                if !matches!(interval, DataType::Interval(IntervalUnit::MonthDayNano)) {
                    return plan_err!("Spark `{}` expects a calendar interval", $spark);
                }
                let native: NativeType = number.into();
                if !(native.is_numeric() || matches!(native, NativeType::Null | NativeType::String))
                {
                    return plan_err!("Spark `{}` cannot scale by {number}", $spark);
                }
                Ok(vec![interval.clone(), DataType::Float64])
            }
        }
    };
}

calendar_scale_udf!(
    SparkMultiplyCalendarInterval,
    "spark_multiply_calendar_interval",
    "MultiplyInterval",
    false
);
calendar_scale_udf!(
    SparkDivideCalendarInterval,
    "spark_divide_calendar_interval",
    "DivideInterval",
    true
);

/// One field of the interval, truncated toward zero. With ANSI on it is `toIntExact(x.toLong)`,
/// so leaving `Int32` raises but a NaN is `0`, since `NaN.toLong` is; with it off the value
/// saturates, which is what Scala's `.toInt` gives for a `Double`
/// (`IntervalUtils.scala:638-639,655-657`), and what `as` gives in Rust.
fn truncate_field(value: f64, ansi_mode: bool) -> std::result::Result<i32, ArrowError> {
    if ansi_mode
        && !value.is_nan()
        && (value <= f64::from(i32::MIN) - 1.0 || value >= f64::from(i32::MAX) + 1.0)
    {
        return Err(ArrowError::ComputeError(
            "[ARITHMETIC_OVERFLOW] integer overflow".to_string(),
        ));
    }
    Ok(value as i32)
}

fn scale_calendar(
    interval: IntervalMonthDayNano,
    number: f64,
    divide: bool,
    ansi_mode: bool,
) -> std::result::Result<Option<IntervalMonthDayNano>, ArrowError> {
    // `IntervalUtils.divide` tests `num == 0` (`IntervalUtils.scala:742-745`), which a negative zero
    // satisfies too.
    if divide && number == 0.0 {
        return if ansi_mode {
            Err(divided_by_zero())
        } else {
            Ok(None)
        };
    }
    let scale = |field: f64| {
        if divide {
            field / number
        } else {
            field * number
        }
    };
    let months = scale(f64::from(interval.months));
    let days = scale(f64::from(interval.days));
    // Spark keeps the time part of a calendar interval in MICROSECONDS and Sail in nanoseconds, so
    // the whole computation runs in micros and only the last step converts. That is not a detail:
    // the rounding below has to land on a microsecond the way Spark's does, or
    // `1 microsecond * 0.5` stays 500 nanoseconds here and reads `0.0000005 seconds` where Spark
    // says `0.000001 seconds`.
    let micros = scale((interval.nanoseconds / NANOS_PER_MICRO) as f64);

    let truncated_days = truncate_field(days, ansi_mode)?;
    // The fraction of a day that truncating threw away is not lost: Spark folds it into the time
    // part, and rounds ONLY there.
    let micros = micros + MICROS_PER_DAY * (days - f64::from(truncated_days));
    Ok(Some(IntervalMonthDayNano::new(
        truncate_field(months, ansi_mode)?,
        truncated_days,
        java_round(micros).saturating_mul(NANOS_PER_MICRO),
    )))
}

/// Java's `Math.round(double)`: the floor of `x + 1/2` computed exactly, so a tie goes toward
/// positive infinity, `0.49999999999999994` stays `0` where `(x + 0.5).floor()` gives `1`, and an
/// odd count past 2^52 is kept. A NaN is `0` and the rest saturates, as `as` does in Rust.
fn java_round(value: f64) -> i64 {
    let floor = value.floor();
    let rounded = if value - floor >= 0.5 {
        floor + 1.0
    } else {
        floor
    };
    rounded as i64
}
