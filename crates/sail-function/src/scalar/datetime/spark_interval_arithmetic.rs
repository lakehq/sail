use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, PrimitiveArray, Scalar};
use datafusion::arrow::compute::kernels::numeric::mul;
use datafusion::arrow::compute::{cast, try_binary, try_unary};
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type,
    DurationMicrosecondType, Field, FieldRef, Float16Type, Float32Type, Float64Type, Int8Type,
    Int16Type, Int32Type, Int64Type, IntervalUnit, IntervalYearMonthType, TimeUnit, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type, i256,
};
use datafusion::arrow::error::ArrowError;
use datafusion_common::{DataFusionError, Result, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use sail_common::spec::{
    DayTimeIntervalField, SAIL_SPARK_INTERVAL_METADATA_KEY, SparkIntervalMetadata,
    YearMonthIntervalField,
};

/// The arithmetic operation of a year-month or day-time interval with a numeric value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IntervalArithmeticOp {
    Multiply,
    Divide,
}

/// A year-month interval (`Interval(YearMonth)`) or a day-time interval
/// (`Duration(Microsecond)`) multiplied or divided by a numeric value.
///
/// The result is exact like in Spark, whether or not ANSI mode is enabled:
/// - An integral product that overflows raises an error. An integral quotient is rounded half away
///   from zero (`HALF_UP`).
/// - A floating point or decimal result is rounded `HALF_UP` to whole months or microseconds, with
///   an error if it is not finite or overflows.
/// - A division by zero raises an error.
///
/// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/intervalExpressions.scala#L596-L890>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkIntervalArithmetic {
    op: IntervalArithmeticOp,
    signature: Signature,
}

impl SparkIntervalArithmetic {
    pub const MULTIPLY_NAME: &'static str = "spark_interval_multiply";
    pub const DIVIDE_NAME: &'static str = "spark_interval_divide";

    pub fn new(op: IntervalArithmeticOp) -> Self {
        Self {
            op,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    pub fn op(&self) -> IntervalArithmeticOp {
        self.op
    }
}

/// <https://github.com/apache/spark/blob/v4.2.0/common/utils/src/main/resources/error/error-conditions.json#L3287-L3292>
const DIVIDED_BY_ZERO: &str = "[INTERVAL_DIVIDED_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead.";

/// <https://github.com/apache/spark/blob/v4.2.0/common/utils/src/main/resources/error/error-conditions.json#L3269-L3286>
const DIVIDE_OVERFLOW: &str = "[INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION] Integer overflow while operating with intervals. Use `try_divide` to tolerate overflow and return NULL instead.";

impl ScalarUDFImpl for SparkIntervalArithmetic {
    fn name(&self) -> &str {
        match self.op {
            IntervalArithmeticOp::Multiply => Self::MULTIPLY_NAME,
            IntervalArithmeticOp::Divide => Self::DIVIDE_NAME,
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types.first() {
            Some(
                interval @ (DataType::Interval(IntervalUnit::YearMonth)
                | DataType::Duration(TimeUnit::Microsecond)),
            ) => Ok(interval.clone()),
            _ => plan_err!("{} expects a year-month or day-time interval", self.name()),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let data_type = self.return_type(
            &args
                .arg_fields
                .iter()
                .map(|f| f.data_type().clone())
                .collect::<Vec<_>>(),
        )?;
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        // Like Spark, the result has the default interval fields (`YEAR TO MONTH` or
        // `DAY TO SECOND`) rather than the fields of the argument.
        let interval = match data_type {
            DataType::Interval(IntervalUnit::YearMonth) => SparkIntervalMetadata::YearMonth {
                start_field: YearMonthIntervalField::Year,
                end_field: YearMonthIntervalField::Month,
            },
            _ => SparkIntervalMetadata::DayTime {
                start_field: DayTimeIntervalField::Day,
                end_field: DayTimeIntervalField::Second,
            },
        };
        let interval = interval
            .to_json()
            .map_err(|e| DataFusionError::Internal(e.to_string()))?;
        Ok(Arc::new(
            Field::new(self.name(), data_type, nullable).with_metadata(HashMap::from([(
                SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(),
                interval,
            )])),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;
        let [interval, num] = args.as_slice() else {
            return exec_err!("{} expects 2 arguments, got {}", self.name(), args.len());
        };
        // A scalar numeric value (e.g. `interval * 2`) is not expanded to an array.
        let (num, scalar) = match num {
            ColumnarValue::Scalar(value) => (value.to_array()?, true),
            ColumnarValue::Array(array) => (Arc::clone(array), false),
        };
        // An array interval keeps its length, and a scalar interval with a scalar numeric value
        // gives a single value.
        let interval = interval.to_array(if scalar { 1 } else { number_rows })?;
        let result: ArrayRef = match interval.data_type() {
            DataType::Interval(IntervalUnit::YearMonth) => Arc::new(evaluate(
                Kind::YearMonth,
                self.op,
                interval.as_primitive::<IntervalYearMonthType>(),
                &num,
                scalar,
            )?),
            DataType::Duration(TimeUnit::Microsecond) => {
                let interval = interval.as_primitive::<DurationMicrosecondType>();
                match day_time_integral(self.op, interval, &num, scalar)? {
                    Some(result) => result,
                    None => Arc::new(evaluate(Kind::DayTime, self.op, interval, &num, scalar)?),
                }
            }
            other => {
                return exec_err!(
                    "{} expects a year-month or day-time interval, got {other}",
                    self.name()
                );
            }
        };
        Ok(ColumnarValue::Array(result))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [interval, num] = arg_types else {
            return plan_err!(
                "{} expects 2 arguments, got {}",
                self.name(),
                arg_types.len()
            );
        };
        if !matches!(
            interval,
            DataType::Interval(IntervalUnit::YearMonth) | DataType::Duration(TimeUnit::Microsecond)
        ) {
            return plan_err!(
                "{} expects a year-month or day-time interval, got {interval}",
                self.name()
            );
        }
        let num = match num {
            DataType::Null => DataType::Int32,
            DataType::Dictionary(_, value) if value.is_numeric() => value.as_ref().clone(),
            num if num.is_numeric() => num.clone(),
            num => {
                return plan_err!("{} expects a numeric value, got {num}", self.name());
            }
        };
        Ok(vec![interval.clone(), num])
    }
}

/// The kind of interval, whose value is a number of months or microseconds.
#[derive(Debug, Clone, Copy)]
enum Kind {
    YearMonth,
    DayTime,
}

impl Kind {
    fn min(self) -> i64 {
        match self {
            Kind::YearMonth => i64::from(i32::MIN),
            Kind::DayTime => i64::MIN,
        }
    }

    /// Converts a result to the interval value, with the error of Spark's `toIntExact` or
    /// `multiplyExact` (year-month interval) or `multiplyExact` (day-time interval).
    fn exact(self, value: i128) -> std::result::Result<i64, ArrowError> {
        match self {
            Kind::YearMonth => i32::try_from(value)
                .map(i64::from)
                .map_err(|_| overflow("integer overflow")),
            Kind::DayTime => i64::try_from(value).map_err(|_| overflow("long overflow")),
        }
    }

    /// Spark rounds a floating point result with `DoubleMath.roundToInt` (year-month interval) or
    /// `DoubleMath.roundToLong` (day-time interval), with `RoundingMode.HALF_UP`.
    fn round(self, value: f64) -> std::result::Result<i64, ArrowError> {
        if !value.is_finite() {
            return Err(overflow("input is infinite or NaN"));
        }
        // `f64::round` rounds half away from zero, which is `HALF_UP`.
        let rounded = value.round();
        let in_range = match self {
            Kind::YearMonth => rounded >= f64::from(i32::MIN) && rounded <= f64::from(i32::MAX),
            // `i64::MAX` is not exactly representable, and rounds up to 2^63.
            Kind::DayTime => rounded >= -(2_f64.powi(63)) && rounded < 2_f64.powi(63),
        };
        if !in_range {
            return Err(overflow(format!(
                "rounded value is out of range for input {value} and rounding mode HALF_UP"
            )));
        }
        Ok(rounded as i64)
    }

    /// Spark rounds a decimal result `HALF_UP` with `BigDecimal.intValueExact` (year-month
    /// interval) or `BigDecimal.longValueExact` (day-time interval), which fail with the message
    /// "Overflow".
    fn exact_decimal(self, value: i256) -> std::result::Result<i64, ArrowError> {
        value
            .to_i128()
            .and_then(|v| match self {
                Kind::YearMonth => i32::try_from(v).ok().map(i64::from),
                Kind::DayTime => i64::try_from(v).ok(),
            })
            .ok_or_else(|| overflow("Overflow"))
    }
}

fn overflow(message: impl Into<String>) -> ArrowError {
    ArrowError::ArithmeticOverflow(message.into())
}

fn evaluate<I>(
    kind: Kind,
    op: IntervalArithmeticOp,
    interval: &PrimitiveArray<I>,
    num: &ArrayRef,
    scalar: bool,
) -> Result<PrimitiveArray<I>>
where
    I: ArrowPrimitiveType,
    I::Native: Into<i64> + TryFrom<i64>,
{
    let int = |long: bool| move |v: i64, n: i64| integral(kind, op, long, v, n);
    let float = move |v: i64, n: f64| floating(kind, op, v, n);
    let result = match num.data_type() {
        DataType::Int8 => {
            apply::<I, Int8Type>(interval, num, scalar, |v, n| int(false)(v, n.into()))
        }
        DataType::Int16 => {
            apply::<I, Int16Type>(interval, num, scalar, |v, n| int(false)(v, n.into()))
        }
        DataType::Int32 => {
            apply::<I, Int32Type>(interval, num, scalar, |v, n| int(false)(v, n.into()))
        }
        DataType::UInt8 => {
            apply::<I, UInt8Type>(interval, num, scalar, |v, n| int(false)(v, n.into()))
        }
        DataType::UInt16 => {
            apply::<I, UInt16Type>(interval, num, scalar, |v, n| int(false)(v, n.into()))
        }
        DataType::Int64 => apply::<I, Int64Type>(interval, num, scalar, int(true)),
        DataType::UInt32 => {
            apply::<I, UInt32Type>(interval, num, scalar, |v, n| int(true)(v, n.into()))
        }
        DataType::UInt64 => apply::<I, UInt64Type>(interval, num, scalar, |v, n| {
            integral_wide(kind, op, v, n.into())
        }),
        DataType::Float16 => {
            apply::<I, Float16Type>(interval, num, scalar, |v, n| float(v, n.into()))
        }
        DataType::Float32 => {
            apply::<I, Float32Type>(interval, num, scalar, |v, n| float(v, n.into()))
        }
        DataType::Float64 => apply::<I, Float64Type>(interval, num, scalar, float),
        DataType::Decimal32(_, scale) => {
            apply::<I, Decimal32Type>(interval, num, scalar, |v, n| {
                decimal(kind, op, v, i256::from(n), *scale)
            })
        }
        DataType::Decimal64(_, scale) => {
            apply::<I, Decimal64Type>(interval, num, scalar, |v, n| {
                decimal(kind, op, v, i256::from(n), *scale)
            })
        }
        DataType::Decimal128(_, scale) => {
            apply::<I, Decimal128Type>(interval, num, scalar, |v, n| {
                decimal(kind, op, v, i256::from_i128(n), *scale)
            })
        }
        DataType::Decimal256(_, scale) => {
            apply::<I, Decimal256Type>(interval, num, scalar, |v, n| {
                decimal(kind, op, v, n, *scale)
            })
        }
        other => return exec_err!("interval arithmetic expects a numeric value, got {other}"),
    };
    // The error is external so that its cause is the Arrow error, which Spark Connect reports as
    // an `ArithmeticException` with the message of the error, like Spark.
    // TODO: Return the Arrow error once `DataFusionError::ArrowError` reports its Arrow error as
    //  the cause (`CommonErrorCause` in `sail-common-datafusion`).
    result.map_err(|e| DataFusionError::External(Box::new(e)))
}

/// Applies `op` to the interval values and the numeric values, or the numeric value of a single
/// row if `scalar` is true.
fn apply<I, N>(
    interval: &PrimitiveArray<I>,
    num: &ArrayRef,
    scalar: bool,
    op: impl Fn(i64, N::Native) -> std::result::Result<i64, ArrowError>,
) -> std::result::Result<PrimitiveArray<I>, ArrowError>
where
    I: ArrowPrimitiveType,
    I::Native: Into<i64> + TryFrom<i64>,
    N: ArrowPrimitiveType,
{
    let to_interval = |v: i64| I::Native::try_from(v).map_err(|_| overflow("integer overflow"));
    let num = num.as_primitive::<N>();
    if scalar {
        if num.is_null(0) {
            return Ok(PrimitiveArray::<I>::new_null(interval.len()));
        }
        let n = num.value(0);
        return try_unary(interval, |v: I::Native| to_interval(op(v.into(), n)?));
    }
    try_binary(interval, num, |v: I::Native, n| {
        to_interval(op(v.into(), n)?)
    })
}

/// Multiplies or divides day-time interval values by integral values (other than unsigned 64-bit
/// values) with Arrow's kernels, which avoid the per-row overhead of [`evaluate`]. The result is
/// the same as with [`integral`]. Returns `None` for values that are evaluated row by row: a
/// divisor that is not a scalar, or a scalar divisor that is NULL, zero, or -1 (where the errors
/// depend on the rows).
fn day_time_integral(
    op: IntervalArithmeticOp,
    interval: &PrimitiveArray<DurationMicrosecondType>,
    num: &ArrayRef,
    scalar: bool,
) -> Result<Option<ArrayRef>> {
    use DataType::*;

    if !matches!(
        num.data_type(),
        Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32
    ) {
        return Ok(None);
    }
    let num = cast(num, &Int64)?;
    let num = num.as_primitive::<Int64Type>();
    let micros = interval.reinterpret_cast::<Int64Type>();
    let result: PrimitiveArray<Int64Type> = match op {
        IntervalArithmeticOp::Multiply => {
            // Like `Math.multiplyExact`, Arrow's multiplication is checked.
            let product = if scalar {
                mul(&micros, &Scalar::new(num))
            } else {
                mul(&micros, num)
            };
            let product = product.map_err(|e| match e {
                ArrowError::ArithmeticOverflow(_) => {
                    DataFusionError::External(Box::new(overflow("long overflow")))
                }
                e => e.into(),
            })?;
            product.as_primitive::<Int64Type>().clone()
        }
        IntervalArithmeticOp::Divide => {
            if !scalar || num.is_null(0) {
                return Ok(None);
            }
            let divisor = num.value(0);
            if divisor == 0 || divisor == -1 {
                return Ok(None);
            }
            micros.unary(|value| half_up_divide(value, divisor))
        }
    };
    Ok(Some(Arc::new(
        result.reinterpret_cast::<DurationMicrosecondType>(),
    )))
}

/// Divides `value` by `divisor` and rounds the quotient half away from zero (`HALF_UP`), for a
/// divisor that is neither zero nor -1 with the minimum value.
fn half_up_divide(value: i64, divisor: i64) -> i64 {
    let quotient = value / divisor;
    let remainder = (value % divisor).unsigned_abs();
    // The adjustment cannot overflow, since the divisor is at least 2 in magnitude if the
    // remainder is not zero.
    if remainder != 0 && remainder >= divisor.unsigned_abs() - remainder {
        quotient + value.signum() * divisor.signum()
    } else {
        quotient
    }
}

/// Spark multiplies with `Math.multiplyExact` and divides with `IntMath.divide` or
/// `LongMath.divide` (`HALF_UP`). For a year-month interval, a long value is multiplied as a
/// long before the product is converted to an integer, and a smaller integral value cannot
/// overflow a long.
fn integral(
    kind: Kind,
    op: IntervalArithmeticOp,
    long: bool,
    value: i64,
    num: i64,
) -> std::result::Result<i64, ArrowError> {
    match op {
        IntervalArithmeticOp::Multiply => match value.checked_mul(num) {
            Some(product) => kind.exact(i128::from(product)),
            None if matches!(kind, Kind::YearMonth) && !long => Err(overflow("integer overflow")),
            None => Err(overflow("long overflow")),
        },
        IntervalArithmeticOp::Divide => {
            if value == kind.min() && num == -1 {
                return Err(overflow(DIVIDE_OVERFLOW));
            }
            if num == 0 {
                return Err(overflow(DIVIDED_BY_ZERO));
            }
            // `i64::MIN / -1` is excluded above, since the value is the minimum of its kind.
            kind.exact(i128::from(half_up_divide(value, num)))
        }
    }
}

/// The same as [`integral`] for an unsigned 64-bit value, which may not fit in a long.
fn integral_wide(
    kind: Kind,
    op: IntervalArithmeticOp,
    value: i64,
    num: i128,
) -> std::result::Result<i64, ArrowError> {
    let value = i128::from(value);
    match op {
        IntervalArithmeticOp::Multiply => {
            // The product of a 64-bit value and a 64-bit value fits in 128 bits.
            let product = value * num;
            if i64::try_from(product).is_err() {
                return Err(overflow("long overflow"));
            }
            kind.exact(product)
        }
        IntervalArithmeticOp::Divide => {
            if value == i128::from(kind.min()) && num == -1 {
                return Err(overflow(DIVIDE_OVERFLOW));
            }
            if num == 0 {
                return Err(overflow(DIVIDED_BY_ZERO));
            }
            let quotient = value / num;
            let remainder = value % num;
            // `|remainder| < |num| <= 2^64`, so the comparison does not overflow.
            let quotient = if 2 * remainder.abs() >= num.abs() {
                quotient + value.signum() * num.signum()
            } else {
                quotient
            };
            kind.exact(quotient)
        }
    }
}

/// Spark computes `value * num` or `value / num` as a double and rounds it `HALF_UP`.
fn floating(
    kind: Kind,
    op: IntervalArithmeticOp,
    value: i64,
    num: f64,
) -> std::result::Result<i64, ArrowError> {
    let result = match op {
        IntervalArithmeticOp::Multiply => value as f64 * num,
        IntervalArithmeticOp::Divide => {
            if num == 0.0 {
                return Err(overflow(DIVIDED_BY_ZERO));
            }
            value as f64 / num
        }
    };
    kind.round(result)
}

/// Spark computes the exact decimal result and rounds it `HALF_UP` to an integer.
///
/// A result that cannot be computed in `i256` (only possible for `Decimal256` values with more
/// than 57 digits) is an overflow.
fn decimal(
    kind: Kind,
    op: IntervalArithmeticOp,
    value: i64,
    num: i256,
    scale: i8,
) -> std::result::Result<i64, ArrowError> {
    let error = || overflow("Overflow");
    let value = i256::from(value);
    let power = i256::from_i128(10)
        .checked_pow(u32::from(scale.unsigned_abs()))
        .ok_or_else(error)?;
    let (numerator, denominator) = match op {
        IntervalArithmeticOp::Multiply => {
            let product = value.checked_mul(num).ok_or_else(error)?;
            if scale > 0 {
                (product, power)
            } else {
                (product.checked_mul(power).ok_or_else(error)?, i256::ONE)
            }
        }
        IntervalArithmeticOp::Divide => {
            if num == i256::ZERO {
                return Err(overflow(DIVIDED_BY_ZERO));
            }
            if scale > 0 {
                (value.checked_mul(power).ok_or_else(error)?, num)
            } else {
                (value, num.checked_mul(power).ok_or_else(error)?)
            }
        }
    };
    let quotient = numerator.checked_div(denominator).ok_or_else(error)?;
    let remainder = numerator.checked_rem(denominator).ok_or_else(error)?;
    let remainder = remainder.checked_abs().ok_or_else(error)?;
    let denominator_abs = denominator.checked_abs().ok_or_else(error)?;
    // `|remainder| >= |denominator| / 2`, written so that it does not overflow.
    let rounded = if remainder != i256::ZERO && remainder >= denominator_abs.wrapping_sub(remainder)
    {
        let sign = numerator.signum().wrapping_mul(denominator.signum());
        quotient.checked_add(sign).ok_or_else(error)?
    } else {
        quotient
    };
    kind.exact_decimal(rounded)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{
        Decimal128Array, Decimal256Array, DurationMicrosecondArray, Float16Array, Float32Array,
        Float64Array, Int8Array, Int32Array, Int64Array, IntervalYearMonthArray, UInt32Array,
        UInt64Array,
    };
    use datafusion_common::ScalarValue;
    use datafusion_common::config::ConfigOptions;
    use half::f16;

    use super::*;

    const YEAR_MONTH: DataType = DataType::Interval(IntervalUnit::YearMonth);
    const DAY_TIME: DataType = DataType::Duration(TimeUnit::Microsecond);

    fn invoke(
        op: IntervalArithmeticOp,
        interval: ArrayRef,
        num: ArrayRef,
    ) -> Result<Vec<Option<i64>>> {
        let len = interval.len();
        let return_type = interval.data_type().clone();
        let arg_fields = vec![
            Arc::new(Field::new("a", interval.data_type().clone(), true)),
            Arc::new(Field::new("b", num.data_type().clone(), true)),
        ];
        let result = SparkIntervalArithmetic::new(op).invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(interval), ColumnarValue::Array(num)],
            arg_fields,
            number_rows: len,
            return_field: Arc::new(Field::new("r", return_type, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })?;
        let ColumnarValue::Array(array) = result else {
            return exec_err!("expected an array");
        };
        Ok(match array.data_type() {
            DataType::Interval(IntervalUnit::YearMonth) => array
                .as_primitive::<IntervalYearMonthType>()
                .iter()
                .map(|v| v.map(i64::from))
                .collect(),
            _ => array
                .as_primitive::<DurationMicrosecondType>()
                .iter()
                .collect(),
        })
    }

    fn ym(months: Vec<Option<i32>>) -> ArrayRef {
        Arc::new(IntervalYearMonthArray::from(months))
    }

    fn dt(micros: Vec<Option<i64>>) -> ArrayRef {
        Arc::new(DurationMicrosecondArray::from(micros))
    }

    fn multiply(interval: ArrayRef, num: ArrayRef) -> Result<Vec<Option<i64>>> {
        invoke(IntervalArithmeticOp::Multiply, interval, num)
    }

    fn divide(interval: ArrayRef, num: ArrayRef) -> Result<Vec<Option<i64>>> {
        invoke(IntervalArithmeticOp::Divide, interval, num)
    }

    fn error(result: Result<Vec<Option<i64>>>) -> String {
        match result {
            Ok(v) => format!("unexpected success: {v:?}"),
            Err(e) => e.to_string(),
        }
    }

    #[test]
    fn test_multiply_year_month_interval() -> Result<()> {
        let num: ArrayRef = Arc::new(Int8Array::from(vec![Some(2), Some(-3), None, Some(2)]));
        assert_eq!(
            multiply(ym(vec![Some(15), Some(15), Some(15), None]), num)?,
            vec![Some(30), Some(-45), None, None]
        );
        let num: ArrayRef = Arc::new(UInt32Array::from(vec![4_000_000_000]));
        assert_eq!(multiply(ym(vec![Some(0)]), num)?, vec![Some(0)]);
        let num: ArrayRef = Arc::new(Int64Array::from(vec![-2_147_483_648]));
        assert_eq!(
            multiply(ym(vec![Some(1)]), num)?,
            vec![Some(i64::from(i32::MIN))]
        );
        let num: ArrayRef = Arc::new(Float64Array::from(vec![1.5, 2.5, -2.5, -0.5, 0.1, 0.49]));
        assert_eq!(
            multiply(
                ym(vec![Some(15), Some(3), Some(3), Some(1), Some(15), Some(1)]),
                num
            )?,
            vec![Some(23), Some(8), Some(-8), Some(-1), Some(2), Some(0)]
        );
        let num: ArrayRef = Arc::new(Float32Array::from(vec![1.5_f32]));
        assert_eq!(multiply(ym(vec![Some(15)]), num)?, vec![Some(23)]);
        let num: ArrayRef = Arc::new(Float16Array::from(vec![f16::from_f32(0.5)]));
        assert_eq!(multiply(ym(vec![Some(-3)]), num)?, vec![Some(-2)]);
        let num: ArrayRef = Arc::new(
            Decimal128Array::from(vec![125, -250, 150, 5, 249]).with_precision_and_scale(10, 2)?,
        );
        assert_eq!(
            multiply(
                ym(vec![Some(7), Some(1), Some(15), Some(-10), Some(1)]),
                num
            )?,
            vec![Some(9), Some(-3), Some(23), Some(-1), Some(2)]
        );
        let num: ArrayRef = Arc::new(
            Decimal128Array::from(vec![25 * 10_i128.pow(36)]).with_precision_and_scale(38, 37)?,
        );
        assert_eq!(multiply(ym(vec![Some(15)]), num)?, vec![Some(38)]);
        let num: ArrayRef =
            Arc::new(Decimal128Array::from(vec![3]).with_precision_and_scale(5, -2)?);
        assert_eq!(multiply(ym(vec![Some(2)]), num)?, vec![Some(600)]);
        let num: ArrayRef = Arc::new(
            Decimal256Array::from(vec![i256::from_i128(5 * 10_i128.pow(37))])
                .with_precision_and_scale(76, 38)?,
        );
        assert_eq!(multiply(ym(vec![Some(3)]), num)?, vec![Some(2)]);
        Ok(())
    }

    #[test]
    fn test_multiply_year_month_interval_overflow() -> Result<()> {
        let cases: Vec<(i32, ArrayRef, &str)> = vec![
            (
                2,
                Arc::new(Int32Array::from(vec![i32::MAX])),
                "integer overflow",
            ),
            (
                2,
                Arc::new(Int64Array::from(vec![2_147_483_647])),
                "integer overflow",
            ),
            (
                2,
                Arc::new(Int64Array::from(vec![i64::MAX])),
                "long overflow",
            ),
            (
                2,
                Arc::new(UInt64Array::from(vec![u64::MAX])),
                "long overflow",
            ),
            (
                1,
                Arc::new(Float64Array::from(vec![2_147_483_647.5])),
                "out of range",
            ),
            (
                1,
                Arc::new(Float64Array::from(vec![-2_147_483_648.5])),
                "out of range",
            ),
            (
                1,
                Arc::new(Float64Array::from(vec![f64::NAN])),
                "infinite or NaN",
            ),
            (
                1,
                Arc::new(Float64Array::from(vec![f64::INFINITY])),
                "infinite or NaN",
            ),
        ];
        for (months, num, message) in cases {
            let e = error(multiply(ym(vec![Some(months)]), num));
            assert!(e.contains(message), "{e}");
        }
        let num: ArrayRef =
            Arc::new(Decimal128Array::from(vec![21_474_836_475]).with_precision_and_scale(11, 1)?);
        assert!(error(multiply(ym(vec![Some(1)]), num)).contains("Overflow"));
        Ok(())
    }

    #[test]
    fn test_divide_year_month_interval() -> Result<()> {
        let num: ArrayRef = Arc::new(Int32Array::from(vec![2, 4, 2, -2, 2, 3]));
        assert_eq!(
            divide(
                ym(vec![
                    Some(15),
                    Some(-15),
                    Some(1),
                    Some(1),
                    Some(-13),
                    Some(13)
                ]),
                num
            )?,
            vec![Some(8), Some(-4), Some(1), Some(-1), Some(-7), Some(4)]
        );
        let num: ArrayRef = Arc::new(Int64Array::from(vec![3_000_000_000]));
        assert_eq!(divide(ym(vec![Some(13)]), num)?, vec![Some(0)]);
        let num: ArrayRef = Arc::new(Float64Array::from(vec![f64::INFINITY, 0.4, -2.5]));
        assert_eq!(
            divide(ym(vec![Some(13), Some(1), Some(5)]), num)?,
            vec![Some(0), Some(3), Some(-2)]
        );
        let num: ArrayRef =
            Arc::new(Decimal128Array::from(vec![26, 3]).with_precision_and_scale(3, 1)?);
        assert_eq!(
            divide(ym(vec![Some(13), Some(15)]), num)?,
            vec![Some(5), Some(50)]
        );
        // NULL values are not divided.
        let num: ArrayRef = Arc::new(Int32Array::from(vec![Some(0), None]));
        assert_eq!(divide(ym(vec![None, Some(1)]), num)?, vec![None, None]);
        Ok(())
    }

    #[test]
    fn test_divide_by_zero_and_overflow() -> Result<()> {
        let zeros: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![0])),
            Arc::new(Int64Array::from(vec![0])),
            Arc::new(Float64Array::from(vec![-0.0])),
            Arc::new(Decimal128Array::from(vec![0]).with_precision_and_scale(3, 1)?),
        ];
        for zero in zeros {
            let e = error(divide(ym(vec![Some(1)]), zero.clone()));
            assert!(e.contains("INTERVAL_DIVIDED_BY_ZERO"), "{e}");
            let e = error(divide(dt(vec![Some(1)]), zero));
            assert!(e.contains("INTERVAL_DIVIDED_BY_ZERO"), "{e}");
        }
        let minus_one: ArrayRef = Arc::new(Int64Array::from(vec![-1]));
        let e = error(divide(ym(vec![Some(i32::MIN)]), minus_one.clone()));
        assert!(e.contains("INTERVAL_ARITHMETIC_OVERFLOW"), "{e}");
        let e = error(divide(dt(vec![Some(i64::MIN)]), minus_one));
        assert!(e.contains("INTERVAL_ARITHMETIC_OVERFLOW"), "{e}");
        let num: ArrayRef = Arc::new(Float64Array::from(vec![f64::NAN]));
        assert!(error(divide(ym(vec![Some(1)]), num)).contains("infinite or NaN"));
        let num: ArrayRef = Arc::new(Float64Array::from(vec![1e-10]));
        assert!(error(divide(ym(vec![Some(1)]), num)).contains("out of range"));
        let num: ArrayRef =
            Arc::new(Decimal128Array::from(vec![-10]).with_precision_and_scale(2, 1)?);
        assert!(error(divide(ym(vec![Some(i32::MIN)]), num)).contains("Overflow"));
        Ok(())
    }

    #[test]
    fn test_day_time_interval() -> Result<()> {
        let num: ArrayRef = Arc::new(Float64Array::from(vec![1.5, 0.5, -0.5, 2.5, -1.5]));
        assert_eq!(
            multiply(dt(vec![Some(1); 5]), num)?,
            vec![Some(2), Some(1), Some(-1), Some(3), Some(-2)]
        );
        let num: ArrayRef = Arc::new(Int32Array::from(vec![2, -2, 3, 7]));
        assert_eq!(
            divide(
                dt(vec![Some(3), Some(3), Some(1), Some(86_400_000_000)]),
                num
            )?,
            vec![Some(2), Some(-2), Some(0), Some(12_342_857_143)]
        );
        let num: ArrayRef = Arc::new(Float64Array::from(vec![0.4]));
        assert_eq!(divide(dt(vec![Some(1)]), num)?, vec![Some(3)]);
        let num: ArrayRef =
            Arc::new(Decimal128Array::from(vec![15]).with_precision_and_scale(2, 1)?);
        assert_eq!(divide(dt(vec![Some(5)]), num)?, vec![Some(3)]);
        let num: ArrayRef = Arc::new(Int32Array::from(vec![2]));
        assert!(error(multiply(dt(vec![Some(i64::MAX)]), num)).contains("long overflow"));
        let num: ArrayRef = Arc::new(Float64Array::from(vec![1.0]));
        assert!(error(multiply(dt(vec![Some(i64::MAX)]), num)).contains("out of range"));
        let num: ArrayRef = Arc::new(Float64Array::from(vec![1.0]));
        assert_eq!(
            multiply(dt(vec![Some(i64::MIN)]), num)?,
            vec![Some(i64::MIN)]
        );
        Ok(())
    }

    #[test]
    fn test_scalar_numeric_value() -> Result<()> {
        let invoke_scalar = |interval: ColumnarValue, num: ScalarValue, rows: usize| {
            let arg_fields = vec![
                Arc::new(Field::new("a", YEAR_MONTH, true)),
                Arc::new(Field::new("b", num.data_type(), true)),
            ];
            SparkIntervalArithmetic::new(IntervalArithmeticOp::Divide).invoke_with_args(
                ScalarFunctionArgs {
                    args: vec![interval, ColumnarValue::Scalar(num)],
                    arg_fields,
                    number_rows: rows,
                    return_field: Arc::new(Field::new("r", YEAR_MONTH, true)),
                    config_options: Arc::new(ConfigOptions::default()),
                },
            )
        };
        let values = |result: ColumnarValue| -> Result<Vec<Option<i32>>> {
            Ok(result
                .to_array(1)?
                .as_primitive::<IntervalYearMonthType>()
                .iter()
                .collect())
        };
        let array = ColumnarValue::Array(ym(vec![Some(15), None, Some(-15)]));
        assert_eq!(
            values(invoke_scalar(
                array.clone(),
                ScalarValue::Int32(Some(2)),
                3
            )?)?,
            vec![Some(8), None, Some(-8)]
        );
        assert_eq!(
            values(invoke_scalar(array, ScalarValue::Int32(None), 3)?)?,
            vec![None, None, None]
        );
        let scalar = ColumnarValue::Scalar(ScalarValue::IntervalYearMonth(Some(15)));
        assert_eq!(
            values(invoke_scalar(scalar, ScalarValue::Float64(Some(2.0)), 4)?)?,
            vec![Some(8)]
        );
        let array = ColumnarValue::Array(ym(vec![Some(1)]));
        let e = error_message(invoke_scalar(array, ScalarValue::Int64(Some(0)), 1));
        assert!(e.contains("INTERVAL_DIVIDED_BY_ZERO"), "{e}");
        Ok(())
    }

    /// Day-time intervals with integral values use Arrow's kernels.
    #[test]
    fn test_day_time_interval_integral_kernels() -> Result<()> {
        let invoke_dt = |op: IntervalArithmeticOp, micros: Vec<Option<i64>>, num: ColumnarValue| {
            let rows = micros.len();
            let num_type = match &num {
                ColumnarValue::Scalar(value) => value.data_type(),
                ColumnarValue::Array(array) => array.data_type().clone(),
            };
            let arg_fields = vec![
                Arc::new(Field::new("a", DAY_TIME, true)),
                Arc::new(Field::new("b", num_type, true)),
            ];
            let result = SparkIntervalArithmetic::new(op).invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(dt(micros)), num],
                arg_fields,
                number_rows: rows,
                return_field: Arc::new(Field::new("r", DAY_TIME, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })?;
            Ok::<_, DataFusionError>(
                result
                    .to_array(rows)?
                    .as_primitive::<DurationMicrosecondType>()
                    .iter()
                    .collect::<Vec<_>>(),
            )
        };
        let values = vec![Some(1), None, Some(-3), Some(86_400_000_000)];
        let multiply = IntervalArithmeticOp::Multiply;
        let divide = IntervalArithmeticOp::Divide;
        let scalar = |value: ScalarValue| ColumnarValue::Scalar(value);
        assert_eq!(
            invoke_dt(
                multiply,
                values.clone(),
                scalar(ScalarValue::Int32(Some(2)))
            )?,
            vec![Some(2), None, Some(-6), Some(172_800_000_000)]
        );
        assert_eq!(
            invoke_dt(
                multiply,
                values.clone(),
                ColumnarValue::Array(Arc::new(Int8Array::from(vec![
                    Some(-1),
                    Some(5),
                    None,
                    Some(3)
                ])))
            )?,
            vec![Some(-1), None, None, Some(259_200_000_000)]
        );
        assert_eq!(
            invoke_dt(divide, values.clone(), scalar(ScalarValue::Int32(Some(2))))?,
            vec![Some(1), None, Some(-2), Some(43_200_000_000)]
        );
        assert_eq!(
            invoke_dt(divide, values.clone(), scalar(ScalarValue::Int64(Some(7))))?,
            vec![Some(0), None, Some(0), Some(12_342_857_143)]
        );
        assert_eq!(
            invoke_dt(divide, values.clone(), scalar(ScalarValue::Int16(Some(-2))))?,
            vec![Some(-1), None, Some(2), Some(-43_200_000_000)]
        );
        assert_eq!(
            invoke_dt(divide, values, scalar(ScalarValue::Int32(None)))?,
            vec![None, None, None, None]
        );
        let error = |result: Result<Vec<Option<i64>>>| match result {
            Ok(v) => format!("unexpected success: {v:?}"),
            Err(e) => e.to_string(),
        };
        let e = error(invoke_dt(
            multiply,
            vec![Some(i64::MAX)],
            scalar(ScalarValue::Int32(Some(2))),
        ));
        assert!(e.contains("long overflow"), "{e}");
        let e = error(invoke_dt(
            divide,
            vec![Some(1)],
            scalar(ScalarValue::Int32(Some(0))),
        ));
        assert!(e.contains("INTERVAL_DIVIDED_BY_ZERO"), "{e}");
        assert_eq!(
            invoke_dt(divide, vec![None], scalar(ScalarValue::Int32(Some(0))))?,
            vec![None]
        );
        let e = error(invoke_dt(
            divide,
            vec![Some(i64::MIN)],
            scalar(ScalarValue::Int64(Some(-1))),
        ));
        assert!(e.contains("INTERVAL_ARITHMETIC_OVERFLOW"), "{e}");
        Ok(())
    }

    fn error_message(result: Result<ColumnarValue>) -> String {
        match result {
            Ok(_) => "unexpected success".to_string(),
            Err(e) => e.to_string(),
        }
    }

    #[test]
    fn test_coerce_types() -> Result<()> {
        let udf = SparkIntervalArithmetic::new(IntervalArithmeticOp::Multiply);
        assert_eq!(
            udf.coerce_types(&[YEAR_MONTH, DataType::Null])?,
            vec![YEAR_MONTH, DataType::Int32]
        );
        assert_eq!(
            udf.coerce_types(&[DAY_TIME, DataType::Decimal128(10, 2)])?,
            vec![DAY_TIME, DataType::Decimal128(10, 2)]
        );
        assert!(udf.coerce_types(&[YEAR_MONTH, DataType::Utf8]).is_err());
        assert!(udf.coerce_types(&[YEAR_MONTH, DataType::Boolean]).is_err());
        assert!(
            udf.coerce_types(&[
                DataType::Interval(IntervalUnit::MonthDayNano),
                DataType::Int32
            ])
            .is_err()
        );
        Ok(())
    }
}
