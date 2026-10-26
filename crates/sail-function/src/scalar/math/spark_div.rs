use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int64Array};
use datafusion::arrow::datatypes::{
    DataType, Decimal128Type, Field, FieldRef, Int64Type, IntervalUnit, IntervalYearMonthType, i256,
};
use datafusion::arrow::error::ArrowError;
use datafusion_common::{DataFusionError, Result, internal_err};
use datafusion_expr::type_coercion::binary::BinaryTypeCoercer;
use datafusion_expr::{
    ColumnarValue, Operator, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};

use crate::error::{
    invalid_arg_count_exec_err, unsupported_data_type_exec_err, unsupported_data_types_exec_err,
};
use crate::functions_nested_utils::make_scalar_function;

/// Spark's `DIVIDE_BY_ZERO` message, verbatim (SQLSTATE 22012).
/// `ArrowError::DivideByZero` renders as "Divide by zero error", which diverges.
pub const DIVIDE_BY_ZERO_MESSAGE: &str = "[DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 \
     and return NULL instead. If necessary set \"spark.sql.ansi.enabled\" to \"false\" \
     to bypass this error. SQLSTATE: 22012";

/// Spark's `ARITHMETIC_OVERFLOW` message for integral divide, verbatim (SQLSTATE 22003).
const INTEGRAL_DIVIDE_OVERFLOW_MESSAGE: &str = "[ARITHMETIC_OVERFLOW] Overflow in integral divide. Use 'try_divide' to tolerate \
     overflow and return NULL instead. If necessary set \"spark.sql.ansi.enabled\" to \
     \"false\" to bypass this error. SQLSTATE: 22003";

/// Spark's `NUMERIC_VALUE_OUT_OF_RANGE` prefix: the quotient does not fit `resultDecimalType`.
/// The value and precision are per-row, so unlike the two above this one is built at runtime.
const DECIMAL_OUT_OF_RANGE_PREFIX: &str = "[NUMERIC_VALUE_OUT_OF_RANGE";

fn decimal_out_of_range_err(value: &i256, precision: u32) -> ArrowError {
    ArrowError::ComputeError(format!(
        "[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION]  {value} cannot be represented as \
         Decimal({precision}, 0). If necessary set \"spark.sql.ansi.enabled\" to \"false\" \
         to bypass this error, and return NULL instead. SQLSTATE: 22003"
    ))
}

/// Preserve Spark's known arithmetic messages as execution errors so Connect maps them to
/// `ArithmeticException`. Other Arrow compute errors retain their normal classification.
/// Only messages this module builds carry a `[CLASS]` tag, so the prefix test cannot capture
/// an Arrow-internal error.
fn spark_error(e: ArrowError) -> DataFusionError {
    match e {
        ArrowError::ComputeError(message)
            if message == DIVIDE_BY_ZERO_MESSAGE
                || message == INTEGRAL_DIVIDE_OVERFLOW_MESSAGE
                || message.starts_with(DECIMAL_OUT_OF_RANGE_PREFIX) =>
        {
            DataFusionError::Execution(message)
        }
        other => DataFusionError::from(other),
    }
}

fn divide_by_zero_err() -> ArrowError {
    ArrowError::ComputeError(DIVIDE_BY_ZERO_MESSAGE.to_string())
}

fn integral_divide_overflow_err() -> ArrowError {
    ArrowError::ComputeError(INTEGRAL_DIVIDE_OVERFLOW_MESSAGE.to_string())
}

/// Spark's div operator for intervals.
/// Performs integer division between two intervals of the same type.
/// Under ANSI=true, zero divisor raises an error; under ANSI=false, returns NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkIntervalDiv {
    signature: Signature,
    ansi_mode: bool,
}

impl SparkIntervalDiv {
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

impl ScalarUDFImpl for SparkIntervalDiv {
    fn name(&self) -> &str {
        "spark_div"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `div` is `IntegralDivide`, which extends `DivModLike`:
    /// `nullable = true`, unconditional (ANSI does not narrow it).
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala#L658>
    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Int64, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ansi = self.ansi_mode;
        make_scalar_function(move |arrs: &[ArrayRef]| interval_div_inner(arrs, ansi))(&args.args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [dividend, divisor] = arg_types else {
            return Err(invalid_arg_count_exec_err(
                "spark_div",
                (2, 2),
                arg_types.len(),
            ));
        };
        match (dividend, divisor) {
            // Only YEAR TO MONTH reaches here: DAY TO SECOND resolves to `Duration`, and
            // `MonthDayNano` is `CalendarIntervalType`, which Spark rejects.
            (
                DataType::Interval(IntervalUnit::YearMonth),
                DataType::Interval(IntervalUnit::YearMonth),
            ) => Ok(arg_types.to_vec()),
            _ => Err(unsupported_data_types_exec_err(
                "spark_interval_div",
                "INTERVAL YEAR TO MONTH / INTERVAL YEAR TO MONTH",
                arg_types,
            )),
        }
    }
}

/// Spark-compatible integer division (`div` / `DIV` operator).
///
/// Spark uses `Integral.quot`, i.e. Java `/`: truncating, and wrapping on `LONG_MIN / -1`
/// outside ANSI. Under ANSI this UDF also owns the zero-divisor error.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkIntegerDiv {
    signature: Signature,
    ansi_mode: bool,
    /// Spark's `IntegralDivide.checkDivideOverflow` (arithmetic.scala:885-888): true only when
    /// the DIVIDEND is LONG and ANSI is on. An interval dividend is never overflow-checked, so
    /// `LONG_MIN / -1` wraps there in BOTH ANSI modes.
    check_overflow: bool,
}

impl SparkIntegerDiv {
    pub fn new(ansi_mode: bool, check_overflow: bool) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            ansi_mode,
            check_overflow,
        }
    }

    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }

    pub fn check_overflow(&self) -> bool {
        self.check_overflow
    }
}

impl ScalarUDFImpl for SparkIntegerDiv {
    fn name(&self) -> &str {
        "spark_integer_div"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return Err(invalid_arg_count_exec_err(
                "spark_integer_div",
                (2, 2),
                arg_types.len(),
            ));
        }
        // Spark does NOT bring the two DECIMAL operands to a common type for `div` — the
        // analyzed plan keeps both — so a decimal pair is passed through untouched and the
        // kernel divides at 256-bit. Integral operands still widen to LONG, per
        // `IntegralDivisionTypeCoercion` (which casts BYTE/SHORT/INT and leaves the rest).
        //
        // A MIXED pair goes through DataFusion's own binary coercion (as #2822 does) rather
        // than `DecimalType.forType`. The builder promotes a mixed pair with `forType`, which
        // is Spark's rule and gives scale 0 — correct when the operand really is integral. But
        // this function also sees pairs the builder could not classify, because it decides from
        // the type `get_type` reports at PLAN time and that can disagree with the resolved one:
        // Sail types `CASE WHEN false THEN 0 ELSE 1.5 END` from its first branch as INT
        // (measured: `typeof` says `int`, the value is 1.5), so the builder takes the integral
        // arm and hands this function INT / DECIMAL. Promoting that to scale 0 would round 1.5
        // to 2 and answer 4 where Spark and `main` answer 3; resolving the common type keeps
        // the scale and the value.
        match arg_types {
            [l, r] if l.is_integer() && r.is_integer() => {
                Ok(vec![DataType::Int64, DataType::Int64])
            }
            [DataType::Decimal128(..), DataType::Decimal128(..)] => Ok(arg_types.to_vec()),
            [l, r] if is_mixed_integral_decimal(l, r) => {
                // Two rules, because neither covers the pair alone.
                //
                // DataFusion's binary coercion first: it resolves a common decimal that keeps
                // the SCALE, which matters because the declared type can understate the value.
                // Sail types `CASE WHEN false THEN 0 ELSE 1.5 END` from its first branch as INT
                // (measured: `typeof` says `int`, the value is 1.5), so this function is handed
                // INT / DECIMAL for an operand that is really decimal; rounding it to scale 0
                // answers 4 where Spark and `main` answer 3 (#2822 pins that shape).
                //
                // It cannot always succeed: `Int64 / DECIMAL(38,37)` has no common type, since
                // the integral side needs 20 digits and only 1 is left. There Spark's own rule
                // applies — `DecimalType.forType` (DecimalType.scala:133-140), scale 0 — and the
                // kernel divides the unaligned pair at 256 bits. Losing nothing: an integral
                // operand has no fractional part for scale 0 to drop, and the resulting
                // `resultDecimalType` precision is unobservable (its bound cannot be reached).
                match BinaryTypeCoercer::new(l, &Operator::Divide, r).get_input_types() {
                    Ok((l, r)) => Ok(vec![l, r]),
                    Err(_) => Ok(vec![decimal_for_integral(l)?, decimal_for_integral(r)?]),
                }
            }
            _ => Err(unsupported_data_types_exec_err(
                "spark_integer_div",
                "integral / integral, DECIMAL / DECIMAL, or integral / DECIMAL",
                arg_types,
            )),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ansi = self.ansi_mode;
        let check_overflow = self.check_overflow;
        make_scalar_function(
            move |arrs: &[ArrayRef]| match arrs.first().map(|a| a.data_type()) {
                Some(DataType::Decimal128(..)) => decimal_div_inner(arrs, ansi),
                _ => integer_div_inner(arrs, ansi, check_overflow),
            },
        )(&args.args)
    }
}

/// Spark's `div` over DECIMAL operands. `IntegralDivide` selects
/// `PhysicalDecimalType(p, s).asIntegral`, i.e. `BigDecimal.divideToIntegralValue`
/// (arithmetic.scala:911-921), so three rules differ from a plain decimal division:
///
///   1. The operand scales are NOT aligned. Spark's analyzed plan keeps both operand types, and
///      BigDecimal divides at arbitrary precision. Rescaling one side first is what makes
///      DataFusion's `/` overflow (or fail to even resolve a result type) on wide scales.
///   2. The quotient is checked against `resultDecimalType` = `bounded(p1 - s1 + s2, 0)`
///      (arithmetic.scala:899-906) — scale 0, because the quotient is integral. Overflow raises
///      `NUMERIC_VALUE_OUT_OF_RANGE` under ANSI and yields NULL otherwise.
///   3. The narrowing to BIGINT is `Decimal.toLong` = `BigDecimal.longValue()`, which keeps the
///      low 64 bits and therefore WRAPS instead of failing.
///
/// `a/10^s1 / (b/10^s2)` is computed as `a*10^s2 / (b*10^s1)` in 256-bit: each factor is below
/// 10^38, so neither product can leave `i256`.
/// `DecimalType.forType` (DecimalType.scala:133-140): the minimal decimal that holds an integral
/// type. A decimal argument is returned untouched, so the pair can be mapped in one pass.
fn decimal_for_integral(data_type: &DataType) -> Result<DataType> {
    let precision = match data_type {
        DataType::Int8 | DataType::UInt8 => 3,
        DataType::Int16 | DataType::UInt16 => 5,
        DataType::Int32 | DataType::UInt32 => 10,
        DataType::Int64 | DataType::UInt64 => 20,
        other @ DataType::Decimal128(..) => return Ok(other.clone()),
        other => {
            return Err(unsupported_data_type_exec_err(
                "spark_integer_div",
                "an integral or DECIMAL type",
                other,
            ));
        }
    };
    Ok(DataType::Decimal128(precision, 0))
}

/// One side integral and the other `Decimal128`, in either order.
fn is_mixed_integral_decimal(left: &DataType, right: &DataType) -> bool {
    matches!(
        (left, right),
        (l, DataType::Decimal128(..)) if l.is_integer()
    ) || matches!(
        (left, right),
        (DataType::Decimal128(..), r) if r.is_integer()
    )
}

fn decimal_div_inner(args: &[ArrayRef], ansi: bool) -> Result<ArrayRef> {
    let [dividend, divisor] = args else {
        return Err(invalid_arg_count_exec_err(
            "spark_integer_div",
            (2, 2),
            args.len(),
        ));
    };
    let (DataType::Decimal128(p1, s1), DataType::Decimal128(_, s2)) =
        (dividend.data_type(), divisor.data_type())
    else {
        return Err(unsupported_data_types_exec_err(
            "spark_integer_div",
            "DECIMAL / DECIMAL",
            &[dividend.data_type().clone(), divisor.data_type().clone()],
        ));
    };
    // `10i128.pow(39)` overflows, so the scale needs the same bound `precision` gets below;
    // Arrow's `Decimal128` scale is an unvalidated `i8`, nothing in the type system caps it at 38.
    let (Ok(scale_dividend @ 0..=38), Ok(scale_divisor @ 0..=38)) =
        (u32::try_from(*s1), u32::try_from(*s2))
    else {
        return Err(unsupported_data_types_exec_err(
            "spark_integer_div",
            "DECIMAL with a scale in [0, 38]",
            &[dividend.data_type().clone(), divisor.data_type().clone()],
        ));
    };
    let int_digits = i32::from(*p1) - i32::from(*s1) + i32::from(*s2);
    let precision = int_digits.clamp(1, 38) as u32;
    let limit = i256::from_i128(10i128.pow(precision));
    let neg_limit = i256::from_i128(-(10i128.pow(precision)));
    let num_factor = i256::from_i128(10i128.pow(scale_divisor));
    let den_factor = i256::from_i128(10i128.pow(scale_dividend));

    let dividend_arr = dividend.as_primitive::<Decimal128Type>();
    let divisor_arr = divisor.as_primitive::<Decimal128Type>();
    let result: Int64Array = dividend_arr
        .iter()
        .zip(divisor_arr.iter())
        .map(|(a, b)| -> std::result::Result<Option<i64>, ArrowError> {
            let (Some(a), Some(b)) = (a, b) else {
                return Ok(None);
            };
            if b == 0 {
                return if ansi {
                    Err(divide_by_zero_err())
                } else {
                    Ok(None)
                };
            }
            // Unreachable by the headroom argument above; an `internal_err` keeps a broken
            // invariant loud instead of fabricating a NULL row under ANSI.
            let (Some(num), Some(den)) = (
                i256::from_i128(a).checked_mul(num_factor),
                i256::from_i128(b).checked_mul(den_factor),
            ) else {
                return Err(ArrowError::ComputeError(
                    "spark_integer_div: decimal rescale left i256 range".to_string(),
                ));
            };
            let Some(quotient) = num.checked_div(den) else {
                return Err(ArrowError::ComputeError(
                    "spark_integer_div: decimal division by a zero divisor escaped the guard"
                        .to_string(),
                ));
            };
            if quotient >= limit || quotient <= neg_limit {
                return if ansi {
                    Err(decimal_out_of_range_err(&quotient, precision))
                } else {
                    Ok(None)
                };
            }
            Ok(Some(quotient.as_i128() as i64))
        })
        .collect::<std::result::Result<Int64Array, ArrowError>>()
        .map_err(spark_error)?;
    Ok(Arc::new(result))
}

fn integer_div_inner(args: &[ArrayRef], ansi: bool, check_overflow: bool) -> Result<ArrayRef> {
    let [dividend, divisor] = args else {
        return Err(invalid_arg_count_exec_err(
            "spark_integer_div",
            (2, 2),
            args.len(),
        ));
    };
    if !matches!(dividend.data_type(), DataType::Int64) {
        return Err(unsupported_data_type_exec_err(
            "spark_integer_div",
            "Int64",
            dividend.data_type(),
        ));
    }
    if !matches!(divisor.data_type(), DataType::Int64) {
        return Err(unsupported_data_type_exec_err(
            "spark_integer_div",
            "Int64",
            divisor.data_type(),
        ));
    }
    let d = dividend.as_primitive::<Int64Type>();
    let s = divisor.as_primitive::<Int64Type>();
    let result: Int64Array = if ansi {
        // `try_binary` visits only rows valid on both sides, so a NULL dividend wins over a
        // zero divisor, as in Spark.
        datafusion::arrow::compute::try_binary(d, s, |x, y| {
            if y == 0 {
                Err(divide_by_zero_err())
            } else if check_overflow && x == i64::MIN && y == -1 {
                Err(integral_divide_overflow_err())
            } else {
                Ok(x.wrapping_div(y))
            }
        })
        .map_err(spark_error)?
    } else {
        // `binary` invokes the closure on masked slots too, so the `y == 0` guard is
        // load-bearing even though `make_safe_divisor` nulled them upstream.
        datafusion::arrow::compute::binary(d, s, |x, y| if y == 0 { 0 } else { x.wrapping_div(y) })
            .map_err(spark_error)?
    };
    Ok(Arc::new(result))
}

fn interval_div_inner(args: &[ArrayRef], ansi: bool) -> Result<ArrayRef> {
    let [dividend, divisor] = args else {
        return Err(invalid_arg_count_exec_err(
            "spark_interval_div",
            (2, 2),
            args.len(),
        ));
    };

    let divide_by_zero = || Err::<Option<i64>, _>(divide_by_zero_err());

    let result: Int64Array = match (dividend.data_type(), divisor.data_type()) {
        (
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Interval(IntervalUnit::YearMonth),
        ) => {
            let dividend_arr = dividend.as_primitive::<IntervalYearMonthType>();
            let divisor_arr = divisor.as_primitive::<IntervalYearMonthType>();
            dividend_arr
                .iter()
                .zip(divisor_arr.iter())
                .map(|(d, s)| match (d, s) {
                    // `IntegralDivide` picks `PhysicalIntegerType.integral` for YEAR TO MONTH
                    // (arithmetic.scala:916-917), so Spark divides in 32-bit and only widens
                    // afterwards with `integral.toLong`. Widening first would turn
                    // `Int.MinValue / -1` into a positive value instead of wrapping, and
                    // `checkDivideOverflow` is LONG-dividend-only, so Spark never raises here.
                    (Some(d_val), Some(s_val)) => {
                        if s_val == 0 {
                            if ansi { divide_by_zero() } else { Ok(None) }
                        } else {
                            Ok(Some(i64::from(d_val.wrapping_div(s_val))))
                        }
                    }
                    _ => Ok(None),
                })
                .collect::<std::result::Result<Int64Array, ArrowError>>()
                .map_err(spark_error)?
        }
        _ => {
            return Err(unsupported_data_types_exec_err(
                "spark_interval_div",
                "Interval / Interval of the same unit",
                &[dividend.data_type().clone(), divisor.data_type().clone()],
            ));
        }
    };

    Ok(Arc::new(result))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, Int64Array, IntervalYearMonthArray};

    use super::*;
    use crate::error::generic_exec_err;

    #[test]
    fn test_interval_year_month_division() -> Result<()> {
        // 13 months / -1 month = -13
        let dividend = Arc::new(IntervalYearMonthArray::from(vec![13])) as ArrayRef;
        let divisor = Arc::new(IntervalYearMonthArray::from(vec![-1])) as ArrayRef;

        let result = interval_div_inner(&[dividend, divisor], false)?;
        let Some(int_array) = result.as_any().downcast_ref::<Int64Array>() else {
            return Err(generic_exec_err("test", "Expected Int64Array"));
        };
        assert_eq!(int_array.value(0), -13);
        Ok(())
    }

    #[test]
    fn test_interval_year_month_division_positive() -> Result<()> {
        // 30 months / 3 months = 10
        let dividend = Arc::new(IntervalYearMonthArray::from(vec![30])) as ArrayRef;
        let divisor = Arc::new(IntervalYearMonthArray::from(vec![3])) as ArrayRef;

        let result = interval_div_inner(&[dividend, divisor], false)?;
        let Some(int_array) = result.as_any().downcast_ref::<Int64Array>() else {
            return Err(generic_exec_err("test", "Expected Int64Array"));
        };
        assert_eq!(int_array.value(0), 10);
        Ok(())
    }

    #[test]
    fn test_interval_year_month_division_equal() -> Result<()> {
        // 12 months / 12 months = 1
        let dividend = Arc::new(IntervalYearMonthArray::from(vec![12])) as ArrayRef;
        let divisor = Arc::new(IntervalYearMonthArray::from(vec![12])) as ArrayRef;

        let result = interval_div_inner(&[dividend, divisor], false)?;
        let Some(int_array) = result.as_any().downcast_ref::<Int64Array>() else {
            return Err(generic_exec_err("test", "Expected Int64Array"));
        };
        assert_eq!(int_array.value(0), 1);
        Ok(())
    }

    #[test]
    fn test_interval_year_month_division_truncate() -> Result<()> {
        // 5 months / 2 months = 2 (truncated)
        let dividend = Arc::new(IntervalYearMonthArray::from(vec![5])) as ArrayRef;
        let divisor = Arc::new(IntervalYearMonthArray::from(vec![2])) as ArrayRef;

        let result = interval_div_inner(&[dividend, divisor], false)?;
        let Some(int_array) = result.as_any().downcast_ref::<Int64Array>() else {
            return Err(generic_exec_err("test", "Expected Int64Array"));
        };
        assert_eq!(int_array.value(0), 2);
        Ok(())
    }

    #[test]
    fn test_interval_division_by_zero() -> Result<()> {
        // 10 months / 0 months = NULL
        let dividend = Arc::new(IntervalYearMonthArray::from(vec![10])) as ArrayRef;
        let divisor = Arc::new(IntervalYearMonthArray::from(vec![0])) as ArrayRef;

        let result = interval_div_inner(&[dividend, divisor], false)?;
        let Some(int_array) = result.as_any().downcast_ref::<Int64Array>() else {
            return Err(generic_exec_err("test", "Expected Int64Array"));
        };
        assert!(int_array.is_null(0));
        Ok(())
    }
}
