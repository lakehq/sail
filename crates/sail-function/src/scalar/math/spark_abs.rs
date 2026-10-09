use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, AsArray, Decimal128Array, DurationMicrosecondArray, DurationMillisecondArray,
    DurationNanosecondArray, DurationSecondArray, Int8Array, Int16Array, Int32Array, Int64Array,
    IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray,
};
use datafusion::arrow::datatypes::{
    DataType, Decimal128Type, DurationMicrosecondType, DurationMillisecondType,
    DurationNanosecondType, DurationSecondType, Field, FieldRef, Int8Type, Int16Type, Int32Type,
    Int64Type, IntervalDayTimeType, IntervalMonthDayNanoType, IntervalUnit, IntervalYearMonthType,
    TimeUnit,
};
use datafusion::functions::math::expr_fn::abs;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err, internal_err};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use sail_common_datafusion::utils::items::ItemTaker;

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};

/// `scala.math.BigDecimal` negation rounds to `MathContext.DECIMAL128`: 34 significant digits,
/// HALF_EVEN. Spark's `Decimal.abs` of a negative value is `-decimalVal`, so a negative decimal
/// wider than 34 digits is rounded to 34 digits; a non-negative one is returned as is.
/// <https://github.com/apache/spark/blob/v4.2.0/sql/api/src/main/scala/org/apache/spark/sql/types/Decimal.scala#L543-L551>
const SPARK_NEGATE_DIGITS: u8 = 34;

fn decimal_digits(magnitude: u128) -> u32 {
    magnitude.checked_ilog10().map_or(1, |d| d + 1)
}

fn format_decimal(unscaled: u128, scale: i8) -> String {
    let digits = unscaled.to_string();
    match usize::try_from(scale) {
        Ok(scale) if scale > 0 => {
            let padded = format!("{digits:0>width$}", width = scale + 1);
            let (int_part, frac_part) = padded.split_at(padded.len() - scale);
            format!("{int_part}.{frac_part}")
        }
        _ => digits,
    }
}

/// Spark's `abs` of one unscaled `Decimal128` value, or the overflow error Spark raises when
/// the rounded value no longer fits the precision.
fn spark_abs_wide_decimal(value: i128, precision: u8, scale: i8) -> Result<i128> {
    if value >= 0 {
        return Ok(value);
    }
    let magnitude = value.unsigned_abs();
    let digits = decimal_digits(magnitude);
    let excess = digits.saturating_sub(u32::from(SPARK_NEGATE_DIGITS));
    let rounded = if excess == 0 {
        magnitude
    } else {
        let unit = 10_u128.pow(excess);
        let (quotient, remainder) = (magnitude / unit, magnitude % unit);
        let half = unit / 2;
        let quotient = if remainder > half || (remainder == half && quotient % 2 == 1) {
            quotient + 1
        } else {
            quotient
        };
        quotient * unit
    };
    if decimal_digits(rounded) > u32::from(precision) {
        let exponent = decimal_digits(rounded) as i64 - 1 - i64::from(scale);
        let leading = rounded.to_string();
        let significant = leading.trim_end_matches('0');
        let mantissa = if significant.len() > 1 {
            format!("{}.{}", &significant[..1], &significant[1..])
        } else {
            format!("{significant}.0")
        };
        return exec_err!(
            "[NUMERIC_VALUE_OUT_OF_RANGE.WITHOUT_SUGGESTION] The {} rounded half up from {mantissa}E+{exponent} cannot be represented as Decimal({precision}, {scale}). \
             If necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error.",
            format_decimal(rounded, scale)
        );
    }
    Ok(rounded as i128)
}

fn abs_wide_decimal(arg: &ColumnarValue, precision: u8, scale: i8) -> Result<ColumnarValue> {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Decimal128(value, _, _)) => {
            let value = value
                .map(|v| spark_abs_wide_decimal(v, precision, scale))
                .transpose()?;
            Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                value, precision, scale,
            )))
        }
        ColumnarValue::Array(array) => {
            let array = array.as_primitive::<Decimal128Type>();
            let result = array
                .iter()
                .map(|v| {
                    v.map(|v| spark_abs_wide_decimal(v, precision, scale))
                        .transpose()
                })
                .collect::<Result<Decimal128Array>>()?
                .with_precision_and_scale(precision, scale)?;
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        other => Err(unsupported_data_type_exec_err(
            "abs",
            "Decimal128 type",
            &other.data_type(),
        )),
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkAbs {
    signature: Signature,
    ansi_mode: bool,
}

impl Default for SparkAbs {
    fn default() -> Self {
        Self::new(false)
    }
}

impl SparkAbs {
    pub fn new(ansi_mode: bool) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            ansi_mode,
        }
    }

    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }

    /// The result keeps the (already coerced) argument type.
    fn output_type(arg_type: &DataType) -> Result<DataType> {
        if arg_type.is_numeric()
            || arg_type.is_null()
            || matches!(
                arg_type,
                DataType::Interval(IntervalUnit::YearMonth | IntervalUnit::DayTime)
                    | DataType::Duration(_)
            )
        {
            Ok(arg_type.clone())
        } else {
            Err(unsupported_data_type_exec_err(
                "abs",
                "Numeric, Interval, or Duration type",
                arg_type,
            ))
        }
    }
}

impl ScalarUDFImpl for SparkAbs {
    fn name(&self) -> &str {
        "spark_abs"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn is_strict(&self) -> bool {
        true
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    /// Spark's `Abs` is `NullIntolerant` and keeps the child's `dataType`, so the result is
    /// nullable exactly when the argument is.
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala#L152-L158>
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field] = args.arg_fields else {
            return Err(invalid_arg_count_exec_err(
                "abs",
                (1, 1),
                args.arg_fields.len(),
            ));
        };
        let data_type = Self::output_type(field.data_type())?;
        Ok(Arc::new(Field::new(
            self.name(),
            data_type,
            field.is_nullable(),
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return Err(invalid_arg_count_exec_err("abs", (1, 1), arg_types.len()));
        }
        match &arg_types[0] {
            t if t.is_numeric() => Ok(vec![t.clone()]),
            // `TypeCollection.NumericAndAnsiInterval` casts a NULL literal to its first member,
            // DOUBLE (`ImplicitTypeCasts`), so `abs(NULL)` has type double.
            DataType::Null => Ok(vec![DataType::Float64]),
            // Spark's legacy CalendarIntervalType is not in `NumericAndAnsiInterval`.
            DataType::Interval(IntervalUnit::YearMonth | IntervalUnit::DayTime)
            | DataType::Duration(_) => Ok(vec![arg_types[0].clone()]),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                Ok(vec![DataType::Float64])
            }
            other => Err(unsupported_data_type_exec_err(
                "abs",
                "Numeric, String, Interval, or Duration type",
                other,
            )),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Float/Decimal/UInt/Null have no ANSI overflow concerns; delegate to
        // DataFusion's built-in abs so the kernel stays correct even on
        // bypass paths where `simplify` has not rewritten the call.
        if let DataType::Decimal128(precision, scale) = args.args[0].data_type()
            && precision > SPARK_NEGATE_DIGITS
        {
            return abs_wide_decimal(&args.args[0], precision, scale);
        }
        if matches!(
            args.args[0].data_type(),
            DataType::Float32
                | DataType::Float64
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Null
        ) {
            return datafusion::functions::math::abs::AbsFunc::new().invoke_with_args(args);
        }
        let ScalarFunctionArgs { args, .. } = args;
        let [arg] = args.as_slice() else {
            return Err(invalid_arg_count_exec_err("abs", (1, 1), args.len()));
        };
        // Skip the kernel pass when every input row is NULL.
        if let ColumnarValue::Array(array) = arg
            && array.null_count() == array.len()
        {
            return Ok(ColumnarValue::Array(Arc::clone(array)));
        }
        match arg {
            // Signed integer abs: ANSI=true errors on MIN; ANSI=false wraps
            // (matches Java's Math.abs(int) — abs(MIN) returns MIN).
            ColumnarValue::Scalar(ScalarValue::Int8(v)) => match v {
                Some(x) => {
                    let r = if self.ansi_mode {
                        x.checked_abs().ok_or_else(|| {
                            exec_datafusion_err!("[ARITHMETIC_OVERFLOW] byte overflow on abs({x})")
                        })?
                    } else {
                        x.wrapping_abs()
                    };
                    Ok(ColumnarValue::Scalar(ScalarValue::Int8(Some(r))))
                }
                None => Ok(ColumnarValue::Scalar(ScalarValue::Int8(None))),
            },
            ColumnarValue::Scalar(ScalarValue::Int16(v)) => match v {
                Some(x) => {
                    let r = if self.ansi_mode {
                        x.checked_abs().ok_or_else(|| {
                            exec_datafusion_err!("[ARITHMETIC_OVERFLOW] short overflow on abs({x})")
                        })?
                    } else {
                        x.wrapping_abs()
                    };
                    Ok(ColumnarValue::Scalar(ScalarValue::Int16(Some(r))))
                }
                None => Ok(ColumnarValue::Scalar(ScalarValue::Int16(None))),
            },
            ColumnarValue::Scalar(ScalarValue::Int32(v)) => match v {
                Some(x) => {
                    let r = if self.ansi_mode {
                        x.checked_abs().ok_or_else(|| {
                            exec_datafusion_err!(
                                "[ARITHMETIC_OVERFLOW] integer overflow on abs({x})"
                            )
                        })?
                    } else {
                        x.wrapping_abs()
                    };
                    Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(r))))
                }
                None => Ok(ColumnarValue::Scalar(ScalarValue::Int32(None))),
            },
            ColumnarValue::Scalar(ScalarValue::Int64(v)) => match v {
                Some(x) => {
                    let r = if self.ansi_mode {
                        x.checked_abs().ok_or_else(|| {
                            exec_datafusion_err!("[ARITHMETIC_OVERFLOW] long overflow on abs({x})")
                        })?
                    } else {
                        x.wrapping_abs()
                    };
                    Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(r))))
                }
                None => Ok(ColumnarValue::Scalar(ScalarValue::Int64(None))),
            },
            // Interval/Duration abs is ALWAYS-checked in Spark — both ANSI=true
            // and ANSI=false raise ARITHMETIC_OVERFLOW on the MIN value, unlike
            // signed integer abs which respects spark.sql.ansi.enabled.
            ColumnarValue::Scalar(ScalarValue::IntervalYearMonth(interval)) => {
                let r = match interval {
                    Some(x) => Some(x.checked_abs().ok_or_else(|| {
                        exec_datafusion_err!(
                            "[ARITHMETIC_OVERFLOW] integer overflow on abs(interval year-month)"
                        )
                    })?),
                    None => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::IntervalYearMonth(r)))
            }
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(interval)) => {
                let r = match interval {
                    Some(x) => Some(x.checked_abs().ok_or_else(|| {
                        exec_datafusion_err!(
                            "[ARITHMETIC_OVERFLOW] long overflow on abs(interval day-time)"
                        )
                    })?),
                    None => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::IntervalDayTime(r)))
            }
            ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(interval)) => {
                let r = match interval {
                    Some(x) => Some(x.checked_abs().ok_or_else(|| {
                        exec_datafusion_err!(
                            "[ARITHMETIC_OVERFLOW] long overflow on abs(interval month-day-nano)"
                        )
                    })?),
                    None => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(r)))
            }
            ColumnarValue::Scalar(ScalarValue::DurationSecond(duration)) => {
                let r = match duration {
                    Some(x) => Some(x.checked_abs().ok_or_else(|| {
                        exec_datafusion_err!(
                            "[ARITHMETIC_OVERFLOW] long overflow on abs(duration second)"
                        )
                    })?),
                    None => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::DurationSecond(r)))
            }
            ColumnarValue::Scalar(ScalarValue::DurationMillisecond(duration)) => {
                let r = match duration {
                    Some(x) => Some(x.checked_abs().ok_or_else(|| {
                        exec_datafusion_err!(
                            "[ARITHMETIC_OVERFLOW] long overflow on abs(duration millisecond)"
                        )
                    })?),
                    None => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::DurationMillisecond(r)))
            }
            ColumnarValue::Scalar(ScalarValue::DurationMicrosecond(duration)) => {
                let r = match duration {
                    Some(x) => Some(x.checked_abs().ok_or_else(|| {
                        exec_datafusion_err!(
                            "[ARITHMETIC_OVERFLOW] long overflow on abs(duration microsecond)"
                        )
                    })?),
                    None => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::DurationMicrosecond(r)))
            }
            ColumnarValue::Scalar(ScalarValue::DurationNanosecond(duration)) => {
                let r = match duration {
                    Some(x) => Some(x.checked_abs().ok_or_else(|| {
                        exec_datafusion_err!(
                            "[ARITHMETIC_OVERFLOW] long overflow on abs(duration nanosecond)"
                        )
                    })?),
                    None => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::DurationNanosecond(r)))
            }
            ColumnarValue::Array(array) => {
                let result = match array.data_type() {
                    DataType::Int8 => {
                        if self.ansi_mode {
                            let result: Int8Array =
                                array.as_primitive::<Int8Type>().try_unary(|x| {
                                    x.checked_abs().ok_or_else(|| {
                                        exec_datafusion_err!(
                                            "[ARITHMETIC_OVERFLOW] byte overflow on abs({x})"
                                        )
                                    })
                                })?;
                            Ok(Arc::new(result) as ArrayRef)
                        } else {
                            let result: Int8Array =
                                array.as_primitive::<Int8Type>().unary(|x| x.wrapping_abs());
                            Ok(Arc::new(result) as ArrayRef)
                        }
                    }
                    DataType::Int16 => {
                        if self.ansi_mode {
                            let result: Int16Array =
                                array.as_primitive::<Int16Type>().try_unary(|x| {
                                    x.checked_abs().ok_or_else(|| {
                                        exec_datafusion_err!(
                                            "[ARITHMETIC_OVERFLOW] short overflow on abs({x})"
                                        )
                                    })
                                })?;
                            Ok(Arc::new(result) as ArrayRef)
                        } else {
                            let result: Int16Array = array
                                .as_primitive::<Int16Type>()
                                .unary(|x| x.wrapping_abs());
                            Ok(Arc::new(result) as ArrayRef)
                        }
                    }
                    DataType::Int32 => {
                        if self.ansi_mode {
                            let result: Int32Array =
                                array.as_primitive::<Int32Type>().try_unary(|x| {
                                    x.checked_abs().ok_or_else(|| {
                                        exec_datafusion_err!(
                                            "[ARITHMETIC_OVERFLOW] integer overflow on abs({x})"
                                        )
                                    })
                                })?;
                            Ok(Arc::new(result) as ArrayRef)
                        } else {
                            let result: Int32Array = array
                                .as_primitive::<Int32Type>()
                                .unary(|x| x.wrapping_abs());
                            Ok(Arc::new(result) as ArrayRef)
                        }
                    }
                    DataType::Int64 => {
                        if self.ansi_mode {
                            let result: Int64Array =
                                array.as_primitive::<Int64Type>().try_unary(|x| {
                                    x.checked_abs().ok_or_else(|| {
                                        exec_datafusion_err!(
                                            "[ARITHMETIC_OVERFLOW] long overflow on abs({x})"
                                        )
                                    })
                                })?;
                            Ok(Arc::new(result) as ArrayRef)
                        } else {
                            let result: Int64Array = array
                                .as_primitive::<Int64Type>()
                                .unary(|x| x.wrapping_abs());
                            Ok(Arc::new(result) as ArrayRef)
                        }
                    }
                    DataType::Interval(IntervalUnit::YearMonth) => {
                        let result: IntervalYearMonthArray = array
                            .as_primitive::<IntervalYearMonthType>()
                            .try_unary(|x| {
                                x.checked_abs().ok_or_else(|| {
                                    exec_datafusion_err!(
                                        "[ARITHMETIC_OVERFLOW] integer overflow on abs(interval year-month)"
                                    )
                                })
                            })?
                            .with_data_type(DataType::Interval(IntervalUnit::YearMonth));
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Interval(IntervalUnit::DayTime) => {
                        let result: IntervalDayTimeArray = array
                            .as_primitive::<IntervalDayTimeType>()
                            .try_unary(|x| {
                                x.checked_abs().ok_or_else(|| {
                                    exec_datafusion_err!(
                                        "[ARITHMETIC_OVERFLOW] long overflow on abs(interval day-time)"
                                    )
                                })
                            })?
                            .with_data_type(DataType::Interval(IntervalUnit::DayTime));
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Interval(IntervalUnit::MonthDayNano) => {
                        let result: IntervalMonthDayNanoArray = array
                            .as_primitive::<IntervalMonthDayNanoType>()
                            .try_unary(|x| {
                                x.checked_abs().ok_or_else(|| {
                                    exec_datafusion_err!(
                                        "[ARITHMETIC_OVERFLOW] long overflow on abs(interval month-day-nano)"
                                    )
                                })
                            })?
                            .with_data_type(DataType::Interval(IntervalUnit::MonthDayNano));
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Duration(TimeUnit::Second) => {
                        let result: DurationSecondArray = array
                            .as_primitive::<DurationSecondType>()
                            .try_unary(|x| {
                                x.checked_abs().ok_or_else(|| {
                                    exec_datafusion_err!(
                                        "[ARITHMETIC_OVERFLOW] long overflow on abs(duration second)"
                                    )
                                })
                            })?
                            .with_data_type(DataType::Duration(TimeUnit::Second));
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Duration(TimeUnit::Millisecond) => {
                        let result: DurationMillisecondArray = array
                            .as_primitive::<DurationMillisecondType>()
                            .try_unary(|x| {
                                x.checked_abs().ok_or_else(|| {
                                    exec_datafusion_err!(
                                        "[ARITHMETIC_OVERFLOW] long overflow on abs(duration millisecond)"
                                    )
                                })
                            })?
                            .with_data_type(DataType::Duration(TimeUnit::Millisecond));
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Duration(TimeUnit::Microsecond) => {
                        let result: DurationMicrosecondArray = array
                            .as_primitive::<DurationMicrosecondType>()
                            .try_unary(|x| {
                                x.checked_abs().ok_or_else(|| {
                                    exec_datafusion_err!(
                                        "[ARITHMETIC_OVERFLOW] long overflow on abs(duration microsecond)"
                                    )
                                })
                            })?
                            .with_data_type(DataType::Duration(TimeUnit::Microsecond));
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    DataType::Duration(TimeUnit::Nanosecond) => {
                        let result: DurationNanosecondArray = array
                            .as_primitive::<DurationNanosecondType>()
                            .try_unary(|x| {
                                x.checked_abs().ok_or_else(|| {
                                    exec_datafusion_err!(
                                        "[ARITHMETIC_OVERFLOW] long overflow on abs(duration nanosecond)"
                                    )
                                })
                            })?
                            .with_data_type(DataType::Duration(TimeUnit::Nanosecond));
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    other => Err(unsupported_data_type_exec_err(
                        "abs",
                        "Numeric, Interval, or Duration type",
                        other,
                    )),
                }?;
                Ok(ColumnarValue::Array(result))
            }
            other => Err(unsupported_data_type_exec_err(
                "abs",
                "Numeric, Interval, or Duration type",
                &other.data_type(),
            )),
        }
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        // Idempotence: abs(abs(x)) = abs(x).
        if args.len() == 1
            && let Expr::ScalarFunction(inner) = &args[0]
            && let Some(inner_abs) = inner.func.inner().downcast_ref::<Self>()
            && inner_abs.ansi_mode == self.ansi_mode
        {
            return Ok(ExprSimplifyResult::Simplified(args[0].clone()));
        }

        if args.len() != 1 {
            return Ok(ExprSimplifyResult::Original(args));
        }
        let dt = info.get_data_type(&args[0])?;
        match dt {
            // Wide decimals keep Spark's 34-digit rounding of a negated value, which
            // DataFusion's `abs` does not do.
            DataType::Decimal128(precision, _) if precision > SPARK_NEGATE_DIGITS => {
                Ok(ExprSimplifyResult::Original(args))
            }
            // Keep in invoke_with_args: interval/duration, and signed integers
            // (where invoke branches on self.ansi_mode between wrapping_abs and
            // checked_abs to honour Spark's ANSI semantics).
            DataType::Interval(_)
            | DataType::Duration(_)
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64 => Ok(ExprSimplifyResult::Original(args)),
            // Floats, decimals, unsigned, null: no ANSI overflow concern — delegate.
            _ => Ok(ExprSimplifyResult::Simplified(abs(args.one()?))),
        }
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        let arg = &input[0];
        let range = &arg.range;
        if range.lower().data_type() != range.upper().data_type() {
            return internal_err!("Endpoints of an Interval should have the same type");
        }
        let zero_point = Interval::make_zero(&range.lower().data_type())?;

        if range.gt_eq(&zero_point)? == Interval::TRUE {
            // Non-decreasing for x ≥ 0
            Ok(arg.sort_properties)
        } else if range.lt_eq(&zero_point)? == Interval::TRUE {
            // Non-increasing for x ≤ 0. E.g., [-5, -3, -1] -> [5, 3, 1]
            Ok(-arg.sort_properties)
        } else {
            Ok(SortProperties::Unordered)
        }
    }
}
