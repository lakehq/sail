use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    new_null_array, ArrayRef, AsArray, DurationMicrosecondArray, DurationMillisecondArray,
    DurationNanosecondArray, DurationSecondArray, Int16Array, Int32Array, Int64Array, Int8Array,
    IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray,
};
use datafusion::arrow::datatypes::{
    DataType, DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
    DurationSecondType, Int16Type, Int32Type, Int64Type, Int8Type, IntervalDayTimeType,
    IntervalMonthDayNanoType, IntervalUnit, IntervalYearMonthType, TimeUnit,
};
use datafusion::functions::math::expr_fn::abs;
use datafusion_common::{exec_datafusion_err, internal_err, Result, ScalarValue};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use sail_common_datafusion::utils::items::ItemTaker;

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};

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
}

impl ScalarUDFImpl for SparkAbs {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_abs"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types[0].is_numeric()
            || arg_types[0].is_null()
            || matches!(arg_types[0], DataType::Interval(_) | DataType::Duration(_))
        {
            Ok(arg_types[0].clone())
        } else {
            Err(unsupported_data_type_exec_err(
                "abs",
                "Numeric, Interval, or Duration type",
                &arg_types[0],
            ))
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return Err(invalid_arg_count_exec_err("abs", (1, 1), arg_types.len()));
        }
        match &arg_types[0] {
            t if t.is_numeric() => Ok(vec![t.clone()]),
            DataType::Null => Ok(vec![DataType::Null]),
            DataType::Interval(_) | DataType::Duration(_) => Ok(vec![arg_types[0].clone()]),
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
        let return_dtype = args.return_field.data_type().clone();
        let ScalarFunctionArgs { args, .. } = args;
        let [arg] = args.as_slice() else {
            return Err(invalid_arg_count_exec_err("abs", (1, 1), args.len()));
        };
        // Skip the kernel pass when every input row is NULL.
        if let ColumnarValue::Array(array) = arg {
            if array.null_count() == array.len() {
                return Ok(ColumnarValue::Array(new_null_array(
                    &return_dtype,
                    array.len(),
                )));
            }
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
        if args.len() == 1 {
            if let Expr::ScalarFunction(inner) = &args[0] {
                if let Some(inner_abs) = inner.func.inner().as_any().downcast_ref::<Self>() {
                    if inner_abs.ansi_mode == self.ansi_mode {
                        return Ok(ExprSimplifyResult::Simplified(args[0].clone()));
                    }
                }
            }
        }

        let dt = info.get_data_type(&args[0])?;
        match dt {
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
