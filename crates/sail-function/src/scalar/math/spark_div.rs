use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int64Array};
use datafusion::arrow::datatypes::{
    DataType, Int64Type, IntervalDayTimeType, IntervalMonthDayNanoType, IntervalUnit,
    IntervalYearMonthType,
};
use datafusion::arrow::error::ArrowError;
use datafusion_common::Result;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::error::{
    invalid_arg_count_exec_err, unsupported_data_type_exec_err, unsupported_data_types_exec_err,
};
use crate::functions_nested_utils::make_scalar_function;

/// Spark's div operator for intervals.
/// Performs integer division between two intervals of the same type.
/// Under ANSI=true, zero divisor raises an error; under ANSI=false, returns NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkIntervalDiv {
    signature: Signature,
    ansi_mode: bool,
}

impl Default for SparkIntervalDiv {
    fn default() -> Self {
        Self::new(true)
    }
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
        "spark_interval_div"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ansi = self.ansi_mode;
        make_scalar_function(move |arrs: &[ArrayRef]| interval_div_inner(arrs, ansi))(&args.args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [dividend, divisor] = arg_types else {
            return Err(invalid_arg_count_exec_err(
                "spark_interval_div",
                (2, 2),
                arg_types.len(),
            ));
        };
        match (dividend, divisor) {
            (DataType::Interval(d), DataType::Interval(s)) if d == s => Ok(arg_types.to_vec()),
            _ => Err(unsupported_data_types_exec_err(
                "spark_interval_div",
                "Interval / Interval of the same unit",
                arg_types,
            )),
        }
    }
}

/// Spark-compatible integer division (`div` / `DIV` operator).
///
/// Handles the overflow edge `LONG_MIN / -1`: ANSI=true errors, ANSI=false
/// wraps to `LONG_MIN` (matching Java's `Math.floorDiv` semantics).
/// Division by zero is expected to be handled upstream by `make_safe_divisor`
/// (see `spark_div` dispatcher) — this UDF propagates NULLs 1:1.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkIntegerDiv {
    signature: Signature,
    ansi_mode: bool,
}

impl Default for SparkIntegerDiv {
    fn default() -> Self {
        Self::new(true)
    }
}

impl SparkIntegerDiv {
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
        Ok(vec![DataType::Int64, DataType::Int64])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ansi = self.ansi_mode;
        make_scalar_function(move |arrs: &[ArrayRef]| integer_div_inner(arrs, ansi))(&args.args)
    }
}

fn integer_div_inner(args: &[ArrayRef], ansi: bool) -> Result<ArrayRef> {
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
    // Zero-divisor positions are masked to NULL upstream by `make_safe_divisor`
    // (see `spark_div` dispatcher), but `arrow::compute::binary` still invokes
    // the closure at those indices before applying the null mask — guard `y == 0`
    // to avoid a wrapping_div panic on the masked slot.
    let result: Int64Array = if ansi {
        datafusion::arrow::compute::try_binary(d, s, |x, y| {
            if y == 0 {
                Ok(0)
            } else if x == i64::MIN && y == -1 {
                Err(ArrowError::ComputeError(format!(
                    "[ARITHMETIC_OVERFLOW] long overflow on div({x}, {y})"
                )))
            } else {
                Ok(x.wrapping_div(y))
            }
        })?
    } else {
        datafusion::arrow::compute::binary(d, s, |x, y| {
            if y == 0 {
                0
            } else if x == i64::MIN && y == -1 {
                i64::MIN
            } else {
                x.wrapping_div(y)
            }
        })?
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

    let divide_by_zero = || Err::<Option<i64>, _>(ArrowError::DivideByZero);

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
                    (Some(d_val), Some(s_val)) => {
                        if s_val == 0 {
                            if ansi {
                                divide_by_zero()
                            } else {
                                Ok(None)
                            }
                        } else {
                            Ok(Some((d_val as i64) / (s_val as i64)))
                        }
                    }
                    _ => Ok(None),
                })
                .collect::<std::result::Result<Int64Array, ArrowError>>()?
        }
        (DataType::Interval(IntervalUnit::DayTime), DataType::Interval(IntervalUnit::DayTime)) => {
            let dividend_arr = dividend.as_primitive::<IntervalDayTimeType>();
            let divisor_arr = divisor.as_primitive::<IntervalDayTimeType>();
            dividend_arr
                .iter()
                .zip(divisor_arr.iter())
                .map(|(d, s)| match (d, s) {
                    (Some(d_val), Some(s_val)) => {
                        let d_millis = d_val.days as i64 * 86_400_000 + d_val.milliseconds as i64;
                        let s_millis = s_val.days as i64 * 86_400_000 + s_val.milliseconds as i64;
                        if s_millis == 0 {
                            if ansi {
                                divide_by_zero()
                            } else {
                                Ok(None)
                            }
                        } else {
                            Ok(Some(d_millis / s_millis))
                        }
                    }
                    _ => Ok(None),
                })
                .collect::<std::result::Result<Int64Array, ArrowError>>()?
        }
        (
            DataType::Interval(IntervalUnit::MonthDayNano),
            DataType::Interval(IntervalUnit::MonthDayNano),
        ) => {
            let dividend_arr = dividend.as_primitive::<IntervalMonthDayNanoType>();
            let divisor_arr = divisor.as_primitive::<IntervalMonthDayNanoType>();
            dividend_arr
                .iter()
                .zip(divisor_arr.iter())
                .map(|(d, s)| match (d, s) {
                    (Some(d_val), Some(s_val)) => {
                        let d_nanos = d_val.months as i64 * 2_592_000_000_000_000
                            + d_val.days as i64 * 86_400_000_000_000
                            + d_val.nanoseconds;
                        let s_nanos = s_val.months as i64 * 2_592_000_000_000_000
                            + s_val.days as i64 * 86_400_000_000_000
                            + s_val.nanoseconds;
                        if s_nanos == 0 {
                            if ansi {
                                divide_by_zero()
                            } else {
                                Ok(None)
                            }
                        } else {
                            Ok(Some(d_nanos / s_nanos))
                        }
                    }
                    _ => Ok(None),
                })
                .collect::<std::result::Result<Int64Array, ArrowError>>()?
        }
        _ => {
            return Err(unsupported_data_types_exec_err(
                "spark_interval_div",
                "Interval / Interval of the same unit",
                &[dividend.data_type().clone(), divisor.data_type().clone()],
            ))
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

    #[test]
    fn test_integer_div_basic() -> Result<()> {
        // 7 div 2 = 3
        let dividend = Arc::new(Int64Array::from(vec![7])) as ArrayRef;
        let divisor = Arc::new(Int64Array::from(vec![2])) as ArrayRef;
        let result = integer_div_inner(&[dividend, divisor], false)?;
        let Some(arr) = result.as_any().downcast_ref::<Int64Array>() else {
            return Err(generic_exec_err("test", "Expected Int64Array"));
        };
        assert_eq!(arr.value(0), 3);
        Ok(())
    }

    #[test]
    fn test_integer_div_long_min_overflow_ansi_errors() -> Result<()> {
        // ANSI=true: LONG_MIN div -1 overflows and must error.
        let dividend = Arc::new(Int64Array::from(vec![i64::MIN])) as ArrayRef;
        let divisor = Arc::new(Int64Array::from(vec![-1])) as ArrayRef;
        match integer_div_inner(&[dividend, divisor], true) {
            Ok(_) => Err(generic_exec_err(
                "test",
                "expected ARITHMETIC_OVERFLOW under ANSI",
            )),
            Err(e) => {
                assert!(e.to_string().contains("ARITHMETIC_OVERFLOW"));
                Ok(())
            }
        }
    }

    #[test]
    fn test_integer_div_long_min_overflow_non_ansi_wraps() -> Result<()> {
        // ANSI=false: LONG_MIN div -1 wraps to LONG_MIN.
        let dividend = Arc::new(Int64Array::from(vec![i64::MIN])) as ArrayRef;
        let divisor = Arc::new(Int64Array::from(vec![-1])) as ArrayRef;
        let result = integer_div_inner(&[dividend, divisor], false)?;
        let Some(arr) = result.as_any().downcast_ref::<Int64Array>() else {
            return Err(generic_exec_err("test", "Expected Int64Array"));
        };
        assert_eq!(arr.value(0), i64::MIN);
        Ok(())
    }

    #[test]
    fn test_integer_div_zero_divisor_guard_no_panic() -> Result<()> {
        // Zero divisors are masked to NULL upstream; the inner `y == 0` guard
        // must not panic in either mode (it yields 0 at the masked slot).
        let dividend = Arc::new(Int64Array::from(vec![10])) as ArrayRef;
        let divisor = Arc::new(Int64Array::from(vec![0])) as ArrayRef;
        let result = integer_div_inner(&[Arc::clone(&dividend), Arc::clone(&divisor)], false)?;
        let Some(arr) = result.as_any().downcast_ref::<Int64Array>() else {
            return Err(generic_exec_err("test", "Expected Int64Array"));
        };
        assert_eq!(arr.value(0), 0);
        let result = integer_div_inner(&[dividend, divisor], true)?;
        let Some(arr) = result.as_any().downcast_ref::<Int64Array>() else {
            return Err(generic_exec_err("test", "Expected Int64Array"));
        };
        assert_eq!(arr.value(0), 0);
        Ok(())
    }

    #[test]
    fn test_integer_div_propagates_nulls() -> Result<()> {
        // NULL positions stay NULL (binary copies the validity mask).
        let dividend = Arc::new(Int64Array::from(vec![Some(10), None])) as ArrayRef;
        let divisor = Arc::new(Int64Array::from(vec![Some(3), Some(2)])) as ArrayRef;
        let result = integer_div_inner(&[dividend, divisor], false)?;
        let Some(arr) = result.as_any().downcast_ref::<Int64Array>() else {
            return Err(generic_exec_err("test", "Expected Int64Array"));
        };
        assert_eq!(arr.value(0), 3);
        assert!(arr.is_null(1));
        Ok(())
    }
}
