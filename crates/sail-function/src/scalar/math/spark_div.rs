use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int64Array};
use datafusion::arrow::datatypes::{DataType, Int64Type, IntervalUnit, IntervalYearMonthType};
use datafusion::arrow::error::ArrowError;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

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

/// Every `ArrowError` variant renders with a prefix ("Compute error: ...") that would
/// corrupt the Spark message; `DataFusionError::Execution` reaches the user unprefixed.
fn spark_error(e: ArrowError) -> DataFusionError {
    match e {
        ArrowError::ComputeError(message) => DataFusionError::Execution(message),
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

impl Default for SparkIntervalDiv {
    fn default() -> Self {
        Self::new(false)
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
}

impl Default for SparkIntegerDiv {
    fn default() -> Self {
        Self::new(false)
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
    let result: Int64Array = if ansi {
        // `try_binary` visits only rows valid on both sides, so a NULL dividend wins over a
        // zero divisor, as in Spark.
        datafusion::arrow::compute::try_binary(d, s, |x, y| {
            if y == 0 {
                Err(divide_by_zero_err())
            } else if x == i64::MIN && y == -1 {
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
                    // Spark divides YEAR TO MONTH in 32-bit and wraps at `Int.MinValue / -1`;
                    // widening to i64 diverges there. Pinned as `@sail-bug`.
                    (Some(d_val), Some(s_val)) => {
                        if s_val == 0 {
                            if ansi { divide_by_zero() } else { Ok(None) }
                        } else {
                            Ok(Some((d_val as i64) / (s_val as i64)))
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
