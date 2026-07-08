use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::datatypes::IntervalUnit::{MonthDayNano, YearMonth};
use datafusion::arrow::datatypes::{
    DataType, Float64Type, Int32Type, Int64Type, IntervalMonthDayNanoType, IntervalYearMonthType,
};
use datafusion_common::{DataFusionError, Result, exec_datafusion_err};
use datafusion_expr::{
    ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_types_exec_err};
use crate::scalar::math::utils::decimal::{decimal_divide, spark_decimal_divide_type};
use crate::scalar::math::utils::try_op::{
    arith_input_types, arith_result_type, is_float_or_decimal, try_binary_op_to_float64,
    try_div_interval_monthdaynano_i32, try_div_interval_monthdaynano_i64,
    try_op_interval_monthdaynano_i32, try_op_interval_yearmonth_i32,
};

/// Spark `/` and `try_divide`, unified. `safe = true` is `try_divide`
/// (div-by-zero / overflow → NULL, ANSI-invariant); `safe = false` is `/` and
/// honors `ansi_mode` (div-by-zero → `DIVIDE_BY_ZERO` under ANSI, NULL under
/// non-ANSI). Spark division promotes any non-decimal numeric (incl. float) to
/// DOUBLE; `DECIMAL / DECIMAL` (and DECIMAL / integral) stays DECIMAL;
/// `DOUBLE / DECIMAL` is DOUBLE.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDivide {
    signature: Signature,
    ansi_mode: bool,
    safe: bool,
}

impl Default for SparkDivide {
    fn default() -> Self {
        Self::new(false, false)
    }
}

impl SparkDivide {
    pub fn new(ansi_mode: bool, safe: bool) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            ansi_mode,
            safe,
        }
    }

    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }

    pub fn safe(&self) -> bool {
        self.safe
    }

    /// Division by zero raises only for `/` under ANSI mode; `try_divide` and
    /// non-ANSI `/` return NULL.
    fn error_on_zero(&self) -> bool {
        self.ansi_mode && !self.safe
    }
}

/// Spark's `[DIVIDE_BY_ZERO]` error, raised by `/` under ANSI mode (the message
/// contains "Division by zero", matching Spark's `DIVIDE_BY_ZERO` error class).
pub fn divide_by_zero_err() -> DataFusionError {
    exec_datafusion_err!(
        "[DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 \
         and return NULL instead."
    )
}

/// Spark's `[INTERVAL_DIVIDED_BY_ZERO]` error (SQLSTATE 22012), raised by dividing
/// an interval by zero. `DivideYMInterval`/`DivideDTInterval` throw it
/// unconditionally (both ANSI modes); the legacy `CalendarInterval` path throws
/// only under ANSI. `try_divide` tolerates it as NULL.
pub fn interval_divided_by_zero_err() -> DataFusionError {
    exec_datafusion_err!(
        "[INTERVAL_DIVIDED_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor \
         being 0 and return NULL instead."
    )
}

/// `true` when a divide operand makes the result FLOAT/DOUBLE (Spark promotes any
/// non-decimal numeric, and `DOUBLE / DECIMAL`, to DOUBLE).
fn is_float(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    )
}

impl ScalarUDFImpl for SparkDivide {
    fn name(&self) -> &str {
        if self.safe {
            "try_divide"
        } else {
            "spark_divide"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [
                DataType::Int32 | DataType::Int64,
                DataType::Int32 | DataType::Int64,
            ] => Ok(DataType::Float64),
            [DataType::Interval(YearMonth), DataType::Int32] => Ok(DataType::Interval(YearMonth)),
            [
                DataType::Interval(MonthDayNano),
                DataType::Int32 | DataType::Int64,
            ] => Ok(DataType::Interval(MonthDayNano)),
            // Spark's decimal division precision/scale rule (Arrow's `div` scale
            // differs); operands reach here as two decimals (int coerced in
            // coerce_types).
            [DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)] => {
                let (precision, scale) = spark_decimal_divide_type(*p1, *s1, *p2, *s2);
                Ok(DataType::Decimal128(precision, scale))
            }
            [left, right] if is_float_or_decimal(left) || is_float_or_decimal(right) => {
                if is_float(left) || is_float(right) {
                    Ok(DataType::Float64)
                } else {
                    arith_result_type(left, Operator::Divide, right)
                }
            }
            _ => Err(invalid_arg_count_exec_err(
                "spark_divide",
                (2, 2),
                arg_types.len(),
            )),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [left, right] = arg_types else {
            return Err(invalid_arg_count_exec_err(
                "spark_divide",
                (2, 2),
                arg_types.len(),
            ));
        };
        if *left == DataType::Null {
            return Ok(vec![right.clone(), right.clone()]);
        } else if *right == DataType::Null {
            return Ok(vec![left.clone(), left.clone()]);
        }
        if matches!(
            (left, right),
            (DataType::Interval(YearMonth), DataType::Int32)
                | (DataType::Interval(MonthDayNano), DataType::Int32)
                | (DataType::Interval(MonthDayNano), DataType::Int64)
        ) {
            return Ok(vec![left.clone(), right.clone()]);
        }
        // DECIMAL / DECIMAL (or DECIMAL / integral) stays decimal; every other
        // numeric combination — int/int, float, and DOUBLE / DECIMAL — divides in
        // DOUBLE (Spark promotes non-decimal division to double).
        let has_decimal = matches!(left, DataType::Decimal128(..) | DataType::Decimal256(..))
            || matches!(right, DataType::Decimal128(..) | DataType::Decimal256(..));
        if has_decimal && !is_float(left) && !is_float(right) {
            // Keep two decimals as-is so their individual scales reach the divide
            // kernel; a decimal/integral pair coerces the integer to decimal.
            if matches!(left, DataType::Decimal128(..)) && matches!(right, DataType::Decimal128(..))
            {
                return Ok(vec![left.clone(), right.clone()]);
            }
            let (l, r) = arith_input_types(left, Operator::Divide, right)?;
            return Ok(vec![l, r]);
        }
        Ok(vec![DataType::Float64, DataType::Float64])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let return_type = args.return_field.data_type().clone();
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        let [left, right] = args.as_slice() else {
            return Err(invalid_arg_count_exec_err(
                "spark_divide",
                (2, 2),
                args.len(),
            ));
        };

        let result: ArrayRef = match (left.data_type(), right.data_type()) {
            // Year-month interval (Spark's `DivideYMInterval`) throws
            // INTERVAL_DIVIDED_BY_ZERO on a zero divisor in BOTH ANSI modes; only
            // `try_divide` (safe) tolerates it as NULL.
            (DataType::Interval(YearMonth), DataType::Int32) => {
                let l = left.as_primitive::<IntervalYearMonthType>();
                let r = right.as_primitive::<Int32Type>();
                if !self.safe && r.iter().flatten().any(|v| v == 0) {
                    return Err(interval_divided_by_zero_err());
                }
                Arc::new(try_op_interval_yearmonth_i32(l, r, i32::checked_div))
            }
            // `make_interval` produces Spark's legacy `CalendarInterval`, whose
            // divide throws only under ANSI and returns NULL otherwise
            // (`error_on_zero`); `try_divide` always tolerates it as NULL.
            (DataType::Interval(MonthDayNano), DataType::Int32) => {
                let l = left.as_primitive::<IntervalMonthDayNanoType>();
                let r = right.as_primitive::<Int32Type>();
                if self.error_on_zero() && r.iter().flatten().any(|v| v == 0) {
                    return Err(interval_divided_by_zero_err());
                }
                Arc::new(try_div_interval_monthdaynano_i32(l, r)?)
            }
            (DataType::Interval(MonthDayNano), DataType::Int64) => {
                let l = left.as_primitive::<IntervalMonthDayNanoType>();
                let r = right.as_primitive::<Int64Type>();
                if self.error_on_zero() && r.iter().flatten().any(|v| v == 0) {
                    return Err(interval_divided_by_zero_err());
                }
                Arc::new(try_div_interval_monthdaynano_i64(l, r)?)
            }
            (DataType::Int32, DataType::Interval(MonthDayNano)) => {
                let l = left.as_primitive::<Int32Type>();
                let r = right.as_primitive::<IntervalMonthDayNanoType>();
                Arc::new(try_op_interval_monthdaynano_i32(r, l, |a, b| {
                    a.checked_div(b as i64)
                }))
            }
            // Float / double (and DOUBLE / DECIMAL, coerced to DOUBLE): IEEE 754,
            // but a zero divisor errors (ANSI `/`) or nulls (try_ / non-ANSI).
            (DataType::Float64, DataType::Float64) => {
                let l = left.as_primitive::<Float64Type>();
                let r = right.as_primitive::<Float64Type>();
                if self.error_on_zero() && r.iter().flatten().any(|v| v == 0.0) {
                    return Err(divide_by_zero_err());
                }
                Arc::new(try_binary_op_to_float64::<Float64Type, _>(l, r, |a, b| {
                    if b == 0.0 { None } else { Some(a / b) }
                }))
            }
            // Decimal / decimal (or decimal / integral coerced to decimal): Spark's
            // precision/scale rule (`decimal_divide`, not Arrow's `div`);
            // div-by-zero and precision overflow error (ANSI `/`) or become
            // per-element NULL.
            (DataType::Decimal128(..), DataType::Decimal128(..)) => {
                decimal_divide(left, right, &return_type, self.error_on_zero())?
            }
            (l, r) => {
                return Err(unsupported_data_types_exec_err(
                    "spark_divide",
                    "Int, Float, Decimal, or Interval / integer",
                    &[l.clone(), r.clone()],
                ));
            }
        };

        Ok(ColumnarValue::Array(result))
    }
}
