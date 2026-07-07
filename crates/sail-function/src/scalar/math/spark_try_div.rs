use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::compute::kernels::numeric::div;
use datafusion::arrow::datatypes::IntervalUnit::{MonthDayNano, YearMonth};
use datafusion::arrow::datatypes::{
    DataType, Float64Type, Int32Type, Int64Type, IntervalMonthDayNanoType, IntervalYearMonthType,
};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_types_exec_err};
use crate::scalar::math::utils::try_op::{
    arith_input_types, arith_result_type, binary_op_scalar_or_array, is_float_or_decimal,
    null_decimal_overflow, try_arrow_arith, try_binary_op_to_float64,
    try_div_interval_monthdaynano_i32, try_div_interval_monthdaynano_i64,
    try_op_interval_monthdaynano_i32, try_op_interval_yearmonth_i32,
};

/// `true` when a `try_divide` operand type makes the result FLOAT/DOUBLE.
/// Spark division promotes any non-decimal numeric (including float and double)
/// to DOUBLE, and `DOUBLE <op> DECIMAL` also yields DOUBLE (double wins).
fn is_float(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    )
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTryDiv {
    signature: Signature,
}

impl Default for SparkTryDiv {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTryDiv {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkTryDiv {
    fn name(&self) -> &str {
        "try_divide"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [DataType::Int32, DataType::Int32]
            | [DataType::Int64, DataType::Int64]
            | [DataType::Int32, DataType::Int64]
            | [DataType::Int64, DataType::Int32] => Ok(DataType::Float64),
            [DataType::Interval(YearMonth), DataType::Int32] => Ok(DataType::Interval(YearMonth)),
            [DataType::Interval(MonthDayNano), DataType::Int32] => {
                Ok(DataType::Interval(MonthDayNano))
            }
            [DataType::Interval(MonthDayNano), DataType::Int64] => {
                Ok(DataType::Interval(MonthDayNano))
            }
            [left, right] if is_float_or_decimal(left) || is_float_or_decimal(right) => {
                // Spark division promotes any float/double (or DOUBLE / DECIMAL) to
                // DOUBLE; DECIMAL / DECIMAL and DECIMAL / integral stay DECIMAL with
                // Spark's division precision/scale rule (inherited from DataFusion).
                if is_float(left) || is_float(right) {
                    Ok(DataType::Float64)
                } else {
                    arith_result_type(left, Operator::Divide, right)
                }
            }
            _ => Err(unsupported_data_types_exec_err(
                "try_divide",
                "Int32, Int64, Float, Decimal o Interval(YearMonth|MonthDayNano)",
                arg_types,
            )),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, return_field, ..
        } = args;
        let [left, right] = args.as_slice() else {
            return Err(invalid_arg_count_exec_err("try_divide", (2, 2), args.len()));
        };

        let len = match (&left, &right) {
            (ColumnarValue::Array(arr), _) => arr.len(),
            (_, ColumnarValue::Array(arr)) => arr.len(),
            (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_)) => 1,
        };

        let left_arr = match left {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(len)?,
        };

        let right_arr = match right {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(len)?,
        };

        match (left_arr.data_type(), right_arr.data_type()) {
            (DataType::Int32, DataType::Int32) => {
                let l = left_arr.as_primitive::<Int32Type>();
                let r = right_arr.as_primitive::<Int32Type>();
                let result = try_binary_op_to_float64::<Int32Type, _>(l, r, |a, b| {
                    if b == 0 {
                        None
                    } else {
                        Some(a as f64 / b as f64)
                    }
                });
                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Int64, DataType::Int64) => {
                let l = left_arr.as_primitive::<Int64Type>();
                let r = right_arr.as_primitive::<Int64Type>();
                let result = try_binary_op_to_float64::<Int64Type, _>(l, r, |a, b| {
                    if b == 0 {
                        None
                    } else {
                        Some(a as f64 / b as f64)
                    }
                });

                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Interval(YearMonth), DataType::Int32) => {
                let l = left_arr.as_primitive::<IntervalYearMonthType>();
                let r = right_arr.as_primitive::<Int32Type>();
                let result = try_op_interval_yearmonth_i32(l, r, i32::checked_div);
                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Int32, DataType::Interval(MonthDayNano)) => {
                let l = left_arr.as_primitive::<Int32Type>();
                let r = right_arr.as_primitive::<IntervalMonthDayNanoType>();
                let result = try_op_interval_monthdaynano_i32(r, l, |a, b| a.checked_div(b as i64));

                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Interval(MonthDayNano), DataType::Int32) => {
                let l = left_arr.as_primitive::<IntervalMonthDayNanoType>();
                let r = right_arr.as_primitive::<Int32Type>();
                let out = try_div_interval_monthdaynano_i32(l, r)?;
                binary_op_scalar_or_array(left, right, out)
            }
            (DataType::Interval(MonthDayNano), DataType::Int64) => {
                let l = left_arr.as_primitive::<IntervalMonthDayNanoType>();
                let r = right_arr.as_primitive::<Int64Type>();
                let out = try_div_interval_monthdaynano_i64(l, r)?;
                binary_op_scalar_or_array(left, right, out)
            }
            (DataType::Float64, DataType::Float64) => {
                // Float/double (and DOUBLE / DECIMAL) are coerced to DOUBLE. Arrow's
                // float division follows IEEE 754 (x/0.0 -> Inf), but Spark
                // `try_divide` yields NULL on a zero divisor, so guard it explicitly;
                // Inf/NaN otherwise pass through as real values.
                let l = left_arr.as_primitive::<Float64Type>();
                let r = right_arr.as_primitive::<Float64Type>();
                let result = try_binary_op_to_float64::<Float64Type, _>(l, r, |a, b| {
                    if b == 0.0 {
                        None
                    } else {
                        Some(a / b)
                    }
                });
                binary_op_scalar_or_array(left, right, result)
            }
            (l, r) if is_float_or_decimal(l) && is_float_or_decimal(r) => {
                // DECIMAL / DECIMAL (or DECIMAL / integral coerced to DECIMAL): reuse
                // Arrow's checked decimal division so precision/scale match Spark;
                // div-by-zero and precision overflow become per-element NULL.
                let out = try_arrow_arith(&left_arr, &right_arr, return_field.data_type(), div)?;
                let out = null_decimal_overflow(out)?;
                if matches!(left, ColumnarValue::Scalar(_))
                    && matches!(right, ColumnarValue::Scalar(_))
                {
                    Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?))
                } else {
                    Ok(ColumnarValue::Array(out))
                }
            }
            (l, r) => Err(unsupported_data_types_exec_err(
                "try_divide",
                "Int32, Int64, Float, Decimal o Interval(YearMonth) / Int32",
                &[l.clone(), r.clone()],
            )),
        }
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        if types.len() != 2 {
            return Err(invalid_arg_count_exec_err(
                "try_divide",
                (2, 2),
                types.len(),
            ));
        }

        let left = &types[0];
        let right = &types[1];

        if *left == DataType::Null {
            return Ok(vec![right.clone(), right.clone()]);
        } else if *right == DataType::Null {
            return Ok(vec![left.clone(), left.clone()]);
        }

        if matches!(
            (left, right),
            (DataType::Interval(YearMonth), DataType::Int32)
                | (DataType::Int32, DataType::Interval(YearMonth))
        ) {
            return Ok(vec![DataType::Interval(YearMonth), DataType::Int32]);
        }

        if matches!(
            (left, right),
            (DataType::Interval(MonthDayNano), DataType::Int32)
                | (DataType::Int32, DataType::Interval(MonthDayNano))
        ) {
            return Ok(vec![DataType::Interval(MonthDayNano), DataType::Int32]);
        }
        if matches!(
            (left, right),
            (DataType::Interval(MonthDayNano), DataType::Int64)
                | (DataType::Int64, DataType::Interval(MonthDayNano))
        ) {
            return Ok(vec![DataType::Interval(MonthDayNano), DataType::Int64]);
        }
        if matches!(
            (left, right),
            (DataType::Int32, DataType::Int32)
                | (DataType::Int64, DataType::Int64)
                | (DataType::Int32, DataType::Int64)
                | (DataType::Int64, DataType::Int32)
        ) {
            if *left == DataType::Int64 || *right == DataType::Int64 {
                return Ok(vec![DataType::Int64, DataType::Int64]);
            } else {
                return Ok(vec![DataType::Int32, DataType::Int32]);
            }
        }

        if is_float_or_decimal(left) || is_float_or_decimal(right) {
            // Any float/double (or DOUBLE / DECIMAL) computes in DOUBLE; a pure
            // DECIMAL division keeps DataFusion's coerced decimal input types.
            if is_float(left) || is_float(right) {
                return Ok(vec![DataType::Float64, DataType::Float64]);
            }
            let (l, r) = arith_input_types(left, Operator::Divide, right)?;
            return Ok(vec![l, r]);
        }

        Err(unsupported_data_types_exec_err(
            "try_divide",
            "Int32, Int64, Float, Decimal o Interval(YearMonth) / Int32",
            types,
        ))
    }
}
