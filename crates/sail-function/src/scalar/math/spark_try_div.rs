use std::any::Any;

use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes::IntervalUnit::{MonthDayNano, YearMonth};
use datafusion::arrow::datatypes::{
    DataType, DurationMicrosecondType, Int32Type, Int64Type, IntervalMonthDayNanoType,
    IntervalYearMonthType, TimeUnit,
};
use datafusion_common::Result;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_types_exec_err};
use crate::scalar::math::utils::try_op::{
    binary_op_scalar_or_array, try_binary_op_to_float64, try_div_interval_monthdaynano_i32,
    try_div_interval_monthdaynano_i64, try_op_duration_microsecond_i32,
    try_op_duration_microsecond_i64, try_op_interval_yearmonth_i32, try_op_interval_yearmonth_i64,
};

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
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "try_divide"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [lhs, rhs] if lhs.is_integer() && rhs.is_integer() => Ok(DataType::Float64),
            [DataType::Interval(unit), rhs]
                if matches!(unit, YearMonth | MonthDayNano) && rhs.is_integer() =>
            {
                Ok(DataType::Interval(*unit))
            }
            [DataType::Duration(TimeUnit::Microsecond), rhs] if rhs.is_integer() => {
                Ok(DataType::Duration(TimeUnit::Microsecond))
            }
            _ => Err(unsupported_data_types_exec_err(
                "try_divide",
                "Int, Interval(YearMonth), Interval(MonthDayNano), or Duration(Microsecond)",
                arg_types,
            )),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
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
            (DataType::Interval(YearMonth), DataType::Int64) => {
                let l = left_arr.as_primitive::<IntervalYearMonthType>();
                let r = right_arr.as_primitive::<Int64Type>();
                let result = try_op_interval_yearmonth_i64(l, r, i64::checked_div);
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
            (DataType::Duration(TimeUnit::Microsecond), DataType::Int32) => {
                let l = left_arr.as_primitive::<DurationMicrosecondType>();
                let r = right_arr.as_primitive::<Int32Type>();
                let result = try_op_duration_microsecond_i32(l, r, i64::checked_div);
                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Duration(TimeUnit::Microsecond), DataType::Int64) => {
                let l = left_arr.as_primitive::<DurationMicrosecondType>();
                let r = right_arr.as_primitive::<Int64Type>();
                let result = try_op_duration_microsecond_i64(l, r, i64::checked_div);
                binary_op_scalar_or_array(left, right, result)
            }
            (l, r) => Err(unsupported_data_types_exec_err(
                "try_divide",
                "Int, Interval(YearMonth), Interval(MonthDayNano), or Duration(Microsecond)",
                &[l.clone(), r.clone()],
            )),
        }
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        let [left, right] = types else {
            return Err(invalid_arg_count_exec_err(
                "try_divide",
                (2, 2),
                types.len(),
            ));
        };

        let widen = |a: &DataType, b: &DataType| -> DataType {
            let is_wide =
                |t: &DataType| matches!(t, DataType::Int64 | DataType::UInt32 | DataType::UInt64);
            if is_wide(a) || is_wide(b) {
                DataType::Int64
            } else {
                DataType::Int32
            }
        };

        match (left, right) {
            (DataType::Null, other) | (other, DataType::Null) => {
                Ok(vec![other.clone(), other.clone()])
            }
            (DataType::Interval(unit), rhs)
                if matches!(unit, YearMonth | MonthDayNano) && rhs.is_integer() =>
            {
                Ok(vec![DataType::Interval(*unit), widen(rhs, rhs)])
            }
            (lhs, DataType::Interval(unit))
                if matches!(unit, YearMonth | MonthDayNano) && lhs.is_integer() =>
            {
                Ok(vec![DataType::Interval(*unit), widen(lhs, lhs)])
            }
            (DataType::Duration(TimeUnit::Microsecond), rhs) if rhs.is_integer() => Ok(vec![
                DataType::Duration(TimeUnit::Microsecond),
                widen(rhs, rhs),
            ]),
            (lhs, DataType::Duration(TimeUnit::Microsecond)) if lhs.is_integer() => Ok(vec![
                DataType::Duration(TimeUnit::Microsecond),
                widen(lhs, lhs),
            ]),
            (lhs, rhs) if lhs.is_integer() && rhs.is_integer() => {
                let t = widen(lhs, rhs);
                Ok(vec![t.clone(), t])
            }
            _ => Err(unsupported_data_types_exec_err(
                "try_divide",
                "Int, Interval(YearMonth), Interval(MonthDayNano), or Duration(Microsecond)",
                types,
            )),
        }
    }
}
