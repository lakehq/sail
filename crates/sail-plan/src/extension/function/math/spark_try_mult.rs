use std::any::Any;

use arrow::array::{Array, AsArray};
use arrow::datatypes::IntervalUnit::YearMonth;
use arrow::datatypes::{DataType, Int32Type, Int64Type, IntervalYearMonthType};
use datafusion_common::Result;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::extension::function::error_utils::{
    invalid_arg_count_exec_err, unsupported_data_types_exec_err,
};
use crate::extension::function::math::common_try::{
    binary_op_scalar_or_array, try_binary_op_primitive, try_op_interval_yearmonth_i32,
};

#[derive(Debug)]
pub struct SparkTryMult {
    signature: Signature,
}

impl Default for SparkTryMult {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTryMult {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkTryMult {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "try_multiply"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.contains(&DataType::Int64) {
            Ok(DataType::Int64)
        } else if matches!(
            arg_types,
            [DataType::Interval(YearMonth), DataType::Int32]
                | [DataType::Int32, DataType::Interval(YearMonth)]
        ) {
            Ok(DataType::Interval(YearMonth))
        } else {
            Ok(DataType::Int32)
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let [left, right] = args.as_slice() else {
            return Err(invalid_arg_count_exec_err(
                "try_multiply",
                (2, 2),
                args.len(),
            ));
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
                let result = try_binary_op_primitive::<Int32Type, _>(l, r, i32::checked_mul);
                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Int64, DataType::Int64) => {
                let l = left_arr.as_primitive::<Int64Type>();
                let r = right_arr.as_primitive::<Int64Type>();
                let result = try_binary_op_primitive::<Int64Type, _>(l, r, i64::checked_mul);
                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Interval(YearMonth), DataType::Int32) => {
                let l = left_arr.as_primitive::<IntervalYearMonthType>();
                let r = right_arr.as_primitive::<Int32Type>();
                let result = try_op_interval_yearmonth_i32(l, r, i32::checked_mul);
                binary_op_scalar_or_array(left, right, result)
            }
            (l, r) => Err(unsupported_data_types_exec_err(
                "try_multiply",
                "Int32 o Int64",
                &[l.clone(), r.clone()],
            )),
        }
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        if types.len() != 2 {
            return Err(invalid_arg_count_exec_err(
                "try_multiply",
                (2, 2),
                types.len(),
            ));
        }

        let left = &types[0];
        let right = &types[1];

        // Null propagation
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

        Err(unsupported_data_types_exec_err(
            "try_multiply",
            "Int32, Int64 o Interval(YearMonth) con escalar",
            types,
        ))
    }
}
