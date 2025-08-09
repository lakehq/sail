use std::any::Any;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Decimal128Type, Int32Type, Int64Type};
use datafusion_common::Result;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::extension::function::error_utils::{
    invalid_arg_count_exec_err, unsupported_data_types_exec_err,
};
use crate::extension::function::math::common_try::{
    binary_op_scalar_or_array, try_binary_op_primitive,
};

#[derive(Debug)]
pub struct SparkTryMod {
    signature: Signature,
}

impl Default for SparkTryMod {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTryMod {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkTryMod {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "try_mod"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if let [DataType::Decimal128(pl, sl), DataType::Decimal128(pr, sr)] = arg_types {
            return Ok(DataType::Decimal128((*pl).max(*pr), (*sl).max(*sr)));
        }
        if arg_types.contains(&DataType::Int64) {
            Ok(DataType::Int64)
        } else {
            Ok(DataType::Int32)
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let [left, right] = args.as_slice() else {
            return Err(invalid_arg_count_exec_err(
                "spark_try_mod",
                (2, 2),
                args.len(),
            ));
        };

        let len: usize = match (&left, &right) {
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
                let result = try_binary_op_primitive::<Int32Type, _>(l, r, i32::checked_rem);

                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Int64, DataType::Int64) => {
                let l = left_arr.as_primitive::<Int64Type>();
                let r = right_arr.as_primitive::<Int64Type>();
                let result = try_binary_op_primitive::<Int64Type, _>(l, r, i64::checked_rem);

                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Decimal128(_, _), DataType::Decimal128(_, _)) => {
                let l = left_arr.as_primitive::<Decimal128Type>();
                let r = right_arr.as_primitive::<Decimal128Type>();
                let result =
                    try_binary_op_primitive::<Decimal128Type, _>(l, r, |a: i128, b: i128| {
                        if b == 0 {
                            None
                        } else {
                            Some(a % b)
                        }
                    });

                binary_op_scalar_or_array(left, right, result)
            }
            (l, r) => Err(unsupported_data_types_exec_err(
                "spark_try_mod",
                "Int32, Int64 o Decimal128",
                &[l.clone(), r.clone()],
            )),
        }
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        if types.len() != 2 {
            return Err(invalid_arg_count_exec_err(
                "spark_try_mod",
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

        let is_int = |t: &DataType| matches!(t, DataType::Int32 | DataType::Int64);

        match (left, right) {
            (DataType::Decimal128(pl, sl), DataType::Decimal128(pr, sr)) => {
                let p = (*pl).max(*pr);
                let s = (*sl).max(*sr);
                Ok(vec![DataType::Decimal128(p, s), DataType::Decimal128(p, s)])
            }
            (DataType::Decimal128(p, s), r) if is_int(r) => Ok(vec![
                DataType::Decimal128(*p, *s),
                DataType::Decimal128(*p, *s),
            ]),
            (l, DataType::Decimal128(p, s)) if is_int(l) => Ok(vec![
                DataType::Decimal128(*p, *s),
                DataType::Decimal128(*p, *s),
            ]),
            (l, r) if is_int(l) && is_int(r) => {
                if *l == DataType::Int64 || *r == DataType::Int64 {
                    Ok(vec![DataType::Int64, DataType::Int64])
                } else {
                    Ok(vec![DataType::Int32, DataType::Int32])
                }
            }
            _ => Err(unsupported_data_types_exec_err(
                "spark_try_mod",
                "Int32, Int64 o Decimal128",
                types,
            )),
        }
    }
}
