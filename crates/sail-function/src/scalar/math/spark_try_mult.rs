use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::compute::{cast_with_options, CastOptions};
use datafusion::arrow::datatypes::IntervalUnit::{MonthDayNano, YearMonth};
use datafusion::arrow::datatypes::{
    DataType, Int32Type, Int64Type, IntervalMonthDayNanoType, IntervalYearMonthType,
};
use datafusion_common::utils::take_function_args;
use datafusion_common::Result;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_functions::utils::make_scalar_function;

use crate::error::unsupported_data_types_exec_err;
use crate::scalar::math::utils::try_op::{
    try_binary_op_primitive, try_op_interval_monthdaynano_i32, try_op_interval_monthdaynano_i64,
    try_op_interval_yearmonth_i32,
};

#[derive(Debug, PartialEq, Eq, Hash)]
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
        match arg_types {
            [DataType::Int32, DataType::Int32] => Ok(DataType::Int32),
            [DataType::Int64, DataType::Int64]
            | [DataType::Int32, DataType::Int64]
            | [DataType::Int64, DataType::Int32] => Ok(DataType::Int64),
            [DataType::Interval(YearMonth), DataType::Int32 | DataType::Int64]
            | [DataType::Int32 | DataType::Int64, DataType::Interval(YearMonth)] => {
                Ok(DataType::Interval(YearMonth))
            }
            [DataType::Interval(MonthDayNano), DataType::Int32 | DataType::Int64]
            | [DataType::Int32 | DataType::Int64, DataType::Interval(MonthDayNano)] => {
                Ok(DataType::Interval(MonthDayNano))
            }

            _ => Err(unsupported_data_types_exec_err(
                "try_multiply",
                "Int32, Int64, Interval(YearMonth), Interval(MonthDayNano)",
                arg_types,
            )),
        }
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        let [left, right] = take_function_args("try_multiply", types)?;

        match (left, right) {
            (DataType::Null, _) => Ok(vec![right.clone(), right.clone()]),
            (_, DataType::Null) => Ok(vec![left.clone(), left.clone()]),
            (DataType::Int32, DataType::Int32)
            | (DataType::Int64, DataType::Int64)
            | (
                DataType::Interval(YearMonth) | DataType::Interval(MonthDayNano),
                DataType::Int32 | DataType::Int64,
            )
            | (
                DataType::Int32 | DataType::Int64,
                DataType::Interval(YearMonth) | DataType::Interval(MonthDayNano),
            ) => Ok(vec![left.clone(), right.clone()]),
            (DataType::Int32, DataType::Int64) | (DataType::Int64, DataType::Int32) => {
                Ok(vec![DataType::Int64, DataType::Int64])
            }
            _ => Err(unsupported_data_types_exec_err(
                "try_multiply",
                "Int32, Int64, Interval(YearMonth), Interval(MonthDayNano)",
                types,
            )),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(try_multiply_inner, vec![])(&args.args)
    }
}

fn try_multiply_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [left_arr, right_arr] = take_function_args("try_multiply", args)?;

    let (left_arr, right_arr) = if matches!(
        right_arr.data_type(),
        DataType::Interval(YearMonth) | DataType::Interval(MonthDayNano)
    ) {
        (right_arr, left_arr)
    } else {
        (left_arr, right_arr)
    };

    match (left_arr.data_type(), right_arr.data_type()) {
        (DataType::Int32, DataType::Int32) => {
            let l = left_arr.as_primitive::<Int32Type>();
            let r = right_arr.as_primitive::<Int32Type>();
            Ok(Arc::new(try_binary_op_primitive(l, r, i32::checked_mul)))
        }
        (DataType::Int64, DataType::Int64) => {
            let l = left_arr.as_primitive::<Int64Type>();
            let r = right_arr.as_primitive::<Int64Type>();
            Ok(Arc::new(try_binary_op_primitive(l, r, i64::checked_mul)))
        }
        (DataType::Interval(YearMonth), DataType::Int32 | DataType::Int64) => {
            let right_arr = if matches!(right_arr.data_type(), DataType::Int32) {
                Ok(right_arr.clone())
            } else {
                cast_with_options(&right_arr, &DataType::Int32, &CastOptions::default())
            }?;
            let l = left_arr.as_primitive::<IntervalYearMonthType>();
            let r = right_arr.as_primitive::<Int32Type>();
            Ok(Arc::new(try_op_interval_yearmonth_i32(
                l,
                r,
                i32::checked_mul,
            )))
        }
        (DataType::Interval(MonthDayNano), DataType::Int32 | DataType::Int64) => {
            let l = left_arr.as_primitive::<IntervalMonthDayNanoType>();

            Ok(Arc::new(
                if matches!(right_arr.data_type(), DataType::Int32) {
                    let r = right_arr.as_primitive::<Int32Type>();
                    try_op_interval_monthdaynano_i32(l, r, |a, b| a.checked_mul(b as i64))
                } else {
                    let r = right_arr.as_primitive::<Int64Type>();
                    try_op_interval_monthdaynano_i64(l, r, i64::checked_mul)
                },
            ))
        }
        (l, r) => Err(unsupported_data_types_exec_err(
            "try_multiply",
            "Int32, Int64, Interval(YearMonth), Interval(MonthDayNano)",
            &[l.clone(), r.clone()],
        )),
    }
}
