use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::IntervalUnit::{MonthDayNano, YearMonth};
use datafusion::arrow::datatypes::{
    DataType, FieldRef, Int32Type, Int64Type, IntervalMonthDayNanoType, IntervalYearMonthType,
};
use datafusion_common::Result;
use datafusion_common::utils::take_function_args;
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
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
    fn name(&self) -> &str {
        "try_multiply"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        sail_type_system::policies::math_spark_try_mult::return_type(_arg_types)
    }

    /// Spark: `TryMultiply` is `RuntimeReplaceable`; both replacement branches declare `true`.
    /// numeric  <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala#L236>
    /// fallback <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/TryEval.scala#L50>
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        sail_type_system::policies::math_spark_try_mult::return_field_from_args(args)
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        sail_type_system::policies::math_spark_try_mult::coerce_types(types)
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
