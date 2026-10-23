use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes::IntervalUnit::{MonthDayNano, YearMonth};
use datafusion::arrow::datatypes::TimeUnit::Microsecond;
use datafusion::arrow::datatypes::{
    DataType, Date32Type, DurationMicrosecondType, FieldRef, Int32Type, Int64Type,
    IntervalMonthDayNanoType, IntervalYearMonthType, TimestampMicrosecondType,
};
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_types_exec_err};
use crate::scalar::math::utils::try_op::{
    add_months, binary_op_scalar_or_array, try_add_interval_monthdaynano, try_binary_op_date32_i32,
    try_binary_op_primitive, try_op_date32_interval_yearmonth, try_op_date32_monthdaynano,
    try_op_interval_yearmonth, try_op_timestamp_duration,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTryAdd {
    signature: Signature,
}

impl Default for SparkTryAdd {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTryAdd {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkTryAdd {
    fn name(&self) -> &str {
        "try_add"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        sail_type_system::policies::math_spark_try_add::return_type(_arg_types)
    }

    /// Spark: `TryAdd` is `RuntimeReplaceable`; both replacement branches declare `true`.
    /// numeric  <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala#L236>
    /// fallback <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/TryEval.scala#L50>
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        sail_type_system::policies::math_spark_try_add::return_field_from_args(args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let [left, right] = args.as_slice() else {
            return Err(invalid_arg_count_exec_err(
                "spark_try_add",
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
                let result = try_binary_op_primitive::<Int32Type, _>(l, r, i32::checked_add);

                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Int64, DataType::Int64) => {
                let l = left_arr.as_primitive::<Int64Type>();
                let r = right_arr.as_primitive::<Int64Type>();
                let result = try_binary_op_primitive::<Int64Type, _>(l, r, i64::checked_add);

                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Date32, DataType::Int32) => {
                let l = left_arr.as_primitive::<Date32Type>();
                let r = right_arr.as_primitive::<Int32Type>();
                let result = try_binary_op_date32_i32(l, r, i32::checked_add);

                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Date32, DataType::Interval(YearMonth)) => {
                let l = left_arr.as_primitive::<Date32Type>();
                let r = right_arr.as_primitive::<IntervalYearMonthType>();
                let result = try_op_date32_interval_yearmonth(l, r, add_months);

                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Interval(YearMonth), DataType::Interval(YearMonth)) => {
                let l = left_arr.as_primitive::<IntervalYearMonthType>();
                let r = right_arr.as_primitive::<IntervalYearMonthType>();
                let result = try_op_interval_yearmonth(l, r, i32::checked_add);

                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Date32, DataType::Interval(MonthDayNano)) => {
                let dates = left_arr.as_primitive::<Date32Type>();
                let intervals = right_arr.as_primitive::<IntervalMonthDayNanoType>();
                let result = try_op_date32_monthdaynano(dates, intervals, |x| x);

                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Interval(MonthDayNano), DataType::Interval(MonthDayNano)) => {
                let l = left_arr.as_primitive::<IntervalMonthDayNanoType>();
                let r = right_arr.as_primitive::<IntervalMonthDayNanoType>();
                let result = try_add_interval_monthdaynano(l, r);

                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Timestamp(Microsecond, _), DataType::Duration(Microsecond)) => {
                let l = left_arr.as_primitive::<TimestampMicrosecondType>();
                let r = right_arr.as_primitive::<DurationMicrosecondType>();
                let result = try_op_timestamp_duration(l, r, i64::checked_add);

                binary_op_scalar_or_array(left, right, result)
            }
            (l, r) => Err(unsupported_data_types_exec_err(
                "spark_try_add",
                "Int32 or Int64",
                &[l.clone(), r.clone()],
            )),
        }
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        sail_type_system::policies::math_spark_try_add::coerce_types(types)
    }
}
