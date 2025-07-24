use std::any::Any;

use arrow::array::{Array, AsArray, PrimitiveArray, PrimitiveBuilder};
use arrow::datatypes::IntervalUnit::{MonthDayNano, YearMonth};
use arrow::datatypes::{
    Date32Type, DurationMicrosecondType, Int32Type, Int64Type, IntervalMonthDayNanoType,
    IntervalYearMonthType, TimeUnit, TimestampMicrosecondType,
};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::extension::function::error_utils::{
    invalid_arg_count_exec_err, unsupported_data_types_exec_err,
};
use crate::extension::function::math::common_try::{
    binary_op_scalar_or_array, try_add_date32_interval_yearmonth, try_add_date32_monthdaynano,
    try_add_interval_monthdaynano, try_add_timestamp_duration, try_binary_op_date32_i32,
    try_binary_op_primitive,
};

#[derive(Debug)]
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
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "try_add"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.contains(&DataType::Date32) {
            Ok(DataType::Date32)
        } else if arg_types.contains(&DataType::Int64) {
            Ok(DataType::Int64)
        } else if matches!(
            arg_types,
            [DataType::Interval(YearMonth), DataType::Interval(YearMonth)]
        ) {
            Ok(DataType::Interval(YearMonth))
        } else if matches!(
            arg_types,
            [
                DataType::Interval(MonthDayNano),
                DataType::Interval(MonthDayNano)
            ]
        ) {
            Ok(DataType::Interval(MonthDayNano))
        } else if matches!(
            arg_types,
            [
                DataType::Timestamp(TimeUnit::Microsecond, _),
                DataType::Duration(TimeUnit::Microsecond)
            ]
        ) {
            Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
        } else {
            Ok(DataType::Int32)
        }
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
                let result = try_add_date32_interval_yearmonth(l, r);

                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Interval(YearMonth), DataType::Interval(YearMonth)) => {
                let l = left_arr.as_primitive::<IntervalYearMonthType>();
                let r = right_arr.as_primitive::<IntervalYearMonthType>();

                let len: usize = l.len();
                let mut builder: PrimitiveBuilder<IntervalYearMonthType> =
                    PrimitiveBuilder::<IntervalYearMonthType>::with_capacity(len);

                for i in 0..len {
                    if l.is_null(i) || r.is_null(i) {
                        builder.append_null();
                    } else {
                        let sum = l.value(i).checked_add(r.value(i));
                        match sum {
                            Some(v) => builder.append_value(v),
                            None => builder.append_null(), // unlikely for i32 but por seguridad
                        }
                    }
                }

                let result: PrimitiveArray<IntervalYearMonthType> = builder.finish();
                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Date32, DataType::Interval(MonthDayNano)) => {
                let dates = left_arr.as_primitive::<Date32Type>();
                let intervals = right_arr.as_primitive::<IntervalMonthDayNanoType>();
                let result = try_add_date32_monthdaynano(dates, intervals);
                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Interval(MonthDayNano), DataType::Interval(MonthDayNano)) => {
                let l = left_arr.as_primitive::<IntervalMonthDayNanoType>();
                let r = right_arr.as_primitive::<IntervalMonthDayNanoType>();
                let result = try_add_interval_monthdaynano(l, r);
                binary_op_scalar_or_array(left, right, result)
            }
            (
                DataType::Timestamp(TimeUnit::Microsecond, _),
                DataType::Duration(TimeUnit::Microsecond),
            ) => {
                let l = left_arr.as_primitive::<TimestampMicrosecondType>();
                let r = right_arr.as_primitive::<DurationMicrosecondType>();
                let result = try_add_timestamp_duration(l, r);

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
        if types.len() != 2 {
            return Err(invalid_arg_count_exec_err(
                "spark_try_add",
                (2, 2),
                types.len(),
            ));
        }
        let left: &DataType = &types[0];
        let right: &DataType = &types[1];

        let valid_pair = matches!(
            (left, right),
            (DataType::Int32, DataType::Int32)
                | (DataType::Int64, DataType::Int64)
                | (DataType::Date32, DataType::Int32)
                | (DataType::Date32, DataType::Interval(YearMonth))
                | (DataType::Date32, DataType::Interval(MonthDayNano))
                | (DataType::Interval(YearMonth), DataType::Date32)
                | (DataType::Interval(YearMonth), DataType::Interval(YearMonth))
                | (
                    DataType::Timestamp(TimeUnit::Microsecond, _),
                    DataType::Duration(TimeUnit::Microsecond)
                )
                | (
                    DataType::Interval(MonthDayNano),
                    DataType::Interval(MonthDayNano)
                )
        );
        if *left == DataType::Null {
            return Ok(vec![right.clone(), right.clone()]);
        } else if *right == DataType::Null {
            return Ok(vec![left.clone(), left.clone()]);
        }

        if valid_pair {
            Ok(vec![left.clone(), right.clone()])
        } else {
            Err(unsupported_data_types_exec_err(
                "spark_try_add",
                "Int32, Int64, Date32 o Interval(YearMonth)",
                types,
            ))
        }
    }
}
