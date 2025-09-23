use std::any::Any;
use std::ops::Neg;
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, Date32Array};
use datafusion::arrow::datatypes::IntervalUnit::{MonthDayNano, YearMonth};
use datafusion::arrow::datatypes::TimeUnit::Microsecond;
use datafusion::arrow::datatypes::{
    DataType, Date32Type, DurationMicrosecondType, Int32Type, Int64Type, IntervalMonthDayNano,
    IntervalMonthDayNanoType, IntervalYearMonthType, TimestampMicrosecondType,
};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_types_exec_err};
use crate::scalar::math::utils::try_op::{
    add_months, binary_op_scalar_or_array, try_add_interval_monthdaynano, try_binary_op_date32_i32,
    try_binary_op_primitive, try_op_date32_interval_yearmonth, try_op_date32_monthdaynano,
    try_op_interval_yearmonth, try_op_timestamp_duration,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTrySubtract {
    signature: Signature,
}

impl Default for SparkTrySubtract {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTrySubtract {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkTrySubtract {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "try_subtract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [DataType::Int32, DataType::Int32]=> Ok(DataType::Int32),
            [DataType::Int64, DataType::Int64]
            | [DataType::Int32,DataType::Int64]
            | [DataType::Int64, DataType::Int32] => Ok(DataType::Int64),
            [DataType::Date32, DataType::Int32]
            | [DataType::Date32, DataType::Interval(YearMonth)]
            | [DataType::Date32, DataType::Interval(MonthDayNano)]
            | [DataType::Int32, DataType::Date32]
            | [DataType::Interval(YearMonth), DataType::Date32]
            | [DataType::Interval(MonthDayNano), DataType::Date32] => Ok(DataType::Date32),
            [DataType::Interval(YearMonth), DataType::Interval(YearMonth)] => Ok(DataType::Interval(YearMonth)),
            [DataType::Interval(MonthDayNano), DataType::Interval(MonthDayNano)] => Ok(DataType::Interval(MonthDayNano)),
            [DataType::Timestamp(Microsecond, _), DataType::Duration(Microsecond)] => {
                Ok(DataType::Timestamp(Microsecond, None))
            }

            _ => Err(unsupported_data_types_exec_err(
                "try_subtract",
                "Int32, Int64, Date32, Interval(YearMonth), Interval(MonthDayNano), Timestamp(Microsecond), Duration(Microsecond)",
                arg_types,
            )),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let [left, right] = args.as_slice() else {
            return Err(invalid_arg_count_exec_err(
                "spark_try_subtract",
                (2, 2),
                args.len(),
            ));
        };

        let len = match (&left, &right) {
            (ColumnarValue::Array(arr), _) => arr.len(),
            (_, ColumnarValue::Array(arr)) => arr.len(),
            _ => 1,
        };

        let left_arr = match left {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(s) => s.to_array_of_size(len)?,
        };

        let right_arr = match right {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(s) => s.to_array_of_size(len)?,
        };

        match (left_arr.data_type(), right_arr.data_type()) {
            (DataType::Int32, DataType::Int32) => {
                let l = left_arr.as_primitive::<Int32Type>();
                let r= right_arr.as_primitive::<Int32Type>();
                let result = try_binary_op_primitive::<Int32Type, _>(l, r, i32::checked_sub);

                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Int64, DataType::Int64) => {
                let l= left_arr.as_primitive::<Int64Type>();
                let r = right_arr.as_primitive::<Int64Type>();
                let result = try_binary_op_primitive::<Int64Type, _>(l, r, i64::checked_sub);

                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Date32, DataType::Int32) => {
                let l = left_arr.as_primitive::<Date32Type>();
                let r= right_arr.as_primitive::<Int32Type>();
                let result= try_binary_op_date32_i32(l, r, i32::checked_sub);

                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Date32, DataType::Interval(YearMonth)) => {
                let l = left_arr.as_primitive::<Date32Type>();
                let r = right_arr.as_primitive::<IntervalYearMonthType>();
                let result: Date32Array = try_op_date32_interval_yearmonth(l, r, |d, m| add_months(d, -m));

                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Date32, DataType::Interval(MonthDayNano)) => {
                let l= left_arr.as_primitive::<Date32Type>();
                let r = right_arr.as_primitive::<IntervalMonthDayNanoType>();
                let result = try_op_date32_monthdaynano(l, r, |x| {
                    IntervalMonthDayNano::new(
                        -x.months,
                        -x.days,
                        -x.nanoseconds,
                    )
                });

                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Interval(YearMonth), DataType::Interval(YearMonth)) => {
                let l= left_arr.as_primitive::<IntervalYearMonthType>();
                let r = right_arr.as_primitive::<IntervalYearMonthType>();
                let result = try_op_interval_yearmonth(l, r, i32::checked_sub);

                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Interval(MonthDayNano), DataType::Interval(MonthDayNano)) => {
                let l = left_arr.as_primitive::<IntervalMonthDayNanoType>();
                let r = right_arr.as_primitive::<IntervalMonthDayNanoType>();
                let negated_r = r.iter().map(|opt| opt.map(|v| v.neg())).collect();
                let result  = try_add_interval_monthdaynano(l, &negated_r);

                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Timestamp(Microsecond, tz_l), DataType::Duration(Microsecond)) => {
                let tz = tz_l.clone();

                let l = left_arr.as_primitive::<TimestampMicrosecondType>();
                let r = right_arr.as_primitive::<DurationMicrosecondType>();
                let result = try_op_timestamp_duration(l, r, i64::checked_sub);


                if matches!(left, ColumnarValue::Scalar(_))
                    && matches!(right, ColumnarValue::Scalar(_))
                {
                    let mut scalar = ScalarValue::try_from_array(&result, 0)?;
                    if let ScalarValue::TimestampMicrosecond(_, tz_field) = &mut scalar {
                        *tz_field = tz;
                    }
                    Ok(ColumnarValue::Scalar(scalar))
                } else {
                    Ok(ColumnarValue::Array(Arc::new(result)))
                }
            }

            (l, r) => Err(unsupported_data_types_exec_err(
                "spark_try_subtract",
                "Int32, Int64, Date32, Interval(YearMonth), Interval(MonthDayNano), Timestamp(Microsecond, [None | Some(tz)]) - Duration(Microsecond)",
                &[l.clone(), r.clone()],
            )),
        }
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        if types.len() != 2 {
            return Err(invalid_arg_count_exec_err(
                "spark_try_subtract",
                (2, 2),
                types.len(),
            ));
        }
        let left: &DataType = &types[0];
        let right: &DataType = &types[1];

        if *left == DataType::Null {
            return Ok(vec![right.clone(), right.clone()]);
        } else if *right == DataType::Null {
            return Ok(vec![left.clone(), left.clone()]);
        }

        match (left, right) {
            (DataType::Int32, DataType::Int32) => Ok(vec![left.clone(), right.clone()]),
            (DataType::Int64, DataType::Int64) => Ok(vec![left.clone(), right.clone()]),
            (DataType::Int32, DataType::Int64) | (DataType::Int64, DataType::Int32) => {
                Ok(vec![DataType::Int64, DataType::Int64])
            }
            (DataType::Date32, DataType::Int32) => Ok(vec![left.clone(), right.clone()]),
            (DataType::Date32, DataType::Interval(YearMonth)) => {
                Ok(vec![left.clone(), right.clone()])
            }
            (DataType::Date32, DataType::Interval(MonthDayNano)) => {
                Ok(vec![left.clone(), right.clone()])
            }
            (DataType::Interval(YearMonth), DataType::Interval(YearMonth)) => {
                Ok(vec![left.clone(), right.clone()])
            }
            (DataType::Interval(MonthDayNano), DataType::Interval(MonthDayNano)) => {
                Ok(vec![left.clone(), right.clone()])
            }
            (DataType::Timestamp(Microsecond, _), DataType::Duration(Microsecond)) => Ok(vec![
                DataType::Timestamp(Microsecond, None),
                DataType::Duration(Microsecond),
            ]),
            (DataType::Utf8, DataType::Int32) => Ok(vec![DataType::Date32, DataType::Int32]),

            _ => Err(unsupported_data_types_exec_err(
                "spark_try_subtract",
                "Int32, Int64, Date32 o Interval(YearMonth)",
                types,
            )),
        }
    }
}
