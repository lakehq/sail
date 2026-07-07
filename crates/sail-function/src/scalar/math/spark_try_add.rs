use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::compute::kernels::numeric::add;
use datafusion::arrow::datatypes::IntervalUnit::{MonthDayNano, YearMonth};
use datafusion::arrow::datatypes::TimeUnit::Microsecond;
use datafusion::arrow::datatypes::{
    DataType, Date32Type, DurationMicrosecondType, Int32Type, Int64Type, IntervalMonthDayNanoType,
    IntervalYearMonthType, TimestampMicrosecondType,
};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_types_exec_err};
use crate::scalar::math::utils::try_op::{
    add_months, arith_input_types, arith_result_type, binary_op_scalar_or_array,
    is_float_or_decimal, null_decimal_overflow, try_add_interval_monthdaynano, try_arrow_arith,
    try_binary_op_date32_i32, try_binary_op_primitive, try_op_date32_interval_yearmonth,
    try_op_date32_monthdaynano, try_op_interval_yearmonth, try_op_timestamp_duration,
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [DataType::Int32, DataType::Int32] => Ok(DataType::Int32),
            [DataType::Int64, DataType::Int64]
            | [DataType::Int32, DataType::Int64]
            | [DataType::Int64, DataType::Int32] => Ok(DataType::Int64),
            [DataType::Date32, _] | [_, DataType::Date32] => Ok(DataType::Date32),
            [DataType::Interval(YearMonth), _] | [_, DataType::Interval(YearMonth)] => {
                Ok(DataType::Interval(YearMonth))
            }
            [DataType::Interval(MonthDayNano), DataType::Int32]
            | [DataType::Int32, DataType::Interval(MonthDayNano)]
            | [DataType::Interval(MonthDayNano), DataType::Int64]
            | [DataType::Int64, DataType::Interval(MonthDayNano)]
            | [
                DataType::Interval(MonthDayNano),
                DataType::Interval(MonthDayNano),
            ] => Ok(DataType::Interval(MonthDayNano)),
            [
                DataType::Timestamp(Microsecond, _),
                DataType::Duration(Microsecond),
            ] => Ok(DataType::Timestamp(Microsecond, None)),
            [left, right] if is_float_or_decimal(left) || is_float_or_decimal(right) => {
                // FLOAT/DOUBLE keep the wider float type; DECIMAL follows Spark's
                // add precision/scale rule (inherited from DataFusion).
                arith_result_type(left, Operator::Plus, right)
            }

            _ => Err(unsupported_data_types_exec_err(
                "try_add",
                "Int32, Int64, Float, Decimal, Interval(YearMonth), Interval(MonthDayNano)",
                arg_types,
            )),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, return_field, ..
        } = args;

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
            (l, r) if is_float_or_decimal(l) || is_float_or_decimal(r) => {
                // FLOAT never overflows to NULL (IEEE 754); DECIMAL overflow becomes
                // a per-element NULL. Reuse Arrow's checked add so precision/scale
                // match Spark.
                let out = try_arrow_arith(&left_arr, &right_arr, return_field.data_type(), add)?;
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
                "spark_try_add",
                "Int32, Int64, Float or Decimal",
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
                    DataType::Timestamp(Microsecond, _),
                    DataType::Duration(Microsecond)
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
        } else if is_float_or_decimal(left) || is_float_or_decimal(right) {
            let (cl, cr) = arith_input_types(left, Operator::Plus, right)?;
            Ok(vec![cl, cr])
        } else {
            Err(unsupported_data_types_exec_err(
                "spark_try_add",
                "Int32, Int64, Float, Decimal, Date32 o Interval(YearMonth)",
                types,
            ))
        }
    }
}
