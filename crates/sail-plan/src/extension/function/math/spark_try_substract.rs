use std::any::Any;
use std::ops::Neg;
use std::sync::Arc;

use arrow::array::{
    Array, AsArray, Date32Array, PrimitiveArray, PrimitiveBuilder, TimestampMicrosecondArray,
};
use arrow::datatypes::IntervalUnit::{MonthDayNano, YearMonth};
use arrow::datatypes::TimeUnit::Microsecond;
use arrow::datatypes::{
    ArrowPrimitiveType, Date32Type, DurationMicrosecondType, Int32Type, Int64Type,
    IntervalMonthDayNanoType, IntervalYearMonthType, TimestampMicrosecondType,
};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::extension::function::error_utils::{
    invalid_arg_count_exec_err, unsupported_data_types_exec_err,
};
use crate::extension::function::math::spark_try_add::{
    try_add_date32_days, try_add_date32_interval_yearmonth, try_add_date32_monthdaynano,
    try_add_i32, try_add_i64, try_add_interval_monthdaynano, try_add_timestamp_duration,
};

#[derive(Debug)]
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
                DataType::Timestamp(Microsecond, _),
                DataType::Duration(Microsecond)
            ]
        ) {
            Ok(DataType::Timestamp(Microsecond, None))
        } else {
            Ok(DataType::Int32)
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
                let l: &PrimitiveArray<Int32Type> = left_arr.as_primitive::<Int32Type>();
                let r: &PrimitiveArray<Int32Type> = right_arr.as_primitive::<Int32Type>();
                let result: PrimitiveArray<Int32Type> = try_sub_i32(l, r);

                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Int64, DataType::Int64) => {
                let l: &PrimitiveArray<Int64Type> = left_arr.as_primitive::<Int64Type>();
                let r: &PrimitiveArray<Int64Type> = right_arr.as_primitive::<Int64Type>();
                let result: PrimitiveArray<Int64Type> = try_sub_i64(l, r);

                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Date32, DataType::Int32) => {
                let l: &PrimitiveArray<Date32Type> = left_arr.as_primitive::<Date32Type>();
                let r: &PrimitiveArray<Int32Type> = right_arr.as_primitive::<Int32Type>();

                let negated_r: PrimitiveArray<Int32Type> =
                    r.iter().map(|opt| opt.map(|v| v.wrapping_neg())).collect();

                let result: PrimitiveArray<Date32Type> = try_add_date32_days(l, &negated_r);

                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Date32, DataType::Interval(YearMonth)) => {
                let l: &PrimitiveArray<Date32Type> = left_arr.as_primitive::<Date32Type>();
                let r: &PrimitiveArray<IntervalYearMonthType> =
                    right_arr.as_primitive::<IntervalYearMonthType>();

                let negated_r: PrimitiveArray<IntervalYearMonthType> =
                    r.iter().map(|opt| opt.map(|v| v.wrapping_neg())).collect();

                let result: Date32Array = try_add_date32_interval_yearmonth(l, &negated_r);

                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Interval(YearMonth), DataType::Interval(YearMonth)) => {
                let l: &PrimitiveArray<IntervalYearMonthType> =
                    left_arr.as_primitive::<IntervalYearMonthType>();
                let r: &PrimitiveArray<IntervalYearMonthType> =
                    right_arr.as_primitive::<IntervalYearMonthType>();

                let negated_r: PrimitiveArray<IntervalYearMonthType> =
                    r.iter().map(|opt| opt.map(|v| v.wrapping_neg())).collect();

                let result: PrimitiveArray<IntervalYearMonthType> =
                    try_add_interval_yearmonth(l, &negated_r);

                binary_op_scalar_or_array(left, right, result)
            }

            (DataType::Date32, DataType::Interval(MonthDayNano)) => {
                let l: &PrimitiveArray<Date32Type> = left_arr.as_primitive::<Date32Type>();
                let r: &PrimitiveArray<IntervalMonthDayNanoType> =
                    right_arr.as_primitive::<IntervalMonthDayNanoType>();

                let negated_r: PrimitiveArray<IntervalMonthDayNanoType> =
                    r.iter().map(|opt| opt.map(|v| v.neg())).collect();

                let result: Date32Array = try_add_date32_monthdaynano(l, &negated_r);

                binary_op_scalar_or_array(left, right, result)
            }

            (DataType::Interval(MonthDayNano), DataType::Interval(MonthDayNano)) => {
                let l: &PrimitiveArray<IntervalMonthDayNanoType> =
                    left_arr.as_primitive::<IntervalMonthDayNanoType>();
                let r: &PrimitiveArray<IntervalMonthDayNanoType> =
                    right_arr.as_primitive::<IntervalMonthDayNanoType>();

                let negated_r: PrimitiveArray<IntervalMonthDayNanoType> =
                    r.iter().map(|opt| opt.map(|v| v.neg())).collect();

                let result: PrimitiveArray<IntervalMonthDayNanoType> =
                    try_add_interval_monthdaynano(l, &negated_r);

                binary_op_scalar_or_array(left, right, result)
            }
            (DataType::Timestamp(Microsecond, tz_l), DataType::Duration(Microsecond)) => {
                let tz = tz_l.clone(); // guarda la zona horaria

                let l: &PrimitiveArray<TimestampMicrosecondType> =
                    left_arr.as_primitive::<TimestampMicrosecondType>();
                let r: &PrimitiveArray<DurationMicrosecondType> =
                    right_arr.as_primitive::<DurationMicrosecondType>();

                let negated_r: PrimitiveArray<DurationMicrosecondType> =
                    r.iter().map(|opt| opt.map(|v| v.wrapping_neg())).collect();

                let result: TimestampMicrosecondArray = try_add_timestamp_duration(l, &negated_r);

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

            _ => Err(unsupported_data_types_exec_err(
                "spark_try_subtract",
                "Int32, Int64, Date32 o Interval(YearMonth)",
                types,
            )),
        }
    }
}

fn binary_op_scalar_or_array<T: ArrowPrimitiveType>(
    left: &ColumnarValue,
    right: &ColumnarValue,
    result: PrimitiveArray<T>,
) -> Result<ColumnarValue> {
    if matches!(left, ColumnarValue::Scalar(_)) && matches!(right, ColumnarValue::Scalar(_)) {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &result, 0,
        )?))
    } else {
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

fn try_sub_i32(
    left: &PrimitiveArray<Int32Type>,
    right: &PrimitiveArray<Int32Type>,
) -> PrimitiveArray<Int32Type> {
    let negated_r: PrimitiveArray<Int32Type> = right
        .iter()
        .map(|opt| opt.map(|v| v.wrapping_neg()))
        .collect();

    try_add_i32(left, &negated_r)
}

fn try_sub_i64(
    left: &PrimitiveArray<Int64Type>,
    right: &PrimitiveArray<Int64Type>,
) -> PrimitiveArray<Int64Type> {
    let negated_r: PrimitiveArray<Int64Type> = right
        .iter()
        .map(|opt| opt.map(|v| v.wrapping_neg()))
        .collect();

    try_add_i64(left, &negated_r)
}

fn try_add_interval_yearmonth(
    l: &PrimitiveArray<IntervalYearMonthType>,
    r: &PrimitiveArray<IntervalYearMonthType>,
) -> PrimitiveArray<IntervalYearMonthType> {
    let mut builder = PrimitiveBuilder::<IntervalYearMonthType>::with_capacity(l.len());
    for i in 0..l.len() {
        if l.is_null(i) || r.is_null(i) {
            builder.append_null();
        } else {
            let sum = l.value(i).checked_add(r.value(i));
            match sum {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            }
        }
    }
    builder.finish()
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int32Array, Int64Array, PrimitiveArray};
    use arrow::datatypes::{Int32Type, Int64Type};

    use super::*;

    #[test]
    fn test_try_sub_i32_no_overflow() {
        let left: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(10), Some(5), Some(3)]);
        let right: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(1), Some(2), Some(3)]);
        let result: Int32Array = try_sub_i32(&left, &right);
        let expected: Int32Array = Int32Array::from(vec![Some(9), Some(3), Some(0)]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_try_sub_i32_with_nulls() {
        let left: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(10), None, Some(3)]);
        let right: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(1), Some(2), None]);
        let result: Int32Array = try_sub_i32(&left, &right);
        let expected: Int32Array = Int32Array::from(vec![Some(9), None, None]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_try_sub_i32_overflow() {
        let left: PrimitiveArray<Int32Type> =
            Int32Array::from(vec![Some(i32::MIN), Some(i32::MAX)]);
        let right: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(1), Some(-1)]);
        let result: Int32Array = try_sub_i32(&left, &right);
        let expected: Int32Array = Int32Array::from(vec![None, None]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_try_sub_i64_no_overflow() {
        let left: PrimitiveArray<Int64Type> = Int64Array::from(vec![Some(10), Some(5), Some(3)]);
        let right: PrimitiveArray<Int64Type> = Int64Array::from(vec![Some(1), Some(2), Some(3)]);
        let result: Int64Array = try_sub_i64(&left, &right);
        let expected: Int64Array = Int64Array::from(vec![Some(9), Some(3), Some(0)]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_try_sub_i64_with_nulls() {
        let left: PrimitiveArray<Int64Type> = Int64Array::from(vec![Some(10), None, Some(3)]);
        let right: PrimitiveArray<Int64Type> = Int64Array::from(vec![Some(1), Some(2), None]);
        let result: Int64Array = try_sub_i64(&left, &right);
        let expected: Int64Array = Int64Array::from(vec![Some(9), None, None]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_try_sub_i64_overflow() {
        let left: PrimitiveArray<Int64Type> =
            Int64Array::from(vec![Some(i64::MIN), Some(i64::MAX)]);
        let right: PrimitiveArray<Int64Type> = Int64Array::from(vec![Some(1), Some(-1)]);
        let result: Int64Array = try_sub_i64(&left, &right);
        let expected: Int64Array = Int64Array::from(vec![None, None]);
        assert_eq!(result, expected);
    }
}
