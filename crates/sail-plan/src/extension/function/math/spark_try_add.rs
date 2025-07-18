use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, AsArray, Int64Array, Int64Builder, PrimitiveArray, PrimitiveBuilder};
use arrow::datatypes::{Date32Type, Int32Type, Int64Type};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::extension::function::error_utils::{
    invalid_arg_count_exec_err, unsupported_data_types_exec_err,
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

                let result = try_add_manual_i32(l, r);
                if matches!(left, ColumnarValue::Scalar(_))
                    && matches!(right, ColumnarValue::Scalar(_))
                {
                    let scalar: ScalarValue = ScalarValue::try_from_array(&result, 0)?;
                    Ok(ColumnarValue::Scalar(scalar))
                } else {
                    Ok(ColumnarValue::Array(Arc::new(result)))
                }
            }
            (DataType::Int64, DataType::Int64) => {
                let l = left_arr.as_primitive::<Int64Type>();
                let r = right_arr.as_primitive::<Int64Type>();

                let result = try_add_manual_i64(l, r);
                if matches!(left, ColumnarValue::Scalar(_))
                    && matches!(right, ColumnarValue::Scalar(_))
                {
                    let scalar: ScalarValue = ScalarValue::try_from_array(&result, 0)?;
                    Ok(ColumnarValue::Scalar(scalar))
                } else {
                    Ok(ColumnarValue::Array(Arc::new(result)))
                }
            }
            (DataType::Date32, DataType::Int32) => {
                let l = left_arr.as_primitive::<Date32Type>();
                let r = right_arr.as_primitive::<Int32Type>();
                let result = try_add_date32_days(l, r);

                if matches!(left, ColumnarValue::Scalar(_))
                    && matches!(right, ColumnarValue::Scalar(_))
                {
                    let scalar: ScalarValue = ScalarValue::try_from_array(&result, 0)?;
                    Ok(ColumnarValue::Scalar(scalar))
                } else {
                    Ok(ColumnarValue::Array(Arc::new(result)))
                }
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
        let valid_left: bool = matches!(left, DataType::Int32 | DataType::Int64 | DataType::Date32);
        let right: &DataType = &types[1];
        let valid_right: bool = matches!(right, DataType::Int32 | DataType::Int64);

        if valid_left && valid_right {
            Ok(vec![left.clone(), right.clone()])
        } else {
            Err(unsupported_data_types_exec_err(
                "spark_try_add",
                "Int32, Int32",
                types,
            ))
        }
    }
}

fn try_add_manual_i32(
    left: &PrimitiveArray<Int32Type>,
    right: &PrimitiveArray<Int32Type>,
) -> PrimitiveArray<Int32Type> {
    let len: usize = left.len();
    let mut builder: PrimitiveBuilder<Int32Type> =
        PrimitiveBuilder::<Int32Type>::with_capacity(len);

    for i in 0..len {
        if left.is_null(i) || right.is_null(i) {
            builder.append_null();
        } else {
            let a = left.value(i);
            let b = right.value(i);

            match a.checked_add(b) {
                Some(sum) => builder.append_value(sum),
                None => builder.append_null(),
            }
        }
    }
    builder.finish()
}

fn try_add_manual_i64(
    left: &PrimitiveArray<Int64Type>,
    right: &PrimitiveArray<Int64Type>,
) -> Int64Array {
    let len: usize = left.len();
    let mut builder: PrimitiveBuilder<Int64Type> = Int64Builder::with_capacity(len);

    for i in 0..len {
        if left.is_null(i) || right.is_null(i) {
            builder.append_null();
        } else {
            let a = left.value(i);
            let b = right.value(i);

            match a.checked_add(b) {
                Some(sum) => builder.append_value(sum),
                None => builder.append_null(),
            }
        }
    }
    builder.finish()
}

fn try_add_date32_days(
    date_array: &PrimitiveArray<Date32Type>,
    days_array: &PrimitiveArray<Int32Type>,
) -> PrimitiveArray<Date32Type> {
    let len = date_array.len();
    let mut builder = PrimitiveBuilder::<Date32Type>::with_capacity(len);

    for i in 0..len {
        if date_array.is_null(i) || days_array.is_null(i) {
            builder.append_null();
        } else {
            let base = date_array.value(i);
            let delta = days_array.value(i);

            match base.checked_add(delta) {
                Some(sum) => builder.append_value(sum),
                None => builder.append_null(),
            }
        }
    }

    builder.finish()
}

#[cfg(test)]
mod tests {
    use arrow::array::{Date32Array, Int32Array, PrimitiveArray};
    use arrow::datatypes::{Int32Type, Int64Type};
    use chrono::NaiveDate;
    use datafusion_common::DataFusionError;

    use super::*;

    #[test]
    fn test_try_add_manual_i32_no_overflow() {
        let left: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(1), Some(2), Some(3)]);
        let right: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(10), Some(20), Some(30)]);
        let result: Int32Array = try_add_manual_i32(&left, &right);
        let expected: PrimitiveArray<Int32Type> =
            Int32Array::from(vec![Some(11), Some(22), Some(33)]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_try_add_manual_i32_with_nulls() {
        let left: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(1), None, Some(3)]);
        let right: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(10), Some(20), None]);
        let result: Int32Array = try_add_manual_i32(&left, &right);
        let expected: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(11), None, None]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_try_add_manual_i32_overflow() {
        let left: PrimitiveArray<Int32Type> =
            Int32Array::from(vec![Some(i32::MAX), Some(i32::MIN)]);
        let right: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(1), Some(-1)]);
        let result: Int32Array = try_add_manual_i32(&left, &right);
        let expected: PrimitiveArray<Int32Type> = Int32Array::from(vec![None, None]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_try_add_manual_i64_no_overflow() {
        let left: PrimitiveArray<Int64Type> = Int64Array::from(vec![Some(1), Some(2), Some(3)]);
        let right: PrimitiveArray<Int64Type> = Int64Array::from(vec![Some(10), Some(20), Some(30)]);
        let result: Int64Array = try_add_manual_i64(&left, &right);
        let expected: PrimitiveArray<Int64Type> =
            Int64Array::from(vec![Some(11), Some(22), Some(33)]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_try_add_manual_i64_with_nulls() {
        let left: PrimitiveArray<Int64Type> = Int64Array::from(vec![Some(1), None, Some(3)]);
        let right: PrimitiveArray<Int64Type> = Int64Array::from(vec![Some(10), Some(20), None]);
        let result: Int64Array = try_add_manual_i64(&left, &right);
        let expected: PrimitiveArray<Int64Type> = Int64Array::from(vec![Some(11), None, None]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_try_add_manual_i64_overflow() {
        let left: PrimitiveArray<Int64Type> =
            Int64Array::from(vec![Some(i64::MAX), Some(i64::MIN)]);
        let right: PrimitiveArray<Int64Type> = Int64Array::from(vec![Some(1), Some(-1)]);
        let result: Int64Array = try_add_manual_i64(&left, &right);
        let expected: PrimitiveArray<Int64Type> = Int64Array::from(vec![None, None]); // overflow en ambos
        assert_eq!(result, expected);
    }

    fn days_since_1970(date: &str) -> Result<i32> {
        let base = NaiveDate::from_ymd_opt(1970, 1, 1)
            .ok_or_else(|| DataFusionError::Execution("Invalid base date: 1970-01-01".into()))?;

        let d = NaiveDate::parse_from_str(date, "%Y-%m-%d").map_err(|e| {
            DataFusionError::Execution(format!("Invalid date string '{date}': {e}"))
        })?;

        Ok(d.signed_duration_since(base).num_days() as i32)
    }

    fn to_date32_array(dates: &[Option<&str>]) -> Result<Date32Array> {
        let mut values = Vec::with_capacity(dates.len());
        for s in dates {
            match s {
                Some(d) => values.push(Some(days_since_1970(d)?)),
                None => values.push(None),
            }
        }
        Ok(Date32Array::from(values))
    }

    #[test]
    fn test_add_days_basic() -> Result<()> {
        let dates = to_date32_array(&[
            Some("2015-09-30"),
            Some("2000-01-01"),
            Some("2021-01-01"),
            None,
        ])?;

        let days = Int32Array::from(vec![Some(1), Some(366), Some(1), Some(100)]);
        let result = try_add_date32_days(&dates, &days);

        let expected = to_date32_array(&[
            Some("2015-10-01"),
            Some("2001-01-01"),
            Some("2021-01-02"),
            None,
        ])?;

        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_add_days_with_nulls() -> Result<()> {
        let dates = to_date32_array(&[Some("2020-01-01"), None, Some("2022-01-01")])?;

        let days = Int32Array::from(vec![None, Some(30), Some(365)]);
        let result = try_add_date32_days(&dates, &days);

        let expected = to_date32_array(&[None, None, Some("2023-01-01")])?;

        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_add_days_overflow() -> Result<()> {
        let dates = Date32Array::from(vec![Some(i32::MAX)]);
        let days = Int32Array::from(vec![Some(1)]);
        let result = try_add_date32_days(&dates, &days);

        let expected = Date32Array::from(vec![None]);
        assert_eq!(result, expected);
        Ok(())
    }
}
