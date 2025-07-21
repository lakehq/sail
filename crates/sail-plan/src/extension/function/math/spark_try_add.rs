use std::any::Any;
use std::sync::Arc;

use arrow::array::{
    Array, AsArray, Date32Array, Date32Builder, DurationMicrosecondArray, Int64Array, Int64Builder,
    PrimitiveArray, PrimitiveBuilder, TimestampMicrosecondArray, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{
    Date32Type, DurationMicrosecondType, Int32Type, Int64Type, IntervalMonthDayNanoType,
    IntervalYearMonthType, TimeUnit, TimestampMicrosecondType,
};
use chrono::{Datelike, Duration, Months, NaiveDate};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, Result, ScalarValue};
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
        } else if matches!(
            arg_types,
            [
                DataType::Interval(arrow::datatypes::IntervalUnit::YearMonth),
                DataType::Interval(arrow::datatypes::IntervalUnit::YearMonth)
            ]
        ) {
            Ok(DataType::Interval(
                arrow::datatypes::IntervalUnit::YearMonth,
            ))
        } else if matches!(
            arg_types,
            [
                DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano),
                DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano)
            ]
        ) {
            Ok(DataType::Interval(
                arrow::datatypes::IntervalUnit::MonthDayNano,
            ))
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
                let l: &PrimitiveArray<Date32Type> = left_arr.as_primitive::<Date32Type>();
                let r: &PrimitiveArray<Int32Type> = right_arr.as_primitive::<Int32Type>();
                let result: PrimitiveArray<Date32Type> = try_add_date32_days(l, r);

                if matches!(left, ColumnarValue::Scalar(_))
                    && matches!(right, ColumnarValue::Scalar(_))
                {
                    let scalar: ScalarValue = ScalarValue::try_from_array(&result, 0)?;
                    Ok(ColumnarValue::Scalar(scalar))
                } else {
                    Ok(ColumnarValue::Array(Arc::new(result)))
                }
            }
            (DataType::Date32, DataType::Interval(arrow::datatypes::IntervalUnit::YearMonth)) => {
                let l: &PrimitiveArray<Date32Type> = left_arr.as_primitive::<Date32Type>();
                let r: &PrimitiveArray<IntervalYearMonthType> =
                    right_arr.as_primitive::<IntervalYearMonthType>();
                let result: Date32Array = try_add_date32_interval_yearmonth(l, r);

                if matches!(left, ColumnarValue::Scalar(_))
                    && matches!(right, ColumnarValue::Scalar(_))
                {
                    let scalar = ScalarValue::try_from_array(&result, 0)?;
                    Ok(ColumnarValue::Scalar(scalar))
                } else {
                    Ok(ColumnarValue::Array(Arc::new(result)))
                }
            }
            (
                DataType::Interval(arrow::datatypes::IntervalUnit::YearMonth),
                DataType::Interval(arrow::datatypes::IntervalUnit::YearMonth),
            ) => {
                let l: &PrimitiveArray<IntervalYearMonthType> =
                    left_arr.as_primitive::<IntervalYearMonthType>();
                let r: &PrimitiveArray<IntervalYearMonthType> =
                    right_arr.as_primitive::<IntervalYearMonthType>();

                let len: usize = l.len();
                let mut builder = PrimitiveBuilder::<IntervalYearMonthType>::with_capacity(len);

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

                let result = builder.finish();
                if matches!(left, ColumnarValue::Scalar(_))
                    && matches!(right, ColumnarValue::Scalar(_))
                {
                    let scalar = ScalarValue::try_from_array(&result, 0)?;
                    Ok(ColumnarValue::Scalar(scalar))
                } else {
                    Ok(ColumnarValue::Array(Arc::new(result)))
                }
            }
            (
                DataType::Date32,
                DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano),
            ) => {
                let dates = left_arr.as_primitive::<Date32Type>();
                let intervals = right_arr.as_primitive::<IntervalMonthDayNanoType>();
                let result = try_add_date32_monthdaynano(dates, intervals);
                if matches!(left, ColumnarValue::Scalar(_))
                    && matches!(right, ColumnarValue::Scalar(_))
                {
                    let scalar = ScalarValue::try_from_array(&result, 0)?;
                    Ok(ColumnarValue::Scalar(scalar))
                } else {
                    Ok(ColumnarValue::Array(Arc::new(result)))
                }
            }
            (
                DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano),
                DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano),
            ) => {
                let l = left_arr.as_primitive::<IntervalMonthDayNanoType>();
                let r = right_arr.as_primitive::<IntervalMonthDayNanoType>();
                let result = try_add_interval_monthdaynano(l, r);
                if matches!(left, ColumnarValue::Scalar(_))
                    && matches!(right, ColumnarValue::Scalar(_))
                {
                    let scalar = ScalarValue::try_from_array(&result, 0)?;
                    Ok(ColumnarValue::Scalar(scalar))
                } else {
                    Ok(ColumnarValue::Array(Arc::new(result)))
                }
            }
            (
                DataType::Timestamp(TimeUnit::Microsecond, _),
                DataType::Duration(TimeUnit::Microsecond),
            ) => {
                let l = left_arr.as_primitive::<TimestampMicrosecondType>();
                let r = right_arr.as_primitive::<DurationMicrosecondType>();
                let result = try_add_timestamp_duration(l, r);

                if matches!(left, ColumnarValue::Scalar(_))
                    && matches!(right, ColumnarValue::Scalar(_))
                {
                    let scalar = ScalarValue::try_from_array(&result, 0)?;
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
        let right: &DataType = &types[1];

        let valid_pair = matches!(
            (left, right),
            (DataType::Int32, DataType::Int32)
                | (DataType::Int64, DataType::Int64)
                | (DataType::Date32, DataType::Int32)
                | (
                    DataType::Date32,
                    DataType::Interval(arrow::datatypes::IntervalUnit::YearMonth)
                )
                | (
                    DataType::Date32,
                    DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano)
                )
                | (
                    DataType::Interval(arrow::datatypes::IntervalUnit::YearMonth),
                    DataType::Date32
                )
                | (
                    DataType::Interval(arrow::datatypes::IntervalUnit::YearMonth),
                    DataType::Interval(arrow::datatypes::IntervalUnit::YearMonth)
                )
                | (
                    DataType::Timestamp(TimeUnit::Microsecond, _),
                    DataType::Duration(TimeUnit::Microsecond)
                )
                | (
                    DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano),
                    DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano)
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

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

fn try_add_date32_interval_yearmonth(
    dates: &PrimitiveArray<Date32Type>,
    intervals: &PrimitiveArray<IntervalYearMonthType>,
) -> Date32Array {
    let mut builder = Date32Builder::with_capacity(dates.len());

    for i in 0..dates.len() {
        if dates.is_null(i) || intervals.is_null(i) {
            builder.append_null();
            continue;
        }

        let base: Option<NaiveDate> = NaiveDate::from_ymd_opt(1970, 1, 1);
        let Some(base_date): Option<NaiveDate> = base else {
            builder.append_null();
            continue;
        };

        let days = dates.value(i);
        let Some(date) = base_date.checked_add_signed(Duration::days(days as i64)) else {
            builder.append_null();
            continue;
        };

        let interval = intervals.value(i); // in months
        let months_to_add = interval % 12;
        let years_to_add = interval / 12;

        // Compute new year and month
        let mut new_year = date.year() + years_to_add;
        let mut new_month = date.month() as i32 + months_to_add;

        if new_month > 12 {
            new_month -= 12;
            new_year += 1;
        } else if new_month < 1 {
            new_month += 12;
            new_year -= 1;
        }

        let new_day = date.day();

        let result_date =
            NaiveDate::from_ymd_opt(new_year, new_month as u32, new_day).or_else(|| {
                let last_day: u32 = match new_month {
                    1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
                    4 | 6 | 9 | 11 => 30,
                    2 => {
                        if is_leap_year(new_year) {
                            29
                        } else {
                            28
                        }
                    }
                    _ => 28,
                };
                NaiveDate::from_ymd_opt(new_year, new_month as u32, last_day)
            });

        match result_date {
            Some(new_date) => {
                let days: i64 = new_date.signed_duration_since(base_date).num_days();
                builder.append_value(days as i32);
            }
            None => builder.append_null(),
        }
    }
    builder.finish()
}

fn try_add_timestamp_duration(
    timestamps: &TimestampMicrosecondArray,
    durations: &DurationMicrosecondArray,
) -> TimestampMicrosecondArray {
    let len = timestamps.len();
    let mut builder = TimestampMicrosecondBuilder::with_capacity(len);

    for i in 0..len {
        if timestamps.is_null(i) || durations.is_null(i) {
            builder.append_null();
        } else {
            let ts = timestamps.value(i);
            let dur = durations.value(i);
            match ts.checked_add(dur) {
                Some(sum) => builder.append_value(sum),
                None => builder.append_null(),
            }
        }
    }

    builder.finish()
}

fn try_add_date32_monthdaynano(
    dates: &PrimitiveArray<Date32Type>,
    intervals: &PrimitiveArray<IntervalMonthDayNanoType>,
) -> Date32Array {
    let mut builder = PrimitiveBuilder::<Date32Type>::with_capacity(dates.len());
    let base = NaiveDate::from_ymd(1970, 1, 1);

    for i in 0..dates.len() {
        if dates.is_null(i) || intervals.is_null(i) {
            builder.append_null();
            continue;
        }

        let days = dates.value(i);
        let Some(date) = base.checked_add_signed(Duration::days(days as i64)) else {
            builder.append_null();
            continue;
        };

        let interval = intervals.value(i);
        let date_with_months = add_months(date, interval.months);

        let final_date = date_with_months
            .and_then(|d| d.checked_add_signed(Duration::days(interval.days as i64)));

        match final_date {
            Some(d) => {
                let result_days = d.signed_duration_since(base).num_days();
                builder.append_value(result_days as i32);
            }
            None => builder.append_null(),
        }
    }

    builder.finish()
}

fn add_months(date: NaiveDate, months: i32) -> Option<NaiveDate> {
    let mut year = date.year();
    let mut month = date.month() as i32;

    let total_months = year * 12 + (month - 1) + months;
    year = total_months / 12;
    month = total_months % 12 + 1;
    date.checked_add_months(Months::new(month as u32));
    let day = date.day();
    NaiveDate::from_ymd_opt(year, month as u32, day).or_else(|| {
        let last_day = match month {
            2 if is_leap_year(year) => 29,
            2 => 28,
            4 | 6 | 9 | 11 => 30,
            _ => 31,
        };
        NaiveDate::from_ymd_opt(year, month as u32, last_day)
    })
}

fn try_add_interval_monthdaynano(
    l: &PrimitiveArray<IntervalMonthDayNanoType>,
    r: &PrimitiveArray<IntervalMonthDayNanoType>,
) -> PrimitiveArray<IntervalMonthDayNanoType> {
    let mut builder = PrimitiveBuilder::<IntervalMonthDayNanoType>::with_capacity(l.len());

    for i in 0..l.len() {
        if l.is_null(i) || r.is_null(i) {
            builder.append_null();
        } else {
            let a = l.value(i);
            let b = r.value(i);

            match a.checked_add(b) {
                Some(sum) => builder.append_value(sum),
                None => builder.append_null(),
            }
        }
    }

    builder.finish()
}

#[cfg(test)]
mod tests {
    use arrow::array::{
        Date32Array, Int32Array, IntervalMonthDayNanoArray, IntervalYearMonthArray, PrimitiveArray,
    };
    use arrow::datatypes::{Int32Type, Int64Type, IntervalMonthDayNano};
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

    #[test]
    fn test_add_yearmonth_interval_basic() -> Result<()> {
        use arrow::array::IntervalYearMonthArray;

        let dates = to_date32_array(&[Some("2015-01-31"), Some("2020-02-29"), None])?;

        let intervals = IntervalYearMonthArray::from(vec![Some(1), Some(12), Some(3)]);
        let result = try_add_date32_interval_yearmonth(&dates, &intervals);

        let expected = to_date32_array(&[Some("2015-02-28"), Some("2021-02-28"), None])?;

        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_add_yearmonth_interval_negative() -> Result<()> {
        let dates = to_date32_array(&[Some("2000-07-31"), Some("2021-01-31")])?;

        let intervals = IntervalYearMonthArray::from(vec![Some(-1), Some(-1)]);
        let result = try_add_date32_interval_yearmonth(&dates, &intervals);

        let expected = to_date32_array(&[Some("2000-06-30"), Some("2020-12-31")])?;

        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_add_yearmonth_interval_nulls() -> Result<()> {
        let dates = to_date32_array(&[Some("2010-06-15"), None])?;
        let intervals = IntervalYearMonthArray::from(vec![None, Some(5)]);
        let result = try_add_date32_interval_yearmonth(&dates, &intervals);

        let expected = to_date32_array(&[None, None])?;
        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_add_date32_interval_years() -> Result<()> {
        let dates = to_date32_array(&[Some("2020-02-29"), Some("2019-06-30")])?;
        let intervals = IntervalYearMonthArray::from(vec![Some(12), Some(24)]); // 1 año y 2 años

        let result = try_add_date32_interval_yearmonth(&dates, &intervals);

        let expected = to_date32_array(&[Some("2021-02-28"), Some("2021-06-30")])?;

        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_add_timestamp_duration() -> Result<()> {
        use arrow::array::{DurationMicrosecondArray, TimestampMicrosecondArray};

        let ts = TimestampMicrosecondArray::from(vec![
            Some(1_609_459_200_000_000),
            Some(1_609_545_600_000_000),
            None,
        ]);
        let dur = DurationMicrosecondArray::from(vec![
            Some(86_400_000_000),
            Some(-86_400_000_000),
            Some(1_000),
        ]);

        let result = try_add_timestamp_duration(&ts, &dur);

        let expected = TimestampMicrosecondArray::from(vec![
            Some(1_609_545_600_000_000),
            Some(1_609_459_200_000_000),
            None,
        ]);

        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_add_date32_monthdaynano_basic() -> Result<()> {
        let dates = to_date32_array(&[Some("2022-12-31"), Some("2010-12-31"), None])?;
        let intervals = IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNano::new(0, 2, 0)), // +2 días
            Some(IntervalMonthDayNano::new(1, 1, 0)), // +1 mes, +1 día
            None,
        ]);

        let result = try_add_date32_monthdaynano(&dates, &intervals);
        let expected = to_date32_array(&[Some("2023-01-02"), Some("2011-02-01"), None])?;

        assert_eq!(result, expected);

        let dates = to_date32_array(&[Some("2023-01-01"), Some("2000-01-01"), None])?;
        let intervals = IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNano::new(0, 1, 0)),   // +1 día
            Some(IntervalMonthDayNano::new(0, 365, 0)), // +365 días
            Some(IntervalMonthDayNano::new(0, 10, 0)),
        ]);

        let result = try_add_date32_monthdaynano(&dates, &intervals);

        let expected = to_date32_array(&[Some("2023-01-02"), Some("2000-12-31"), None])?;

        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_add_date32_monthdaynano_overflow() -> Result<()> {
        use arrow::array::IntervalMonthDayNanoArray;

        let dates = Date32Array::from(vec![Some(i32::MAX)]);
        let intervals =
            IntervalMonthDayNanoArray::from(vec![Some(IntervalMonthDayNano::new(0, 1, 0))]);

        let result = try_add_date32_monthdaynano(&dates, &intervals);
        let expected = Date32Array::from(vec![None]);

        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_add_interval_monthdaynano_basic() {
        let l = IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNano::new(1, 2, 3)),
            Some(IntervalMonthDayNano::new(0, 100, 1000)),
            None,
        ]);
        let r = IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNano::new(4, 5, 6)),
            Some(IntervalMonthDayNano::new(0, 50, 2000)),
            Some(IntervalMonthDayNano::new(1, 1, 1)),
        ]);

        let result = try_add_interval_monthdaynano(&l, &r);

        let expected = IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNano::new(5, 7, 9)),
            Some(IntervalMonthDayNano::new(0, 150, 3000)),
            None,
        ]);

        assert_eq!(result, expected);
    }
}
