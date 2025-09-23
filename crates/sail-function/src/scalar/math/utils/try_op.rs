use std::sync::Arc;

use chrono::{Duration, Months, NaiveDate};
use datafusion::arrow::array::{
    Array, ArrowPrimitiveType, Date32Array, Date32Builder, DurationMicrosecondArray, Int32Array,
    Int64Array, IntervalMonthDayNanoArray, IntervalMonthDayNanoBuilder, PrimitiveArray,
    PrimitiveBuilder, TimestampMicrosecondArray, TimestampMicrosecondBuilder,
};
use datafusion::arrow::datatypes::{
    Date32Type, Float64Type, Int32Type, Int64Type, IntervalMonthDayNano, IntervalMonthDayNanoType,
    IntervalYearMonthType,
};
use datafusion_common::ScalarValue;
use datafusion_expr_common::columnar_value::ColumnarValue;

const DAY_NANOS_I128: i128 = 86_400_000_000_000;

pub fn binary_op_scalar_or_array<T: ArrowPrimitiveType>(
    left: &ColumnarValue,
    right: &ColumnarValue,
    result: PrimitiveArray<T>,
) -> datafusion_common::Result<ColumnarValue> {
    if matches!(left, ColumnarValue::Scalar(_)) && matches!(right, ColumnarValue::Scalar(_)) {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &result, 0,
        )?))
    } else {
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

pub fn try_op_interval_yearmonth<F>(
    left: &PrimitiveArray<IntervalYearMonthType>,
    right: &PrimitiveArray<IntervalYearMonthType>,
    op: F,
) -> PrimitiveArray<IntervalYearMonthType>
where
    F: Fn(i32, i32) -> Option<i32>,
{
    let mut builder = PrimitiveBuilder::<IntervalYearMonthType>::with_capacity(left.len());

    for i in 0..left.len() {
        if left.is_null(i) || right.is_null(i) {
            builder.append_null();
        } else {
            let a = left.value(i);
            let b = right.value(i);

            match op(a, b) {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            }
        }
    }
    builder.finish()
}

pub fn try_binary_op_primitive<T, F>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    op: F,
) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    F: Fn(T::Native, T::Native) -> Option<T::Native>,
{
    let mut builder = PrimitiveBuilder::<T>::with_capacity(left.len());
    for i in 0..left.len() {
        if left.is_null(i) || right.is_null(i) {
            builder.append_null();
        } else {
            let a = left.value(i);
            let b = right.value(i);
            match op(a, b) {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            }
        }
    }
    builder.finish()
}

pub fn try_binary_op_to_float64<T, F>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    op: F,
) -> PrimitiveArray<Float64Type>
where
    T: ArrowPrimitiveType,
    F: Fn(T::Native, T::Native) -> Option<f64>,
{
    let mut builder = PrimitiveBuilder::<Float64Type>::with_capacity(left.len());
    for i in 0..left.len() {
        if left.is_null(i) || right.is_null(i) {
            builder.append_null();
        } else {
            let a = left.value(i);
            let b = right.value(i);
            match op(a, b) {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            }
        }
    }
    builder.finish()
}

pub fn try_binary_op_date32_i32<F>(
    date_array: &PrimitiveArray<Date32Type>,
    days_array: &PrimitiveArray<Int32Type>,
    op: F,
) -> PrimitiveArray<Date32Type>
where
    F: Fn(i32, i32) -> Option<i32>,
{
    let len = date_array.len();
    let mut builder = PrimitiveBuilder::<Date32Type>::with_capacity(len);

    for i in 0..len {
        if date_array.is_null(i) || days_array.is_null(i) {
            builder.append_null();
        } else {
            let base = date_array.value(i);
            let delta = days_array.value(i);

            match op(base, delta) {
                Some(result) => builder.append_value(result),
                None => builder.append_null(),
            }
        }
    }

    builder.finish()
}

pub fn try_op_date32_interval_yearmonth<F>(
    dates: &PrimitiveArray<Date32Type>,
    intervals: &PrimitiveArray<IntervalYearMonthType>,
    op: F,
) -> Date32Array
where
    F: Fn(NaiveDate, i32) -> Option<NaiveDate>,
{
    let mut builder = Date32Builder::with_capacity(dates.len());
    let Some(base) = NaiveDate::from_ymd_opt(1970, 1, 1) else {
        builder.append_nulls(dates.len());
        return builder.finish();
    };

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

        let interval_months = intervals.value(i);
        let Some(new_date) = op(date, interval_months) else {
            builder.append_null();
            continue;
        };

        let result_days = new_date.signed_duration_since(base).num_days();
        builder.append_value(result_days as i32);
    }

    builder.finish()
}

pub fn try_op_date32_monthdaynano<F>(
    dates: &PrimitiveArray<Date32Type>,
    intervals: &PrimitiveArray<IntervalMonthDayNanoType>,
    transform: F,
) -> Date32Array
where
    F: Fn(IntervalMonthDayNano) -> IntervalMonthDayNano,
{
    let mut builder = PrimitiveBuilder::<Date32Type>::with_capacity(dates.len());
    let Some(base) = NaiveDate::from_ymd_opt(1970, 1, 1) else {
        builder.append_nulls(dates.len());
        return builder.finish();
    };

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

        let interval = transform(intervals.value(i));
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

pub fn try_op_timestamp_duration<F>(
    timestamps: &TimestampMicrosecondArray,
    durations: &DurationMicrosecondArray,
    op: F,
) -> TimestampMicrosecondArray
where
    F: Fn(i64, i64) -> Option<i64>,
{
    let len = timestamps.len();
    let mut builder = TimestampMicrosecondBuilder::with_capacity(len);

    for i in 0..len {
        if timestamps.is_null(i) || durations.is_null(i) {
            builder.append_null();
        } else {
            let ts = timestamps.value(i);
            let dur = durations.value(i);
            match op(ts, dur) {
                Some(result) => builder.append_value(result),
                None => builder.append_null(),
            }
        }
    }

    builder.finish()
}

pub fn try_add_interval_monthdaynano(
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

pub fn add_months(date: NaiveDate, months: i32) -> Option<NaiveDate> {
    if months >= 0 {
        date.checked_add_months(Months::new(months as u32))
    } else {
        date.checked_sub_months(Months::new(months.unsigned_abs()))
    }
}

pub fn try_op_interval_yearmonth_i32<F>(
    intervals: &PrimitiveArray<IntervalYearMonthType>,
    scalars: &PrimitiveArray<Int32Type>,
    op: F,
) -> PrimitiveArray<IntervalYearMonthType>
where
    F: Fn(i32, i32) -> Option<i32>,
{
    let mut builder = PrimitiveBuilder::<IntervalYearMonthType>::with_capacity(intervals.len());

    for i in 0..intervals.len() {
        if intervals.is_null(i) || scalars.is_null(i) {
            builder.append_null();
        } else {
            let a = intervals.value(i);
            let b = scalars.value(i);
            match op(a, b) {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            }
        }
    }

    builder.finish()
}

pub fn try_op_interval_monthdaynano_i32<F>(
    intervals: &PrimitiveArray<IntervalMonthDayNanoType>,
    scalars: &PrimitiveArray<Int32Type>,
    op: F,
) -> PrimitiveArray<IntervalMonthDayNanoType>
where
    F: Fn(i64, i32) -> Option<i64>,
{
    let mut builder = PrimitiveBuilder::<IntervalMonthDayNanoType>::with_capacity(intervals.len());

    for i in 0..intervals.len() {
        if intervals.is_null(i) || scalars.is_null(i) {
            builder.append_null();
        } else {
            let interval = intervals.value(i);
            let scalar = scalars.value(i);

            let months = op(interval.months as i64, scalar).and_then(|v| v.try_into().ok());
            let days = op(interval.days as i64, scalar).and_then(|v| v.try_into().ok());
            let nanos = op(interval.nanoseconds, scalar);

            match (months, days, nanos) {
                (Some(m), Some(d), Some(n)) => {
                    builder.append_value(IntervalMonthDayNanoType::make_value(m, d, n));
                }
                _ => builder.append_null(),
            }
        }
    }

    builder.finish()
}

pub fn try_op_interval_monthdaynano_i64<F>(
    intervals: &PrimitiveArray<IntervalMonthDayNanoType>,
    scalars: &PrimitiveArray<Int64Type>,
    op: F,
) -> PrimitiveArray<IntervalMonthDayNanoType>
where
    F: Fn(i64, i64) -> Option<i64>,
{
    let mut builder = PrimitiveBuilder::<IntervalMonthDayNanoType>::with_capacity(intervals.len());

    for i in 0..intervals.len() {
        if intervals.is_null(i) || scalars.is_null(i) {
            builder.append_null();
        } else {
            let interval = intervals.value(i);
            let scalar = scalars.value(i);

            let months = op(interval.months as i64, scalar).and_then(|v| i32::try_from(v).ok());
            let days = op(interval.days as i64, scalar).and_then(|v| i32::try_from(v).ok());
            let nanos = op(interval.nanoseconds, scalar);

            match (months, days, nanos) {
                (Some(m), Some(d), Some(n)) => {
                    builder.append_value(IntervalMonthDayNanoType::make_value(m, d, n));
                }
                _ => builder.append_null(),
            }
        }
    }

    builder.finish()
}

fn div_monthdaynano_by_i64_one(
    iv: IntervalMonthDayNano,
    denom: i64,
) -> Option<IntervalMonthDayNano> {
    if denom == 0 {
        return None;
    }

    let months_out = (iv.months as i64).checked_div(denom)? as i32;

    let total_i128 = (iv.days as i128)
        .checked_mul(DAY_NANOS_I128)
        .and_then(|v| v.checked_add(iv.nanoseconds as i128))?;

    let q = (total_i128 as f64) / (denom as f64);
    let q_rounded = q.round() as i128;

    let days = (q_rounded / DAY_NANOS_I128) as i32;
    let nanos = (q_rounded % DAY_NANOS_I128) as i64;

    Some(IntervalMonthDayNano::new(months_out, days, nanos))
}

pub fn try_div_interval_monthdaynano_i32(
    intervals: &IntervalMonthDayNanoArray,
    divisors: &Int32Array,
) -> datafusion_common::Result<IntervalMonthDayNanoArray> {
    let len = intervals.len().max(divisors.len());
    let mut b = IntervalMonthDayNanoBuilder::with_capacity(len);

    for i in 0..len {
        let iv_is_null = if i < intervals.len() {
            intervals.is_null(i)
        } else {
            true
        };
        let d_is_null = if i < divisors.len() {
            divisors.is_null(i)
        } else {
            true
        };

        if iv_is_null || d_is_null {
            b.append_null();
            continue;
        }

        let iv = intervals.value(i.min(intervals.len() - 1));
        let denom = divisors.value(i.min(divisors.len() - 1)) as i64;

        match div_monthdaynano_by_i64_one(iv, denom) {
            Some(out) => b.append_value(out),
            None => b.append_null(), // denom=0
        }
    }
    Ok(b.finish())
}

pub fn try_div_interval_monthdaynano_i64(
    intervals: &IntervalMonthDayNanoArray,
    divisors: &Int64Array,
) -> datafusion_common::Result<IntervalMonthDayNanoArray> {
    let len = intervals.len().max(divisors.len());
    let mut b = IntervalMonthDayNanoBuilder::with_capacity(len);

    for i in 0..len {
        let iv_is_null = if i < intervals.len() {
            intervals.is_null(i)
        } else {
            true
        };
        let d_is_null = if i < divisors.len() {
            divisors.is_null(i)
        } else {
            true
        };

        if iv_is_null || d_is_null {
            b.append_null();
            continue;
        }

        let iv = intervals.value(i.min(intervals.len() - 1));
        let denom = divisors.value(i.min(divisors.len() - 1));

        match div_monthdaynano_by_i64_one(iv, denom) {
            Some(out) => b.append_value(out),
            None => b.append_null(), // denom=0
        }
    }
    Ok(b.finish())
}

#[cfg(test)]
mod tests {
    use datafusion_common::DataFusionError;

    use super::*;
    fn days_since_1970(date: &str) -> datafusion_common::Result<i32> {
        let base = NaiveDate::from_ymd_opt(1970, 1, 1)
            .ok_or_else(|| DataFusionError::Execution("Invalid base date: 1970-01-01".into()))?;

        let d = NaiveDate::parse_from_str(date, "%Y-%m-%d").map_err(|e| {
            DataFusionError::Execution(format!("Invalid date string '{date}': {e}"))
        })?;
        Ok(d.signed_duration_since(base).num_days() as i32)
    }

    fn to_date32_array(dates: &[Option<&str>]) -> datafusion_common::Result<Date32Array> {
        let mut values = Vec::with_capacity(dates.len());
        for s in dates {
            match s {
                Some(d) => values.push(Some(days_since_1970(d)?)),
                None => values.push(None),
            }
        }
        Ok(Date32Array::from(values))
    }
    mod tests_add {
        use datafusion::arrow::array::{
            Date32Array, Int32Array, Int64Array, IntervalMonthDayNanoArray, IntervalYearMonthArray,
        };
        use datafusion::arrow::datatypes::{Int64Type, IntervalMonthDayNano};

        use super::*;

        #[test]
        fn test_try_add_manual_i32_no_overflow() {
            let left = Int32Array::from(vec![Some(1), Some(2), Some(3)]);
            let right = Int32Array::from(vec![Some(10), Some(20), Some(30)]);
            let result = try_binary_op_primitive::<Int32Type, _>(&left, &right, i32::checked_add);
            let expected = Int32Array::from(vec![Some(11), Some(22), Some(33)]);
            assert_eq!(result, expected);
        }

        #[test]
        fn test_try_add_manual_i32_with_nulls() {
            let left = Int32Array::from(vec![Some(1), None, Some(3)]);
            let right = Int32Array::from(vec![Some(10), Some(20), None]);
            let result = try_binary_op_primitive::<Int32Type, _>(&left, &right, i32::checked_add);
            let expected = Int32Array::from(vec![Some(11), None, None]);
            assert_eq!(result, expected);
        }

        #[test]
        fn test_try_add_manual_i32_overflow() {
            let left = Int32Array::from(vec![Some(i32::MAX), Some(i32::MIN)]);
            let right = Int32Array::from(vec![Some(1), Some(-1)]);
            let result = try_binary_op_primitive::<Int32Type, _>(&left, &right, i32::checked_add);
            let expected = Int32Array::from(vec![None, None]);
            assert_eq!(result, expected);
        }

        #[test]
        fn test_try_add_manual_i64_no_overflow() {
            let left = Int64Array::from(vec![Some(1), Some(2), Some(3)]);
            let right = Int64Array::from(vec![Some(10), Some(20), Some(30)]);
            let result = try_binary_op_primitive::<Int64Type, _>(&left, &right, i64::checked_add);
            let expected = Int64Array::from(vec![Some(11), Some(22), Some(33)]);
            assert_eq!(result, expected);
        }

        #[test]
        fn test_try_add_manual_i64_with_nulls() {
            let left = Int64Array::from(vec![Some(1), None, Some(3)]);
            let right = Int64Array::from(vec![Some(10), Some(20), None]);
            let result = try_binary_op_primitive::<Int64Type, _>(&left, &right, i64::checked_add);
            let expected = Int64Array::from(vec![Some(11), None, None]);
            assert_eq!(result, expected);
        }

        #[test]
        fn test_try_add_manual_i64_overflow() -> datafusion_common::Result<()> {
            let left = Int64Array::from(vec![Some(i64::MAX), Some(i64::MIN)]);
            let right = Int64Array::from(vec![Some(1), Some(-1)]);
            let result = try_binary_op_primitive::<Int64Type, _>(&left, &right, i64::checked_add);
            let expected = Int64Array::from(vec![None, None]);
            assert_eq!(result, expected);
            Ok(())
        }

        #[test]
        fn test_add_days_basic() -> datafusion_common::Result<()> {
            let dates = to_date32_array(&[
                Some("2015-09-30"),
                Some("2000-01-01"),
                Some("2021-01-01"),
                None,
            ])?;
            let days = Int32Array::from(vec![Some(1), Some(366), Some(1), Some(100)]);
            let result = try_binary_op_date32_i32(&dates, &days, i32::checked_add);
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
        fn test_add_days_with_nulls() -> datafusion_common::Result<()> {
            let dates = to_date32_array(&[Some("2020-01-01"), None, Some("2022-01-01")])?;
            let days = Int32Array::from(vec![None, Some(30), Some(365)]);
            let result = try_binary_op_date32_i32(&dates, &days, i32::checked_add);
            let expected = to_date32_array(&[None, None, Some("2023-01-01")])?;
            assert_eq!(result, expected);
            Ok(())
        }

        #[test]
        fn test_add_days_overflow() -> datafusion_common::Result<()> {
            let dates = Date32Array::from(vec![Some(i32::MAX)]);
            let days = Int32Array::from(vec![Some(1)]);
            let result = try_binary_op_date32_i32(&dates, &days, i32::checked_add);
            let expected = Date32Array::from(vec![None]);
            assert_eq!(result, expected);
            Ok(())
        }

        #[test]
        fn test_add_yearmonth_interval_basic() -> datafusion_common::Result<()> {
            let dates = to_date32_array(&[Some("2015-01-31"), Some("2020-02-29"), None])?;
            let intervals = IntervalYearMonthArray::from(vec![Some(1), Some(12), Some(3)]);
            let result = try_op_date32_interval_yearmonth(&dates, &intervals, add_months);
            let expected = to_date32_array(&[Some("2015-02-28"), Some("2021-02-28"), None])?;
            assert_eq!(result, expected);
            Ok(())
        }

        #[test]
        fn test_add_yearmonth_interval_negative() -> datafusion_common::Result<()> {
            let dates = to_date32_array(&[Some("2000-07-31"), Some("2021-01-31")])?;
            let intervals = IntervalYearMonthArray::from(vec![Some(-1), Some(-1)]);
            let result = try_op_date32_interval_yearmonth(&dates, &intervals, add_months);
            let expected = to_date32_array(&[Some("2000-06-30"), Some("2020-12-31")])?;
            assert_eq!(result, expected);
            Ok(())
        }

        #[test]
        fn test_add_yearmonth_interval_nulls() -> datafusion_common::Result<()> {
            let dates = to_date32_array(&[Some("2010-06-15"), None])?;
            let intervals = IntervalYearMonthArray::from(vec![None, Some(5)]);
            let result = try_op_date32_interval_yearmonth(&dates, &intervals, add_months);
            let expected = to_date32_array(&[None, None])?;
            assert_eq!(result, expected);
            Ok(())
        }

        #[test]
        fn test_add_date32_interval_years() -> datafusion_common::Result<()> {
            let dates = to_date32_array(&[Some("2020-02-29"), Some("2019-06-30")])?;
            let intervals = IntervalYearMonthArray::from(vec![Some(12), Some(24)]);
            let result = try_op_date32_interval_yearmonth(&dates, &intervals, add_months);
            let expected = to_date32_array(&[Some("2021-02-28"), Some("2021-06-30")])?;
            assert_eq!(result, expected);
            Ok(())
        }

        #[test]
        fn test_add_timestamp_duration() -> datafusion_common::Result<()> {
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
            let result = try_op_timestamp_duration(&ts, &dur, i64::checked_add);
            let expected = TimestampMicrosecondArray::from(vec![
                Some(1_609_545_600_000_000),
                Some(1_609_459_200_000_000),
                None,
            ]);
            assert_eq!(result, expected);
            Ok(())
        }

        #[test]
        fn test_add_date32_monthdaynano_basic() -> datafusion_common::Result<()> {
            let dates = to_date32_array(&[
                Some("2022-12-31"),
                Some("2010-12-31"),
                Some("2000-01-01"),
                None,
            ])?;
            let intervals = IntervalMonthDayNanoArray::from(vec![
                Some(IntervalMonthDayNano::new(0, 2, 0)),
                Some(IntervalMonthDayNano::new(1, 1, 0)),
                Some(IntervalMonthDayNano::new(0, 365, 0)),
                None,
            ]);
            let result = try_op_date32_monthdaynano(&dates, &intervals, |x| x);
            let expected = to_date32_array(&[
                Some("2023-01-02"),
                Some("2011-02-01"),
                Some("2000-12-31"),
                None,
            ])?;
            assert_eq!(result, expected);
            Ok(())
        }

        #[test]
        fn test_add_date32_monthdaynano_overflow() -> datafusion_common::Result<()> {
            let dates = Date32Array::from(vec![Some(i32::MAX)]);
            let intervals =
                IntervalMonthDayNanoArray::from(vec![Some(IntervalMonthDayNano::new(0, 1, 0))]);
            let result = try_op_date32_monthdaynano(&dates, &intervals, |x| x);
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

    #[cfg(test)]
    mod tests_sub {
        use datafusion::arrow::array::{Date32Array, Int32Array, Int64Array};
        use datafusion::arrow::datatypes::Int64Type;

        use super::*;

        #[test]
        fn test_try_sub_i32_no_overflow() {
            let left = Int32Array::from(vec![Some(10), Some(5), Some(3)]);
            let right = Int32Array::from(vec![Some(1), Some(2), Some(3)]);
            let result = try_binary_op_primitive::<Int32Type, _>(&left, &right, i32::checked_sub);
            let expected = Int32Array::from(vec![Some(9), Some(3), Some(0)]);
            assert_eq!(result, expected);
        }

        #[test]
        fn test_try_sub_i32_with_nulls() {
            let left = Int32Array::from(vec![Some(10), None, Some(3)]);
            let right = Int32Array::from(vec![Some(1), Some(2), None]);
            let result = try_binary_op_primitive::<Int32Type, _>(&left, &right, i32::checked_sub);
            let expected = Int32Array::from(vec![Some(9), None, None]);
            assert_eq!(result, expected);
        }

        #[test]
        fn test_try_sub_i32_overflow() {
            let left = Int32Array::from(vec![Some(i32::MIN), Some(i32::MAX)]);
            let right = Int32Array::from(vec![Some(1), Some(-1)]);
            let result = try_binary_op_primitive::<Int32Type, _>(&left, &right, i32::checked_sub);
            let expected = Int32Array::from(vec![None, None]);
            assert_eq!(result, expected);
        }

        #[test]
        fn test_try_sub_i64_no_overflow() {
            let left = Int64Array::from(vec![Some(10), Some(5), Some(3)]);
            let right = Int64Array::from(vec![Some(1), Some(2), Some(3)]);
            let result = try_binary_op_primitive::<Int64Type, _>(&left, &right, i64::checked_sub);
            let expected = Int64Array::from(vec![Some(9), Some(3), Some(0)]);
            assert_eq!(result, expected);
        }

        #[test]
        fn test_try_sub_i64_with_nulls() {
            let left = Int64Array::from(vec![Some(10), None, Some(3)]);
            let right = Int64Array::from(vec![Some(1), Some(2), None]);
            let result = try_binary_op_primitive::<Int64Type, _>(&left, &right, i64::checked_sub);
            let expected = Int64Array::from(vec![Some(9), None, None]);
            assert_eq!(result, expected);
        }

        #[test]
        fn test_try_sub_i64_overflow() {
            let left = Int64Array::from(vec![Some(i64::MIN), Some(i64::MAX)]);
            let right = Int64Array::from(vec![Some(1), Some(-1)]);
            let result = try_binary_op_primitive::<Int64Type, _>(&left, &right, i64::checked_sub);
            let expected = Int64Array::from(vec![None, None]);
            assert_eq!(result, expected);
        }

        #[test]
        fn test_sub_days_basic() -> datafusion_common::Result<()> {
            let dates = to_date32_array(&[
                Some("2015-10-01"),
                Some("2001-01-01"),
                Some("2021-01-02"),
                None,
            ])?;
            let days = Int32Array::from(vec![Some(1), Some(366), Some(1), Some(100)]);
            let result = try_binary_op_date32_i32(&dates, &days, i32::checked_sub);
            let expected = to_date32_array(&[
                Some("2015-09-30"),
                Some("2000-01-01"),
                Some("2021-01-01"),
                None,
            ])?;
            assert_eq!(result, expected);
            Ok(())
        }

        #[test]
        fn test_sub_days_with_nulls() -> datafusion_common::Result<()> {
            let dates = to_date32_array(&[Some("2020-01-01"), None, Some("2023-01-01")])?;
            let days = Int32Array::from(vec![None, Some(30), Some(365)]);
            let result = try_binary_op_date32_i32(&dates, &days, i32::checked_sub);
            let expected = to_date32_array(&[None, None, Some("2022-01-01")])?;
            assert_eq!(result, expected);
            Ok(())
        }

        #[test]
        fn test_sub_days_overflow() -> datafusion_common::Result<()> {
            let dates = Date32Array::from(vec![Some(i32::MIN)]);
            let days = Int32Array::from(vec![Some(1)]);
            let result = try_binary_op_date32_i32(&dates, &days, i32::checked_sub);
            let expected = Date32Array::from(vec![None]);
            assert_eq!(result, expected);
            Ok(())
        }
    }

    #[cfg(test)]
    mod tests_mult {
        use datafusion::arrow::array::{
            Date32Array, Int32Array, Int64Array, IntervalYearMonthArray,
        };
        use datafusion::arrow::datatypes::{Int32Type, Int64Type};

        use super::*;

        #[test]
        fn test_try_mult_i32_no_overflow() {
            let left = Int32Array::from(vec![Some(2), Some(3), Some(4)]);
            let right = Int32Array::from(vec![Some(5), Some(6), Some(7)]);
            let result = try_binary_op_primitive::<Int32Type, _>(&left, &right, i32::checked_mul);
            let expected = Int32Array::from(vec![Some(10), Some(18), Some(28)]);
            assert_eq!(result, expected);
        }

        #[test]
        fn test_try_mult_i32_with_nulls() {
            let left = Int32Array::from(vec![Some(2), None, Some(4)]);
            let right = Int32Array::from(vec![Some(5), Some(6), None]);
            let result = try_binary_op_primitive::<Int32Type, _>(&left, &right, i32::checked_mul);
            let expected = Int32Array::from(vec![Some(10), None, None]);
            assert_eq!(result, expected);
        }

        #[test]
        fn test_try_mult_i32_overflow() {
            let left = Int32Array::from(vec![Some(i32::MAX), Some(i32::MIN)]);
            let right = Int32Array::from(vec![Some(2), Some(2)]);
            let result = try_binary_op_primitive::<Int32Type, _>(&left, &right, i32::checked_mul);
            let expected = Int32Array::from(vec![None, None]);
            assert_eq!(result, expected);
        }

        #[test]
        fn test_try_mult_i64_no_overflow() {
            let left = Int64Array::from(vec![Some(2), Some(3), Some(4)]);
            let right = Int64Array::from(vec![Some(5), Some(6), Some(7)]);
            let result = try_binary_op_primitive::<Int64Type, _>(&left, &right, i64::checked_mul);
            let expected = Int64Array::from(vec![Some(10), Some(18), Some(28)]);
            assert_eq!(result, expected);
        }

        #[test]
        fn test_try_mult_i64_with_nulls() {
            let left = Int64Array::from(vec![Some(2), None, Some(4)]);
            let right = Int64Array::from(vec![Some(5), Some(6), None]);
            let result = try_binary_op_primitive::<Int64Type, _>(&left, &right, i64::checked_mul);
            let expected = Int64Array::from(vec![Some(10), None, None]);
            assert_eq!(result, expected);
        }

        #[test]
        fn test_try_mult_i64_overflow() {
            let left = Int64Array::from(vec![Some(i64::MAX), Some(i64::MIN)]);
            let right = Int64Array::from(vec![Some(2), Some(2)]);
            let result = try_binary_op_primitive::<Int64Type, _>(&left, &right, i64::checked_mul);
            let expected = Int64Array::from(vec![None, None]);
            assert_eq!(result, expected);
        }

        #[test]
        fn test_mult_days_basic() -> datafusion_common::Result<()> {
            let dates =
                to_date32_array(&[Some("2000-01-01"), Some("1970-01-02"), Some("2020-01-01")])?;
            let multipliers = Int32Array::from(vec![Some(2), Some(0), Some(1)]);
            let result = try_binary_op_date32_i32(&dates, &multipliers, i32::checked_mul);

            // FIXME review
            let expected =
                to_date32_array(&[Some("2029-12-31"), Some("1970-01-01"), Some("2020-01-01")])?;

            assert_eq!(result, expected);
            Ok(())
        }

        #[test]
        fn test_mult_days_with_nulls() -> datafusion_common::Result<()> {
            let dates = to_date32_array(&[Some("2000-01-01"), None, Some("2020-01-01")])?;
            let factors = Int32Array::from(vec![None, Some(3), Some(1)]);
            let result = try_binary_op_date32_i32(&dates, &factors, i32::checked_mul);
            let expected = Date32Array::from(vec![None, None, Some(18262)]);
            assert_eq!(result, expected);
            Ok(())
        }

        #[test]
        fn test_mult_days_overflow() -> datafusion_common::Result<()> {
            let dates = Date32Array::from(vec![Some(i32::MAX)]);
            let factors = Int32Array::from(vec![Some(2)]);
            let result = try_binary_op_date32_i32(&dates, &factors, i32::checked_mul);
            let expected = Date32Array::from(vec![None]);
            assert_eq!(result, expected);
            Ok(())
        }

        #[test]
        fn test_try_mult_interval_yearmonth_i32_basic() {
            let intervals = IntervalYearMonthArray::from(vec![Some(12), Some(6), Some(0)]);
            let scalars = Int32Array::from(vec![Some(2), Some(4), Some(5)]);

            let result = try_op_interval_yearmonth_i32(&intervals, &scalars, i32::checked_mul);
            let expected = IntervalYearMonthArray::from(vec![Some(24), Some(24), Some(0)]);

            assert_eq!(result, expected);
        }

        #[test]
        fn test_try_mult_interval_yearmonth_i32_with_nulls() {
            let intervals = IntervalYearMonthArray::from(vec![Some(12), None, Some(30)]);
            let scalars = Int32Array::from(vec![Some(2), Some(4), None]);

            let result = try_op_interval_yearmonth_i32(&intervals, &scalars, i32::checked_mul);
            let expected = IntervalYearMonthArray::from(vec![Some(24), None, None]);

            assert_eq!(result, expected);
        }

        #[test]
        fn test_try_mult_interval_yearmonth_i32_with_zero() {
            let intervals = IntervalYearMonthArray::from(vec![Some(12), Some(24)]);
            let scalars = Int32Array::from(vec![Some(0), Some(0)]);

            let result = try_op_interval_yearmonth_i32(&intervals, &scalars, i32::checked_mul);
            let expected = IntervalYearMonthArray::from(vec![Some(0), Some(0)]);

            assert_eq!(result, expected);
        }

        #[test]
        fn test_try_mult_interval_yearmonth_i32_with_negative() {
            let intervals = IntervalYearMonthArray::from(vec![Some(12), Some(-24)]);
            let scalars = Int32Array::from(vec![Some(-2), Some(3)]);

            let result = try_op_interval_yearmonth_i32(&intervals, &scalars, i32::checked_mul);
            let expected = IntervalYearMonthArray::from(vec![Some(-24), Some(-72)]);

            assert_eq!(result, expected);
        }

        #[test]
        fn test_try_mult_interval_yearmonth_i32_overflow() {
            let intervals = IntervalYearMonthArray::from(vec![Some(i32::MAX), Some(i32::MIN)]);
            let scalars = Int32Array::from(vec![Some(2), Some(2)]);

            let result = try_op_interval_yearmonth_i32(&intervals, &scalars, i32::checked_mul);
            let expected = IntervalYearMonthArray::from(vec![None, None]); // overflow

            assert_eq!(result, expected);
        }
    }

    #[cfg(test)]
    mod tests_div {
        use datafusion::arrow::array::{Int32Array, Int64Array, IntervalYearMonthArray};
        use datafusion::arrow::datatypes::{Int32Type, Int64Type};

        use super::*;

        #[test]
        fn test_try_div_i32_no_overflow() {
            let left = Int32Array::from(vec![Some(10), Some(20), Some(30)]);
            let right = Int32Array::from(vec![Some(2), Some(4), Some(5)]);
            let result = try_binary_op_primitive::<Int32Type, _>(&left, &right, i32::checked_div);
            let expected = Int32Array::from(vec![Some(5), Some(5), Some(6)]);
            assert_eq!(result, expected);
        }

        #[test]
        fn test_try_div_i32_with_nulls() {
            let left = Int32Array::from(vec![Some(10), None, Some(30)]);
            let right = Int32Array::from(vec![Some(2), Some(4), None]);
            let result = try_binary_op_primitive::<Int32Type, _>(&left, &right, i32::checked_div);
            let expected = Int32Array::from(vec![Some(5), None, None]);
            assert_eq!(result, expected);
        }

        #[test]
        fn test_try_div_i32_div_by_zero() {
            let left = Int32Array::from(vec![Some(10), Some(20), Some(30)]);
            let right = Int32Array::from(vec![Some(2), Some(0), Some(3)]);
            let result = try_binary_op_primitive::<Int32Type, _>(&left, &right, i32::checked_div);
            let expected = Int32Array::from(vec![Some(5), None, Some(10)]);
            assert_eq!(result, expected);
        }

        #[test]
        fn test_try_div_i64_no_overflow() {
            let left = Int64Array::from(vec![Some(100), Some(200), Some(300)]);
            let right = Int64Array::from(vec![Some(10), Some(20), Some(25)]);
            let result = try_binary_op_primitive::<Int64Type, _>(&left, &right, i64::checked_div);
            let expected = Int64Array::from(vec![Some(10), Some(10), Some(12)]);
            assert_eq!(result, expected);
        }

        #[test]
        fn test_try_div_i64_decimal_no_overflow() {
            let left = Int64Array::from(vec![Some(404)]);
            let right = Int64Array::from(vec![Some(10)]);
            let result = try_binary_op_primitive::<Int64Type, _>(&left, &right, i64::checked_div);
            // FIXME not decimal result
            let expected = Int64Array::from(vec![Some(40)]);
            assert_eq!(result, expected);
        }

        #[test]
        fn test_try_div_i64_div_by_zero() {
            let left = Int64Array::from(vec![Some(10), Some(20)]);
            let right = Int64Array::from(vec![Some(0), Some(2)]);
            let result = try_binary_op_primitive::<Int64Type, _>(&left, &right, i64::checked_div);
            let expected = Int64Array::from(vec![None, Some(10)]);
            assert_eq!(result, expected);
        }
        #[test]
        fn test_try_div_interval_yearmonth_i32_basic() {
            let intervals = IntervalYearMonthArray::from(vec![Some(12), Some(24), Some(30)]);
            let divisors = Int32Array::from(vec![Some(3), Some(4), Some(5)]);

            let result = try_op_interval_yearmonth_i32(&intervals, &divisors, i32::checked_div);
            let expected = IntervalYearMonthArray::from(vec![Some(4), Some(6), Some(6)]);

            assert_eq!(result, expected);
        }

        #[test]
        fn test_try_div_interval_yearmonth_i32_with_nulls() {
            let intervals = IntervalYearMonthArray::from(vec![Some(12), None, Some(30)]);
            let divisors = Int32Array::from(vec![Some(3), Some(4), None]);

            let result = try_op_interval_yearmonth_i32(&intervals, &divisors, i32::checked_div);
            let expected = IntervalYearMonthArray::from(vec![Some(4), None, None]);

            assert_eq!(result, expected);
        }

        #[test]
        fn test_try_div_interval_yearmonth_i32_div_by_zero() {
            let intervals = IntervalYearMonthArray::from(vec![Some(12), Some(24)]);
            let divisors = Int32Array::from(vec![Some(0), Some(6)]);

            let result = try_op_interval_yearmonth_i32(&intervals, &divisors, i32::checked_div);
            let expected = IntervalYearMonthArray::from(vec![None, Some(4)]);

            assert_eq!(result, expected);
        }

        #[test]
        fn test_try_div_interval_yearmonth_i32_negative() {
            let intervals = IntervalYearMonthArray::from(vec![Some(-12), Some(24)]);
            let divisors = Int32Array::from(vec![Some(3), Some(-6)]);

            let result = try_op_interval_yearmonth_i32(&intervals, &divisors, i32::checked_div);
            let expected = IntervalYearMonthArray::from(vec![Some(-4), Some(-4)]);

            assert_eq!(result, expected);
        }

        #[test]
        fn test_try_div_interval_yearmonth_i32_zero_interval() {
            let intervals = IntervalYearMonthArray::from(vec![Some(0), Some(0)]);
            let divisors = Int32Array::from(vec![Some(1), Some(-1)]);

            let result = try_op_interval_yearmonth_i32(&intervals, &divisors, i32::checked_div);
            let expected = IntervalYearMonthArray::from(vec![Some(0), Some(0)]);

            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_try_op_interval_monthdaynano_i32_mul() {
        use datafusion::arrow::array::{Int32Array, IntervalMonthDayNanoArray};
        use datafusion::arrow::datatypes::IntervalMonthDayNanoType;

        let intervals = IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNanoType::make_value(2, 0, 0)), // 2 months
        ]);
        let scalars = Int32Array::from(vec![Some(3)]);
        let result =
            try_op_interval_monthdaynano_i32(&intervals, &scalars, |a, b| a.checked_mul(b as i64));

        let res = result.value(0);
        assert_eq!(res.months, 6);
        assert_eq!(res.days, 0);
        assert_eq!(res.nanoseconds, 0);
    }
}
