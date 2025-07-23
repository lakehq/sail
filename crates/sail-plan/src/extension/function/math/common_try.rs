use std::sync::Arc;

use arrow::array::{
    Array, ArrowPrimitiveType, Date32Array, Date32Builder, DurationMicrosecondArray, Int64Array,
    Int64Builder, PrimitiveArray, PrimitiveBuilder, TimestampMicrosecondArray,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{
    Date32Type, Int32Type, Int64Type, IntervalMonthDayNanoType, IntervalYearMonthType,
};
use chrono::{Duration, Months, NaiveDate};
use datafusion_common::ScalarValue;
use datafusion_expr_common::columnar_value::ColumnarValue;

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

pub fn try_add_interval_yearmonth(
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

pub fn try_add_i32(
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

pub fn try_add_i64(
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

pub fn try_add_date32_days(
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

pub fn try_add_date32_interval_yearmonth(
    dates: &PrimitiveArray<Date32Type>,
    intervals: &PrimitiveArray<IntervalYearMonthType>,
) -> Date32Array {
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
        let Some(new_date) = add_months(date, interval_months) else {
            builder.append_null();
            continue;
        };

        let result_days = new_date.signed_duration_since(base).num_days();
        builder.append_value(result_days as i32);
    }

    builder.finish()
}

pub fn try_add_date32_monthdaynano(
    dates: &PrimitiveArray<Date32Type>,
    intervals: &PrimitiveArray<IntervalMonthDayNanoType>,
) -> Date32Array {
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

pub fn try_add_timestamp_duration(
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

#[cfg(test)]
mod tests {
    use arrow::array::{
        Date32Array, Int32Array, IntervalMonthDayNanoArray, IntervalYearMonthArray, PrimitiveArray,
    };
    use arrow::datatypes::{
        DurationMicrosecondType, Int32Type, Int64Type, IntervalMonthDayNano,
        TimestampMicrosecondType,
    };
    use chrono::NaiveDate;
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

    #[test]
    fn test_try_add_manual_i32_no_overflow() {
        let left: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(1), Some(2), Some(3)]);
        let right: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(10), Some(20), Some(30)]);
        let result: Int32Array = try_add_i32(&left, &right);
        let expected: PrimitiveArray<Int32Type> =
            Int32Array::from(vec![Some(11), Some(22), Some(33)]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_try_add_manual_i32_with_nulls() {
        let left: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(1), None, Some(3)]);
        let right: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(10), Some(20), None]);
        let result: Int32Array = try_add_i32(&left, &right);
        let expected: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(11), None, None]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_try_add_manual_i32_overflow() {
        let left: PrimitiveArray<Int32Type> =
            Int32Array::from(vec![Some(i32::MAX), Some(i32::MIN)]);
        let right: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(1), Some(-1)]);
        let result: Int32Array = try_add_i32(&left, &right);
        let expected: PrimitiveArray<Int32Type> = Int32Array::from(vec![None, None]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_try_add_manual_i64_no_overflow() {
        let left: PrimitiveArray<Int64Type> = Int64Array::from(vec![Some(1), Some(2), Some(3)]);
        let right: PrimitiveArray<Int64Type> = Int64Array::from(vec![Some(10), Some(20), Some(30)]);
        let result: Int64Array = try_add_i64(&left, &right);
        let expected: PrimitiveArray<Int64Type> =
            Int64Array::from(vec![Some(11), Some(22), Some(33)]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_try_add_manual_i64_with_nulls() {
        let left: PrimitiveArray<Int64Type> = Int64Array::from(vec![Some(1), None, Some(3)]);
        let right: PrimitiveArray<Int64Type> = Int64Array::from(vec![Some(10), Some(20), None]);
        let result: Int64Array = try_add_i64(&left, &right);
        let expected: PrimitiveArray<Int64Type> = Int64Array::from(vec![Some(11), None, None]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_try_add_manual_i64_overflow() {
        let left: PrimitiveArray<Int64Type> =
            Int64Array::from(vec![Some(i64::MAX), Some(i64::MIN)]);
        let right: PrimitiveArray<Int64Type> = Int64Array::from(vec![Some(1), Some(-1)]);
        let result: Int64Array = try_add_i64(&left, &right);
        let expected: PrimitiveArray<Int64Type> = Int64Array::from(vec![None, None]); // overflow en ambos
        assert_eq!(result, expected);
    }

    #[test]
    fn test_add_days_basic() -> datafusion_common::Result<()> {
        let dates: Date32Array = to_date32_array(&[
            Some("2015-09-30"),
            Some("2000-01-01"),
            Some("2021-01-01"),
            None,
        ])?;
        let days: PrimitiveArray<Int32Type> =
            Int32Array::from(vec![Some(1), Some(366), Some(1), Some(100)]);
        let result: PrimitiveArray<Date32Type> = try_add_date32_days(&dates, &days);
        let expected: Date32Array = to_date32_array(&[
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
        let dates: Date32Array = to_date32_array(&[Some("2020-01-01"), None, Some("2022-01-01")])?;
        let days: PrimitiveArray<Int32Type> = Int32Array::from(vec![None, Some(30), Some(365)]);
        let result: PrimitiveArray<Date32Type> = try_add_date32_days(&dates, &days);
        let expected: Date32Array = to_date32_array(&[None, None, Some("2023-01-01")])?;
        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_add_days_overflow() -> datafusion_common::Result<()> {
        let dates: PrimitiveArray<Date32Type> = Date32Array::from(vec![Some(i32::MAX)]);
        let days: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(1)]);
        let result: PrimitiveArray<Date32Type> = try_add_date32_days(&dates, &days);
        let expected: PrimitiveArray<Date32Type> = Date32Array::from(vec![None]);
        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_add_yearmonth_interval_basic() -> datafusion_common::Result<()> {
        let dates: Date32Array = to_date32_array(&[Some("2015-01-31"), Some("2020-02-29"), None])?;
        let intervals: PrimitiveArray<IntervalYearMonthType> =
            IntervalYearMonthArray::from(vec![Some(1), Some(12), Some(3)]);
        let result: Date32Array = try_add_date32_interval_yearmonth(&dates, &intervals);
        let expected: Date32Array =
            to_date32_array(&[Some("2015-02-28"), Some("2021-02-28"), None])?;
        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_add_yearmonth_interval_negative() -> datafusion_common::Result<()> {
        let dates: Date32Array = to_date32_array(&[Some("2000-07-31"), Some("2021-01-31")])?;
        let intervals: PrimitiveArray<IntervalYearMonthType> =
            IntervalYearMonthArray::from(vec![Some(-1), Some(-1)]);
        let result: Date32Array = try_add_date32_interval_yearmonth(&dates, &intervals);
        let expected: Date32Array = to_date32_array(&[Some("2000-06-30"), Some("2020-12-31")])?;
        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_add_yearmonth_interval_nulls() -> datafusion_common::Result<()> {
        let dates: Date32Array = to_date32_array(&[Some("2010-06-15"), None])?;
        let intervals: PrimitiveArray<IntervalYearMonthType> =
            IntervalYearMonthArray::from(vec![None, Some(5)]);
        let result: Date32Array = try_add_date32_interval_yearmonth(&dates, &intervals);
        let expected: Date32Array = to_date32_array(&[None, None])?;
        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_add_date32_interval_years() -> datafusion_common::Result<()> {
        let dates: Date32Array = to_date32_array(&[Some("2020-02-29"), Some("2019-06-30")])?;
        let intervals: PrimitiveArray<IntervalYearMonthType> =
            IntervalYearMonthArray::from(vec![Some(12), Some(24)]);
        let result: Date32Array = try_add_date32_interval_yearmonth(&dates, &intervals);
        let expected: Date32Array = to_date32_array(&[Some("2021-02-28"), Some("2021-06-30")])?;
        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_add_timestamp_duration() -> datafusion_common::Result<()> {
        let ts: PrimitiveArray<TimestampMicrosecondType> = TimestampMicrosecondArray::from(vec![
            Some(1_609_459_200_000_000),
            Some(1_609_545_600_000_000),
            None,
        ]);
        let dur: PrimitiveArray<DurationMicrosecondType> = DurationMicrosecondArray::from(vec![
            Some(86_400_000_000),
            Some(-86_400_000_000),
            Some(1_000),
        ]);
        let result: TimestampMicrosecondArray = try_add_timestamp_duration(&ts, &dur);
        let expected: PrimitiveArray<TimestampMicrosecondType> =
            TimestampMicrosecondArray::from(vec![
                Some(1_609_545_600_000_000),
                Some(1_609_459_200_000_000),
                None,
            ]);
        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_add_date32_monthdaynano_basic() -> datafusion_common::Result<()> {
        let dates: Date32Array = to_date32_array(&[
            Some("2022-12-31"),
            Some("2010-12-31"),
            Some("2000-01-01"),
            None,
        ])?;
        let intervals: PrimitiveArray<IntervalMonthDayNanoType> =
            IntervalMonthDayNanoArray::from(vec![
                Some(IntervalMonthDayNano::new(0, 2, 0)),
                Some(IntervalMonthDayNano::new(1, 1, 0)),
                Some(IntervalMonthDayNano::new(0, 365, 0)),
                None,
            ]);
        let result: Date32Array = try_add_date32_monthdaynano(&dates, &intervals);
        let expected: Date32Array = to_date32_array(&[
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
        let dates: PrimitiveArray<Date32Type> = Date32Array::from(vec![Some(i32::MAX)]);
        let intervals: PrimitiveArray<IntervalMonthDayNanoType> =
            IntervalMonthDayNanoArray::from(vec![Some(IntervalMonthDayNano::new(0, 1, 0))]);
        let result: Date32Array = try_add_date32_monthdaynano(&dates, &intervals);
        let expected: PrimitiveArray<Date32Type> = Date32Array::from(vec![None]);
        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_add_interval_monthdaynano_basic() {
        let l: PrimitiveArray<IntervalMonthDayNanoType> = IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNano::new(1, 2, 3)),
            Some(IntervalMonthDayNano::new(0, 100, 1000)),
            None,
        ]);
        let r: PrimitiveArray<IntervalMonthDayNanoType> = IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNano::new(4, 5, 6)),
            Some(IntervalMonthDayNano::new(0, 50, 2000)),
            Some(IntervalMonthDayNano::new(1, 1, 1)),
        ]);
        let result: PrimitiveArray<IntervalMonthDayNanoType> =
            try_add_interval_monthdaynano(&l, &r);
        let expected: PrimitiveArray<IntervalMonthDayNanoType> =
            IntervalMonthDayNanoArray::from(vec![
                Some(IntervalMonthDayNano::new(5, 7, 9)),
                Some(IntervalMonthDayNano::new(0, 150, 3000)),
                None,
            ]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_try_sub_i32_no_overflow() {
        let left: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(10), Some(5), Some(3)]);
        let right: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(-1), Some(-2), Some(-3)]);
        let result: Int32Array = try_add_i32(&left, &right);
        let expected: Int32Array = Int32Array::from(vec![Some(9), Some(3), Some(0)]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_try_sub_i32_with_nulls() {
        let left: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(10), None, Some(3)]);
        let right: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(-1), Some(-2), None]);
        let result: Int32Array = try_add_i32(&left, &right);
        let expected: Int32Array = Int32Array::from(vec![Some(9), None, None]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_try_sub_i32_overflow() {
        let left: PrimitiveArray<Int32Type> =
            Int32Array::from(vec![Some(i32::MIN), Some(i32::MAX)]);
        let right: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(-1), Some(1)]);
        let result: Int32Array = try_add_i32(&left, &right);
        let expected: Int32Array = Int32Array::from(vec![None, None]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_try_sub_i64_no_overflow() {
        let left: PrimitiveArray<Int64Type> = Int64Array::from(vec![Some(10), Some(5), Some(3)]);
        let right: PrimitiveArray<Int64Type> = Int64Array::from(vec![Some(-1), Some(-2), Some(-3)]);
        let result: Int64Array = try_add_i64(&left, &right);
        let expected: Int64Array = Int64Array::from(vec![Some(9), Some(3), Some(0)]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_try_sub_i64_with_nulls() {
        let left: PrimitiveArray<Int64Type> = Int64Array::from(vec![Some(10), None, Some(3)]);
        let right: PrimitiveArray<Int64Type> = Int64Array::from(vec![Some(-1), Some(-2), None]);
        let result: Int64Array = try_add_i64(&left, &right);
        let expected: Int64Array = Int64Array::from(vec![Some(9), None, None]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_try_sub_i64_overflow() {
        let left: PrimitiveArray<Int64Type> =
            Int64Array::from(vec![Some(i64::MIN), Some(i64::MAX)]);
        let right: PrimitiveArray<Int64Type> = Int64Array::from(vec![Some(-1), Some(1)]);
        let result: Int64Array = try_add_i64(&left, &right);
        let expected: Int64Array = Int64Array::from(vec![None, None]);
        assert_eq!(result, expected);
    }
}
