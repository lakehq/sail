use std::sync::Arc;

use chrono::{Duration, Months, NaiveDate};
use datafusion::arrow::array::{
    new_null_array, Array, ArrayRef, ArrowPrimitiveType, AsArray, Date32Array, Date32Builder,
    Datum, DurationMicrosecondArray, Int32Array, Int64Array, IntervalMonthDayNanoArray,
    IntervalMonthDayNanoBuilder, PrimitiveArray, PrimitiveBuilder, TimestampMicrosecondArray,
    TimestampMicrosecondBuilder,
};
use datafusion::arrow::compute::concat;
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Decimal128Type, Decimal256Type, DecimalType, Float64Type, Int32Type,
    Int64Type, IntervalMonthDayNano, IntervalMonthDayNanoType, IntervalYearMonthType,
};
use datafusion::arrow::error::ArrowError;
use datafusion_common::ScalarValue;
use datafusion_expr::type_coercion::binary::BinaryTypeCoercer;
use datafusion_expr::Operator;
use datafusion_expr_common::columnar_value::ColumnarValue;

const DAY_NANOS_I128: i128 = 86_400_000_000_000;

/// `true` for the numeric types that Spark arithmetic promotes through
/// DataFusion's decimal/float coercion (as opposed to the integer, interval,
/// date and timestamp cases handled by the dedicated kernels above).
pub fn is_float_or_decimal(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

/// Coerced input types Spark/DataFusion would use for `lhs <op> rhs`.
pub fn arith_input_types(
    lhs: &DataType,
    op: Operator,
    rhs: &DataType,
) -> datafusion_common::Result<(DataType, DataType)> {
    BinaryTypeCoercer::new(lhs, &op, rhs).get_input_types()
}

/// Result type Spark/DataFusion would produce for `lhs <op> rhs`.
pub fn arith_result_type(
    lhs: &DataType,
    op: Operator,
    rhs: &DataType,
) -> datafusion_common::Result<DataType> {
    BinaryTypeCoercer::new(lhs, &op, rhs).get_result_type()
}

/// Apply a checked Arrow arithmetic kernel with Spark `try_*` semantics.
///
/// Arrow's checked kernels (`add`/`sub`/`mul`/`div`) return an error when any
/// element overflows or divides by zero (floats follow IEEE 754 instead). Spark
/// `try_*` turns exactly those checked exceptions into a per-element NULL, and it
/// does so regardless of `spark.sql.ansi.enabled` (the functions are
/// ANSI-invariant). This runs the kernel over the whole array (fast path) and,
/// only if that errors, re-runs it element-by-element so the offending rows
/// become NULL while the rest keep their computed value.
///
/// `result_type` must be the type the kernel itself produces (e.g. from
/// [`arith_result_type`] or the UDF's return field) so the NULL placeholders
/// concatenate cleanly with the successful pieces.
///
/// Only the two *arithmetic* exceptions (overflow and division by zero) are
/// turned into NULL; any other kernel error (e.g. a genuine type mismatch) is
/// propagated so it never silently masquerades as an all-NULL result.
pub fn try_arrow_arith(
    left: &ArrayRef,
    right: &ArrayRef,
    result_type: &DataType,
    kernel: fn(&dyn Datum, &dyn Datum) -> Result<ArrayRef, ArrowError>,
) -> datafusion_common::Result<ArrayRef> {
    match kernel(left, right) {
        Ok(out) => return Ok(out),
        Err(e) if !is_arithmetic_exception(&e) => return Err(e.into()),
        Err(_) => {} // overflow / divide-by-zero: recompute per element below
    }
    let len = left.len();
    let mut pieces: Vec<ArrayRef> = Vec::with_capacity(len);
    for i in 0..len {
        let l = left.slice(i, 1);
        let r = right.slice(i, 1);
        match kernel(&l, &r) {
            Ok(v) => pieces.push(v),
            Err(e) if !is_arithmetic_exception(&e) => return Err(e.into()),
            Err(_) => pieces.push(new_null_array(result_type, 1)),
        }
    }
    let refs: Vec<&dyn Array> = pieces.iter().map(|a| a.as_ref()).collect();
    Ok(concat(&refs)?)
}

/// The checked-arithmetic exceptions Spark `try_*` swallows into NULL. Any other
/// [`ArrowError`] is a real failure and must be propagated, not nulled.
fn is_arithmetic_exception(err: &ArrowError) -> bool {
    matches!(
        err,
        ArrowError::ArithmeticOverflow(_) | ArrowError::DivideByZero
    )
}

fn null_overflow_decimal<T: DecimalType>(
    arr: &PrimitiveArray<T>,
    precision: u8,
    scale: i8,
) -> Result<PrimitiveArray<T>, ArrowError> {
    let mut builder = PrimitiveBuilder::<T>::with_capacity(arr.len());
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
        } else {
            let v = arr.value(i);
            if T::is_valid_decimal_precision(v, precision) {
                builder.append_value(v);
            } else {
                builder.append_null();
            }
        }
    }
    builder.finish().with_precision_and_scale(precision, scale)
}

/// Null out DECIMAL values that do not fit the array's declared precision.
///
/// Arrow's checked arithmetic kernels only error on native (i128/i256) overflow,
/// not when a result exceeds the *declared* decimal precision. Spark's `try_*`
/// yields NULL in that case, so this post-pass (applied after
/// [`try_arrow_arith`]) enforces the precision bound per element. Non-decimal
/// arrays pass through unchanged.
pub fn null_decimal_overflow(array: ArrayRef) -> datafusion_common::Result<ArrayRef> {
    match array.data_type() {
        DataType::Decimal128(p, s) => {
            let (p, s) = (*p, *s);
            Ok(Arc::new(null_overflow_decimal::<Decimal128Type>(
                array.as_primitive::<Decimal128Type>(),
                p,
                s,
            )?))
        }
        DataType::Decimal256(p, s) => {
            let (p, s) = (*p, *s);
            Ok(Arc::new(null_overflow_decimal::<Decimal256Type>(
                array.as_primitive::<Decimal256Type>(),
                p,
                s,
            )?))
        }
        _ => Ok(array),
    }
}

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

    mod tests_arrow_arith {
        use datafusion::arrow::array::{AsArray, Decimal128Array, Int32Array};
        use datafusion::arrow::compute::kernels::numeric::{div, mul};
        use datafusion::arrow::datatypes::{DataType, Decimal128Type, Int32Type};

        use super::*;

        #[test]
        fn test_fast_path_and_null_propagation() -> datafusion_common::Result<()> {
            let l: ArrayRef = Arc::new(Int32Array::from(vec![Some(2), Some(3), None]));
            let r: ArrayRef = Arc::new(Int32Array::from(vec![Some(5), Some(6), Some(7)]));
            let out = try_arrow_arith(&l, &r, &DataType::Int32, mul)?;
            let out = out.as_primitive::<Int32Type>();
            assert_eq!(out.value(0), 10);
            assert_eq!(out.value(1), 18);
            assert!(out.is_null(2));
            Ok(())
        }

        #[test]
        fn test_overflow_becomes_null() -> datafusion_common::Result<()> {
            // i32::MAX * 2 overflows the checked kernel -> that row is NULL, the
            // rest still compute (per-element fallback).
            let l: ArrayRef = Arc::new(Int32Array::from(vec![Some(i32::MAX), Some(4)]));
            let r: ArrayRef = Arc::new(Int32Array::from(vec![Some(2), Some(5)]));
            let out = try_arrow_arith(&l, &r, &DataType::Int32, mul)?;
            let out = out.as_primitive::<Int32Type>();
            assert!(out.is_null(0));
            assert_eq!(out.value(1), 20);
            Ok(())
        }

        #[test]
        fn test_div_by_zero_becomes_null() -> datafusion_common::Result<()> {
            let l: ArrayRef = Arc::new(Int32Array::from(vec![Some(10), Some(20)]));
            let r: ArrayRef = Arc::new(Int32Array::from(vec![Some(0), Some(5)]));
            let out = try_arrow_arith(&l, &r, &DataType::Int32, div)?;
            let out = out.as_primitive::<Int32Type>();
            assert!(out.is_null(0));
            assert_eq!(out.value(1), 4);
            Ok(())
        }

        #[test]
        fn test_null_decimal_overflow_enforces_declared_precision() -> datafusion_common::Result<()>
        {
            // 10^38 fits in i128 but has 39 digits, so it exceeds the default
            // DECIMAL(38,10) precision. `Decimal128Array::from` builds it unvalidated
            // (exactly how Arrow's checked `mul` can leave an over-precision result),
            // and `null_decimal_overflow` must turn it into NULL while keeping 5.
            let over = 10i128.pow(38);
            let arr: ArrayRef = Arc::new(Decimal128Array::from(vec![Some(over), Some(5)]));
            let out = null_decimal_overflow(arr)?;
            let out = out.as_primitive::<Decimal128Type>();
            assert!(out.is_null(0));
            assert_eq!(out.value(1), 5);
            Ok(())
        }

        #[test]
        fn test_decimal_precision_overflow_becomes_null() -> datafusion_common::Result<()> {
            // 10^37 * 10^37 exceeds DECIMAL precision 38 -> NULL; 2 * 3 stays 6.
            let big = 10i128.pow(37);
            let l: ArrayRef = Arc::new(
                Decimal128Array::from(vec![Some(big), Some(2)]).with_precision_and_scale(38, 0)?,
            );
            let r: ArrayRef = Arc::new(
                Decimal128Array::from(vec![Some(big), Some(3)]).with_precision_and_scale(38, 0)?,
            );
            let result_type = arith_result_type(l.data_type(), Operator::Multiply, r.data_type())?;
            let out = try_arrow_arith(&l, &r, &result_type, mul)?;
            let out = out.as_primitive::<Decimal128Type>();
            assert!(out.is_null(0));
            assert_eq!(out.value(1), 6);
            Ok(())
        }
    }
}
