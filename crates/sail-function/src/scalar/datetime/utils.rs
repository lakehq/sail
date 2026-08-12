use chrono::{Datelike, Days, Months, NaiveDate, NaiveDateTime, Offset, TimeDelta, TimeZone, Utc};
use datafusion::arrow::array::types::{Decimal128Type, Int32Type, Time64MicrosecondType};
use datafusion::arrow::array::{AsArray, Int32Array, PrimitiveArray};
use datafusion::arrow::datatypes::TimestampMicrosecondType;
use datafusion::arrow::temporal_conversions::as_datetime;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_expr::ColumnarValue;
use sail_common_datafusion::utils::datetime::{SparkTimeZone, localize_with_preferred_offset};

const MICROS_PER_DAY: i64 = 86_400_000_000;

// Shared array conversion helpers for make_timestamp functions

pub(crate) fn to_time64_array(
    col: &ColumnarValue,
    arg_name: &str,
    fn_name: &str,
    number_rows: usize,
) -> Result<PrimitiveArray<Time64MicrosecondType>> {
    match col {
        ColumnarValue::Array(array) => Ok(array.as_primitive::<Time64MicrosecondType>().to_owned()),
        ColumnarValue::Scalar(ScalarValue::Time64Microsecond(Some(value))) => {
            Ok(PrimitiveArray::<Time64MicrosecondType>::from_value(
                *value,
                number_rows,
            ))
        }
        other => {
            exec_err!("Unsupported {arg_name} arg {other:?} for Spark function `{fn_name}`")
        }
    }
}

/// Reads a `Decimal128` column as its raw unscaled `i128` values.
pub(crate) fn to_decimal128_array(
    col: &ColumnarValue,
    arg_name: &str,
    fn_name: &str,
    number_rows: usize,
) -> Result<PrimitiveArray<Decimal128Type>> {
    match col {
        ColumnarValue::Array(array) => Ok(array.as_primitive::<Decimal128Type>().to_owned()),
        ColumnarValue::Scalar(ScalarValue::Decimal128(Some(value), _, _)) => {
            Ok(PrimitiveArray::<Decimal128Type>::from_value(
                *value,
                number_rows,
            ))
        }
        other => {
            exec_err!("Unsupported {arg_name} arg {other:?} for Spark function `{fn_name}`")
        }
    }
}

/// Mirrors Spark's `INVALID_PARAMETER_VALUE.TIME_UNIT`, raised by
/// `QueryExecutionErrors.invalidTimeUnitError` for `time_diff` and `time_trunc`.
/// The doubled quotes around the unit are Spark's: `toSQLValue` already quotes the value and
/// the message template quotes it again.
pub(crate) fn invalid_time_unit_err<T>(fn_name: &str, unit: &str) -> Result<T> {
    exec_err!(
        "The value of parameter(s) `unit` in `{fn_name}` is invalid: expects one of the units \
         'HOUR', 'MINUTE', 'SECOND', 'MILLISECOND', 'MICROSECOND', but got ''{unit}''."
    )
}

pub(crate) fn to_int32_array(
    col: &ColumnarValue,
    arg_name: &str,
    fn_name: &str,
    number_rows: usize,
) -> Result<Int32Array> {
    match col {
        ColumnarValue::Array(array) => Ok(array.as_primitive::<Int32Type>().to_owned()),
        ColumnarValue::Scalar(ScalarValue::Int32(Some(value))) => {
            Ok(Int32Array::from_value(*value, number_rows))
        }
        other => {
            exec_err!("Unsupported {arg_name} arg {other:?} for Spark function `{fn_name}`")
        }
    }
}

fn checked_add_months(datetime: NaiveDateTime, months: i32) -> Option<NaiveDateTime> {
    if months >= 0 {
        datetime.checked_add_months(Months::new(months as u32))
    } else {
        datetime.checked_sub_months(Months::new(months.unsigned_abs()))
    }
}

fn checked_add_days(datetime: NaiveDateTime, days: i32) -> Option<NaiveDateTime> {
    if days >= 0 {
        datetime.checked_add_days(Days::new(days as u64))
    } else {
        datetime.checked_sub_days(Days::new(u64::from(days.unsigned_abs())))
    }
}

fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year.rem_euclid(4) == 0
            && (year.rem_euclid(100) != 0 || year.rem_euclid(400) == 0) =>
        {
            29
        }
        2 => 28,
        _ => unreachable!("month is normalized to 1..=12"),
    }
}

fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = year - if month <= 2 { 1 } else { 0 };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let shifted_month = i64::from(month) + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let days = days + 719_468;
    let era = days.div_euclid(146_097);
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = shifted_month + if shifted_month < 10 { 3 } else { -9 };
    year += if month <= 2 { 1 } else { 0 };
    (year, month as u32, day as u32)
}

fn add_wide_calendar_interval(start: i128, months: i32, days: i32, micros: i64) -> Result<i128> {
    let epoch_day = i64::try_from(start.div_euclid(i128::from(MICROS_PER_DAY)))
        .map_err(|_| exec_datafusion_err!("cannot convert sequence timestamp {start}"))?;
    let micros_of_day = start.rem_euclid(i128::from(MICROS_PER_DAY));
    let (year, month, day) = civil_from_days(epoch_day);

    let month_index = year
        .checked_mul(12)
        .and_then(|value| value.checked_add(i64::from(month - 1)))
        .and_then(|value| value.checked_add(i64::from(months)))
        .ok_or_else(|| exec_datafusion_err!("cannot add {months} months to {start}"))?;
    let year = month_index.div_euclid(12);
    let month = u32::try_from(month_index.rem_euclid(12) + 1)
        .map_err(|_| exec_datafusion_err!("cannot add {months} months to {start}"))?;
    let day = day.min(days_in_month(year, month));
    let epoch_day = days_from_civil(year, month, day)
        .checked_add(i64::from(days))
        .ok_or_else(|| exec_datafusion_err!("cannot add {days} days to {start}"))?;

    Ok(i128::from(epoch_day) * i128::from(MICROS_PER_DAY) + micros_of_day + i128::from(micros))
}

fn add_calendar_interval_wide(
    datetime: NaiveDateTime,
    months: i32,
    days: i32,
) -> Option<NaiveDateTime> {
    let date = datetime.date();
    let month_index = i64::from(date.year())
        .checked_mul(12)?
        .checked_add(i64::from(date.month0()))?
        .checked_add(i64::from(months))?;
    let year = month_index.div_euclid(12);
    let month = u32::try_from(month_index.rem_euclid(12) + 1).ok()?;
    let day = date.day().min(days_in_month(year, month));
    let epoch_day = days_from_civil(year, month, day).checked_add(i64::from(days))?;
    let (year, month, day) = civil_from_days(epoch_day);
    let year = i32::try_from(year).ok()?;

    NaiveDate::from_ymd_opt(year, month, day).map(|date| date.and_time(datetime.time()))
}

fn add_ltz_interval(
    start: i64,
    months: i32,
    days: i32,
    micros: i64,
    timezone: SparkTimeZone,
) -> Result<i64> {
    if let SparkTimeZone::Fixed(offset) = timezone {
        let offset_micros = i128::from(offset.local_minus_utc()) * 1_000_000;
        let local_start = i128::from(start) + offset_micros;
        let local_result =
            add_wide_calendar_interval(local_start, months, days, micros)? - offset_micros;
        return i64::try_from(local_result)
            .map_err(|_| exec_datafusion_err!("cannot add interval to {start}"));
    }

    let mut datetime = as_datetime::<TimestampMicrosecondType>(start)
        .map(|value| Utc.from_utc_datetime(&value).with_timezone(&timezone))
        .ok_or_else(|| exec_datafusion_err!("cannot convert sequence timestamp {start}"))?;

    let calendar_fits = checked_add_months(datetime.naive_local(), months)
        .and_then(|value| checked_add_days(value, days))
        .is_some();

    if calendar_fits {
        if months != 0 {
            let preferred_offset = datetime.offset().fix().local_minus_utc();
            let local = checked_add_months(datetime.naive_local(), months)
                .ok_or_else(|| exec_datafusion_err!("cannot add {months} months to {start}"))?;
            datetime = localize_with_preferred_offset(&timezone, &local, preferred_offset)?;
        }
        if days != 0 {
            let preferred_offset = datetime.offset().fix().local_minus_utc();
            let local = checked_add_days(datetime.naive_local(), days)
                .ok_or_else(|| exec_datafusion_err!("cannot add {days} days to {start}"))?;
            datetime = localize_with_preferred_offset(&timezone, &local, preferred_offset)?;
        }
    } else {
        let preferred_offset = datetime.offset().fix().local_minus_utc();
        let local =
            add_calendar_interval_wide(datetime.naive_local(), months, days).ok_or_else(|| {
                exec_datafusion_err!("cannot add {months} months and {days} days to {start}")
            })?;
        datetime = localize_with_preferred_offset(&timezone, &local, preferred_offset)?;
    }

    datetime
        .with_timezone(&Utc)
        .checked_add_signed(TimeDelta::microseconds(micros))
        .map(|value| value.timestamp_micros())
        .ok_or_else(|| exec_datafusion_err!("cannot add {micros} microseconds to {start}"))
}

fn add_ntz_interval(start: i64, months: i32, days: i32, micros: i64) -> Result<i64> {
    let result = add_wide_calendar_interval(i128::from(start), months, days, micros)?;
    i64::try_from(result).map_err(|_| exec_datafusion_err!("cannot add interval to {start}"))
}

pub(crate) fn add_timestamp_interval(
    start: i64,
    months: i32,
    days: i32,
    micros: i64,
    timezone: SparkTimeZone,
    timestamp_ntz: bool,
) -> Result<i64> {
    if timestamp_ntz {
        add_ntz_interval(start, months, days, micros)
    } else {
        add_ltz_interval(start, months, days, micros, timezone)
    }
}
