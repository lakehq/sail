use chrono::{
    DateTime, Datelike, Days, LocalResult, Months, NaiveDate, NaiveDateTime, Offset, TimeDelta,
    TimeZone, Utc,
};
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::types::{
    Decimal128Type, Int32Type, Time64MicrosecondType, TimestampMicrosecondType,
};
use datafusion::arrow::array::{Array, AsArray, Int32Array, PrimitiveArray};
use datafusion::arrow::temporal_conversions::as_datetime;
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_expr::ColumnarValue;
use sail_common::utils::datetime::MICROS_PER_DAY;
use sail_common_datafusion::utils::datetime::localize_with_fallback;

/// Zones whose local microseconds equal UTC, including outside Chrono's date range.
pub fn is_fixed_zero_offset_timezone(timezone: &str) -> bool {
    matches!(
        timezone,
        "+00" | "-00" | "+0000" | "-0000" | "+00:00" | "-00:00"
    ) || matches!(
        timezone.strip_prefix("Etc/").unwrap_or(timezone),
        "GMT" | "GMT+0" | "GMT-0" | "GMT0" | "Greenwich" | "UCT" | "UTC" | "Universal" | "Zulu"
    )
}

pub(crate) fn string_array_iter(
    array: &dyn Array,
) -> Result<Box<dyn Iterator<Item = Option<&str>> + '_>> {
    match array.data_type() {
        arrow::datatypes::DataType::Utf8 => Ok(Box::new(as_string_array(array)?.iter())),
        arrow::datatypes::DataType::LargeUtf8 => Ok(Box::new(as_large_string_array(array)?.iter())),
        arrow::datatypes::DataType::Utf8View => Ok(Box::new(as_string_view_array(array)?.iter())),
        data_type => exec_err!("expected string array, got {data_type}"),
    }
}

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

// Helper function to create timestamp from Date32 (days since epoch) and Time64 (microseconds since midnight)
pub(crate) fn make_timestamp_from_date_time(date: i32, time: i64) -> Option<i64> {
    (date as i64)
        .checked_mul(MICROS_PER_DAY)
        .and_then(|v| v.checked_add(time))
}

fn checked_add_months(datetime: NaiveDateTime, months: i32) -> Option<NaiveDateTime> {
    if months >= 0 {
        datetime.checked_add_months(Months::new(months as u32))
    } else {
        datetime.checked_sub_months(Months::new(months.unsigned_abs()))
    }
}

pub(crate) fn checked_add_days(datetime: NaiveDateTime, days: i32) -> Option<NaiveDateTime> {
    if days >= 0 {
        datetime.checked_add_days(Days::new(days as u64))
    } else {
        datetime.checked_sub_days(Days::new(u64::from(days.unsigned_abs())))
    }
}

pub(crate) fn days_in_month(year: i64, month: u32) -> u32 {
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

pub(crate) fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = year - if month <= 2 { 1 } else { 0 };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let shifted_month = i64::from(month) + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

pub(crate) fn civil_from_days(days: i64) -> (i64, u32, u32) {
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

pub(crate) fn add_wide_calendar_interval(
    start: i128,
    months: i32,
    days: i32,
    micros: i64,
) -> Result<i128> {
    let epoch_day = i64::try_from(start.div_euclid(i128::from(MICROS_PER_DAY)))
        .map_err(|_| exec_datafusion_err!("cannot convert timestamp {start}"))?;
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

pub(crate) fn add_calendar_interval_wide(
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

pub(crate) fn localize_with_preferred_offset(
    timezone: Tz,
    datetime: NaiveDateTime,
    preferred_offset: i32,
) -> Result<DateTime<Tz>> {
    match timezone.from_local_datetime(&datetime) {
        LocalResult::Single(value) => Ok(value),
        LocalResult::Ambiguous(first, second) => {
            if first.offset().fix().local_minus_utc() == preferred_offset {
                Ok(first)
            } else if second.offset().fix().local_minus_utc() == preferred_offset {
                Ok(second)
            } else {
                Ok(first)
            }
        }
        LocalResult::None => {
            Ok(localize_with_fallback(&timezone, &datetime)?.with_timezone(&timezone))
        }
    }
}

pub(crate) fn add_ltz_interval(
    start: i64,
    months: i32,
    days: i32,
    micros: i64,
    timezone: Tz,
) -> Result<i64> {
    let mut datetime = as_datetime::<TimestampMicrosecondType>(start)
        .map(|value| Utc.from_utc_datetime(&value).with_timezone(&timezone))
        .ok_or_else(|| exec_datafusion_err!("cannot convert timestamp {start}"))?;

    let calendar_fits = checked_add_months(datetime.naive_local(), months)
        .and_then(|value| checked_add_days(value, days))
        .is_some();

    if calendar_fits {
        if months != 0 {
            let preferred_offset = datetime.offset().fix().local_minus_utc();
            let local = checked_add_months(datetime.naive_local(), months)
                .ok_or_else(|| exec_datafusion_err!("cannot add {months} months to {start}"))?;
            datetime = localize_with_preferred_offset(timezone, local, preferred_offset)?;
        }
        if days != 0 {
            let preferred_offset = datetime.offset().fix().local_minus_utc();
            let local = checked_add_days(datetime.naive_local(), days)
                .ok_or_else(|| exec_datafusion_err!("cannot add {days} days to {start}"))?;
            datetime = localize_with_preferred_offset(timezone, local, preferred_offset)?;
        }
    } else {
        let preferred_offset = datetime.offset().fix().local_minus_utc();
        let local =
            add_calendar_interval_wide(datetime.naive_local(), months, days).ok_or_else(|| {
                exec_datafusion_err!("cannot add {months} months and {days} days to {start}")
            })?;
        datetime = localize_with_preferred_offset(timezone, local, preferred_offset)?;
    }

    datetime
        .with_timezone(&Utc)
        .checked_add_signed(TimeDelta::microseconds(micros))
        .map(|value| value.timestamp_micros())
        .ok_or_else(|| exec_datafusion_err!("cannot add {micros} microseconds to {start}"))
}

pub(crate) fn add_ntz_interval(start: i64, months: i32, days: i32, micros: i64) -> Result<i64> {
    let result = add_wide_calendar_interval(i128::from(start), months, days, micros)?;
    i64::try_from(result).map_err(|_| exec_datafusion_err!("long overflow"))
}
