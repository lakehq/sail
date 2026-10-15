use std::sync::Arc;

use chrono::{
    DateTime, Datelike, Days, LocalResult, Months, NaiveDate, NaiveDateTime, Offset, TimeDelta,
    TimeZone, Utc,
};
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{
    DataType, DurationMicrosecondType, Field, FieldRef, Int64Type, IntervalDayTimeType,
    IntervalMonthDayNanoType, IntervalUnit, IntervalYearMonthType, TimeUnit,
    TimestampMicrosecondType,
};
use datafusion::arrow::temporal_conversions::as_datetime;
use datafusion_common::{Result, exec_datafusion_err, exec_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use sail_common_datafusion::utils::datetime::localize_with_fallback;

use crate::functions_nested_utils::make_scalar_function;
use crate::scalar::datetime::utils::is_fixed_zero_offset_timezone;

const MICROS_PER_DAY: i64 = 86_400_000_000;

/// Spark calendar arithmetic: months, then local days, then elapsed microseconds.
/// The same kernel is used for interval addition, TIMESTAMPADD, and SEQUENCE.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTimestampAdd {
    signature: Signature,
}

impl Default for SparkTimestampAdd {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTimestampAdd {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkTimestampAdd {
    fn name(&self) -> &str {
        "spark_timestamp_add"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types
            .first()
            .cloned()
            .ok_or_else(|| exec_datafusion_err!("timestamp addition requires a timestamp"))
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type(
                &args
                    .arg_fields
                    .iter()
                    .map(|field| field.data_type().clone())
                    .collect::<Vec<_>>(),
            )?,
            args.arg_fields.iter().any(|field| field.is_nullable()),
        )))
    }
    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        match types {
            [DataType::Timestamp(_, timezone), interval]
                if matches!(
                    interval,
                    DataType::Interval(_) | DataType::Duration(TimeUnit::Microsecond)
                ) =>
            {
                Ok(vec![
                    DataType::Timestamp(TimeUnit::Microsecond, timezone.clone()),
                    interval.clone(),
                ])
            }
            [DataType::Timestamp(_, timezone), _, unit] if unit.is_string() => Ok(vec![
                DataType::Timestamp(TimeUnit::Microsecond, timezone.clone()),
                DataType::Int64,
                DataType::Utf8,
            ]),
            _ => exec_err!("invalid timestamp addition arguments: {types:?}"),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(timestamp_add)(&args.args)
    }
}

fn timestamp_add(args: &[ArrayRef]) -> Result<ArrayRef> {
    let timestamps = args[0].as_primitive::<TimestampMicrosecondType>();
    let timezone = match timestamps.data_type() {
        DataType::Timestamp(_, timezone) => timezone.clone(),
        _ => return exec_err!("expected timestamp"),
    };
    let tz = timezone.as_deref().unwrap_or("UTC").parse::<Tz>()?;
    let utc_or_ntz = timezone
        .as_deref()
        .is_none_or(is_fixed_zero_offset_timezone);
    let intervals = &args[1];
    let unit = args.get(2).map(|unit| unit.as_string::<i32>());
    let result = (0..timestamps.len())
        .map(|row| {
            if timestamps.is_null(row)
                || intervals.is_null(row)
                || unit.is_some_and(|unit| unit.is_null(row))
            {
                return Ok(None);
            }
            let (months, days, micros) = if let Some(unit) = unit {
                interval_for_unit(
                    unit.value(row),
                    intervals.as_primitive::<Int64Type>().value(row),
                )?
            } else {
                match intervals.data_type() {
                    DataType::Duration(TimeUnit::Microsecond) => {
                        let micros = intervals
                            .as_primitive::<DurationMicrosecondType>()
                            .value(row);
                        (
                            0,
                            i32::try_from(micros / MICROS_PER_DAY)
                                .map_err(|_| exec_datafusion_err!("day interval overflow"))?,
                            micros % MICROS_PER_DAY,
                        )
                    }
                    DataType::Interval(IntervalUnit::YearMonth) => (
                        intervals.as_primitive::<IntervalYearMonthType>().value(row),
                        0,
                        0,
                    ),
                    DataType::Interval(IntervalUnit::DayTime) => {
                        let value = intervals.as_primitive::<IntervalDayTimeType>().value(row);
                        (0, value.days, i64::from(value.milliseconds) * 1_000)
                    }
                    DataType::Interval(IntervalUnit::MonthDayNano) => {
                        let value = intervals
                            .as_primitive::<IntervalMonthDayNanoType>()
                            .value(row);
                        (value.months, value.days, value.nanoseconds / 1_000)
                    }
                    data_type => return exec_err!("invalid timestamp interval type: {data_type}"),
                }
            };
            let start = timestamps.value(row);
            // Pure elapsed arithmetic must not depend on Chrono's narrower calendar range.
            let value = if months == 0 && days == 0 {
                start
                    .checked_add(micros)
                    .ok_or_else(|| exec_datafusion_err!("timestamp overflow"))?
            } else if utc_or_ntz {
                add_ntz_interval(start, months, days, micros)?
            } else {
                add_ltz_interval(start, months, days, micros, tz)?
            };
            Ok(Some(value))
        })
        .collect::<Result<TimestampMicrosecondArray>>()?
        .with_timezone_opt(timezone);
    Ok(Arc::new(result))
}

fn interval_for_unit(unit: &str, quantity: i64) -> Result<(i32, i32, i64)> {
    let overflow = || exec_datafusion_err!("timestampadd overflow for {quantity} {unit}");
    let months = |factor: i32| -> Result<_> {
        Ok((
            i32::try_from(quantity)
                .map_err(|_| overflow())?
                .checked_mul(factor)
                .ok_or_else(overflow)?,
            0,
            0,
        ))
    };
    let days = |factor: i32| -> Result<_> {
        Ok((
            0,
            i32::try_from(quantity)
                .map_err(|_| overflow())?
                .checked_mul(factor)
                .ok_or_else(overflow)?,
            0,
        ))
    };
    let micros = |factor: i64| -> Result<_> {
        Ok((0, 0, quantity.checked_mul(factor).ok_or_else(overflow)?))
    };
    match unit {
        "YEAR" => months(12),
        "QUARTER" => months(3),
        "MONTH" => months(1),
        "WEEK" => days(7),
        "DAY" | "DAYOFYEAR" => days(1),
        "HOUR" => micros(3_600_000_000),
        "MINUTE" => micros(60_000_000),
        "SECOND" => micros(1_000_000),
        "MILLISECOND" => micros(1_000),
        "MICROSECOND" => micros(1),
        _ => exec_err!("unsupported timestampadd unit: {unit}"),
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

fn localize_with_preferred_offset(
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
