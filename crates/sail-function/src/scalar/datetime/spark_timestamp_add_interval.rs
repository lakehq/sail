use std::sync::Arc;

use chrono::{
    DateTime, FixedOffset, MappedLocalTime, Months, NaiveDateTime, Offset, TimeDelta, TimeZone, Utc,
};
use chrono_tz::{GapInfo, Tz};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{
    DataType, DurationMicrosecondType, Field, FieldRef, IntervalMonthDayNanoType, IntervalUnit,
    IntervalYearMonthType, TimeUnit, TimestampMicrosecondType,
};
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::scalar::datetime::convert_tz::{CastZone, parse_cast_zone};

const MICROS_PER_DAY: i64 = 86_400_000_000;

/// Spark's addition of an interval to a TIMESTAMP, which adds the months and then the days of the
/// interval to the wall-clock time in the session time zone, and then adds the rest of the
/// interval to the instant.
///
/// The arguments are the TIMESTAMP in microseconds, the interval, the session time zone, and whether
/// to subtract the interval instead. A day-time interval adds its whole days as days, like Spark's
/// `timestampAddDayTime`.
///
/// Reference:
///   `org.apache.spark.sql.catalyst.util.DateTimeUtils#timestampAddInterval`
///   `org.apache.spark.sql.catalyst.util.DateTimeUtils#timestampAddDayTime`
///   `org.apache.spark.sql.catalyst.util.DateTimeUtils#timestampAddMonths`
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTimestampAddInterval {
    signature: Signature,
}

impl Default for SparkTimestampAddInterval {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTimestampAddInterval {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(4, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkTimestampAddInterval {
    fn name(&self) -> &str {
        "spark_timestamp_add_interval"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types.first() {
            Some(t @ DataType::Timestamp(TimeUnit::Microsecond, _)) => Ok(t.clone()),
            other => exec_err!("`spark_timestamp_add_interval` expects a TIMESTAMP, got {other:?}"),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let arg_types = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        let data_type = self.return_type(&arg_types)?;
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(self.name(), data_type, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;
        let [timestamp, interval, zone, negate] = args.as_slice() else {
            return exec_err!(
                "`spark_timestamp_add_interval` requires 4 arguments, got {}",
                args.len()
            );
        };
        let zone = match zone {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(zone))) => parse_zone(zone)?,
            other => {
                return exec_err!(
                    "invalid time zone for `spark_timestamp_add_interval`: {other:?}"
                );
            }
        };
        let negate = match negate {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(negate))) => *negate,
            other => {
                return exec_err!("invalid flag for `spark_timestamp_add_interval`: {other:?}");
            }
        };
        let all_scalar = matches!(
            (timestamp, interval),
            (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_))
        );
        let rows = if all_scalar { 1 } else { number_rows };
        let timestamps = timestamp.to_array(rows)?;
        let label = match timestamps.data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, label) => label.clone(),
            other => {
                return exec_err!(
                    "`spark_timestamp_add_interval` expects a TIMESTAMP, got {other}"
                );
            }
        };
        let timestamps = timestamps.as_primitive::<TimestampMicrosecondType>();
        let intervals = interval.to_array(rows)?;
        let parts = interval_parts(&intervals)?;
        let result = timestamps
            .iter()
            .zip(parts)
            .map(|(value, parts)| match (value, parts) {
                (Some(value), Some((months, days, micros))) => {
                    let (months, days, micros) = if negate {
                        (
                            months.checked_neg().ok_or_else(overflow)?,
                            days.checked_neg().ok_or_else(overflow)?,
                            micros.checked_neg().ok_or_else(overflow)?,
                        )
                    } else {
                        (months, days, micros)
                    };
                    add_interval(value, months, days, micros, &zone).map(Some)
                }
                _ => Ok(None),
            })
            .collect::<Result<TimestampMicrosecondArray>>()?
            .with_timezone_opt(label);
        let result: ArrayRef = Arc::new(result);
        if all_scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}

fn overflow() -> datafusion_common::DataFusionError {
    exec_datafusion_err!("[ARITHMETIC_OVERFLOW] long overflow in TIMESTAMP interval arithmetic")
}

/// The months, days and microseconds of each interval.
fn interval_parts(intervals: &ArrayRef) -> Result<Vec<Option<(i32, i32, i64)>>> {
    let parts = match intervals.data_type() {
        // The resolver only passes the intervals that Sail creates.
        DataType::Duration(TimeUnit::Microsecond) => intervals
            .as_primitive::<DurationMicrosecondType>()
            .iter()
            .map(|v| {
                v.map(|micros| {
                    // A day-time interval has fewer than `i32::MAX` days.
                    let days = micros / MICROS_PER_DAY;
                    (0, days as i32, micros - days * MICROS_PER_DAY)
                })
            })
            .collect(),
        DataType::Interval(IntervalUnit::YearMonth) => intervals
            .as_primitive::<IntervalYearMonthType>()
            .iter()
            .map(|v| v.map(|months| (months, 0, 0)))
            .collect(),
        DataType::Interval(IntervalUnit::MonthDayNano) => intervals
            .as_primitive::<IntervalMonthDayNanoType>()
            .iter()
            // Spark rounds seconds to microseconds half away from zero.
            .map(|v| {
                v.map(|v| {
                    let micros = v.nanoseconds / 1_000 + v.nanoseconds % 1_000 / 500;
                    (v.months, v.days, micros)
                })
            })
            .collect(),
        other => {
            return exec_err!("`spark_timestamp_add_interval` expects an interval, got {other}");
        }
    };
    Ok(parts)
}

#[derive(Debug, Clone, Copy)]
enum Zone {
    Named(Tz),
    Fixed(FixedOffset),
}

/// Parses the session time zone like a cast does, including Spark's short zone IDs.
fn parse_zone(zone: &str) -> Result<Zone> {
    match parse_cast_zone(zone)? {
        CastZone::Named(tz) => Ok(Zone::Named(tz)),
        CastZone::Fixed(micros) => i32::try_from(micros / 1_000_000)
            .ok()
            .and_then(FixedOffset::east_opt)
            .map(Zone::Fixed)
            .ok_or_else(|| {
                exec_datafusion_err!("[INVALID_TIMEZONE] The timezone: {zone:?} is invalid.")
            }),
    }
}

/// Adds the months and days to the wall-clock time of `micros` in `zone` like
/// `java.time.ZonedDateTime#plusMonths` and `#plusDays`, and then adds `rest` microseconds.
fn add_interval(micros: i64, months: i32, days: i32, rest: i64, zone: &Zone) -> Result<i64> {
    let calendar = if months == 0 && days == 0 {
        micros
    } else {
        match zone {
            // A fixed offset has no DST changes, so only the calendar arithmetic depends on it.
            Zone::Fixed(offset) => add_calendar(micros, months, days, offset)?,
            Zone::Named(tz) => add_calendar(micros, months, days, tz)?,
        }
    };
    calendar.checked_add(rest).ok_or_else(overflow)
}

fn add_calendar<T>(micros: i64, months: i32, days: i32, tz: &T) -> Result<i64>
where
    T: TimeZone + ResolveLocal,
{
    match add_wall_clock(micros, months, days, tz)? {
        Some(value) => Ok(value),
        // Chrono cannot represent every Spark timestamp, so days beyond its range are 24 hours.
        None if months == 0 => i64::from(days)
            .checked_mul(MICROS_PER_DAY)
            .and_then(|days| micros.checked_add(days))
            .ok_or_else(overflow),
        None => exec_err!("TIMESTAMP out of range for interval arithmetic: {micros}"),
    }
}

/// Adds the months and days to the wall-clock time, or returns `None` beyond the range of chrono.
fn add_wall_clock<T>(micros: i64, months: i32, days: i32, tz: &T) -> Result<Option<i64>>
where
    T: TimeZone + ResolveLocal,
{
    let Some(start) = DateTime::<Utc>::from_timestamp_micros(micros) else {
        return Ok(None);
    };
    let mut offset = start.with_timezone(tz).offset().fix();
    let Some(mut local) = start.naive_utc().checked_add_offset(offset) else {
        return Ok(None);
    };
    if months != 0 {
        let shifted = if months > 0 {
            local.checked_add_months(Months::new(months.unsigned_abs()))
        } else {
            local.checked_sub_months(Months::new(months.unsigned_abs()))
        };
        let Some(resolved) = shifted.and_then(|shifted| tz.resolve(shifted, offset)) else {
            return Ok(None);
        };
        (local, offset) = resolved;
    }
    if days != 0 {
        let Some(resolved) = TimeDelta::try_days(i64::from(days))
            .and_then(|days| local.checked_add_signed(days))
            .and_then(|shifted| tz.resolve(shifted, offset))
        else {
            return Ok(None);
        };
        (local, offset) = resolved;
    }
    Ok(local
        .checked_sub_offset(offset)
        .map(|utc| utc.and_utc().timestamp_micros()))
}

/// Resolves a wall-clock time to the time and offset of `java.time.ZonedDateTime#ofLocal`, which
/// keeps the preferred offset in a DST overlap if it is valid, or takes the earlier offset, and
/// moves a time in a DST gap forward by the length of the gap. Returns `None` beyond the range of
/// chrono.
trait ResolveLocal {
    fn resolve(
        &self,
        local: NaiveDateTime,
        preferred: FixedOffset,
    ) -> Option<(NaiveDateTime, FixedOffset)>;
}

impl ResolveLocal for FixedOffset {
    fn resolve(
        &self,
        local: NaiveDateTime,
        _preferred: FixedOffset,
    ) -> Option<(NaiveDateTime, FixedOffset)> {
        Some((local, *self))
    }
}

impl ResolveLocal for Tz {
    fn resolve(
        &self,
        local: NaiveDateTime,
        preferred: FixedOffset,
    ) -> Option<(NaiveDateTime, FixedOffset)> {
        match local.and_local_timezone(*self) {
            MappedLocalTime::Single(value) => Some((local, value.offset().fix())),
            MappedLocalTime::Ambiguous(earlier, later) => {
                if later.offset().fix() == preferred {
                    Some((local, preferred))
                } else {
                    Some((local, earlier.offset().fix()))
                }
            }
            // Chrono also finds no time beyond its range, which is not in a gap.
            MappedLocalTime::None => {
                let gap = GapInfo::new(&local, self)?;
                let (start, _) = gap.begin?;
                let value = gap.end?.checked_add_signed(local - start)?;
                Some((value.naive_local(), value.offset().fix()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{DurationMicrosecondArray, IntervalMonthDayNanoArray};

    use super::*;

    const LA: &str = "America/Los_Angeles";

    fn add(micros: i64, months: i32, days: i32, rest: i64, zone: &str) -> Result<i64> {
        add_interval(micros, months, days, rest, &parse_zone(zone)?)
    }

    #[test]
    fn test_days_into_dst_overlap_keep_valid_offset() -> Result<()> {
        // 2020-10-31 01:30 PDT + 1 day = 2020-11-01 01:30 PDT, the earlier instant.
        assert_eq!(
            add(1_604_133_000_000_000, 0, 1, 0, LA)?,
            1_604_219_400_000_000
        );
        // 2020-01-15 01:30 PST + 291 days = 2020-11-01 01:30 PST, the later instant, since
        // `ZonedDateTime#plusDays` keeps the offset when it is still valid.
        assert_eq!(
            add(1_579_080_600_000_000, 0, 291, 0, LA)?,
            1_604_223_000_000_000
        );
        Ok(())
    }

    #[test]
    fn test_month_into_dst_gap_moves_forward() -> Result<()> {
        // 2020-02-08 02:30 PST + 1 month = 2020-03-08 02:30, in the gap, so 03:30 PDT.
        assert_eq!(
            add(1_581_157_800_000_000, 1, 0, 0, LA)?,
            1_583_663_400_000_000
        );
        Ok(())
    }

    #[test]
    fn test_day_time_interval_whole_days() -> Result<()> {
        let interval: ArrayRef =
            Arc::new(DurationMicrosecondArray::from(vec![Some(86_400_000_000)]));
        // 24 hours of a day-time interval is one day: 2020-11-01 01:30 PDT -> 2020-11-02 01:30 PST.
        assert_eq!(interval_parts(&interval)?, vec![Some((0, 1, 0))]);
        assert_eq!(
            add(1_604_219_400_000_000, 0, 1, 0, LA)?,
            1_604_309_400_000_000
        );
        Ok(())
    }

    #[test]
    fn test_months_in_fixed_offset_zone() -> Result<()> {
        // 2020-02-29 20:00Z is 2020-03-01 01:30 at +05:30, and a month later is 2020-04-01 01:30.
        assert_eq!(
            add(1_583_006_400_000_000, 1, 0, 0, "+05:30")?,
            1_585_684_800_000_000
        );
        Ok(())
    }

    #[test]
    fn test_short_zone_id() -> Result<()> {
        // Spark's short ID PST is America/Los_Angeles.
        assert_eq!(
            add(1_604_133_000_000_000, 0, 1, 0, "PST")?,
            add(1_604_133_000_000_000, 0, 1, 0, LA)?
        );
        Ok(())
    }

    #[test]
    fn test_days_beyond_chrono_range() -> Result<()> {
        // Chrono cannot represent the year 270000, so days are 24 hours and months fail.
        let micros = 8_458_000_000_000_000_000;
        assert_eq!(add(micros, 0, 1, 0, LA)?, micros + 86_400_000_000);
        assert!(add(micros, 1, 0, 0, LA).is_err());
        // The wall-clock time of the last instant of chrono in Shanghai, and the day after the
        // last instant of chrono in Los Angeles, are beyond its range too.
        let max = 8_210_266_873_199_999_999;
        assert_eq!(add(max, 0, 1, 0, "Asia/Shanghai")?, max + 86_400_000_000);
        assert_eq!(add(max, 0, 1, 0, LA)?, max + 86_400_000_000);
        // The wall-clock time a day later is in range, but its instant is not.
        let late = 8_210_266_804_799_999_999;
        assert_eq!(add(late, 0, 1, 0, LA)?, late + 86_400_000_000);
        let early = -8_334_601_156_800_000_000;
        assert_eq!(
            add(early, 0, -1, 0, "Asia/Shanghai")?,
            early - 86_400_000_000
        );
        Ok(())
    }

    #[test]
    fn test_sub_microsecond_seconds_round_half_away_from_zero() -> Result<()> {
        let interval: ArrayRef = Arc::new(IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNanoType::make_value(0, 0, 2_500),
            IntervalMonthDayNanoType::make_value(0, 0, -1_500),
            IntervalMonthDayNanoType::make_value(0, 0, 1_499),
        ]));
        assert_eq!(
            interval_parts(&interval)?,
            vec![Some((0, 0, 3)), Some((0, 0, -2)), Some((0, 0, 1))]
        );
        Ok(())
    }
}
