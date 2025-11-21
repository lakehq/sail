use std::str::FromStr;
use std::sync::Arc;

use chrono::{Duration, NaiveDate, NaiveDateTime};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::scalar::ScalarValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Transform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
    Unknown,
}

impl FromStr for Transform {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(match value.trim().to_ascii_lowercase().as_str() {
            "identity" => Self::Identity,
            "year" => Self::Year,
            "month" => Self::Month,
            "day" => Self::Day,
            "hour" => Self::Hour,
            _ => Self::Unknown,
        })
    }
}

impl Transform {
    pub fn get_range(
        &self,
        part_value: &str,
        target_type: &DataType,
    ) -> Option<(ScalarValue, ScalarValue)> {
        match self {
            Self::Identity => {
                let scalar =
                    ScalarValue::try_from_string(part_value.to_string(), target_type).ok()?;
                Some((scalar.clone(), scalar))
            }
            Self::Year => {
                let year = parse_year(part_value)?;
                let kind = classify_temporal_type(target_type)?;
                let start = year_start(year)?;
                let end = year_end_exclusive(year)?;
                build_scalar_range(&kind, start, end)
            }
            Self::Month => {
                let (year, month) = parse_year_month(part_value)?;
                let kind = classify_temporal_type(target_type)?;
                let start = month_start(year, month)?;
                let end = month_end_exclusive(year, month)?;
                build_scalar_range(&kind, start, end)
            }
            Self::Day => {
                let date = parse_year_month_day(part_value)?;
                let kind = classify_temporal_type(target_type)?;
                let start = date.and_hms_opt(0, 0, 0)?;
                let end = start.checked_add_signed(Duration::days(1))?;
                build_scalar_range(&kind, start, end)
            }
            Self::Hour => {
                let dt = parse_year_month_day_hour(part_value)?;
                let kind = classify_temporal_type(target_type)?;
                let end = dt.checked_add_signed(Duration::hours(1))?;
                build_scalar_range(&kind, dt, end)
            }
            Self::Unknown => None,
        }
    }
}

pub fn parse_year(value: &str) -> Option<i32> {
    value.trim().parse::<i32>().ok()
}

pub fn parse_year_month(value: &str) -> Option<(i32, u32)> {
    let trimmed = value.trim();
    let normalized = trimmed.replace(['_', '/', '.'], "-");
    if let Some((year, month)) = normalized.split_once('-') {
        let year_val = year.parse::<i32>().ok()?;
        let month_val = month.parse::<u32>().ok()?;
        if (1..=12).contains(&month_val) {
            return Some((year_val, month_val));
        }
    }
    if normalized.len() == 6 {
        let (year_part, month_part) = normalized.split_at(4);
        let year_val = year_part.parse::<i32>().ok()?;
        let month_val = month_part.parse::<u32>().ok()?;
        if (1..=12).contains(&month_val) {
            return Some((year_val, month_val));
        }
    }
    None
}

pub fn parse_year_month_day(value: &str) -> Option<NaiveDate> {
    let trimmed = value.trim();
    if let Ok(date) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        return Some(date);
    }
    if let Ok(date) = NaiveDate::parse_from_str(trimmed, "%Y%m%d") {
        return Some(date);
    }
    let parts: Vec<&str> = trimmed.split('-').collect();
    if parts.len() == 3 {
        let year = parts[0].parse::<i32>().ok()?;
        let month = parts[1].parse::<u32>().ok()?;
        let day = parts[2].parse::<u32>().ok()?;
        return NaiveDate::from_ymd_opt(year, month, day);
    }
    None
}

pub fn parse_year_month_day_hour(value: &str) -> Option<NaiveDateTime> {
    let trimmed = value.trim();
    for fmt in ["%Y-%m-%d-%H", "%Y-%m-%d %H", "%Y-%m-%dT%H", "%Y%m%d%H"] {
        if let Ok(dt) = NaiveDateTime::parse_from_str(trimmed, fmt) {
            return Some(dt);
        }
    }
    let parts: Vec<&str> = trimmed.split('-').collect();
    if parts.len() == 4 {
        let year = parts[0].parse::<i32>().ok()?;
        let month = parts[1].parse::<u32>().ok()?;
        let day = parts[2].parse::<u32>().ok()?;
        let hour = parts[3].parse::<u32>().ok()?;
        let date = NaiveDate::from_ymd_opt(year, month, day)?;
        return date.and_hms_opt(hour, 0, 0);
    }
    None
}

fn year_start(year: i32) -> Option<NaiveDateTime> {
    NaiveDate::from_ymd_opt(year, 1, 1)?.and_hms_opt(0, 0, 0)
}

fn year_end_exclusive(year: i32) -> Option<NaiveDateTime> {
    NaiveDate::from_ymd_opt(year + 1, 1, 1)?.and_hms_opt(0, 0, 0)
}

fn month_start(year: i32, month: u32) -> Option<NaiveDateTime> {
    NaiveDate::from_ymd_opt(year, month, 1)?.and_hms_opt(0, 0, 0)
}

fn month_end_exclusive(year: i32, month: u32) -> Option<NaiveDateTime> {
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    NaiveDate::from_ymd_opt(next_year, next_month, 1)?.and_hms_opt(0, 0, 0)
}

enum TemporalType {
    Timestamp(TimeUnit, Option<Arc<str>>),
    Date32,
    Date64,
}

fn classify_temporal_type(data_type: &DataType) -> Option<TemporalType> {
    match data_type {
        DataType::Timestamp(unit, tz) => Some(TemporalType::Timestamp(*unit, tz.clone())),
        DataType::Date32 => Some(TemporalType::Date32),
        DataType::Date64 => Some(TemporalType::Date64),
        _ => None,
    }
}

fn to_timestamp_value(dt: NaiveDateTime, unit: TimeUnit) -> Option<i64> {
    match unit {
        TimeUnit::Second => Some(dt.and_utc().timestamp()),
        TimeUnit::Millisecond => Some(dt.and_utc().timestamp_millis()),
        TimeUnit::Microsecond => Some(dt.and_utc().timestamp_micros()),
        TimeUnit::Nanosecond => dt.and_utc().timestamp_nanos_opt(),
    }
}

fn days_since_epoch(date: NaiveDate) -> Option<i32> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    let duration = date.signed_duration_since(epoch);
    i32::try_from(duration.num_days()).ok()
}

fn build_scalar_range(
    kind: &TemporalType,
    start: NaiveDateTime,
    end_exclusive: NaiveDateTime,
) -> Option<(ScalarValue, ScalarValue)> {
    match kind {
        TemporalType::Timestamp(unit, tz) => {
            let start_val = to_timestamp_value(start, *unit)?;
            let end_exclusive_val = to_timestamp_value(end_exclusive, *unit)?;
            if end_exclusive_val <= start_val {
                return None;
            }
            let end_val = end_exclusive_val.checked_sub(1)?;
            let min = match unit {
                TimeUnit::Second => ScalarValue::TimestampSecond(Some(start_val), tz.clone()),
                TimeUnit::Millisecond => {
                    ScalarValue::TimestampMillisecond(Some(start_val), tz.clone())
                }
                TimeUnit::Microsecond => {
                    ScalarValue::TimestampMicrosecond(Some(start_val), tz.clone())
                }
                TimeUnit::Nanosecond => {
                    ScalarValue::TimestampNanosecond(Some(start_val), tz.clone())
                }
            };
            let max = match unit {
                TimeUnit::Second => ScalarValue::TimestampSecond(Some(end_val), tz.clone()),
                TimeUnit::Millisecond => {
                    ScalarValue::TimestampMillisecond(Some(end_val), tz.clone())
                }
                TimeUnit::Microsecond => {
                    ScalarValue::TimestampMicrosecond(Some(end_val), tz.clone())
                }
                TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(end_val), tz.clone()),
            };
            Some((min, max))
        }
        TemporalType::Date32 => {
            let start_days = days_since_epoch(start.date())?;
            let end_days_exclusive = days_since_epoch(end_exclusive.date())?;
            if end_days_exclusive <= start_days {
                return None;
            }
            let max_days = end_days_exclusive - 1;
            Some((
                ScalarValue::Date32(Some(start_days)),
                ScalarValue::Date32(Some(max_days)),
            ))
        }
        TemporalType::Date64 => {
            let start_ms = start.and_utc().timestamp_millis();
            let end_ms_exclusive = end_exclusive.and_utc().timestamp_millis();
            if end_ms_exclusive <= start_ms {
                return None;
            }
            Some((
                ScalarValue::Date64(Some(start_ms)),
                ScalarValue::Date64(Some(end_ms_exclusive - 1)),
            ))
        }
    }
}
