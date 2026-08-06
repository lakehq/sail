use chrono::{Datelike, FixedOffset, NaiveDateTime, Timelike};
use datafusion_common::Result;

use super::locale::LocaleData;
use super::pattern::{
    DateTimeField, DateTimeFieldSpec, DateTimeFormat, DateTimeItem, FieldStyle, FractionField,
    FractionSpec, ZoneField, ZoneSpec,
};

#[derive(Debug, Clone, Copy)]
pub struct TimeZoneDisplay<'a> {
    pub offset: FixedOffset,
    pub name: Option<&'a str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampKind {
    Normal,
    LocalTimestamp,
    NonlocalTimestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimePrecision {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

#[derive(Debug, Clone, Copy)]
pub struct DateTimeFormatInput<'a> {
    pub datetime: NaiveDateTime,
    pub timezone: Option<TimeZoneDisplay<'a>>,
    pub zone_id: Option<&'a str>,
    pub timestamp_kind: TimestampKind,
    pub precision: TimePrecision,
}

impl DateTimeFormat {
    pub fn format(&self, input: DateTimeFormatInput<'_>) -> Result<String> {
        let mut output = String::new();
        format_items(&self.items, input, self.locale.data(), &mut output)?;
        Ok(output)
    }
}

fn format_items(
    items: &[DateTimeItem],
    input: DateTimeFormatInput<'_>,
    locale: &LocaleData,
    output: &mut String,
) -> Result<()> {
    for item in items {
        match item {
            DateTimeItem::Literal(value) => output.push_str(value),
            DateTimeItem::Field(field_spec) => {
                format_field_spec(field_spec, input, locale, output);
            }
            DateTimeItem::Fraction(fraction_spec) => {
                format_fraction_spec(fraction_spec, input, output);
            }
            DateTimeItem::Zone(zone_spec) => {
                format_zone_spec(zone_spec, input, output);
            }
            DateTimeItem::Optional(items) => {
                format_items(items, input, locale, output)?;
            }
        }
    }
    Ok(())
}

fn format_year(year: i32, count: usize, output: &mut String) {
    if count == 2 {
        push_exact_padded((year % 100).abs() as i64, 2, output);
    } else {
        push_padded(year as i64, if count == 1 { 0 } else { count }, output);
    }
}

fn format_month(month: u32, count: usize, locale: &LocaleData, output: &mut String) {
    match count {
        1 => output.push_str(&month.to_string()),
        2 => push_exact_padded(month as i64, 2, output),
        3 => output.push_str(locale.months_short[(month - 1) as usize]),
        4 => output.push_str(locale.months_full[(month - 1) as usize]),
        _ => output.push_str(first_char(locale.months_full[(month - 1) as usize])),
    }
}

fn format_weekday_text(weekday: usize, count: usize, locale: &LocaleData, output: &mut String) {
    match count {
        4 => output.push_str(locale.weekdays_full[weekday]),
        5.. => output.push_str(first_char(locale.weekdays_full[weekday])),
        _ => output.push_str(locale.weekdays_short[weekday]),
    }
}

fn first_char(value: &str) -> &str {
    value
        .char_indices()
        .nth(1)
        .map(|(index, _)| &value[..index])
        .unwrap_or(value)
}

fn format_quarter(quarter: u32, count: usize, locale: &LocaleData, output: &mut String) {
    match count {
        1 => output.push_str(&quarter.to_string()),
        2 => push_exact_padded(quarter as i64, 2, output),
        3 => output.push_str(locale.quarters_short[(quarter - 1) as usize]),
        4 => output.push_str(locale.quarters_full[(quarter - 1) as usize]),
        5 => output.push_str(&quarter.to_string()), // narrow style
        _ => output.push_str(&quarter.to_string()),
    }
}

fn format_fraction(nanos: u32, count: usize, output: &mut String) {
    let digits = format!("{nanos:09}");
    output.push_str(&digits[..count.min(9)]);
}

fn format_era(is_bc: bool, count: usize, locale: &LocaleData, output: &mut String) {
    let index = usize::from(!is_bc);
    match count {
        1..=3 => output.push_str(locale.eras_short[index]),
        4 => output.push_str(locale.eras_full[index]),
        5 => output.push_str(locale.eras_narrow[index]),
        _ => output.push_str(locale.eras_short[index]),
    }
}

fn format_offset(
    timezone: Option<TimeZoneDisplay<'_>>,
    count: usize,
    zero_as_z: bool,
    output: &mut String,
) {
    let seconds = timezone.map(|tz| tz.offset.local_minus_utc()).unwrap_or(0);
    if zero_as_z && seconds == 0 {
        output.push('Z');
        return;
    }
    let minutes = seconds.abs() % 3600 / 60;
    let offset_seconds = seconds.abs() % 60;
    match count {
        1 => push_offset(seconds, false, minutes != 0, false, output),
        2 => push_offset(seconds, false, true, false, output),
        3 => push_offset(seconds, true, true, false, output),
        4 => push_offset(seconds, false, true, offset_seconds != 0, output),
        5 => push_offset(seconds, true, true, offset_seconds != 0, output),
        _ => unreachable!("offset pattern width is validated when the pattern is parsed"),
    }
}

fn format_localized_offset(
    timezone: Option<TimeZoneDisplay<'_>>,
    count: usize,
    output: &mut String,
) {
    let seconds = timezone.map(|tz| tz.offset.local_minus_utc()).unwrap_or(0);
    output.push_str("GMT");
    if seconds == 0 {
        return;
    }
    push_offset(
        seconds,
        count == 4,
        true,
        count == 4 && seconds % 60 != 0,
        output,
    );
}

fn push_offset(
    seconds: i32,
    colon: bool,
    include_minutes: bool,
    include_seconds: bool,
    output: &mut String,
) {
    let sign = if seconds < 0 { '-' } else { '+' };
    let seconds = seconds.abs();
    let hours = seconds / 3600;
    let minutes = seconds % 3600 / 60;
    let seconds = seconds % 60;
    output.push(sign);
    push_exact_padded(hours as i64, 2, output);
    if include_minutes {
        if colon {
            output.push(':');
        }
        push_exact_padded(minutes as i64, 2, output);
    }
    if include_seconds {
        if colon {
            output.push(':');
        }
        push_exact_padded(seconds as i64, 2, output);
    }
}

fn push_padded(value: i64, width: usize, output: &mut String) {
    if width == 0 {
        output.push_str(&value.to_string());
    } else {
        push_exact_padded(value, width, output);
    }
}

fn push_exact_padded(value: i64, width: usize, output: &mut String) {
    if value < 0 {
        output.push('-');
        output.push_str(&format!("{:0width$}", value.abs(), width = width));
    } else {
        output.push_str(&format!("{value:0width$}"));
    }
}

// New formatting functions for typed field specifications

fn format_field_spec(
    spec: &DateTimeFieldSpec,
    input: DateTimeFormatInput<'_>,
    locale: &LocaleData,
    output: &mut String,
) {
    let datetime = input.datetime;
    match spec.kind {
        DateTimeField::Era => {
            format_era(datetime.year() <= 0, spec.width, locale, output);
        }
        DateTimeField::ProlepticYear => {
            format_year(datetime.year(), spec.width, output);
        }
        DateTimeField::YearOfEra => {
            let year = datetime.year();
            let year_of_era = if year <= 0 { 1 - year } else { year };
            format_year(year_of_era, spec.width, output);
        }
        DateTimeField::WeekBasedYear => {
            format_year(datetime.iso_week().year(), spec.width, output);
        }
        DateTimeField::QuarterOfYear => {
            format_quarter((datetime.month0() / 3) + 1, spec.width, locale, output);
        }
        DateTimeField::MonthOfYear => {
            format_month(datetime.month(), spec.width, locale, output);
        }
        DateTimeField::DayOfMonth => {
            push_padded(
                datetime.day() as i64,
                if spec.width == 1 { 0 } else { spec.width },
                output,
            );
        }
        DateTimeField::DayOfYear => {
            push_padded(datetime.ordinal() as i64, spec.width, output);
        }
        DateTimeField::DayOfWeek => match spec.style {
            FieldStyle::Numeric | FieldStyle::LocalizedNumeric => {
                push_padded(
                    datetime.weekday().number_from_monday() as i64,
                    if spec.width == 1 { 0 } else { spec.width },
                    output,
                );
            }
            _ => {
                format_weekday_text(
                    datetime.weekday().num_days_from_monday() as usize,
                    spec.width,
                    locale,
                    output,
                );
            }
        },
        DateTimeField::WeekOfWeekBasedYear => {
            push_padded(
                datetime.iso_week().week() as i64,
                if spec.width == 1 { 0 } else { spec.width },
                output,
            );
        }
        DateTimeField::WeekOfMonth => {
            push_padded(
                ((datetime.day() - 1) / 7 + 1) as i64,
                if spec.width == 1 { 0 } else { spec.width },
                output,
            );
        }
        DateTimeField::AlignedWeekOfMonth => {
            push_padded(
                ((datetime.day() - 1) % 7 + 1) as i64,
                if spec.width == 1 { 0 } else { spec.width },
                output,
            );
        }
        DateTimeField::AmPmOfDay => {
            output.push_str(locale.am_pm[usize::from(datetime.hour() >= 12)]);
        }
        DateTimeField::HourOfDay => {
            push_padded(
                datetime.hour() as i64,
                if spec.width == 1 { 0 } else { spec.width },
                output,
            );
        }
        DateTimeField::ClockHourOfDay => {
            let hour = if datetime.hour() == 0 {
                24
            } else {
                datetime.hour()
            };
            push_padded(
                hour as i64,
                if spec.width == 1 { 0 } else { spec.width },
                output,
            );
        }
        DateTimeField::HourOfAmPm => {
            push_padded(
                (datetime.hour() % 12) as i64,
                if spec.width == 1 { 0 } else { spec.width },
                output,
            );
        }
        DateTimeField::ClockHourOfAmPm => {
            let hour = datetime.hour() % 12;
            push_padded(
                if hour == 0 { 12 } else { hour } as i64,
                if spec.width == 1 { 0 } else { spec.width },
                output,
            );
        }
        DateTimeField::MinuteOfHour => {
            push_padded(
                datetime.minute() as i64,
                if spec.width == 1 { 0 } else { spec.width },
                output,
            );
        }
        DateTimeField::SecondOfMinute => {
            push_padded(
                datetime.second() as i64,
                if spec.width == 1 { 0 } else { spec.width },
                output,
            );
        }
        DateTimeField::MilliOfDay => {
            let millis = ((((datetime.hour() * 60) + datetime.minute()) * 60 + datetime.second())
                * 1000)
                + datetime.nanosecond() / 1_000_000;
            push_padded(
                millis as i64,
                if spec.width == 1 { 0 } else { spec.width },
                output,
            );
        }
        DateTimeField::NanoOfSecond => {
            push_padded(
                datetime.nanosecond() as i64,
                if spec.width == 1 { 0 } else { spec.width },
                output,
            );
        }
        DateTimeField::NanoOfDay => {
            let nanos = (((datetime.hour() as u64 * 60 + datetime.minute() as u64) * 60
                + datetime.second() as u64)
                * 1_000_000_000)
                + datetime.nanosecond() as u64;
            push_padded(
                nanos as i64,
                if spec.width == 1 { 0 } else { spec.width },
                output,
            );
        }
    }
}

fn format_fraction_spec(spec: &FractionSpec, input: DateTimeFormatInput<'_>, output: &mut String) {
    let datetime = input.datetime;
    match spec.field {
        FractionField::NanoOfSecond => {
            format_fraction(datetime.nanosecond(), spec.min_width, output)
        }
        FractionField::NanoOfDay => {
            let nanos = ((datetime.hour() as u64 * 60 + datetime.minute() as u64) * 60
                + datetime.second() as u64)
                * 1_000_000_000
                + datetime.nanosecond() as u64;
            push_padded(
                nanos as i64,
                if spec.min_width == 1 {
                    0
                } else {
                    spec.min_width
                },
                output,
            );
        }
        FractionField::MilliOfDay => {
            let millis = ((((datetime.hour() * 60) + datetime.minute()) * 60 + datetime.second())
                * 1000)
                + datetime.nanosecond() / 1_000_000;
            push_padded(
                millis as i64,
                if spec.min_width == 1 {
                    0
                } else {
                    spec.min_width
                },
                output,
            );
        }
    }
}

fn format_zone_spec(spec: &ZoneSpec, input: DateTimeFormatInput<'_>, output: &mut String) {
    match spec.kind {
        ZoneField::IsoOffset => {
            format_offset(input.timezone, spec.width, spec.zero_as_z, output);
        }
        ZoneField::Rfc822Offset => {
            let seconds = input
                .timezone
                .map(|tz| tz.offset.local_minus_utc())
                .unwrap_or(0);
            push_offset(seconds, false, true, false, output);
        }
        ZoneField::LocalizedOffset => {
            format_localized_offset(input.timezone, spec.width, output);
        }
        ZoneField::ZoneId => {
            output.push_str(
                input
                    .zone_id
                    .or_else(|| input.timezone.and_then(|tz| tz.name))
                    .unwrap_or("UTC"),
            );
        }
        ZoneField::ZoneName => {
            output.push_str(input.timezone.and_then(|tz| tz.name).unwrap_or("UTC"));
        }
    }
}
