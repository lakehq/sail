use chrono::format::Parsed;
use chrono::{Datelike, Duration, FixedOffset, NaiveDate, NaiveDateTime, Timelike, Weekday};
use datafusion_common::{exec_datafusion_err, Result};

use super::locale::LocaleData;
use super::pattern::{
    DateTimeField, DateTimeFieldSpec, DateTimeFormat, DateTimeItem, FieldStyle, FractionField,
    FractionSpec, ZoneField, ZoneSpec,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedDateTime {
    pub datetime: NaiveDateTime,
    pub offset: Option<FixedOffset>,
    pub timezone: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct ParseState {
    parsed: Parsed,
    era_bc: Option<bool>,
    year_of_era: Option<i32>,
    clock_hour_of_day: Option<u32>,
    hour_of_ampm: Option<u32>,
    clock_hour_of_ampm: Option<u32>,
    week_of_month: Option<u32>,
    aligned_week_of_month: Option<u32>,
    milli_of_day: Option<u32>,
    nano_of_day: Option<u64>,
    timezone: Option<String>,
    /// Flag indicating hour was 24 (midnight of next day)
    hour_24: Option<bool>,
    leap_second: bool,
}

impl DateTimeFormat {
    pub fn parse_datetime_value(&self, value: &str) -> Result<ParsedDateTime> {
        let mut state = ParseState::default();
        let value = value.trim();
        let mut position = parse_items(&self.items, value, 0, self.locale.data(), &mut state)?;
        position = parse_optional_zone_region(value, position);
        if position != value.len() {
            return Err(exec_datafusion_err!(
                "datetime value does not match format at byte offset {position}: {value}"
            ));
        }
        state.resolve()
    }

    pub fn parse_date_value(&self, value: &str) -> Result<NaiveDate> {
        Ok(self.parse_datetime_value(value)?.datetime.date())
    }
}

fn parse_items(
    items: &[DateTimeItem],
    value: &str,
    mut position: usize,
    locale: &LocaleData,
    state: &mut ParseState,
) -> Result<usize> {
    let mut pad_next = false;
    for item in items {
        match item {
            DateTimeItem::Literal(literal) => {
                position = consume_literal(value, position, literal)?;
            }
            DateTimeItem::Field(field_spec) => {
                if pad_next {
                    position = skip_padding(value, position);
                    pad_next = false;
                }
                position = parse_field_spec(field_spec, value, position, locale, state)?;
            }
            DateTimeItem::Fraction(fraction_spec) => {
                if pad_next {
                    position = skip_padding(value, position);
                    pad_next = false;
                }
                position = parse_fraction_spec(fraction_spec, value, position, state)?;
            }
            DateTimeItem::Zone(zone_spec) => {
                if pad_next {
                    position = skip_padding(value, position);
                    pad_next = false;
                }
                position = parse_zone_spec(zone_spec, value, position, state)?;
            }
            DateTimeItem::Optional(items) => {
                let snapshot = state.clone();
                match parse_items(items, value, position, locale, state) {
                    Ok(next) => position = next,
                    Err(_) => {
                        *state = snapshot.clone();
                        // Try partial literal matching for the first item
                        if let Some(DateTimeItem::Literal(literal)) = items.first() {
                            // Try to find a suffix of the literal that matches
                            let mut matched = false;
                            for i in 0..literal.len() {
                                let suffix = &literal[i..];
                                if value[position..].starts_with(suffix) {
                                    // Found matching suffix, parse rest of optional items
                                    let new_position = position + suffix.len();
                                    match parse_items(
                                        &items[1..],
                                        value,
                                        new_position,
                                        locale,
                                        state,
                                    ) {
                                        Ok(next) => {
                                            position = next;
                                            matched = true;
                                            break;
                                        }
                                        Err(_) => {
                                            *state = snapshot.clone();
                                        }
                                    }
                                }
                            }
                            // Also try the original special case for single space
                            if !matched && literal == " " {
                                match parse_items(&items[1..], value, position, locale, state) {
                                    Ok(next) => position = next,
                                    Err(_) => *state = snapshot,
                                }
                            }
                        }
                    }
                }
            }
            DateTimeItem::PadNext { .. } => {
                pad_next = true;
            }
        }
    }
    Ok(position)
}

fn consume_literal(value: &str, position: usize, literal: &str) -> Result<usize> {
    if literal == " " {
        let next = skip_padding(value, position);
        if next > position {
            return Ok(next);
        }
    }
    if value[position..].starts_with(literal) {
        Ok(position + literal.len())
    } else {
        Err(exec_datafusion_err!(
            "expected datetime literal '{literal}' at byte offset {position}"
        ))
    }
}

fn skip_padding(value: &str, mut position: usize) -> usize {
    while value[position..].starts_with(' ') {
        position += 1;
    }
    position
}

fn parse_quarter(
    value: &str,
    position: usize,
    count: usize,
    locale: &LocaleData,
    state: &mut ParseState,
) -> Result<usize> {
    let (next, quarter) = match count {
        1 | 2 => {
            let position = if value[position..].starts_with('Q') {
                position + 1
            } else {
                position
            };
            parse_number(value, position, number_bounds(count, 1))?
        }
        3 => {
            let (_, quarter) = consume_literal(value, position, "Q")
                .and_then(|next| parse_number(value, next, (1, 1)))?;
            (position + 2, quarter)
        }
        4 => parse_text(value, position, &locale.quarters_full)
            .map(|(next, index)| (next, index as i32 + 1))?,
        5 => parse_number(value, position, (1, 1))?,
        _ => unreachable!(),
    };
    if !(1..=4).contains(&quarter) {
        return Err(exec_datafusion_err!("invalid quarter: {quarter}"));
    }
    state.set_quarter(quarter)?;
    Ok(next)
}

fn number_bounds(count: usize, natural_max: usize) -> (usize, usize) {
    if count == 1 {
        (1, natural_max)
    } else {
        (count, count)
    }
}

fn parse_number(value: &str, position: usize, bounds: (usize, usize)) -> Result<(usize, i32)> {
    let (min, max) = bounds;
    let mut end = position;
    let mut digits = 0;
    for ch in value[position..].chars() {
        if !ch.is_ascii_digit() || digits == max {
            break;
        }
        end += ch.len_utf8();
        digits += 1;
    }
    if digits < min {
        return Err(exec_datafusion_err!(
            "expected numeric datetime field at byte offset {position}"
        ));
    }
    value[position..end]
        .parse::<i32>()
        .map(|number| (end, number))
        .map_err(|e| exec_datafusion_err!("invalid numeric datetime field: {e}"))
}

fn parse_signed_number(
    value: &str,
    position: usize,
    bounds: (usize, usize),
) -> Result<(usize, i32)> {
    let negative = value[position..].starts_with('-');
    let start = if negative || value[position..].starts_with('+') {
        position + 1
    } else {
        position
    };
    let (next, number) = parse_number(value, start, bounds)?;
    Ok((next, if negative { -number } else { number }))
}

fn expand_year(year: i32, count: usize) -> i32 {
    if count == 2 {
        2000 + year
    } else {
        year
    }
}

fn parse_fraction(
    value: &str,
    position: usize,
    min_width: usize,
    max_width: usize,
) -> Result<(usize, u32)> {
    let max_width = max_width.min(9);
    let (next, fraction) = parse_number(value, position, (min_width, max_width))?;
    let scale = 9usize.saturating_sub(next - position);
    Ok((next, (fraction as u32) * 10u32.pow(scale as u32)))
}

fn parse_era(
    value: &str,
    position: usize,
    locale: &LocaleData,
    state: &mut ParseState,
) -> Result<usize> {
    if starts_with_ignore_case(&value[position..], "CE") {
        state.era_bc = Some(false);
        return Ok(position + 2);
    }
    let choices = [
        (locale.eras_full[0], true),
        (locale.eras_full[1], false),
        (locale.eras_short[0], true),
        (locale.eras_short[1], false),
        (locale.eras_narrow[0], true),
        (locale.eras_narrow[1], false),
    ];
    choices
        .iter()
        .filter(|(text, _)| starts_with_ignore_case(&value[position..], text))
        .max_by_key(|(text, _)| text.len())
        .map(|(text, is_bc)| {
            state.era_bc = Some(*is_bc);
            position + text.len()
        })
        .ok_or_else(|| exec_datafusion_err!("expected era text at byte offset {position}"))
}

fn parse_month(
    value: &str,
    position: usize,
    count: usize,
    locale: &LocaleData,
    state: &mut ParseState,
) -> Result<usize> {
    if count <= 2 {
        let (next, month) = parse_number(value, position, number_bounds(count, 2))?;
        state.set_month(month)?;
        return Ok(next);
    }
    parse_text(value, position, &locale.months_full)
        .or_else(|_| parse_text(value, position, &locale.months_short))
        .and_then(|(next, index)| {
            state.set_month(index as i32 + 1)?;
            Ok(next)
        })
}

fn parse_weekday_text(
    value: &str,
    position: usize,
    locale: &LocaleData,
    state: &mut ParseState,
) -> Result<usize> {
    parse_text(value, position, &locale.weekdays_full)
        .or_else(|_| parse_text(value, position, &locale.weekdays_short))
        .and_then(|(next, index)| {
            state.set_weekday(weekday_from_monday(index as u32 + 1)?)?;
            Ok(next)
        })
}

fn parse_am_pm(
    value: &str,
    position: usize,
    locale: &LocaleData,
    state: &mut ParseState,
) -> Result<usize> {
    if starts_with_ignore_case(&value[position..], locale.am_pm[0]) {
        state.set_ampm(false)?;
        Ok(position + locale.am_pm[0].len())
    } else if starts_with_ignore_case(&value[position..], locale.am_pm[1]) {
        state.set_ampm(true)?;
        Ok(position + locale.am_pm[1].len())
    } else {
        Err(exec_datafusion_err!(
            "expected AM/PM marker at byte offset {position}"
        ))
    }
}

fn parse_text(value: &str, position: usize, choices: &[&str]) -> Result<(usize, usize)> {
    choices
        .iter()
        .enumerate()
        .filter(|(_, choice)| starts_with_ignore_case(&value[position..], choice))
        .max_by_key(|(_, choice)| choice.len())
        .map(|(index, choice)| (position + choice.len(), index))
        .ok_or_else(|| exec_datafusion_err!("expected datetime text at byte offset {position}"))
}

fn starts_with_ignore_case(value: &str, prefix: &str) -> bool {
    let mut value_chars = value.chars();
    prefix.chars().all(|prefix_char| match value_chars.next() {
        Some(value_char) => {
            value_char.eq_ignore_ascii_case(&prefix_char)
                || value_char.to_lowercase().eq(prefix_char.to_lowercase())
        }
        None => false,
    })
}

fn parse_offset(
    value: &str,
    position: usize,
    count: usize,
    zero_z: bool,
) -> Result<(usize, FixedOffset)> {
    if zero_z && value[position..].starts_with('Z') {
        let offset = FixedOffset::east_opt(0)
            .ok_or_else(|| exec_datafusion_err!("invalid UTC offset seconds: 0"))?;
        return Ok((position + 1, offset));
    }
    let colon = count >= 3;
    let include_seconds = count >= 4;
    let require_minutes = count != 1;
    parse_numeric_offset(value, position, colon, include_seconds, require_minutes)
}

fn parse_localized_offset(value: &str, position: usize) -> Result<(usize, FixedOffset)> {
    if !value[position..].starts_with("GMT") {
        return Err(exec_datafusion_err!(
            "expected localized offset at byte offset {position}"
        ));
    }
    let next = position + 3;
    if next == value.len() || !matches!(value.as_bytes().get(next), Some(b'+') | Some(b'-')) {
        let offset = FixedOffset::east_opt(0)
            .ok_or_else(|| exec_datafusion_err!("invalid UTC offset seconds: 0"))?;
        return Ok((next, offset));
    }
    parse_numeric_offset(value, next, value[next..].contains(':'), false, true)
}

fn parse_numeric_offset(
    value: &str,
    position: usize,
    colon: bool,
    include_seconds: bool,
    require_minutes: bool,
) -> Result<(usize, FixedOffset)> {
    let sign = match value.as_bytes().get(position) {
        Some(b'+') => 1,
        Some(b'-') => -1,
        _ => {
            return Err(exec_datafusion_err!(
                "expected offset sign at byte offset {position}"
            ));
        }
    };
    let (mut next, hours) = parse_number(value, position + 1, (2, 2))?;
    if !require_minutes && !matches!(value.as_bytes().get(next), Some(b'0'..=b'9') | Some(b':')) {
        let total = sign * hours * 3600;
        let offset = FixedOffset::east_opt(total)
            .ok_or_else(|| exec_datafusion_err!("invalid datetime offset seconds: {total}"))?;
        return Ok((next, offset));
    }
    if colon {
        next = consume_literal(value, next, ":")?;
    }
    let (next_after_minutes, minutes) = parse_number(value, next, (2, 2))?;
    next = next_after_minutes;
    let mut seconds = 0;
    if include_seconds {
        if colon {
            next = consume_literal(value, next, ":")?;
        }
        let parsed = parse_number(value, next, (2, 2))?;
        next = parsed.0;
        seconds = parsed.1;
    }
    let total = sign * (hours * 3600 + minutes * 60 + seconds);
    let offset = FixedOffset::east_opt(total)
        .ok_or_else(|| exec_datafusion_err!("invalid datetime offset seconds: {total}"))?;
    Ok((next, offset))
}

fn parse_zone_name(value: &str, position: usize) -> Result<(usize, String)> {
    let mut next = position;
    for ch in value[position..].chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '/' | '_' | '-' | '+') {
            next += ch.len_utf8();
        } else {
            break;
        }
    }
    if next == position {
        Err(exec_datafusion_err!(
            "expected timezone name at byte offset {position}"
        ))
    } else {
        Ok((next, value[position..next].to_string()))
    }
}

fn parse_optional_zone_region(value: &str, position: usize) -> usize {
    if !value[position..].starts_with('[') {
        return position;
    }
    value[position + 1..]
        .find(']')
        .map(|offset| position + offset + 2)
        .unwrap_or(position)
}

fn parse_week_of_month_field(
    value: &str,
    position: usize,
    count: usize,
    state: &mut ParseState,
) -> Result<usize> {
    let (next, week) = parse_number(value, position, number_bounds(count, 1))?;
    if !(1..=5).contains(&week) {
        return Err(exec_datafusion_err!("invalid week-of-month: {week}"));
    }
    state.week_of_month = Some(week as u32);
    Ok(next)
}

fn parse_aligned_week_of_month_field(
    value: &str,
    position: usize,
    count: usize,
    state: &mut ParseState,
) -> Result<usize> {
    let (next, week) = parse_number(value, position, number_bounds(count, 1))?;
    if !(1..=7).contains(&week) {
        return Err(exec_datafusion_err!(
            "invalid aligned week-of-month: {week}"
        ));
    }
    state.aligned_week_of_month = Some(week as u32);
    Ok(next)
}

fn validate_week_of_month(state: &ParseState, date: NaiveDate) -> Result<()> {
    if let Some(expected) = state.week_of_month {
        let actual = (date.day() - 1) / 7 + 1;
        if expected != actual {
            return Err(exec_datafusion_err!(
                "week-of-month {expected} is inconsistent with resolved date {date}"
            ));
        }
    }
    if let Some(expected) = state.aligned_week_of_month {
        let actual = (date.day() - 1) % 7 + 1;
        if expected != actual {
            return Err(exec_datafusion_err!(
                "aligned week-of-month {expected} is inconsistent with resolved date {date}"
            ));
        }
    }
    Ok(())
}

impl ParseState {
    fn resolve(mut self) -> Result<ParsedDateTime> {
        self.resolve_sail_fields()?;
        self.apply_defaults()?;
        let add_day = self.hour_24.unwrap_or(false);
        let mut date = self
            .parsed
            .to_naive_date()
            .map_err(|e| exec_datafusion_err!("invalid parsed date: {e}"))?;
        if add_day {
            date = date.succ_opt().unwrap_or(date);
        }
        validate_week_of_month(&self, date)?;
        let mut datetime = date.and_time(
            self.parsed
                .to_naive_time()
                .map_err(|e| exec_datafusion_err!("invalid parsed time: {e}"))?,
        );
        if self.leap_second {
            if datetime.hour() != 23 || datetime.minute() != 59 {
                return Err(exec_datafusion_err!(
                    "Invalid value for SecondOfMinute (valid leap second must be 23:59:60)"
                ));
            }
            datetime += Duration::seconds(1);
        }
        let date = datetime.date();
        let time = datetime.time();
        let offset = self
            .parsed
            .offset
            .map(|seconds| {
                FixedOffset::east_opt(seconds).ok_or_else(|| {
                    exec_datafusion_err!("invalid datetime offset seconds: {seconds}")
                })
            })
            .transpose()?;
        Ok(ParsedDateTime {
            datetime: date.and_time(time),
            offset,
            timezone: self.timezone,
        })
    }

    fn resolve_sail_fields(&mut self) -> Result<()> {
        if let Some(year) = self.year_of_era {
            let year = if self.era_bc.unwrap_or(false) {
                1 - year
            } else {
                year
            };
            self.set_year(year)?;
        }

        if let Some(nanos) = self.nano_of_day {
            let seconds = nanos / 1_000_000_000;
            let nanosecond = (nanos % 1_000_000_000) as u32;
            if seconds > 86_399 {
                return Err(exec_datafusion_err!("invalid nano-of-day: {nanos}"));
            }
            self.set_time_of_day(seconds as u32, nanosecond)?;
        }
        if let Some(millis) = self.milli_of_day {
            let seconds = millis / 1000;
            let nanosecond = (millis % 1000) * 1_000_000;
            if seconds > 86_399 {
                return Err(exec_datafusion_err!("invalid milli-of-day: {millis}"));
            }
            self.set_time_of_day(seconds, nanosecond)?;
        }

        if let Some(hour) = self.clock_hour_of_day {
            if !(1..=24).contains(&hour) {
                return Err(exec_datafusion_err!(
                    "Invalid value for ClockHourOfDay (valid values 1 - 24): {hour}"
                ));
            }
            if hour == 24 {
                self.hour_24 = Some(true);
            }
            self.set_hour(if hour == 24 { 0 } else { hour as i32 })?;
        }
        if let Some(hour) = self.hour_of_ampm {
            let hour = hour + self.parsed.hour_div_12.unwrap_or(0) * 12;
            self.set_hour(hour as i32)?;
        }
        if let Some(hour) = self.clock_hour_of_ampm {
            self.set_hour12(hour as i32)?;
        }
        Ok(())
    }

    fn apply_defaults(&mut self) -> Result<()> {
        if self.parsed.year.is_none() && self.parsed.isoyear.is_none() {
            self.set_year(1970)?;
        }
        // Convert quarter to month if quarter is set and month is not
        // Chrono's to_naive_date() doesn't handle quarter resolution, so we must convert it manually
        if let Some(quarter) = self.parsed.quarter() {
            if self.parsed.month.is_none() {
                // Quarter 1 -> January (month 1)
                // Quarter 2 -> April (month 4)
                // Quarter 3 -> July (month 7)
                // Quarter 4 -> October (month 10)
                let month = (quarter - 1) * 3 + 1;
                self.set_month(month as i32)?;
            }
        }
        if self.parsed.month.is_none()
            && self.parsed.ordinal.is_none()
            && self.parsed.isoweek.is_none()
            && self.parsed.week_from_mon.is_none()
            && self.parsed.week_from_sun.is_none()
        {
            self.set_month(1)?;
        }
        if self.parsed.day.is_none()
            && self.parsed.ordinal.is_none()
            && self.parsed.isoweek.is_none()
            && self.parsed.week_from_mon.is_none()
            && self.parsed.week_from_sun.is_none()
        {
            let day = self
                .week_of_month
                .map(|week| (week - 1) * 7 + 1)
                .unwrap_or(1);
            self.set_day(day as i32)?;
        }
        if self.parsed.hour_div_12.is_none() && self.parsed.hour_mod_12.is_none() {
            self.set_hour(0)?;
        }
        if self.parsed.minute.is_none() {
            self.set_minute(0)?;
        }
        if self.parsed.second.is_none() {
            self.set_second(0)?;
        }
        Ok(())
    }

    fn set_time_of_day(&mut self, seconds: u32, nanosecond: u32) -> Result<()> {
        self.set_hour((seconds / 3600) as i32)?;
        self.set_minute((seconds % 3600 / 60) as i32)?;
        self.set_second((seconds % 60) as i32)?;
        self.set_nanosecond(nanosecond as i32)
    }

    fn set_year(&mut self, value: i32) -> Result<()> {
        self.parsed
            .set_year(value as i64)
            .map_err(|e| exec_datafusion_err!("invalid parsed year: {e}"))
    }

    fn set_isoyear(&mut self, value: i32) -> Result<()> {
        self.parsed
            .set_isoyear(value as i64)
            .map_err(|e| exec_datafusion_err!("invalid parsed ISO year: {e}"))
    }

    fn set_quarter(&mut self, value: i32) -> Result<()> {
        self.parsed
            .set_quarter(value as i64)
            .map_err(|e| exec_datafusion_err!("invalid parsed quarter: {e}"))
    }

    fn set_month(&mut self, value: i32) -> Result<()> {
        self.parsed
            .set_month(value as i64)
            .map_err(|e| exec_datafusion_err!("invalid parsed month: {e}"))
    }

    fn set_day(&mut self, value: i32) -> Result<()> {
        self.parsed
            .set_day(value as i64)
            .map_err(|e| exec_datafusion_err!("invalid parsed day: {e}"))
    }

    fn set_ordinal(&mut self, value: i32) -> Result<()> {
        self.parsed
            .set_ordinal(value as i64)
            .map_err(|e| exec_datafusion_err!("invalid parsed ordinal: {e}"))
    }

    fn set_isoweek(&mut self, value: i32) -> Result<()> {
        self.parsed
            .set_isoweek(value as i64)
            .map_err(|e| exec_datafusion_err!("invalid parsed ISO week: {e}"))
    }

    fn set_weekday(&mut self, value: Weekday) -> Result<()> {
        self.parsed
            .set_weekday(value)
            .map_err(|e| exec_datafusion_err!("invalid parsed weekday: {e}"))
    }

    fn set_ampm(&mut self, value: bool) -> Result<()> {
        self.parsed
            .set_ampm(value)
            .map_err(|e| exec_datafusion_err!("invalid parsed AM/PM marker: {e}"))
    }

    fn set_hour(&mut self, value: i32) -> Result<()> {
        self.parsed
            .set_hour(value as i64)
            .map_err(|e| exec_datafusion_err!("invalid parsed hour: {e}"))
    }

    fn set_hour12(&mut self, value: i32) -> Result<()> {
        self.parsed
            .set_hour12(value as i64)
            .map_err(|e| exec_datafusion_err!("invalid parsed 12-hour value: {e}"))
    }

    fn set_minute(&mut self, value: i32) -> Result<()> {
        self.parsed
            .set_minute(value as i64)
            .map_err(|e| exec_datafusion_err!("invalid parsed minute: {e}"))
    }

    fn set_second(&mut self, value: i32) -> Result<()> {
        self.parsed
            .set_second(value as i64)
            .map_err(|e| exec_datafusion_err!("invalid parsed second: {e}"))
    }

    fn set_nanosecond(&mut self, value: i32) -> Result<()> {
        self.parsed
            .set_nanosecond(value as i64)
            .map_err(|e| exec_datafusion_err!("invalid parsed nanosecond: {e}"))
    }

    fn set_offset(&mut self, value: FixedOffset) -> Result<()> {
        self.parsed
            .set_offset(value.local_minus_utc() as i64)
            .map_err(|e| exec_datafusion_err!("invalid parsed offset: {e}"))
    }
}

fn weekday_from_monday(value: u32) -> Result<Weekday> {
    match value {
        1 => Ok(Weekday::Mon),
        2 => Ok(Weekday::Tue),
        3 => Ok(Weekday::Wed),
        4 => Ok(Weekday::Thu),
        5 => Ok(Weekday::Fri),
        6 => Ok(Weekday::Sat),
        7 => Ok(Weekday::Sun),
        _ => Err(exec_datafusion_err!("invalid ISO weekday: {value}")),
    }
}

fn parse_field_spec(
    spec: &DateTimeFieldSpec,
    value: &str,
    position: usize,
    locale: &LocaleData,
    state: &mut ParseState,
) -> Result<usize> {
    match spec.kind {
        DateTimeField::Era => parse_era(value, position, locale, state),
        DateTimeField::YearOfEra => {
            parse_signed_number(value, position, number_bounds(spec.width, 10)).map(
                |(next, year)| {
                    state.year_of_era = Some(expand_year(year, spec.width));
                    next
                },
            )
        }
        DateTimeField::ProlepticYear => {
            parse_signed_number(value, position, number_bounds(spec.width, 10)).and_then(
                |(next, year)| {
                    state.set_year(expand_year(year, spec.width))?;
                    Ok(next)
                },
            )
        }
        DateTimeField::WeekBasedYear => {
            parse_number(value, position, number_bounds(spec.width, 10)).and_then(|(next, year)| {
                state.set_isoyear(expand_year(year, spec.width))?;
                Ok(next)
            })
        }
        DateTimeField::QuarterOfYear => parse_quarter(value, position, spec.width, locale, state),
        DateTimeField::MonthOfYear => parse_month(value, position, spec.width, locale, state),
        DateTimeField::DayOfMonth => parse_number(value, position, number_bounds(spec.width, 2))
            .and_then(|(next, day)| {
                state.set_day(day)?;
                Ok(next)
            }),
        DateTimeField::DayOfYear => parse_number(value, position, number_bounds(spec.width, 3))
            .and_then(|(next, ordinal)| {
                state.set_ordinal(ordinal)?;
                Ok(next)
            }),
        DateTimeField::DayOfWeek => match spec.style {
            FieldStyle::Numeric => parse_number(value, position, number_bounds(spec.width, 1))
                .and_then(|(next, weekday)| {
                    state.set_weekday(weekday_from_monday(weekday as u32)?)?;
                    Ok(next)
                }),
            _ => parse_weekday_text(value, position, locale, state),
        },
        DateTimeField::WeekOfWeekBasedYear => {
            parse_number(value, position, number_bounds(spec.width, 2)).and_then(|(next, week)| {
                state.set_isoweek(week)?;
                Ok(next)
            })
        }
        DateTimeField::WeekOfMonth => parse_week_of_month_field(value, position, spec.width, state),
        DateTimeField::AlignedWeekOfMonth => {
            parse_aligned_week_of_month_field(value, position, spec.width, state)
        }
        DateTimeField::AmPmOfDay => parse_am_pm(value, position, locale, state),
        DateTimeField::HourOfDay => parse_number(value, position, number_bounds(spec.width, 2))
            .and_then(|(next, hour)| {
                if hour == 24 {
                    // Hour 24 means midnight of the next day
                    state.hour_24 = Some(true);
                    state.set_hour(0)?;
                } else {
                    state.set_hour(hour)?;
                }
                Ok(next)
            }),
        DateTimeField::ClockHourOfDay => {
            parse_number(value, position, number_bounds(spec.width, 2)).map(|(next, hour)| {
                state.clock_hour_of_day = Some(hour as u32);
                next
            })
        }
        DateTimeField::HourOfAmPm => parse_number(value, position, number_bounds(spec.width, 2))
            .map(|(next, hour)| {
                state.hour_of_ampm = Some(hour as u32);
                next
            }),
        DateTimeField::ClockHourOfAmPm => {
            parse_number(value, position, number_bounds(spec.width, 2)).map(|(next, hour)| {
                state.clock_hour_of_ampm = Some(hour as u32);
                next
            })
        }
        DateTimeField::MinuteOfHour => parse_number(value, position, number_bounds(spec.width, 2))
            .and_then(|(next, minute)| {
                if !(0..60).contains(&minute) {
                    return Err(exec_datafusion_err!(
                        "Invalid value for MinuteOfHour (valid values 0 - 59): {minute}"
                    ));
                }
                state.set_minute(minute)?;
                Ok(next)
            }),
        DateTimeField::SecondOfMinute => {
            parse_number(value, position, number_bounds(spec.width, 2)).and_then(
                |(next, second)| {
                    if !(0..=60).contains(&second) {
                        return Err(exec_datafusion_err!(
                            "Invalid value for SecondOfMinute (valid values 0 - 60): {second}"
                        ));
                    }
                    if second == 60 {
                        state.leap_second = true;
                        state.set_second(59)?;
                    } else {
                        state.set_second(second)?;
                    }
                    Ok(next)
                },
            )
        }
        DateTimeField::MilliOfDay => parse_number(value, position, (1, 8)).map(|(next, millis)| {
            state.milli_of_day = Some(millis as u32);
            next
        }),
        DateTimeField::NanoOfSecond => parse_number(value, position, number_bounds(spec.width, 9))
            .and_then(|(next, nanos)| {
                state.set_nanosecond(nanos)?;
                Ok(next)
            }),
        DateTimeField::NanoOfDay => parse_number(value, position, (1, 14)).map(|(next, nanos)| {
            state.nano_of_day = Some(nanos as u64);
            next
        }),
    }
}

fn parse_fraction_spec(
    spec: &FractionSpec,
    value: &str,
    position: usize,
    state: &mut ParseState,
) -> Result<usize> {
    match spec.field {
        FractionField::NanoOfSecond => {
            parse_fraction(value, position, spec.min_width, spec.max_width).and_then(
                |(next, nanos)| {
                    state.set_nanosecond(nanos as i32)?;
                    Ok(next)
                },
            )
        }
        FractionField::NanoOfDay => parse_number(value, position, (1, 14)).map(|(next, nanos)| {
            state.nano_of_day = Some(nanos as u64);
            next
        }),
        FractionField::MilliOfDay => parse_number(value, position, (1, 8)).map(|(next, millis)| {
            state.milli_of_day = Some(millis as u32);
            next
        }),
    }
}

fn parse_zone_spec(
    spec: &ZoneSpec,
    value: &str,
    position: usize,
    state: &mut ParseState,
) -> Result<usize> {
    match spec.kind {
        ZoneField::Offset => {
            parse_offset(value, position, spec.width, spec.zero_as_z).and_then(|(next, offset)| {
                state.set_offset(offset)?;
                Ok(next)
            })
        }
        ZoneField::LocalizedOffset => {
            parse_localized_offset(value, position).and_then(|(next, offset)| {
                state.set_offset(offset)?;
                Ok(next)
            })
        }
        ZoneField::ZoneId | ZoneField::ZoneName => {
            parse_zone_name(value, position).map(|(next, timezone)| {
                state.timezone = Some(match timezone.as_str() {
                    "GMT" => "UTC".to_string(),
                    _ => timezone,
                });
                next
            })
        }
    }
}
