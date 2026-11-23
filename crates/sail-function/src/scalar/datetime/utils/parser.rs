// SPDX-License-Identifier: Apache-2.0

/// The parsed pieces of a timestamp string: nine integer segments
/// (year, month, day, hour, minute, second, fractional second, and two spare zone
/// segments), an optional timezone string, and whether only a time was given.
pub(crate) struct ParsedTimestamp {
    pub(crate) segments: [i64; 9],
    pub(crate) timezone: Option<String>,
    pub(crate) just_time: bool,
}

fn is_valid_timestamp_digits(segment: usize, digits: usize) -> bool {
    const MAX_DIGITS_YEAR: usize = 6;
    segment == 6
        || (segment == 0 && (4..=MAX_DIGITS_YEAR).contains(&digits))
        || (segment == 7 && digits <= 2)
        || (segment != 0 && segment != 6 && segment != 7 && (1..=2).contains(&digits))
}

fn is_whitespace_or_iso_control(value: u8) -> bool {
    value <= b' ' || value == 0x7f
}

/// Splits a Spark timestamp string into numeric and timezone components.
pub(crate) fn parse_timestamp_string(value: &str) -> Option<ParsedTimestamp> {
    parse_timestamp_components(value, 6)
}

fn parse_timestamp_components(value: &str, fraction_precision: usize) -> Option<ParsedTimestamp> {
    let bytes = value.as_bytes();
    let mut segments: [i64; 9] = [1, 1, 1, 0, 0, 0, 0, 0, 0];
    let mut segment = 0usize;
    let mut current_value = 0i64;
    let mut current_digits = 0usize;

    let mut start = 0;
    while start < bytes.len() && is_whitespace_or_iso_control(bytes[start]) {
        start += 1;
    }
    let mut end = bytes.len();
    while end > start && is_whitespace_or_iso_control(bytes[end - 1]) {
        end -= 1;
    }
    if start == end {
        return None;
    }

    let mut fractional_digits = 0usize;
    let mut just_time = false;
    let mut timezone = None;
    let mut year_sign = None;
    let mut position = start;
    if bytes[position] == b'-' || bytes[position] == b'+' {
        year_sign = Some(if bytes[position] == b'-' { -1 } else { 1 });
        position += 1;
    }

    while position < end {
        let byte = bytes[position];
        if byte.is_ascii_digit() {
            let parsed = i64::from(byte - b'0');
            if segment == 6 {
                fractional_digits += 1;
            }
            if segment != 6 || current_digits < fraction_precision {
                current_value = current_value.checked_mul(10)?.checked_add(parsed)?;
            }
            current_digits += 1;
        } else if position == 0 && byte == b'T' {
            just_time = true;
            segment += 3;
        } else if segment < 2 {
            if byte == b'-' {
                if !is_valid_timestamp_digits(segment, current_digits) {
                    return None;
                }
                segments[segment] = current_value;
                current_value = 0;
                current_digits = 0;
                segment += 1;
            } else if segment == 0 && byte == b':' && year_sign.is_none() {
                just_time = true;
                if !is_valid_timestamp_digits(3, current_digits) {
                    return None;
                }
                segments[3] = current_value;
                current_value = 0;
                current_digits = 0;
                segment = 4;
            } else {
                return None;
            }
        } else if segment == 2 {
            if byte == b' ' || byte == b'T' {
                if !is_valid_timestamp_digits(segment, current_digits) {
                    return None;
                }
                segments[segment] = current_value;
                current_value = 0;
                current_digits = 0;
                segment += 1;
            } else {
                return None;
            }
        } else if segment == 3 || segment == 4 {
            if byte == b':' {
                if !is_valid_timestamp_digits(segment, current_digits) {
                    return None;
                }
                segments[segment] = current_value;
                current_value = 0;
                current_digits = 0;
                segment += 1;
            } else {
                return None;
            }
        } else if segment == 5 || segment == 6 {
            if byte == b'.' && segment == 5 {
                if !is_valid_timestamp_digits(segment, current_digits) {
                    return None;
                }
                segments[segment] = current_value;
                current_value = 0;
                current_digits = 0;
                segment += 1;
            } else {
                if !is_valid_timestamp_digits(segment, current_digits) {
                    return None;
                }
                segments[segment] = current_value;
                current_value = 0;
                current_digits = 0;
                segment += 1;
                timezone = Some(String::from_utf8_lossy(&bytes[position..end]).into_owned());
                position = end - 1;
            }
            if segment == 6 && byte != b'.' {
                segment += 1;
            }
        } else if segment < segments.len() && (byte == b':' || byte == b' ') {
            if !is_valid_timestamp_digits(segment, current_digits) {
                return None;
            }
            segments[segment] = current_value;
            current_value = 0;
            current_digits = 0;
            segment += 1;
        } else {
            return None;
        }
        position += 1;
    }

    if segment >= segments.len() || !is_valid_timestamp_digits(segment, current_digits) {
        return None;
    }
    segments[segment] = current_value;

    while fractional_digits < fraction_precision {
        segments[6] *= 10;
        fractional_digits += 1;
    }

    segments[0] *= year_sign.unwrap_or(1);
    Some(ParsedTimestamp {
        segments,
        timezone,
        just_time,
    })
}

/// Parses a Spark TIME cast as nanoseconds since midnight.
pub fn parse_time_nanoseconds(value: &str) -> Option<i64> {
    let value = value.trim_end_matches(' ');
    let (clock, pm) = match value.split_at_checked(value.len().saturating_sub(2)) {
        Some((clock, suffix)) if suffix.eq_ignore_ascii_case("AM") => (clock, Some(false)),
        Some((clock, suffix)) if suffix.eq_ignore_ascii_case("PM") => (clock, Some(true)),
        _ => (value, None),
    };
    let parsed = parse_timestamp_components(clock, 9)?;
    let [_, _, _, hour, minute, second, nanos, _, _] = parsed.segments;
    if !parsed.just_time || parsed.timezone.is_some() || minute >= 60 || second >= 60 {
        return None;
    }
    let hour = match pm {
        Some(pm) if (1..=12).contains(&hour) => hour % 12 + if pm { 12 } else { 0 },
        None if hour < 24 => hour,
        _ => return None,
    };
    let seconds = (hour * 60 + minute) * 60 + second;
    Some(seconds * 1_000_000_000 + nanos)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leading_whitespace_before_time_only_t_is_invalid() {
        assert!(parse_timestamp_string("T12:34:56").is_some());
        assert!(parse_timestamp_string(" T12:34:56 ").is_none());
        assert!(parse_timestamp_string(" 12:34:56 ").is_some());
    }
}
