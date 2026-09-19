/// The parsed pieces of a timestamp string: nine integer segments
/// (year, month, day, hour, minute, second, microsecond, and two spare zone
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
            if segment != 6 || current_digits < 6 {
                current_value = current_value * 10 + parsed;
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

    while fractional_digits < 6 {
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
