use datafusion_common::{exec_err, Result};

/// Spark's default pattern when an empty string is passed to format_number.
const DEFAULT_PATTERN: &str = "#,###,###,###,###,###,##0";

/// Parsed representation of a DecimalFormat pattern.
#[derive(Debug, Clone)]
pub struct ParsedPattern {
    pub grouping_size: usize,
    pub min_integer_digits: usize,
    pub decimal_digits: usize,
    pub min_decimal_digits: usize,
}

/// Validates that a pattern contains only valid DecimalFormat characters.
fn validate_pattern(pattern: &str) -> Result<()> {
    let dot_count = pattern.chars().filter(|c| *c == '.').count();
    if dot_count > 1 {
        return exec_err!(
            "`format_number` pattern contains multiple decimal separators: '{pattern}'"
        );
    }
    for ch in pattern.chars() {
        if !matches!(ch, '#' | '0' | ',' | '.') {
            return exec_err!(
                "`format_number` pattern contains invalid character '{ch}': '{pattern}'"
            );
        }
    }
    Ok(())
}

/// Parses a DecimalFormat pattern string into its components.
pub fn parse_pattern(pattern: &str) -> Result<ParsedPattern> {
    let pattern = if pattern.is_empty() {
        DEFAULT_PATTERN
    } else {
        pattern
    };
    validate_pattern(pattern)?;

    let (integer_part, frac_part) = match pattern.find('.') {
        Some(pos) => (&pattern[..pos], Some(&pattern[pos + 1..])),
        None => (pattern, None),
    };

    let grouping_size = match integer_part.rfind(',') {
        Some(last_comma) => integer_part.len() - last_comma - 1,
        None => 0,
    };

    let min_integer_digits = integer_part.chars().filter(|c| *c == '0').count();

    let decimal_digits =
        frac_part.map_or(0, |f| f.chars().filter(|c| *c == '#' || *c == '0').count());
    let min_decimal_digits = frac_part.map_or(0, |f| f.chars().filter(|c| *c == '0').count());

    Ok(ParsedPattern {
        grouping_size,
        min_integer_digits,
        decimal_digits,
        min_decimal_digits,
    })
}

/// Formats a number using a parsed DecimalFormat pattern.
pub fn format_with_parsed_pattern(value: f64, parsed: &ParsedPattern) -> String {
    let formatted = format!("{:.prec$}", value, prec = parsed.decimal_digits);
    let trimmed =
        trim_optional_decimals(&formatted, parsed.decimal_digits, parsed.min_decimal_digits);
    let padded = pad_integer_digits(&trimmed, parsed.min_integer_digits);
    if parsed.grouping_size > 0 {
        insert_grouping(&padded, parsed.grouping_size)
    } else {
        padded
    }
}

/// Trims trailing optional decimal zeros (those beyond min_decimal_digits).
fn trim_optional_decimals(
    formatted: &str,
    decimal_digits: usize,
    min_decimal_digits: usize,
) -> String {
    if decimal_digits <= min_decimal_digits {
        return formatted.to_string();
    }

    let (int_part, dec_part) = match formatted.split_once('.') {
        Some((i, d)) => (i, d),
        None => return formatted.to_string(),
    };

    let mut dec_chars: Vec<char> = dec_part.chars().collect();
    while dec_chars.len() > min_decimal_digits && dec_chars.last() == Some(&'0') {
        dec_chars.pop();
    }

    if dec_chars.is_empty() {
        int_part.to_string()
    } else {
        format!("{}.{}", int_part, dec_chars.iter().collect::<String>())
    }
}

/// Pads the integer part with leading zeros to meet the minimum digit count.
fn pad_integer_digits(s: &str, min_digits: usize) -> String {
    if min_digits <= 1 {
        return s.to_string();
    }

    let (integer_part, rest) = match s.find('.') {
        Some(pos) => (&s[..pos], Some(&s[pos..])),
        None => (s, None),
    };

    let negative = integer_part.starts_with('-');
    let digits = if negative {
        &integer_part[1..]
    } else {
        integer_part
    };

    if digits.len() >= min_digits {
        return s.to_string();
    }

    let padding = min_digits - digits.len();
    let mut result = String::with_capacity(s.len() + padding);
    if negative {
        result.push('-');
    }
    for _ in 0..padding {
        result.push('0');
    }
    result.push_str(digits);
    if let Some(r) = rest {
        result.push_str(r);
    }
    result
}

/// Inserts grouping separators into the integer part of a formatted number string.
pub fn insert_grouping(s: &str, grouping_size: usize) -> String {
    if grouping_size == 0 {
        return s.to_string();
    }

    let (integer_part, decimal_part) = match s.find('.') {
        Some(pos) => (&s[..pos], Some(&s[pos..])),
        None => (s, None),
    };

    let negative = integer_part.starts_with('-');
    let digits = if negative {
        &integer_part[1..]
    } else {
        integer_part
    };

    let mut result = String::with_capacity(s.len() + digits.len() / grouping_size);
    if negative {
        result.push('-');
    }

    let len = digits.len();
    for (i, ch) in digits.chars().enumerate() {
        if i > 0 && (len - i) % grouping_size == 0 {
            result.push(',');
        }
        result.push(ch);
    }

    if let Some(dec) = decimal_part {
        result.push_str(dec);
    }

    result
}
