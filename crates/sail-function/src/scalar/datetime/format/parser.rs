use datafusion_common::{Result, exec_err};

use super::pattern::{
    DateTimeField, DateTimeFieldSpec, DateTimeFormat, DateTimeItem, FieldStyle, FractionField,
    FractionSpec, PatternUse, ResolverStyle, SignStyle, ZoneField, ZoneSpec, ZoneStyle,
};

pub(crate) fn parse_datetime_pattern(
    pattern: &str,
    pattern_use: PatternUse,
) -> Result<DateTimeFormat> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut position = 0;
    let items = parse_items(&chars, &mut position, false, pattern_use)?;
    if position != chars.len() {
        return exec_err!("invalid datetime pattern: unexpected closing optional section");
    }
    Ok(DateTimeFormat {
        items,
        locale: Default::default(),
        resolver_style: ResolverStyle::Strict,
    })
}

fn parse_items(
    chars: &[char],
    position: &mut usize,
    optional: bool,
    pattern_use: PatternUse,
) -> Result<Vec<DateTimeItem>> {
    let mut items = Vec::new();
    let mut literal = String::new();

    while *position < chars.len() {
        let ch = chars[*position];
        match ch {
            '\'' => {
                flush_literal(&mut items, &mut literal);
                literal.push_str(&parse_quoted_literal(chars, position)?);
            }
            '[' => {
                flush_literal(&mut items, &mut literal);
                *position += 1;
                items.push(DateTimeItem::Optional(parse_items(
                    chars,
                    position,
                    true,
                    pattern_use,
                )?));
            }
            ']' => {
                if optional {
                    *position += 1;
                    flush_literal(&mut items, &mut literal);
                    return Ok(items);
                }
                return exec_err!("invalid datetime pattern: unexpected ']'");
            }
            ch if ch.is_ascii_alphabetic() => {
                let symbol = ch;
                let token_start = *position;
                *position += 1;
                while *position < chars.len() && chars[*position] == symbol {
                    *position += 1;
                }
                let count = *position - token_start;

                validate_pattern_field(symbol, count, pattern_use)?;
                flush_literal(&mut items, &mut literal);
                items.push(build_field_item(symbol, count, pattern_use)?);
            }
            _ => {
                literal.push(ch);
                *position += 1;
            }
        }
    }

    if optional {
        return exec_err!("invalid datetime pattern: unclosed optional section");
    }
    flush_literal(&mut items, &mut literal);
    Ok(items)
}

fn parse_quoted_literal(chars: &[char], position: &mut usize) -> Result<String> {
    *position += 1;
    if *position < chars.len() && chars[*position] == '\'' {
        *position += 1;
        return Ok("'".to_string());
    }

    let mut literal = String::new();
    while *position < chars.len() {
        match chars[*position] {
            '\'' if *position + 1 < chars.len() && chars[*position + 1] == '\'' => {
                literal.push('\'');
                *position += 2;
            }
            '\'' => {
                *position += 1;
                return Ok(literal);
            }
            ch => {
                literal.push(ch);
                *position += 1;
            }
        }
    }

    exec_err!("invalid datetime pattern: unclosed quoted literal")
}

fn flush_literal(items: &mut Vec<DateTimeItem>, literal: &mut String) {
    if !literal.is_empty() {
        items.push(DateTimeItem::Literal(std::mem::take(literal)));
    }
}

fn validate_pattern_field(symbol: char, count: usize, pattern_use: PatternUse) -> Result<()> {
    if matches!(symbol, 'Y' | 'W' | 'w' | 'u' | 'e' | 'c') {
        return exec_err!("invalid datetime pattern: week-based pattern letter '{symbol}'");
    }
    if matches!(symbol, 'A' | 'B' | 'n' | 'N' | 'p') {
        return exec_err!("invalid datetime pattern: unsupported pattern letter '{symbol}'");
    }
    if pattern_use == PatternUse::Parsing && matches!(symbol, 'E' | 'F' | 'q' | 'Q') {
        return exec_err!(
            "invalid datetime pattern: pattern letter '{symbol}' is unsupported for parsing"
        );
    }

    match symbol {
        'G' | 'M' | 'L' | 'E' | 'Q' | 'q' if count > 4 => {
            exec_err!("invalid datetime pattern: text field width must be 1 through 4")
        }
        'y' if count > 6 => {
            exec_err!("invalid datetime pattern: 'y' width must be 1 through 6")
        }
        'F' | 'a' if count != 1 => {
            exec_err!("invalid datetime pattern: '{symbol}' requires width 1")
        }
        'd' if count > 2 => {
            exec_err!("invalid datetime pattern: 'd' width must be 1 or 2")
        }
        'D' if count > 3 => {
            exec_err!("invalid datetime pattern: 'D' width must be 1 through 3")
        }
        'H' | 'h' | 'k' | 'K' | 'm' | 's' if count > 2 => {
            exec_err!("invalid datetime pattern: '{symbol}' width must be 1 or 2")
        }
        'S' if count > 9 => {
            exec_err!("invalid datetime pattern: 'S' width must be 1 through 9")
        }
        'V' if count != 2 => exec_err!("invalid datetime pattern: 'V' requires width 2"),
        'O' if count != 1 && count != 4 => {
            exec_err!("invalid datetime pattern: 'O' requires width 1 or 4")
        }
        'X' | 'x' | 'Z' if count > 5 => {
            exec_err!("invalid datetime pattern: offset width must be 1 through 5")
        }
        'z' if count > 4 => {
            exec_err!("invalid datetime pattern: 'z' width must be 1 through 4")
        }
        'D' | 'E' | 'F' | 'G' | 'H' | 'K' | 'L' | 'M' | 'O' | 'Q' | 'S' | 'V' | 'X' | 'Z' | 'a'
        | 'd' | 'h' | 'k' | 'm' | 'q' | 's' | 'x' | 'y' | 'z' => Ok(()),
        _ if symbol.is_ascii_alphabetic() => {
            exec_err!("invalid datetime pattern: unsupported pattern letter '{symbol}'")
        }
        _ => Ok(()),
    }
}

fn build_field_item(symbol: char, count: usize, pattern_use: PatternUse) -> Result<DateTimeItem> {
    match symbol {
        'G' => Ok(DateTimeItem::Field(DateTimeFieldSpec {
            kind: DateTimeField::Era,
            width: count,
            style: match count {
                1..=3 => FieldStyle::TextShort,
                4 => FieldStyle::TextFull,
                _ => FieldStyle::TextNarrow,
            },
            sign_style: SignStyle::Normal,
        })),
        'y' => Ok(DateTimeItem::Field(DateTimeFieldSpec {
            kind: DateTimeField::YearOfEra,
            width: count,
            style: FieldStyle::Numeric,
            sign_style: if count == 2 {
                SignStyle::Never
            } else {
                SignStyle::Normal
            },
        })),
        'u' => Ok(DateTimeItem::Field(DateTimeFieldSpec {
            kind: DateTimeField::ProlepticYear,
            width: count,
            style: FieldStyle::Numeric,
            sign_style: SignStyle::Normal,
        })),
        'Y' => Ok(DateTimeItem::Field(DateTimeFieldSpec {
            kind: DateTimeField::WeekBasedYear,
            width: count,
            style: FieldStyle::Numeric,
            sign_style: if count == 2 {
                SignStyle::Never
            } else {
                SignStyle::Normal
            },
        })),
        'Q' | 'q' => Ok(DateTimeItem::Field(DateTimeFieldSpec {
            kind: DateTimeField::QuarterOfYear,
            width: count,
            style: match count {
                1 | 2 => FieldStyle::Numeric,
                3 => FieldStyle::TextShort,
                4 => FieldStyle::TextFull,
                _ => FieldStyle::TextNarrow,
            },
            sign_style: SignStyle::NotNegative,
        })),
        'M' | 'L' => Ok(DateTimeItem::Field(DateTimeFieldSpec {
            kind: DateTimeField::MonthOfYear,
            width: count,
            style: match count {
                1 => FieldStyle::Numeric,
                2 => FieldStyle::LocalizedNumeric,
                3 => FieldStyle::TextShort,
                4 => FieldStyle::TextFull,
                _ => FieldStyle::TextNarrow,
            },
            sign_style: SignStyle::NotNegative,
        })),
        'd' => Ok(DateTimeItem::Field(DateTimeFieldSpec {
            kind: DateTimeField::DayOfMonth,
            width: count,
            style: if count == 1 {
                FieldStyle::Numeric
            } else {
                FieldStyle::LocalizedNumeric
            },
            sign_style: SignStyle::NotNegative,
        })),
        'D' => Ok(DateTimeItem::Field(DateTimeFieldSpec {
            kind: DateTimeField::DayOfYear,
            width: count,
            style: FieldStyle::LocalizedNumeric,
            sign_style: SignStyle::NotNegative,
        })),
        'E' => Ok(DateTimeItem::Field(DateTimeFieldSpec {
            kind: DateTimeField::DayOfWeek,
            width: count,
            style: match count {
                1..=3 => FieldStyle::TextShort,
                4 => FieldStyle::TextFull,
                _ => FieldStyle::TextNarrow,
            },
            sign_style: SignStyle::Normal,
        })),
        'e' | 'c' => Ok(DateTimeItem::Field(DateTimeFieldSpec {
            kind: DateTimeField::DayOfWeek,
            width: count,
            style: match count {
                1 | 2 => FieldStyle::Numeric,
                3 => FieldStyle::TextShort,
                4 => FieldStyle::TextFull,
                _ => FieldStyle::TextNarrow,
            },
            sign_style: SignStyle::NotNegative,
        })),
        'w' => Ok(DateTimeItem::Field(DateTimeFieldSpec {
            kind: DateTimeField::WeekOfWeekBasedYear,
            width: count,
            style: if count == 1 {
                FieldStyle::Numeric
            } else {
                FieldStyle::LocalizedNumeric
            },
            sign_style: SignStyle::NotNegative,
        })),
        'W' => Ok(DateTimeItem::Field(DateTimeFieldSpec {
            kind: DateTimeField::WeekOfMonth,
            width: count,
            style: FieldStyle::Numeric,
            sign_style: SignStyle::NotNegative,
        })),
        'F' => Ok(DateTimeItem::Field(DateTimeFieldSpec {
            kind: DateTimeField::AlignedWeekOfMonth,
            width: count,
            style: FieldStyle::Numeric,
            sign_style: SignStyle::NotNegative,
        })),
        'a' => Ok(DateTimeItem::Field(DateTimeFieldSpec {
            kind: DateTimeField::AmPmOfDay,
            width: count,
            style: FieldStyle::TextShort,
            sign_style: SignStyle::Normal,
        })),
        'H' => Ok(DateTimeItem::Field(DateTimeFieldSpec {
            kind: DateTimeField::HourOfDay,
            width: count,
            style: if count == 1 {
                FieldStyle::Numeric
            } else {
                FieldStyle::LocalizedNumeric
            },
            sign_style: SignStyle::NotNegative,
        })),
        'k' => Ok(DateTimeItem::Field(DateTimeFieldSpec {
            kind: DateTimeField::ClockHourOfDay,
            width: count,
            style: if count == 1 {
                FieldStyle::Numeric
            } else {
                FieldStyle::LocalizedNumeric
            },
            sign_style: SignStyle::NotNegative,
        })),
        'K' => Ok(DateTimeItem::Field(DateTimeFieldSpec {
            kind: DateTimeField::HourOfAmPm,
            width: count,
            style: if count == 1 {
                FieldStyle::Numeric
            } else {
                FieldStyle::LocalizedNumeric
            },
            sign_style: SignStyle::NotNegative,
        })),
        'h' => Ok(DateTimeItem::Field(DateTimeFieldSpec {
            kind: DateTimeField::ClockHourOfAmPm,
            width: count,
            style: if count == 1 {
                FieldStyle::Numeric
            } else {
                FieldStyle::LocalizedNumeric
            },
            sign_style: SignStyle::NotNegative,
        })),
        'm' => Ok(DateTimeItem::Field(DateTimeFieldSpec {
            kind: DateTimeField::MinuteOfHour,
            width: count,
            style: if count == 1 {
                FieldStyle::Numeric
            } else {
                FieldStyle::LocalizedNumeric
            },
            sign_style: SignStyle::NotNegative,
        })),
        's' => Ok(DateTimeItem::Field(DateTimeFieldSpec {
            kind: DateTimeField::SecondOfMinute,
            width: count,
            style: if count == 1 {
                FieldStyle::Numeric
            } else {
                FieldStyle::LocalizedNumeric
            },
            sign_style: SignStyle::NotNegative,
        })),
        'S' => Ok(DateTimeItem::Fraction(FractionSpec {
            field: FractionField::NanoOfSecond,
            min_width: if pattern_use == PatternUse::Parsing {
                1
            } else {
                count
            },
            max_width: count.min(9),
            decimal_point: false,
        })),
        'A' => Ok(DateTimeItem::Field(DateTimeFieldSpec {
            kind: DateTimeField::MilliOfDay,
            width: count,
            style: FieldStyle::LocalizedNumeric,
            sign_style: SignStyle::NotNegative,
        })),
        'n' => Ok(DateTimeItem::Field(DateTimeFieldSpec {
            kind: DateTimeField::NanoOfSecond,
            width: count,
            style: FieldStyle::LocalizedNumeric,
            sign_style: SignStyle::NotNegative,
        })),
        'N' => Ok(DateTimeItem::Field(DateTimeFieldSpec {
            kind: DateTimeField::NanoOfDay,
            width: count,
            style: FieldStyle::LocalizedNumeric,
            sign_style: SignStyle::NotNegative,
        })),
        'X' | 'x' => Ok(DateTimeItem::Zone(ZoneSpec {
            kind: ZoneField::IsoOffset,
            width: count,
            zero_as_z: symbol == 'X',
            style: match count {
                1..=2 => ZoneStyle::Short,
                3..=4 => ZoneStyle::Full,
                _ => ZoneStyle::Full,
            },
        })),
        'Z' => Ok(DateTimeItem::Zone(ZoneSpec {
            kind: if count == 4 {
                ZoneField::LocalizedOffset
            } else if count == 5 {
                ZoneField::IsoOffset
            } else {
                ZoneField::Rfc822Offset
            },
            width: count,
            zero_as_z: count == 5,
            style: match count {
                1..=3 => ZoneStyle::Short,
                4 => ZoneStyle::Full,
                _ => ZoneStyle::Full,
            },
        })),
        'O' => Ok(DateTimeItem::Zone(ZoneSpec {
            kind: ZoneField::LocalizedOffset,
            width: count,
            zero_as_z: false,
            style: if count == 1 {
                ZoneStyle::Short
            } else {
                ZoneStyle::Full
            },
        })),
        'V' => Ok(DateTimeItem::Zone(ZoneSpec {
            kind: ZoneField::ZoneId,
            width: count,
            zero_as_z: false,
            style: ZoneStyle::Id,
        })),
        'z' => Ok(DateTimeItem::Zone(ZoneSpec {
            kind: ZoneField::ZoneName,
            width: count,
            zero_as_z: false,
            style: if count <= 3 {
                ZoneStyle::Short
            } else {
                ZoneStyle::Full
            },
        })),
        _ => exec_err!("unsupported datetime pattern symbol: '{}'", symbol),
    }
}
