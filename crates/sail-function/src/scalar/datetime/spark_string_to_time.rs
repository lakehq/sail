use std::sync::Arc;

use datafusion::arrow::array::Time64MicrosecondArray;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use sail_common_datafusion::utils::items::ItemTaker;

use crate::scalar::datetime::utils::string_array_iter;

/// Spark's `CAST(string AS TIME)`: `SparkDateTimeUtils.stringToTime`, ported for the
/// TIME-only subset of `parseTimestampString` (no date, no time zone). Accepts an
/// optional leading `T`, variable-width `H`/`M`/`S` fields, a fractional-second part of
/// any width (truncated beyond 6 digits), and a trailing `AM`/`PM` suffix (12-hour
/// range); rejects anything with a date part, a bare number, or an out-of-range field.
/// Always returns `Time64(Microsecond)`; a further `CAST ... AS TIME(p)` truncates to
/// the target precision the way `Time -> Time` already does.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkStringToTime {
    signature: Signature,
    is_try: bool,
}

impl SparkStringToTime {
    /// When `is_try` is true, returns NULL on invalid input (for `try_cast`, or `CAST`
    /// under ANSI off). When `is_try` is false, throws `CAST_INVALID_INPUT`.
    pub fn new(is_try: bool) -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View],
                Volatility::Immutable,
            ),
            is_try,
        }
    }

    pub fn is_try(&self) -> bool {
        self.is_try
    }

    fn string_to_time_micros(value: &str, is_try: bool) -> Result<Option<i64>> {
        match parse_spark_time(value) {
            Some(micros) => Ok(Some(micros)),
            None if is_try => Ok(None),
            None => {
                let sql_value = value.replace('\\', "\\\\").replace('\'', "\\'");
                Err(exec_datafusion_err!(
                    "[CAST_INVALID_INPUT] The value '{sql_value}' of the type \"STRING\" cannot be cast to \"TIME\" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead."
                ))
            }
        }
    }
}

impl ScalarUDFImpl for SparkStringToTime {
    fn name(&self) -> &str {
        "spark_string_to_time"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Time64(TimeUnit::Microsecond))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let arg = args.one()?;
        let is_try = self.is_try;
        match arg {
            ColumnarValue::Array(array) => {
                let array = string_array_iter(array.as_ref())?
                    .map(|value| {
                        value
                            .map(|value| Self::string_to_time_micros(value, is_try))
                            .transpose()
                            .map(Option::flatten)
                    })
                    .collect::<Result<Time64MicrosecondArray>>()?;
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            ColumnarValue::Scalar(scalar) => {
                let value = match scalar.try_as_str() {
                    Some(x) => x
                        .map(|v| Self::string_to_time_micros(v, is_try))
                        .transpose()?
                        .flatten(),
                    _ => return exec_err!("expected string scalar for `spark_string_to_time`"),
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Time64Microsecond(value)))
            }
        }
    }
}

/// Ported from `SparkDateTimeUtils.stringToTime`, restricted to the TIME-only shape
/// (no date, no time zone) that `parseTimestampString` would otherwise also accept.
fn parse_spark_time(input: &str) -> Option<i64> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return None;
    }
    let bytes = trimmed.as_bytes();
    let mut is_am = false;
    let mut is_pm = false;
    let mut has_suffix = false;
    if bytes.len() > 2 {
        let last = bytes[bytes.len() - 1];
        if last == b'M' || last == b'm' {
            let prev = bytes[bytes.len() - 2];
            if prev == b'A' || prev == b'a' {
                is_am = true;
                has_suffix = true;
            } else if prev == b'P' || prev == b'p' {
                is_pm = true;
                has_suffix = true;
            }
        }
    }
    let time_str = if has_suffix {
        std::str::from_utf8(&bytes[..bytes.len() - 2])
            .ok()?
            .trim_end()
    } else {
        trimmed
    };
    let bytes = time_str.as_bytes();
    if bytes.is_empty() {
        return None;
    }

    // Segment index mirrors `parseTimestampString`: 3 = hour, 4 = minute, 5 = second,
    // 6 = fractional-second digits (padded/truncated to 6 at the end). Segments 0-2
    // (year/month/day) are never populated for a TIME-only input; reaching the end of
    // the string with `i < 3` means no `:`/leading `T` was ever seen, so the input
    // never looked like a time at all (e.g. a bare number or a date).
    let mut segments = [0_i64; 7];
    let mut i: usize = 0;
    let mut current_value: i64 = 0;
    let mut current_digits: u32 = 0;
    let mut just_time = false;
    let mut digits_fraction: u32 = 0;

    let mut idx = 0usize;
    while idx < bytes.len() {
        let b = bytes[idx];
        if idx == 0 && b == b'T' {
            just_time = true;
            i = 3;
            idx += 1;
            continue;
        }
        if b.is_ascii_digit() {
            let d = i64::from(b - b'0');
            if i == 6 {
                digits_fraction += 1;
                if current_digits < 6 {
                    current_value = current_value * 10 + d;
                }
            } else {
                current_value = current_value * 10 + d;
            }
            current_digits += 1;
            idx += 1;
            continue;
        }
        match i {
            0 if b == b':' => {
                just_time = true;
                if !(1..=2).contains(&current_digits) {
                    return None;
                }
                segments[3] = current_value;
                current_value = 0;
                current_digits = 0;
                i = 4;
            }
            3 | 4 if b == b':' => {
                if !(1..=2).contains(&current_digits) {
                    return None;
                }
                segments[i] = current_value;
                current_value = 0;
                current_digits = 0;
                i += 1;
            }
            5 if b == b'.' => {
                if !(1..=2).contains(&current_digits) {
                    return None;
                }
                segments[5] = current_value;
                current_value = 0;
                current_digits = 0;
                i = 6;
            }
            _ => return None,
        }
        idx += 1;
    }
    match i {
        3..=5 => {
            if !(1..=2).contains(&current_digits) {
                return None;
            }
            segments[i] = current_value;
        }
        6 => segments[6] = current_value,
        _ => return None,
    }
    if !just_time {
        return None;
    }
    while digits_fraction < 6 {
        segments[6] *= 10;
        digits_fraction += 1;
    }

    let (mut hour, minute, second, fraction_micros) =
        (segments[3], segments[4], segments[5], segments[6]);
    if has_suffix {
        if !(1..=12).contains(&hour) {
            return None;
        }
        if is_am {
            if hour == 12 {
                hour = 0;
            }
        } else if is_pm && hour != 12 {
            hour += 12;
        }
    } else if !(0..=23).contains(&hour) {
        return None;
    }
    if !(0..=59).contains(&minute) || !(0..=59).contains(&second) {
        return None;
    }
    Some(hour * 3_600_000_000 + minute * 60_000_000 + second * 1_000_000 + fraction_micros)
}

#[cfg(test)]
mod tests {
    use super::parse_spark_time;

    #[test]
    fn accepts_spark_lenient_forms() {
        assert_eq!(parse_spark_time("00:00:00"), Some(0));
        assert_eq!(
            parse_spark_time("9:5:3.5"),
            Some((9 * 3600 + 5 * 60 + 3) * 1_000_000 + 500_000)
        );
        assert_eq!(
            parse_spark_time(" 23:59:59.999999 "),
            Some((23 * 3600 + 59 * 60 + 59) * 1_000_000 + 999_999)
        );
        assert_eq!(
            parse_spark_time("T12:34:56"),
            Some((12 * 3600 + 34 * 60 + 56) * 1_000_000)
        );
    }

    #[test]
    fn rejects_malformed_input() {
        assert_eq!(parse_spark_time("garbage"), None);
        assert_eq!(parse_spark_time("24:00:00"), None);
        assert_eq!(parse_spark_time("25:00:00"), None);
        assert_eq!(parse_spark_time("2024-01-15 12:34:56"), None);
        assert_eq!(parse_spark_time("12"), None);
    }

    #[test]
    fn handles_am_pm_suffix() {
        assert_eq!(parse_spark_time("12:00:00AM"), Some(0));
        assert_eq!(parse_spark_time("12:00:00PM"), Some(12 * 3_600_000_000));
        assert_eq!(parse_spark_time("1:00:00PM"), Some(13 * 3_600_000_000));
        assert_eq!(parse_spark_time("11:00:00pm"), Some(23 * 3_600_000_000));
    }
}
