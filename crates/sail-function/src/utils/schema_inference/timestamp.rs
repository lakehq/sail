use chrono::{NaiveDate, NaiveTime};
use sail_common_datafusion::utils::datetime::SparkZone;

use crate::scalar::datetime::timestamp_parser::parse_timestamp_string;

/// Returns true if the string matches a timestamp/date that Spark's
/// `inferTimestamp` option would promote to `TIMESTAMP`. This is a faithful
/// port of Spark's `SparkDateTimeUtils.stringToTimestamp` (used for the default
/// `TIMESTAMP_LTZ` type), including year-only and time-only forms, fractional
/// seconds, and timezone suffixes, with range validation via `chrono`.
pub(crate) fn is_timestamp_string(s: &str) -> bool {
    let Some(parsed) = parse_timestamp_string(s) else {
        return false;
    };
    let (Ok(hour), Ok(minute), Ok(second)) = (
        u32::try_from(parsed.segments[3]),
        u32::try_from(parsed.segments[4]),
        u32::try_from(parsed.segments[5]),
    ) else {
        return false;
    };
    let micros = u32::try_from(parsed.segments[6]).unwrap_or(u32::MAX);
    if NaiveTime::from_hms_nano_opt(hour, minute, second, micros.saturating_mul(1000)).is_none() {
        return false;
    }
    if !parsed.just_time {
        let (Ok(year), Ok(month), Ok(day)) = (
            i32::try_from(parsed.segments[0]),
            u32::try_from(parsed.segments[1]),
            u32::try_from(parsed.segments[2]),
        ) else {
            return false;
        };
        if NaiveDate::from_ymd_opt(year, month, day).is_none() {
            return false;
        }
    }
    match &parsed.timezone {
        Some(tz) => SparkZone::parse(tz).is_ok(),
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use super::is_timestamp_string;

    #[test]
    fn timestamp_inference_matches_spark() {
        // (input, expected) pairs verified against Spark 4.x JVM
        // (`schema_of_json(..., map('inferTimestamp','true'))`).
        let cases: &[(&str, bool)] = &[
            ("2024", true),
            ("2024-01", true),
            ("2024-01-02", true),
            ("2024-1-2", true),
            ("2024-01-02 03", true),
            ("2024-01-02 03:04", true),
            ("2024-01-02 03:04:05", true),
            ("2024-01-02T03:04:05", true),
            ("2024-01-02T03:04:05.1", true),
            ("2024-01-02 03:04:05.123", true),
            ("2024-01-02 03:04:05.123456789", true),
            ("2024-01-02T03:04:05.123456", true),
            ("2024-01-02T03:04:05Z", true),
            ("2024-01-02 03:04:05+02:00", true),
            ("2024-01-02 03:04:05-0800", true),
            ("2024-01-02 03:04:05 America/Los_Angeles", true),
            ("03:04:05", true),
            ("03:04", true),
            ("3:4:5", true),
            ("T03:04:05", true),
            (" 2024-01-02 03:04:05 ", true),
            ("-2024-01-02", true),
            ("2024-01-02 03:04:05.", true),
            // Rejected by Spark (inferred as STRING):
            ("2024-13-02", false),
            ("2024-01-32", false),
            ("2024/01/02", false),
            ("20240102", false),
            ("2024-01-02x", false),
            ("2024-01-02  03:04:05", false),
            ("2024-01-02 25:00:00", false),
            ("not-a-date", false),
            ("hello", false),
            ("", false),
            ("2024-01-02 03:04:05 Foo/Bar", false),
        ];
        for (input, expected) in cases {
            assert_eq!(
                is_timestamp_string(input),
                *expected,
                "mismatch for input {input:?}"
            );
        }
    }
}
