use std::str::FromStr;

use datafusion::arrow::array::timezone::Tz;

/// The aliases of `java.time.ZoneId.SHORT_IDS`, which `DateTimeUtils.getZoneId` passes to `ZoneId.of`.
const SHORT_IDS: [&str; 28] = [
    "ACT", "AET", "AGT", "ART", "AST", "BET", "BST", "CAT", "CNT", "CST", "CTT", "EAT", "ECT",
    "EST", "HST", "IET", "IST", "JST", "MIT", "MST", "NET", "NST", "PLT", "PNT", "PRT", "PST",
    "SST", "VST",
];

/// The largest absolute offset of a `java.time.ZoneOffset`, in seconds.
const MAX_OFFSET_SECONDS: u32 = 18 * 3_600;

/// Returns whether Spark accepts `id` as the value of `spark.sql.session.timeZone`, that is,
/// whether `SparkDateTimeUtils.getZoneId` resolves it (`ZoneId.of(id, ZoneId.SHORT_IDS)` once the
/// hour of `+h:mm` and the minute of `+hh:m` are padded). The match is case-sensitive and the id
/// is not trimmed.
///
/// This is only used to reject a value before it is stored. It does not say which zone `id` is.
// TODO: Replace this with the resolution of the session time zone once it lives in `sail-plan`.
pub(super) fn is_valid_time_zone(id: &str) -> bool {
    if id == "Z" || SHORT_IDS.contains(&id) {
        return true;
    }
    if id.starts_with(['+', '-']) {
        return is_valid_offset(id);
    }
    for prefix in ["UTC", "GMT", "UT"] {
        // A prefix followed by anything but an offset is still a region (e.g. `GMT0`).
        if let Some(rest) = id.strip_prefix(prefix)
            && (rest.is_empty() || rest.starts_with(['+', '-']))
        {
            return rest.is_empty() || is_valid_offset(rest);
        }
    }
    !id.is_empty() && Tz::from_str(id).is_ok()
}

/// Checks `(+|-)h`, `(+|-)hh`, `(+|-)hhmm`, `(+|-)hhmmss`, `(+|-)h:m`, `(+|-)hh:mm` and
/// `(+|-)hh:mm:ss`, where the last field of `h:m` forms may have one digit.
fn is_valid_offset(offset: &str) -> bool {
    let Some(body) = offset.strip_prefix(['+', '-']) else {
        return false;
    };
    let digits_only = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    let number = |s: &str| s.parse::<u32>().ok();
    let fields: Vec<&str> = body.split(':').collect();
    let (hours, minutes, seconds) = match fields.as_slice() {
        [all] if digits_only(all) => match all.len() {
            1 | 2 => (number(all), Some(0), Some(0)),
            4 => (number(&all[..2]), number(&all[2..]), Some(0)),
            6 => (number(&all[..2]), number(&all[2..4]), number(&all[4..])),
            _ => return false,
        },
        [h, m] if digits_only(h) && digits_only(m) && h.len() <= 2 && m.len() <= 2 => {
            (number(h), number(m), Some(0))
        }
        [h, m, s]
            if digits_only(h)
                && digits_only(m)
                && digits_only(s)
                && h.len() <= 2
                && m.len() == 2
                && s.len() == 2 =>
        {
            (number(h), number(m), number(s))
        }
        _ => return false,
    };
    let (Some(hours), Some(minutes), Some(seconds)) = (hours, minutes, seconds) else {
        return false;
    };
    minutes <= 59 && seconds <= 59 && hours * 3_600 + minutes * 60 + seconds <= MAX_OFFSET_SECONDS
}
