use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::Debug;
use std::sync::Arc;

use chrono::{Datelike, FixedOffset, NaiveDate, Utc};
use datafusion::arrow::array::Array;
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::datatypes::{DataType, TimeUnit, TimestampMicrosecondType};
use datafusion_common::arrow::array::PrimitiveArray;
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use sail_common_datafusion::utils::datetime::localize_with_fallback;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::scalar::datetime::format::DateTimeFormat;
use crate::scalar::json::schema_of_json::{ParsedTimestamp, parse_timestamp_string};

/// Truncates a DateTime's nanoseconds to microseconds.
/// This preserves fractional seconds when converting from nanosecond precision to microsecond precision.
fn truncate_datetime_to_microseconds(datetime: &chrono::DateTime<chrono::Utc>) -> i64 {
    use chrono::Timelike;

    let timestamp_secs = datetime.timestamp();
    let nanos = datetime.nanosecond();

    // Convert nanoseconds to microseconds by truncation
    // 1 microsecond = 1000 nanoseconds
    let micros_from_nanos = nanos as i64 / 1000;

    // Combine seconds and microseconds
    timestamp_secs * 1_000_000 + micros_from_nanos
}

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};

#[derive(Debug, PartialEq, Eq, Hash)]
enum TimestampParser {
    Ltz { default_timezone: String },
    Ntz,
}

#[derive(Debug)]
enum SparkZone {
    Fixed(FixedOffset),
    Named(Tz),
}

impl SparkZone {
    fn parse(value: &str) -> Result<Self> {
        let value = value.trim();
        if value.is_empty() {
            return exec_err!("invalid empty time zone");
        }
        if value == "Z" {
            return Self::fixed(0);
        }

        if let Some(suffix) = ["UTC", "GMT", "UT"]
            .into_iter()
            .find_map(|prefix| value.strip_prefix(prefix))
        {
            if suffix.is_empty() {
                return Self::fixed(0);
            }
            if suffix.starts_with(['+', '-']) {
                let offset = parse_zone_offset(suffix)
                    .ok_or_else(|| exec_datafusion_err!("invalid time zone: {value}"))?;
                return Ok(Self::Fixed(offset));
            }
        }

        let zone_id = match value {
            "ACT" => "Australia/Darwin",
            "AET" => "Australia/Sydney",
            "AGT" => "America/Argentina/Buenos_Aires",
            "ART" => "Africa/Cairo",
            "AST" => "America/Anchorage",
            "BET" => "America/Sao_Paulo",
            "BST" => "Asia/Dhaka",
            "CAT" => "Africa/Harare",
            "CNT" => "America/St_Johns",
            "CST" => "America/Chicago",
            "CTT" => "Asia/Shanghai",
            "EAT" => "Africa/Addis_Ababa",
            "ECT" => "Europe/Paris",
            "IET" => "America/Indiana/Indianapolis",
            "IST" => "Asia/Kolkata",
            "JST" => "Asia/Tokyo",
            "MIT" => "Pacific/Apia",
            "NET" => "Asia/Yerevan",
            "NST" => "Pacific/Auckland",
            "PLT" => "Asia/Karachi",
            "PNT" => "America/Phoenix",
            "PRT" => "America/Puerto_Rico",
            "PST" => "America/Los_Angeles",
            "SST" => "Pacific/Guadalcanal",
            "VST" => "Asia/Ho_Chi_Minh",
            "EST" => "-05:00",
            "MST" => "-07:00",
            "HST" => "-10:00",
            _ => value,
        };

        if zone_id.starts_with(['+', '-']) {
            let offset = parse_zone_offset(zone_id)
                .ok_or_else(|| exec_datafusion_err!("invalid time zone: {value}"))?;
            return Ok(Self::Fixed(offset));
        }

        let timezone = zone_id
            .parse::<Tz>()
            .map_err(|_| exec_datafusion_err!("invalid time zone: {value}"))?;
        Ok(Self::Named(timezone))
    }

    fn fixed(seconds: i32) -> Result<Self> {
        FixedOffset::east_opt(seconds)
            .map(Self::Fixed)
            .ok_or_else(|| exec_datafusion_err!("invalid time-zone offset: {seconds}"))
    }

    fn current_date(&self) -> (i64, u32, u32) {
        let now = Utc::now();
        let date = match self {
            Self::Fixed(zone) => now.with_timezone(zone).date_naive(),
            Self::Named(zone) => now.with_timezone(zone).date_naive(),
        };
        (i64::from(date.year()), date.month(), date.day())
    }

    #[expect(clippy::too_many_arguments)]
    fn to_utc_micros(
        &self,
        year: i64,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
        micros: u32,
        local_micros: i128,
    ) -> Result<i64> {
        let offset_micros = match self {
            Self::Fixed(offset) => i128::from(offset.local_minus_utc()) * 1_000_000,
            Self::Named(zone) => {
                let representative_year = i32::try_from(year)
                    .ok()
                    .filter(|year| NaiveDate::from_ymd_opt(*year, month, day).is_some())
                    .map(Ok)
                    .unwrap_or_else(|| {
                        let year = if year >= 0 {
                            2400 + year.rem_euclid(400)
                        } else {
                            -200_000 + year.rem_euclid(400)
                        };
                        i32::try_from(year)
                            .map_err(|_| exec_datafusion_err!("invalid timestamp year: {year}"))
                    })?;
                let date = NaiveDate::from_ymd_opt(representative_year, month, day)
                    .ok_or_else(|| exec_datafusion_err!("invalid timestamp date"))?;
                let datetime = date
                    .and_hms_micro_opt(hour, minute, second, micros)
                    .ok_or_else(|| exec_datafusion_err!("invalid timestamp time"))?;
                let instant = localize_with_fallback(zone, &datetime)?;
                i128::from(datetime.and_utc().timestamp_micros())
                    - i128::from(instant.timestamp_micros())
            }
        };
        i64::try_from(local_micros - offset_micros)
            .map_err(|_| exec_datafusion_err!("timestamp is outside the microsecond range"))
    }
}

fn parse_digits(value: &str, min: usize, max: usize) -> Option<u32> {
    if !(min..=max).contains(&value.len()) || !value.bytes().all(|value| value.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

fn parse_zone_offset(value: &str) -> Option<FixedOffset> {
    let (sign, body) = match value.as_bytes().first() {
        Some(b'+') => (1_i32, &value[1..]),
        Some(b'-') => (-1_i32, &value[1..]),
        _ => return None,
    };
    let (hours, minutes, seconds) = if body.contains(':') {
        let parts = body.split(':').collect::<Vec<_>>();
        match parts.as_slice() {
            [hours, minutes] => (parse_digits(hours, 1, 2)?, parse_digits(minutes, 1, 2)?, 0),
            [hours, minutes, seconds] => (
                parse_digits(hours, 1, 2)?,
                parse_digits(minutes, 2, 2)?,
                parse_digits(seconds, 2, 2)?,
            ),
            _ => return None,
        }
    } else {
        match body.len() {
            1 => (parse_digits(body, 1, 1)?, 0, 0),
            2 => (parse_digits(body, 2, 2)?, 0, 0),
            4 => (
                parse_digits(&body[..2], 2, 2)?,
                parse_digits(&body[2..], 2, 2)?,
                0,
            ),
            6 => (
                parse_digits(&body[..2], 2, 2)?,
                parse_digits(&body[2..4], 2, 2)?,
                parse_digits(&body[4..], 2, 2)?,
            ),
            _ => return None,
        }
    };
    if hours > 18 || minutes >= 60 || seconds >= 60 {
        return None;
    }
    if hours == 18 && (minutes != 0 || seconds != 0) {
        return None;
    }
    let seconds = i32::try_from(hours * 3600 + minutes * 60 + seconds).ok()?;
    FixedOffset::east_opt(sign * seconds)
}

fn days_in_month(year: i64, month: u32) -> Option<u32> {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => Some(31),
        4 | 6 | 9 | 11 => Some(30),
        2 if year.rem_euclid(4) == 0
            && (year.rem_euclid(100) != 0 || year.rem_euclid(400) == 0) =>
        {
            Some(29)
        }
        2 => Some(28),
        _ => None,
    }
}

fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let shifted_month = i64::from(month) + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn local_timestamp_micros(
    year: i64,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    micros: u32,
) -> Option<i128> {
    if day == 0
        || day > days_in_month(year, month)?
        || hour >= 24
        || minute >= 60
        || second >= 60
        || micros >= 1_000_000
    {
        return None;
    }
    Some(
        i128::from(days_from_civil(year, month, day)) * 86_400_000_000
            + i128::from(hour) * 3_600_000_000
            + i128::from(minute) * 60_000_000
            + i128::from(second) * 1_000_000
            + i128::from(micros),
    )
}

enum ScalarFormat {
    Omitted,
    Null,
    Format(DateTimeFormat),
}

impl TimestampParser {
    fn formatted_string_to_microseconds(
        &self,
        value: &str,
        format: &DateTimeFormat,
        is_try: bool,
    ) -> Result<Option<i64>> {
        let parsed = match format.parse_datetime_value(value) {
            Ok(v) => v,
            Err(_e) if is_try => return Ok(None),
            Err(e) => return Err(e),
        };
        match self {
            TimestampParser::Ltz { default_timezone } => {
                let datetime = if let Some(offset) = parsed.offset {
                    parsed
                        .datetime
                        .and_local_timezone(offset)
                        .single()
                        .map(|x| x.to_utc())
                        .ok_or_else(|| exec_datafusion_err!("cannot apply parsed offset"))?
                } else {
                    let timezone_name = parsed.timezone.as_deref().unwrap_or(default_timezone);
                    let timezone: Tz = match timezone_name.parse() {
                        Ok(v) => v,
                        Err(_e) if is_try => return Ok(None),
                        Err(e) => return Err(e.into()),
                    };
                    match localize_with_fallback(&timezone, &parsed.datetime) {
                        Ok(v) => v,
                        Err(_e) if is_try => return Ok(None),
                        Err(e) => return Err(e),
                    }
                };
                // Truncate nanoseconds to microseconds to preserve fractional seconds
                let micros = truncate_datetime_to_microseconds(&datetime);
                Ok(Some(micros))
            }
            TimestampParser::Ntz => Ok(Some(parsed.datetime.and_utc().timestamp_micros())),
        }
    }

    fn string_to_microseconds(&self, value: &str, safe: bool) -> Result<Option<i64>> {
        match self.parse_unformatted(value) {
            Ok(value) => Ok(Some(value)),
            Err(_error) if safe => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn parse_unformatted(&self, value: &str) -> Result<i64> {
        let ParsedTimestamp {
            segments,
            timezone,
            just_time,
        } = parse_timestamp_string(value)
            .ok_or_else(|| exec_datafusion_err!("invalid timestamp: {value}"))?;

        let parsed_zone = timezone.as_deref().map(SparkZone::parse).transpose()?;
        let session_zone = match self {
            TimestampParser::Ltz { default_timezone } => Some(SparkZone::parse(default_timezone)?),
            TimestampParser::Ntz => None,
        };
        let effective_zone = parsed_zone.as_ref().or(session_zone.as_ref());

        let (year, month, day) = if just_time {
            match self {
                TimestampParser::Ltz { .. } => effective_zone
                    .ok_or_else(|| exec_datafusion_err!("missing LTZ time zone"))?
                    .current_date(),
                TimestampParser::Ntz => {
                    return exec_err!("time-only input is not a TIMESTAMP_NTZ");
                }
            }
        } else {
            (
                segments[0],
                u32::try_from(segments[1])
                    .map_err(|_| exec_datafusion_err!("invalid timestamp month"))?,
                u32::try_from(segments[2])
                    .map_err(|_| exec_datafusion_err!("invalid timestamp day"))?,
            )
        };
        let hour = u32::try_from(segments[3])
            .map_err(|_| exec_datafusion_err!("invalid timestamp hour"))?;
        let minute = u32::try_from(segments[4])
            .map_err(|_| exec_datafusion_err!("invalid timestamp minute"))?;
        let second = u32::try_from(segments[5])
            .map_err(|_| exec_datafusion_err!("invalid timestamp second"))?;
        let micros = u32::try_from(segments[6])
            .map_err(|_| exec_datafusion_err!("invalid timestamp fraction"))?;
        let local_micros = local_timestamp_micros(year, month, day, hour, minute, second, micros)
            .ok_or_else(|| exec_datafusion_err!("invalid timestamp: {value}"))?;

        match self {
            TimestampParser::Ltz { .. } => effective_zone
                .ok_or_else(|| exec_datafusion_err!("missing LTZ time zone"))?
                .to_utc_micros(year, month, day, hour, minute, second, micros, local_micros),
            TimestampParser::Ntz => i64::try_from(local_micros)
                .map_err(|_| exec_datafusion_err!("timestamp is outside the microsecond range")),
        }
    }
}

/// Spark-compatible `to_timestamp` / `try_to_timestamp` (and their `_ntz`
/// counterparts / `CAST(str AS TIMESTAMP[_NTZ])`).
///
/// Honors `spark.sql.ansi.enabled` via two flags (same shape as the ANSI-aware
/// date/make_interval UDFs):
/// - `is_try` selects the safe variant (`try_to_timestamp`) and drives `name()`.
/// - `ansi_mode` is the session flag captured at planning.
///
/// A parse/cast failure returns NULL when `is_try || !ansi_mode` and errors
/// otherwise. The `timezone` field selects LTZ (Some, applies the input's
/// timezone or the session default) vs NTZ (None, keeps wall clock).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTimestamp {
    timezone: Option<Arc<str>>,
    parser: TimestampParser,
    signature: Signature,
    ansi_mode: bool,
    is_try: bool,
}

impl SparkTimestamp {
    pub fn try_new(timezone: Option<Arc<str>>, ansi_mode: bool, is_try: bool) -> Result<Self> {
        let parser = if let Some(ref timezone) = timezone {
            TimestampParser::Ltz {
                default_timezone: timezone.as_ref().to_string(),
            }
        } else {
            TimestampParser::Ntz
        };
        Ok(Self {
            timezone,
            parser,
            signature: Signature::variadic_any(Volatility::Stable),
            ansi_mode,
            is_try,
        })
    }

    pub fn timezone(&self) -> Option<&str> {
        self.timezone.as_deref()
    }

    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }

    pub fn is_try(&self) -> bool {
        self.is_try
    }

    /// Whether a parse/cast failure yields NULL: `try_*` always, or the strict
    /// variant when ANSI is disabled.
    fn safe(&self) -> bool {
        self.is_try || !self.ansi_mode
    }
}

impl ScalarUDFImpl for SparkTimestamp {
    fn name(&self) -> &str {
        match (&self.parser, self.is_try) {
            (TimestampParser::Ltz { .. }, false) => "to_timestamp",
            (TimestampParser::Ltz { .. }, true) => "try_to_timestamp",
            (TimestampParser::Ntz, false) => "to_timestamp_ntz",
            (TimestampParser::Ntz, true) => "try_to_timestamp_ntz",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            self.timezone.clone(),
        ))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if !matches!(arg_types.len(), 1 | 2) {
            return Err(invalid_arg_count_exec_err(
                self.name(),
                (1, 2),
                arg_types.len(),
            ));
        }
        match &arg_types[0] {
            // String-only, matching the kernel (which parses strings) and the
            // sibling parsers `SparkDate`/`SparkTime`. The planner casts/handles
            // DATE/TIMESTAMP inputs directly, so they never reach this UDF.
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => {}
            other => {
                return Err(unsupported_data_type_exec_err(
                    self.name(),
                    "STRING or NULL",
                    other,
                ));
            }
        }
        let mut coerced = arg_types.to_vec();
        if let Some(format) = arg_types.get(1) {
            match format {
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {}
                // A NULL format yields a NULL result; coerce it to a Utf8 null.
                DataType::Null => coerced[1] = DataType::Utf8,
                other => {
                    return Err(unsupported_data_type_exec_err(self.name(), "STRING", other));
                }
            }
        }
        Ok(coerced)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let (arg, format) = match args.len() {
            1 => (args.one()?, None),
            2 => {
                let (arg, format) = args.two()?;
                (arg, Some(format))
            }
            _ => return exec_err!("spark_timestamp requires 1 or 2 arguments"),
        };
        let safe = self.safe();
        match (arg, format) {
            (ColumnarValue::Array(array), Some(ColumnarValue::Array(format_array))) => {
                self.parse_array_with_format_array(&array, &format_array, safe)
            }
            (ColumnarValue::Scalar(scalar), Some(ColumnarValue::Array(format_array))) => {
                let arrays = ColumnarValue::values_to_arrays(&[
                    ColumnarValue::Scalar(scalar),
                    ColumnarValue::Array(format_array),
                ])?;
                let array = arrays[0].clone();
                let format_array = arrays[1].clone();
                self.parse_array_with_format_array(&array, &format_array, safe)
            }
            (ColumnarValue::Array(array), format) => {
                let format = parse_scalar_format(format)?;
                let array: PrimitiveArray<TimestampMicrosecondType> = match array.data_type() {
                    DataType::Utf8 => as_string_array(&array)?
                        .iter()
                        .map(|x| {
                            x.map(|v| match &format {
                                ScalarFormat::Format(format) => self
                                    .parser
                                    .formatted_string_to_microseconds(v, format, safe),
                                ScalarFormat::Omitted => {
                                    self.parser.string_to_microseconds(v, safe)
                                }
                                ScalarFormat::Null => Ok(None),
                            })
                            .transpose()
                            .map(|opt| opt.flatten())
                        })
                        .collect::<Result<_>>()?,
                    DataType::LargeUtf8 => as_large_string_array(&array)?
                        .iter()
                        .map(|x| {
                            x.map(|v| match &format {
                                ScalarFormat::Format(format) => self
                                    .parser
                                    .formatted_string_to_microseconds(v, format, safe),
                                ScalarFormat::Omitted => {
                                    self.parser.string_to_microseconds(v, safe)
                                }
                                ScalarFormat::Null => Ok(None),
                            })
                            .transpose()
                            .map(|opt| opt.flatten())
                        })
                        .collect::<Result<_>>()?,
                    DataType::Utf8View => as_string_view_array(&array)?
                        .iter()
                        .map(|x| {
                            x.map(|v| match &format {
                                ScalarFormat::Format(format) => self
                                    .parser
                                    .formatted_string_to_microseconds(v, format, safe),
                                ScalarFormat::Omitted => {
                                    self.parser.string_to_microseconds(v, safe)
                                }
                                ScalarFormat::Null => Ok(None),
                            })
                            .transpose()
                            .map(|opt| opt.flatten())
                        })
                        .collect::<Result<_>>()?,
                    _ => return exec_err!("expected string array for `timestamp`"),
                };
                let array = array.with_timezone_opt(self.timezone.clone());
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            (ColumnarValue::Scalar(scalar), format) => {
                let format = parse_scalar_format(format)?;
                if matches!(format, ScalarFormat::Null) {
                    return Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                        None,
                        self.timezone.clone(),
                    )));
                }

                let value = match scalar.try_as_str() {
                    Some(x) => x
                        .map(|v| match &format {
                            ScalarFormat::Format(format) => self
                                .parser
                                .formatted_string_to_microseconds(v, format, safe),
                            ScalarFormat::Omitted => self.parser.string_to_microseconds(v, safe),
                            ScalarFormat::Null => unreachable!(),
                        })
                        .transpose()?
                        .flatten(),
                    _ => {
                        return exec_err!("expected string scalar for `timestamp`");
                    }
                };
                Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    value,
                    self.timezone.clone(),
                )))
            }
        }
    }
}

fn parse_scalar_format(format: Option<ColumnarValue>) -> Result<ScalarFormat> {
    match format {
        Some(ColumnarValue::Scalar(scalar)) => match scalar.try_as_str() {
            Some(Some(format)) => Ok(ScalarFormat::Format(DateTimeFormat::for_parsing(format)?)),
            Some(None) => Ok(ScalarFormat::Null),
            None => exec_err!("spark_timestamp format argument must be a string scalar"),
        },
        Some(ColumnarValue::Array(_)) => unreachable!(),
        None => Ok(ScalarFormat::Omitted),
    }
}

impl SparkTimestamp {
    fn parse_array_with_format_array(
        &self,
        array: &Arc<dyn datafusion::arrow::array::Array>,
        format_array: &Arc<dyn datafusion::arrow::array::Array>,
        safe: bool,
    ) -> Result<ColumnarValue> {
        if array.len() != format_array.len() {
            return exec_err!("spark_timestamp value and format arrays must have the same length");
        }
        let mut cache = HashMap::<String, DateTimeFormat>::new();
        let array = match format_array.data_type() {
            DataType::Utf8 => {
                let formats = as_string_array(format_array)?;
                self.parse_array_with_formats(array, formats.iter(), &mut cache, safe)?
            }
            DataType::LargeUtf8 => {
                let formats = as_large_string_array(format_array)?;
                self.parse_array_with_formats(array, formats.iter(), &mut cache, safe)?
            }
            DataType::Utf8View => {
                let formats = as_string_view_array(format_array)?;
                self.parse_array_with_formats(array, formats.iter(), &mut cache, safe)?
            }
            _ => return exec_err!("spark_timestamp format argument must be a string array"),
        };
        Ok(ColumnarValue::Array(Arc::new(
            array.with_timezone_opt(self.timezone.clone()),
        )))
    }

    fn parse_array_with_formats<'f>(
        &self,
        array: &Arc<dyn datafusion::arrow::array::Array>,
        formats: impl Iterator<Item = Option<&'f str>>,
        cache: &mut HashMap<String, DateTimeFormat>,
        safe: bool,
    ) -> Result<PrimitiveArray<TimestampMicrosecondType>> {
        match array.data_type() {
            DataType::Utf8 => {
                self.parse_values_with_formats(as_string_array(array)?.iter(), formats, cache, safe)
            }
            DataType::LargeUtf8 => self.parse_values_with_formats(
                as_large_string_array(array)?.iter(),
                formats,
                cache,
                safe,
            ),
            DataType::Utf8View => self.parse_values_with_formats(
                as_string_view_array(array)?.iter(),
                formats,
                cache,
                safe,
            ),
            _ => exec_err!("expected string array for `timestamp`"),
        }
    }

    fn parse_values_with_formats<'v, 'f>(
        &self,
        values: impl Iterator<Item = Option<&'v str>>,
        formats: impl Iterator<Item = Option<&'f str>>,
        cache: &mut HashMap<String, DateTimeFormat>,
        safe: bool,
    ) -> Result<PrimitiveArray<TimestampMicrosecondType>> {
        values
            .zip(formats)
            .map(|(value, format)| match (value, format) {
                (Some(value), Some(format)) => {
                    let format = get_or_parse_format(cache, format)?;
                    self.parser
                        .formatted_string_to_microseconds(value, format, safe)
                }
                _ => Ok(None),
            })
            .collect::<Result<_>>()
    }
}

fn get_or_parse_format<'a>(
    cache: &'a mut HashMap<String, DateTimeFormat>,
    pattern: &str,
) -> Result<&'a DateTimeFormat> {
    match cache.entry(pattern.to_string()) {
        Entry::Occupied(entry) => Ok(entry.into_mut()),
        Entry::Vacant(entry) => Ok(entry.insert(DateTimeFormat::for_parsing(pattern)?)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unformatted_parser_rejects_leap_seconds_in_safe_and_strict_modes() -> Result<()> {
        let parser = TimestampParser::Ltz {
            default_timezone: "UTC".to_string(),
        };

        assert_eq!(
            parser.string_to_microseconds("2026-06-15 23:59:60", true)?,
            None
        );
        assert!(
            parser
                .string_to_microseconds("2026-06-15 23:59:60", false)
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn unformatted_parser_matches_spark_range_and_zone_forms() -> Result<()> {
        let parser = TimestampParser::Ltz {
            default_timezone: "UTC".to_string(),
        };

        assert_eq!(
            parser.string_to_microseconds("294247-01-10T04:00:54.775807Z", false)?,
            Some(i64::MAX)
        );
        assert_eq!(
            parser.string_to_microseconds("-290308-12-21 19:59:05.224192Z", false)?,
            Some(i64::MIN)
        );
        assert_eq!(
            parser.string_to_microseconds("2024-01-01 00:00:00 PST", false)?,
            parser.string_to_microseconds("2024-01-01 08:00:00Z", false)?
        );
        assert_eq!(
            parser.string_to_microseconds("2024-01-01 01:00:00 GMT+01:00", false)?,
            parser.string_to_microseconds("2024-01-01 00:00:00Z", false)?
        );
        assert_eq!(
            parser.string_to_microseconds("  2024-05-01 12:00:00.1234567890  ", false)?,
            parser.string_to_microseconds("2024-05-01 12:00:00.123456", false)?
        );
        Ok(())
    }

    #[test]
    fn unformatted_parser_rejects_non_spark_syntax() -> Result<()> {
        let parser = TimestampParser::Ltz {
            default_timezone: "UTC".to_string(),
        };

        for value in [
            "-0200000-01-01 00:00:00",
            "2024-01-01 00:00:00+23:59",
            "2024-01-01t00:00:00",
            "2024-01-01 00:00:00z",
        ] {
            assert_eq!(parser.string_to_microseconds(value, true)?, None);
            assert!(parser.string_to_microseconds(value, false).is_err());
        }
        Ok(())
    }
}
