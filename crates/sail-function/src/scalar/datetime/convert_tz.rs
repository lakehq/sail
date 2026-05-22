use std::collections::HashMap;

use chrono::{DateTime, MappedLocalTime, NaiveDateTime, TimeZone};
use chrono_tz::{GapInfo, Tz};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int64Array, UInt64Array};
use datafusion::arrow::compute::kernels::{cast, numeric, take};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::error::DataFusionError;
use datafusion_common::{exec_err, plan_err, Result};
use datafusion_expr::function::Hint;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Volatility};
use datafusion_expr_common::signature::Signature;
use datafusion_functions::utils::make_scalar_function;
use sail_common::datetime::time_unit_to_multiplier;

/// A helper scalar UDF for converting time zones for timestamps.
/// The timestamp must be NTZ timestamp, which should have [`None`] time zone
/// in the Arrow data type.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ConvertTz {
    /// Whether to use the "classic" algorithm to convert time zone.
    /// The "classic" algorithm is used by the `convert_timezone` function in Spark,
    /// while the "non-classic" algorithm is used by the `from_utc_timestamp` and
    /// `to_utc_timestamp` functions in Spark.
    classic: bool,
    signature: Signature,
}

impl ConvertTz {
    pub fn new(classic: bool) -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
            classic,
        }
    }

    pub fn classic(&self) -> bool {
        self.classic
    }
}

impl ScalarUDFImpl for ConvertTz {
    fn name(&self) -> &str {
        "convert_tz"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [_, _, ts] = arg_types else {
            return plan_err!("`convert_tz` takes 3 arguments: from, to, timestamp");
        };
        match ts {
            DataType::Timestamp(unit, None) => Ok(DataType::Timestamp(*unit, None)),
            _ => plan_err!("`convert_tz` expects NTZ timestamp but got {ts:?}"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(
            |args| convert_tz_inner(args, self.classic),
            [Hint::AcceptsSingular].repeat(args.args.len()),
        )(args.args.as_slice())
    }
}

fn convert_tz_inner(args: &[ArrayRef], classic: bool) -> Result<ArrayRef> {
    let legacy_timezones = HashMap::from([
        ("ACT", "Australia/Darwin"),
        ("AET", "Australia/Sydney"),
        ("AGT", "America/Argentina/Buenos_Aires"),
        ("ART", "Africa/Cairo"),
        ("AST", "America/Anchorage"),
        ("BET", "America/Sao_Paulo"),
        ("BST", "Asia/Dhaka"),
        ("CAT", "Africa/Harare"),
        ("CNT", "America/St_Johns"),
        ("CST", "America/Chicago"),
        ("CTT", "Asia/Shanghai"),
        ("EAT", "Africa/Addis_Ababa"),
        ("ECT", "Europe/Paris"),
        ("EST", "America/New_York"),
        ("HST", "Pacific/Honolulu"),
        ("IET", "America/Indianapolis"),
        ("IST", "Asia/Calcutta"),
        ("JST", "Asia/Tokyo"),
        ("MIT", "Pacific/Apia"),
        ("MST", "America/Denver"),
        ("NET", "Asia/Yerevan"),
        ("NST", "Pacific/Auckland"),
        ("PLT", "Asia/Karachi"),
        ("PNT", "America/Phoenix"),
        ("PRT", "America/Puerto_Rico"),
        ("PST", "America/Los_Angeles"),
        ("SST", "Pacific/Guadalcanal"),
        ("VST", "Asia/Saigon"),
    ]);

    let parse_tz = |input: Option<&str>| {
        input
            .map(|tz_str_opt| {
                let tz_err = |tz_str| {
                    exec_err!(
                        "[INVALID_TIMEZONE] The timezone: {tz_str:?} is invalid. \
        The timezone must be either a region-based zone ID or a zone offset. \
        Region IDs must have the form 'area/city', such as 'America/Los_Angeles'. \
        Zone offsets must be in the format '(+|-)HH', '(+|-)HH:mm’ or '(+|-)HH:mm:ss', \
        e.g '-08' , '+01:00' or '-13:33:33', and must be in the range from -18:00 to +18:00. \
        'Z' and 'UTC' are accepted as synonyms for '+00:00'."
                    )
                };

                match tz_str_opt.parse::<Tz>() {
                    Ok(tz) => Ok(Some(tz)),
                    Err(_) => match legacy_timezones.get(tz_str_opt).cloned() {
                        Some(tz_str) => match tz_str.parse::<Tz>() {
                            Ok(tz) => Ok(Some(tz)),
                            Err(_) => tz_err(tz_str),
                        },
                        None => tz_err(tz_str_opt),
                    },
                }
            })
            .transpose()
            .map(|opt| opt.flatten())
    };

    let convert = if classic {
        convert_tz_classic
    } else {
        convert_tz_non_classic
    };

    let from_to_utc_timestamp_func =
        |inputs: (Option<i64>, Result<Option<Tz>>, Result<Option<Tz>>)| match inputs {
            (Some(ts_nanos), Ok(Some(from_tz)), Ok(Some(to_tz))) => {
                Ok(convert(ts_nanos, &from_tz, &to_tz))
            }
            (_, Err(e), _) | (_, _, Err(e)) => Err(e),
            _ => Ok(None),
        };

    let from_tz_strs_arr = cast::cast(&args[0], &DataType::Utf8)?;
    let to_tz_strs_arr = cast::cast(&args[1], &DataType::Utf8)?;
    let ts_arr = &args[2];

    let results: Int64Array = {
        let (from_tz_strs, to_tz_strs) = match (from_tz_strs_arr.as_string_opt::<i32>(), to_tz_strs_arr.as_string_opt::<i32>()) {
            (Some(f), Some(t)) => (f, t),
            _ => return exec_err!(
                "`convert_timezone` first and second arguments must be string literal or array, received {:?}, {:?}",
                args[0], args[1]
            ),
        };

        let arr_lens = args.iter().map(|a| a.len()).collect::<Vec<_>>();
        let max_len = *arr_lens.iter().max().map_or_else(
            || exec_err!("`convert_timezone`: could not get array lengths max"),
            Ok,
        )?;

        let ts_arr = if ts_arr.len() != max_len && ts_arr.len() == 1 {
            let indices = (0..max_len).map(|_| 0u64).collect::<UInt64Array>();
            take::take(&ts_arr, &indices, None)?
        } else {
            ts_arr.clone()
        };

        let nanos_arr = timestamp_to_nanoseconds(&ts_arr)?;

        let first = |iter: &mut dyn Iterator<Item = Result<Option<Tz>>>| {
            iter.next().transpose().map(|opt| opt.flatten())
        };
        // lazy evaluated iterators
        let mut from_tzs = from_tz_strs.iter().map(parse_tz);
        let mut to_tzs = to_tz_strs.iter().map(parse_tz);

        match (arr_lens[0] == 1, arr_lens[1] == 1) {
            (true, true) => {
                let from_tz = first(&mut from_tzs)?;
                let to_tz = first(&mut to_tzs)?;

                nanos_arr
                    .iter()
                    .map(|ts| from_to_utc_timestamp_func((ts, Ok(from_tz), Ok(to_tz))))
                    .collect::<Result<Int64Array>>()
            }
            (true, false) => {
                let from_tz = first(&mut from_tzs)?;
                nanos_arr
                    .iter()
                    .zip(to_tzs)
                    .map(|(ts, to_tz)| from_to_utc_timestamp_func((ts, Ok(from_tz), to_tz)))
                    .collect::<Result<Int64Array>>()
            }
            (false, true) => {
                let to_tz = first(&mut to_tzs)?;

                nanos_arr
                    .iter()
                    .zip(from_tzs)
                    .map(|(ts, from_tz)| from_to_utc_timestamp_func((ts, from_tz, Ok(to_tz))))
                    .collect::<Result<Int64Array>>()
            }
            (false, false) => nanos_arr
                .iter()
                .zip(from_tzs.zip(to_tzs))
                .map(|(a, (b, c))| (a, b, c))
                .map(|(ts, from_tz, to_tz)| from_to_utc_timestamp_func((ts, from_tz, to_tz)))
                .collect::<Result<Int64Array>>(),
        }
    }?;

    let time_unit = match args[2].data_type() {
        DataType::Timestamp(unit, None) => *unit,
        x => return exec_err!("invalid timestamp type for `convert_tz`: {x:?}"),
    };

    nanoseconds_to_timestamp(results, time_unit)
}

fn disambiguate_local_datetime(local: NaiveDateTime, tz: &Tz) -> Option<DateTime<Tz>> {
    // Handle ambiguous or non-existent local date time
    // in the same way as `java.time.ZonedDateTime#atZone`.
    match local.and_local_timezone(*tz) {
        MappedLocalTime::Single(x) => Some(x),
        MappedLocalTime::Ambiguous(earliest, _latest) => Some(earliest),
        MappedLocalTime::None => GapInfo::new(&local, tz).and_then(|gap| {
            if let (Some((start, _)), Some(end)) = (gap.begin, gap.end) {
                end.checked_add_signed(local - start)
            } else {
                None
            }
        }),
    }
}

/// Reference:
///   `org.apache.spark.sql.catalyst.util.DateTimeUtils#convertTimestampNtzToAnotherTz`
fn convert_tz_classic(ts_nanos: i64, from_zone: &Tz, to_zone: &Tz) -> Option<i64> {
    let local = DateTime::from_timestamp_nanos(ts_nanos).naive_utc();
    let dt = disambiguate_local_datetime(local, from_zone)?;
    dt.with_timezone(to_zone)
        .naive_local()
        .and_utc()
        .timestamp_nanos_opt()
}

/// Reference:
///   `org.apache.spark.sql.catalyst.util.SparkDateTimeUtils#convertTz`
fn convert_tz_non_classic(ts_nanos: i64, from_zone: &Tz, to_zone: &Tz) -> Option<i64> {
    let local = to_zone.timestamp_nanos(ts_nanos).naive_local();
    let dt = disambiguate_local_datetime(local, from_zone)?;
    dt.timestamp_nanos_opt()
}

fn timestamp_to_nanoseconds(array: &dyn Array) -> Result<Int64Array> {
    match array.data_type() {
        DataType::Timestamp(time_unit, None) => numeric::mul(
            &cast::cast(array, &DataType::Int64)?,
            &Int64Array::new_scalar(1_000_000_000i64 / time_unit_to_multiplier(time_unit)),
        )?
        .as_any()
        .downcast_ref::<Int64Array>()
        .cloned()
        .ok_or_else(|| DataFusionError::Execution("".to_string())),
        _ => {
            exec_err!(
                "`convert_timezone`: third argument type must coerce to NTZ timestamp, received {:?}",
                array.data_type()
            )
        }
    }
}

fn nanoseconds_to_timestamp(array: Int64Array, time_unit: TimeUnit) -> Result<ArrayRef> {
    Ok(cast::cast(
        &numeric::div(
            &array,
            &Int64Array::new_scalar(1_000_000_000i64 / time_unit_to_multiplier(&time_unit)),
        )?,
        &DataType::Timestamp(time_unit, None),
    )?)
}
