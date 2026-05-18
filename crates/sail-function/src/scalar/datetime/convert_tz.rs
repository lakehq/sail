use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use chrono::{Duration, Local, LocalResult, Offset, TimeZone};
use chrono_tz::Tz;
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

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ConvertTz {
    signature: Signature,
}

impl ConvertTz {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl Default for ConvertTz {
    fn default() -> Self {
        ConvertTz::new()
    }
}

impl ScalarUDFImpl for ConvertTz {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "convert_tz"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 3 {
            return plan_err!("`convert_tz` takes 3 arguments: from, to, timestamp");
        }
        match &arg_types[2] {
            DataType::Timestamp(unit, tz) => Ok(DataType::Timestamp(*unit, tz.clone())),
            _ => Ok(DataType::Timestamp(
                TimeUnit::Microsecond,
                local_offset_opt(),
            )), // TODO: strict type coersion
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(
            convert_tz_inner,
            [Hint::AcceptsSingular].repeat(args.args.len()),
        )(args.args.as_slice())
    }
}

fn convert_tz_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
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

    let from_to_utc_timestamp_func =
        |inputs: (Option<i64>, Result<Option<Tz>>, Result<Option<Tz>>)| match inputs {
            (Some(ts_nanos), Ok(Some(from_tz)), Ok(Some(to_tz))) => {
                tz_shifted_utc_nanos(ts_nanos, &from_tz, &to_tz).map_or_else(
                    || exec_err!("convert_timezone``: failed to set timezone offset"),
                    |ts| Ok(Some(ts)),
                )
            }
            (_, Err(e), _) | (_, _, Err(e)) => Err(e),
            _ => Ok(None),
        };

    let ts_arr = match args[2].data_type() {
        DataType::Timestamp(_time_unit, _tz) => args[2].clone(),
        _ => cast::cast(
            &args[2],
            &DataType::Timestamp(TimeUnit::Microsecond, local_offset_opt()),
        )?,
    };

    let from_tz_strs_arr = cast::cast(&args[0], &DataType::Utf8)?;
    let to_tz_strs_arr = cast::cast(&args[1], &DataType::Utf8)?;

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
            let indices = (0..max_len as u64).collect::<UInt64Array>();
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

    let (time_unit, tz) = match ts_arr.data_type() {
        DataType::Timestamp(unit, tz) => (*unit, tz.clone()),
        _ => (TimeUnit::Microsecond, local_offset_opt()),
    };

    nanoseconds_to_timestamp(results, &DataType::Timestamp(time_unit, tz))
}

fn local_offset_opt() -> Option<Arc<str>> {
    Some(Arc::from(Local::now().offset().to_string()))
}

fn tz_shifted_utc_nanos<T1: TimeZone + Clone, T2: TimeZone + Clone>(
    ts_nanos: i64,
    from_zone: &T1,
    to_zone: &T2,
) -> Option<i64> {
    let naive = to_zone.timestamp_nanos(ts_nanos).naive_local();
    resolve_local_datetime(from_zone, naive).and_then(|ts| ts.to_utc().timestamp_nanos_opt())
}

fn resolve_local_datetime<T: TimeZone + Clone>(
    tz: &T,
    naive: chrono::NaiveDateTime,
) -> Option<chrono::DateTime<T>> {
    match naive.and_local_timezone(tz.clone()) {
        LocalResult::Single(ts) => Some(ts),
        LocalResult::Ambiguous(ts1, ts2) => match (
            ts1.to_utc().timestamp_nanos_opt(),
            ts2.to_utc().timestamp_nanos_opt(),
        ) {
            (Some(n1), Some(n2)) => Some(if n1 <= n2 { ts1 } else { ts2 }),
            _ => Some(ts1),
        },
        LocalResult::None => resolve_nonexistent_local_datetime(tz, naive),
    }
}

fn resolve_nonexistent_local_datetime<T: TimeZone + Clone>(
    tz: &T,
    naive: chrono::NaiveDateTime,
) -> Option<chrono::DateTime<T>> {
    // Spark shifts nonexistent local times (DST gaps) forward by the offset change,
    // e.g. `2025-03-09 02:30:00` in `America/Los_Angeles` becomes `03:30:00`.
    let before = find_valid_datetime_nearby(tz, naive, -1)?;
    let after = find_valid_datetime_nearby(tz, naive, 1)?;

    let before_offset = before.offset().fix().local_minus_utc();
    let after_offset = after.offset().fix().local_minus_utc();
    let shift_seconds = i64::from(after_offset - before_offset);

    if shift_seconds == 0 {
        return Some(after);
    }

    let shifted = naive + Duration::seconds(shift_seconds);
    shifted
        .and_local_timezone(tz.clone())
        .earliest()
        .or_else(|| shifted.and_local_timezone(tz.clone()).latest())
        .or(Some(after))
}

fn find_valid_datetime_nearby<T: TimeZone + Clone>(
    tz: &T,
    naive: chrono::NaiveDateTime,
    direction: i32,
) -> Option<chrono::DateTime<T>> {
    // Look up to 24 hours away in 1-hour steps to find a valid datetime on the
    // requested side of the gap.
    for i in 1..=24i64 {
        let step = Duration::hours(i);
        let candidate = if direction < 0 {
            naive - step
        } else {
            naive + step
        };
        let res = candidate.and_local_timezone(tz.clone());
        let ts = match res {
            LocalResult::Single(ts) => Some(ts),
            LocalResult::Ambiguous(ts1, ts2) => match (
                ts1.to_utc().timestamp_nanos_opt(),
                ts2.to_utc().timestamp_nanos_opt(),
            ) {
                (Some(n1), Some(n2)) => Some(if n1 <= n2 { ts1 } else { ts2 }),
                _ => Some(ts1),
            },
            LocalResult::None => None,
        };
        if let Some(ts) = ts {
            return Some(ts);
        }
    }
    None
}

fn timestamp_to_nanoseconds(array: &dyn Array) -> Result<Int64Array> {
    match array.data_type() {
        DataType::Timestamp(time_unit, _tz) => numeric::mul(
            &cast::cast(array, &DataType::Int64)?,
            &numeric::div(
                &Int64Array::new_scalar(1_000_000_000i64),
                &Int64Array::new_scalar(time_unit_to_multiplier(time_unit)),
            )?,
        )?
        .as_any()
        .downcast_ref::<Int64Array>()
        .cloned()
        .ok_or_else(|| DataFusionError::Execution("".to_string())),
        _ => {
            exec_err!(
                "`convert_timezone`: third argument type must coerce to timestamp, received {:?}",
                array.data_type()
            )
        }
    }
}

fn nanoseconds_to_timestamp(array: Int64Array, data_type: &DataType) -> Result<ArrayRef> {
    match data_type {
        DataType::Timestamp(time_unit, tz) => Ok(cast::cast(
            &numeric::div(
                &array,
                &numeric::div(
                    &Int64Array::new_scalar(1_000_000_000i64),
                    &Int64Array::new_scalar(time_unit_to_multiplier(time_unit)),
                )?,
            )?,
            &DataType::Timestamp(*time_unit, tz.clone()),
        )?),
        _ => {
            exec_err!(
                "`convert_timezone`: result type must coerce to timestamp, received {:?}",
                array.data_type()
            )
        }
    }
}
