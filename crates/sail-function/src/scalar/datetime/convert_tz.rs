use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int64Array, UInt64Array, new_null_array};
use datafusion::arrow::compute::kernels::{cast, numeric, take};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion_common::error::DataFusionError;
use datafusion_common::{Result, exec_err, internal_err, plan_err};
use datafusion_expr::function::Hint;
use datafusion_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDFImpl, LambdaParametersProgress, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDFImpl, ValueOrLambda, Volatility,
};
use datafusion_expr_common::signature::Signature;
use datafusion_functions::utils::make_scalar_function;
use jiff::tz::AmbiguousOffset;
use sail_common_datafusion::utils::datetime::{
    local_timezone_offsets, localize_with_fallback, timezone_offset, to_local_datetime,
};

use crate::functions_nested_utils::{evaluate_lambdas_until_null, scatter_active_rows};

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
    /// Whether NULL rows short-circuit before parsing their time zones.
    null_short_circuit: bool,
    /// Whether this conversion implements an LTZ-to-NTZ cast.
    ltz_to_ntz: bool,
    signature: Signature,
}

impl ConvertTz {
    pub fn new(classic: bool) -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
            classic,
            null_short_circuit: false,
            ltz_to_ntz: false,
        }
    }

    pub fn with_null_short_circuit(mut self) -> Self {
        self.null_short_circuit = true;
        self
    }

    pub fn with_ltz_to_ntz(mut self) -> Self {
        self.ltz_to_ntz = true;
        self
    }

    pub fn classic(&self) -> bool {
        self.classic
    }

    pub fn null_short_circuit(&self) -> bool {
        self.null_short_circuit
    }

    pub fn ltz_to_ntz(&self) -> bool {
        self.ltz_to_ntz
    }
}

/// Evaluates `convert_timezone` arguments from left to right and stops at the first NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ConvertTzLazy {
    signature: HigherOrderSignature,
    convert_tz: ConvertTz,
}

impl ConvertTzLazy {
    pub fn new(convert_tz: ConvertTz) -> Self {
        Self {
            signature: HigherOrderSignature::variadic_any(Volatility::Immutable),
            convert_tz,
        }
    }

    pub fn classic(&self) -> bool {
        self.convert_tz.classic()
    }

    pub fn null_short_circuit(&self) -> bool {
        self.convert_tz.null_short_circuit()
    }

    pub fn ltz_to_ntz(&self) -> bool {
        self.convert_tz.ltz_to_ntz()
    }
}

impl HigherOrderUDFImpl for ConvertTzLazy {
    fn name(&self) -> &str {
        "convert_tz_lazy"
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        check_lazy_convert_tz_args(fields)?;
        let dummy = Arc::new(Field::new("", DataType::Null, true));
        Ok(LambdaParametersProgress::Complete(
            fields.iter().map(|_| vec![Arc::clone(&dummy)]).collect(),
        ))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        check_lazy_convert_tz_args(args.arg_fields)?;
        let fields = args
            .arg_fields
            .iter()
            .map(|arg| match arg {
                ValueOrLambda::Lambda(field) => Ok(Arc::clone(field)),
                ValueOrLambda::Value(_) => {
                    exec_err!("convert_timezone expected lambda arguments")
                }
            })
            .collect::<Result<Vec<_>>>()?;

        ScalarUDFImpl::return_field_from_args(
            &self.convert_tz,
            ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: args.scalar_arguments,
            },
        )
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        check_lazy_convert_tz_args(&args.args)?;
        let lambdas = args
            .args
            .iter()
            .map(|arg| match arg {
                ValueOrLambda::Lambda(lambda) => Ok(lambda),
                ValueOrLambda::Value(_) => {
                    exec_err!("convert_timezone expected lambda arguments")
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let arg_fields = args
            .arg_fields
            .iter()
            .map(|arg| match arg {
                ValueOrLambda::Lambda(field) => Ok(Arc::clone(field)),
                ValueOrLambda::Value(_) => {
                    exec_err!("convert_timezone expected lambda arguments")
                }
            })
            .collect::<Result<Vec<_>>>()?;

        // TODO: Add single-pass row-level lambda evaluation so errors follow Spark's row-major
        // order. Replaying batches is not equivalent because it re-evaluates volatile/stateful
        // bodies, which Spark requires to run at most once per row.
        let (values, active_rows) = evaluate_lambdas_until_null(&lambdas, args.number_rows)?;
        if active_rows.is_empty() {
            return Ok(ColumnarValue::Array(new_null_array(
                args.return_type(),
                args.number_rows,
            )));
        }

        let value = ScalarUDFImpl::invoke_with_args(
            &self.convert_tz,
            ScalarFunctionArgs {
                args: values.into_iter().map(ColumnarValue::Array).collect(),
                arg_fields,
                number_rows: active_rows.len(),
                return_field: Arc::clone(&args.return_field),
                config_options: Arc::clone(&args.config_options),
            },
        )?
        .into_array(active_rows.len())?;

        Ok(ColumnarValue::Array(scatter_active_rows(
            value,
            &active_rows,
            args.number_rows,
        )?))
    }

    fn short_circuits(&self) -> bool {
        true
    }
}

fn check_lazy_convert_tz_args<V, L>(args: &[ValueOrLambda<V, L>]) -> Result<()> {
    if args.len() != 3 {
        return exec_err!(
            "convert_timezone takes 3 internal arguments, got {}",
            args.len()
        );
    }
    if args
        .iter()
        .any(|arg| matches!(arg, ValueOrLambda::Value(_)))
    {
        return exec_err!("convert_timezone expected lambda arguments");
    }
    Ok(())
}

impl ScalarUDFImpl for ConvertTz {
    fn name(&self) -> &str {
        "convert_tz"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let arg_types = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        let [_, _, ts] = arg_types.as_slice() else {
            return plan_err!("`convert_tz` takes 3 arguments: from, to, timestamp");
        };
        let data_type = match ts {
            DataType::Timestamp(unit, None) => DataType::Timestamp(*unit, None),
            _ => return plan_err!("`convert_tz` expects NTZ timestamp but got {ts:?}"),
        };
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(self.name(), data_type, nullable)))
    }

    fn is_strict(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(
            |args| convert_tz_inner(args, self.classic, self.null_short_circuit, self.ltz_to_ntz),
            [Hint::AcceptsSingular].repeat(args.args.len()),
        )(args.args.as_slice())
    }
}

fn convert_tz_inner(
    args: &[ArrayRef],
    classic: bool,
    null_short_circuit: bool,
    ltz_to_ntz: bool,
) -> Result<ArrayRef> {
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

    let convert = |ts_micros, from_zone: &Tz, to_zone: &Tz| {
        if ltz_to_ntz && !classic && *from_zone == Tz::UTC {
            let Some(utc) = DateTime::<Utc>::from_timestamp_micros(ts_micros) else {
                // shortcut: retain pre-PR wide cast values until full-range regional offsets are supported.
                return Some(ts_micros);
            };
            let offset = timezone_offset(to_zone, utc.timestamp())
                .ok()?
                .local_minus_utc();
            // Integer shifting avoids overflowing Chrono's local calendar at its bounds.
            ts_micros.checked_add(i64::from(offset) * 1_000_000)
        } else if classic {
            convert_tz_classic(ts_micros, from_zone, to_zone)
        } else {
            convert_tz_non_classic(ts_micros, from_zone, to_zone)
        }
    };

    let mut singular_timezones = [None, None];
    let mut parse_row_zone = |index: usize, name: &str| {
        if args[index].len() == 1 {
            if singular_timezones[index].is_none() {
                singular_timezones[index] = parse_tz(Some(name))?;
            }
            Ok(singular_timezones[index])
        } else {
            parse_tz(Some(name))
        }
    };
    let mut from_to_utc_timestamp_func =
        |inputs: (Option<i64>, Option<&str>, Option<&str>)| match inputs {
            (Some(ts_micros), Some(from_tz), Some(to_tz)) => {
                match (parse_row_zone(0, from_tz)?, parse_row_zone(1, to_tz)?) {
                    (Some(from_tz), Some(to_tz)) => Ok(convert(ts_micros, &from_tz, &to_tz)),
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        };

    let eager_from_to_utc_timestamp_func =
        |inputs: (Option<i64>, Result<Option<Tz>>, Result<Option<Tz>>)| match inputs {
            (Some(ts_micros), Ok(Some(from_tz)), Ok(Some(to_tz))) => {
                Ok(convert(ts_micros, &from_tz, &to_tz))
            }
            (_, Err(e), _) | (_, _, Err(e)) => Err(e),
            _ => Ok(None),
        };

    let from_tz_strs_arr = cast::cast(&args[0], &DataType::Utf8)?;
    let to_tz_strs_arr = cast::cast(&args[1], &DataType::Utf8)?;
    let ts_arr = &args[2];

    let results: Int64Array = {
        let (from_tz_strs, to_tz_strs) = match (
            from_tz_strs_arr.as_string_opt::<i32>(),
            to_tz_strs_arr.as_string_opt::<i32>(),
        ) {
            (Some(f), Some(t)) => (f, t),
            _ => {
                return exec_err!(
                    "`convert_timezone` first and second arguments must be string literal or array, received {:?}, {:?}",
                    args[0],
                    args[1]
                );
            }
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

        let micros_arr = timestamp_to_microseconds(&ts_arr)?;

        if null_short_circuit {
            // Time zones are parsed only after all three row values pass null checks.
            let mut from_tzs = from_tz_strs.iter();
            let mut to_tzs = to_tz_strs.iter();

            match (arr_lens[0] == 1, arr_lens[1] == 1) {
                (true, true) => {
                    let from_tz = from_tzs.next().flatten();
                    let to_tz = to_tzs.next().flatten();

                    micros_arr
                        .iter()
                        .map(|ts| from_to_utc_timestamp_func((ts, from_tz, to_tz)))
                        .collect::<Result<Int64Array>>()
                }
                (true, false) => {
                    let from_tz = from_tzs.next().flatten();
                    micros_arr
                        .iter()
                        .zip(to_tzs)
                        .map(|(ts, to_tz)| from_to_utc_timestamp_func((ts, from_tz, to_tz)))
                        .collect::<Result<Int64Array>>()
                }
                (false, true) => {
                    let to_tz = to_tzs.next().flatten();

                    micros_arr
                        .iter()
                        .zip(from_tzs)
                        .map(|(ts, from_tz)| from_to_utc_timestamp_func((ts, from_tz, to_tz)))
                        .collect::<Result<Int64Array>>()
                }
                (false, false) => micros_arr
                    .iter()
                    .zip(from_tzs.zip(to_tzs))
                    .map(|(a, (b, c))| (a, b, c))
                    .map(|(ts, from_tz, to_tz)| from_to_utc_timestamp_func((ts, from_tz, to_tz)))
                    .collect::<Result<Int64Array>>(),
            }
        } else {
            let first = |iter: &mut dyn Iterator<Item = Result<Option<Tz>>>| {
                iter.next().transpose().map(|opt| opt.flatten())
            };
            let mut from_tzs = from_tz_strs.iter().map(parse_tz);
            let mut to_tzs = to_tz_strs.iter().map(parse_tz);

            match (arr_lens[0] == 1, arr_lens[1] == 1) {
                (true, true) => {
                    let from_tz = first(&mut from_tzs)?;
                    let to_tz = first(&mut to_tzs)?;

                    micros_arr
                        .iter()
                        .map(|ts| eager_from_to_utc_timestamp_func((ts, Ok(from_tz), Ok(to_tz))))
                        .collect::<Result<Int64Array>>()
                }
                (true, false) => {
                    let from_tz = first(&mut from_tzs)?;
                    micros_arr
                        .iter()
                        .zip(to_tzs)
                        .map(|(ts, to_tz)| {
                            eager_from_to_utc_timestamp_func((ts, Ok(from_tz), to_tz))
                        })
                        .collect::<Result<Int64Array>>()
                }
                (false, true) => {
                    let to_tz = first(&mut to_tzs)?;

                    micros_arr
                        .iter()
                        .zip(from_tzs)
                        .map(|(ts, from_tz)| {
                            eager_from_to_utc_timestamp_func((ts, from_tz, Ok(to_tz)))
                        })
                        .collect::<Result<Int64Array>>()
                }
                (false, false) => micros_arr
                    .iter()
                    .zip(from_tzs.zip(to_tzs))
                    .map(|(a, (b, c))| (a, b, c))
                    .map(|(ts, from_tz, to_tz)| {
                        eager_from_to_utc_timestamp_func((ts, from_tz, to_tz))
                    })
                    .collect::<Result<Int64Array>>(),
            }
        }
    }?;

    let time_unit = match args[2].data_type() {
        DataType::Timestamp(unit, None) => *unit,
        x => return exec_err!("invalid timestamp type for `convert_tz`: {x:?}"),
    };

    microseconds_to_timestamp(results, time_unit)
}

/// Reference:
///   `org.apache.spark.sql.catalyst.util.DateTimeUtils#convertTimestampNtzToAnotherTz`
fn convert_tz_classic(ts_micros: i64, from_zone: &Tz, to_zone: &Tz) -> Option<i64> {
    let local = match DateTime::<Utc>::from_timestamp_micros(ts_micros) {
        Some(datetime) => datetime.naive_utc(),
        None if from_zone == to_zone => return Some(ts_micros),
        None => return None,
    };
    let dt = localize_with_fallback(from_zone, &local).ok()?;
    Some(
        to_local_datetime(to_zone, &dt)
            .ok()?
            .naive_local()
            .and_utc()
            .timestamp_micros(),
    )
}

/// Reference:
///   `org.apache.spark.sql.catalyst.util.SparkDateTimeUtils#convertTz`
fn convert_tz_non_classic(ts_micros: i64, from_zone: &Tz, to_zone: &Tz) -> Option<i64> {
    let local = match DateTime::from_timestamp_micros(ts_micros) {
        Some(datetime) => to_local_datetime(to_zone, &datetime).ok()?.naive_local(),
        None if from_zone == to_zone => return Some(ts_micros),
        None => return None,
    };
    let offset = match local_timezone_offsets(from_zone, &local).ok()? {
        AmbiguousOffset::Unambiguous { offset } => offset,
        AmbiguousOffset::Fold { before, .. } | AmbiguousOffset::Gap { before, .. } => before,
    };
    // A valid local calendar can map to an instant outside Chrono's UTC range.
    local
        .and_utc()
        .timestamp_micros()
        .checked_sub(i64::from(offset.seconds()) * 1_000_000)
}

fn timestamp_to_microseconds(array: &dyn Array) -> Result<Int64Array> {
    let values = cast::cast(array, &DataType::Int64)?;
    let values = values
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Execution("expected Int64 timestamp values".to_string()))?;
    let scaled = match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, None) => {
            numeric::mul(values, &Int64Array::new_scalar(1_000_000))?
        }
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            numeric::mul(values, &Int64Array::new_scalar(1_000))?
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => return Ok(values.clone()),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            numeric::div(values, &Int64Array::new_scalar(1_000))?
        }
        _ => {
            return exec_err!(
                "`convert_timezone`: third argument type must coerce to NTZ timestamp, received {:?}",
                array.data_type()
            );
        }
    };
    scaled
        .as_any()
        .downcast_ref::<Int64Array>()
        .cloned()
        .ok_or_else(|| DataFusionError::Execution("expected Int64 timestamp values".to_string()))
}

fn microseconds_to_timestamp(array: Int64Array, time_unit: TimeUnit) -> Result<ArrayRef> {
    if time_unit == TimeUnit::Microsecond {
        return Ok(cast::cast(&array, &DataType::Timestamp(time_unit, None))?);
    }
    let values = match time_unit {
        TimeUnit::Second => numeric::div(&array, &Int64Array::new_scalar(1_000_000))?,
        TimeUnit::Millisecond => numeric::div(&array, &Int64Array::new_scalar(1_000))?,
        TimeUnit::Microsecond => unreachable!(),
        TimeUnit::Nanosecond => numeric::mul(&array, &Int64Array::new_scalar(1_000))?,
    };
    Ok(cast::cast(&values, &DataType::Timestamp(time_unit, None))?)
}
