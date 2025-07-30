use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, PrimitiveArray};
use arrow::datatypes::TimestampMicrosecondType;
use chrono::{Local, TimeZone};
use chrono_tz::Tz;
use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion_common::error::DataFusionError;
use datafusion_common::{exec_err, internal_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Volatility,
};
use datafusion_expr_common::signature::{Signature, TypeSignature, TIMEZONE_WILDCARD};

#[derive(Debug)]
pub struct SparkFromToUtcTimestamp {
    signature: Signature,
    time_unit: TimeUnit,
    is_to: bool,
}

impl SparkFromToUtcTimestamp {
    pub fn new(time_unit: TimeUnit, is_to: bool) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Second, Some(TIMEZONE_WILDCARD.into())),
                        DataType::Utf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Second, Some(TIMEZONE_WILDCARD.into())),
                        DataType::Utf8View,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Second, Some(TIMEZONE_WILDCARD.into())),
                        DataType::LargeUtf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Second, None),
                        DataType::Utf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Second, None),
                        DataType::Utf8View,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Second, None),
                        DataType::LargeUtf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Millisecond, Some(TIMEZONE_WILDCARD.into())),
                        DataType::Utf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Millisecond, Some(TIMEZONE_WILDCARD.into())),
                        DataType::Utf8View,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Millisecond, Some(TIMEZONE_WILDCARD.into())),
                        DataType::LargeUtf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        DataType::Utf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        DataType::Utf8View,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        DataType::LargeUtf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Microsecond, Some(TIMEZONE_WILDCARD.into())),
                        DataType::Utf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Microsecond, Some(TIMEZONE_WILDCARD.into())),
                        DataType::Utf8View,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Microsecond, Some(TIMEZONE_WILDCARD.into())),
                        DataType::LargeUtf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        DataType::Utf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        DataType::Utf8View,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        DataType::LargeUtf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Nanosecond, Some(TIMEZONE_WILDCARD.into())),
                        DataType::Utf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Nanosecond, Some(TIMEZONE_WILDCARD.into())),
                        DataType::Utf8View,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Nanosecond, Some(TIMEZONE_WILDCARD.into())),
                        DataType::LargeUtf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        DataType::Utf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        DataType::Utf8View,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        DataType::LargeUtf8,
                    ]),
                ],
                Volatility::Immutable,
            ),
            time_unit,
            is_to,
        }
    }

    pub fn time_unit(&self) -> &TimeUnit {
        &self.time_unit
    }

    pub fn is_to(&self) -> bool {
        self.is_to
    }
}

impl ScalarUDFImpl for SparkFromToUtcTimestamp {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        if self.is_to {
            "to_utc_timestamp"
        } else {
            "from_utc_timestamp"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("`return_type` should not be called, call `return_type_from_args` instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if args.arg_fields.len() != 2 {
            return exec_err!(
                "Spark `{}` function requires 2 arguments, got {}",
                self.name(),
                args.arg_fields.len()
            );
        }
        match &args.arg_fields[1].data_type() {
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => Ok(Arc::new(Field::new(
                self.name(),
                DataType::Timestamp(
                    *self.time_unit(),
                    Some(Arc::from(Local::now().offset().to_string())),
                ),
                true,
            ))),
            other => exec_err!(
                "Second argument for `{}` must be string, received {:?}",
                self.name(),
                other
            ),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = &args.args;
        let now = Local::now();
        let local_offset = now.offset();
        let local_offset_arc = Some(Arc::from(local_offset.to_string()));

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

        let from_to_utc_timestamp_func = |inputs: (Option<i64>, Option<&str>)| match inputs {
            (Some(ts_nanos), Some(tz_str)) => {
                let tz_err = |tz_str| {
                    exec_err!(
                "[INVALID_TIMEZONE] The timezone: {tz_str:?} is invalid. \
                The timezone must be either a region-based zone ID or a zone offset. \
                Region IDs must have the form 'area/city', such as 'America/Los_Angeles'. \
                Zone offsets must be in the format '(+|-)HH', '(+|-)HH:mm’ or '(+|-)HH:mm:ss', \
                e.g '-08' , '+01:00' or '-13:33:33', and must be in the range from -18:00 to +18:00. \
                'Z' and 'UTC' are accepted as synonyms for '+00:00'.")
                };

                let to_zone = match tz_str.parse::<Tz>() {
                    Ok(to_zone) => Ok(to_zone),
                    Err(_) => match legacy_timezones.get(tz_str).cloned() {
                        Some(tz_str) => match tz_str.parse::<Tz>() {
                            Ok(to_zone) => Ok(to_zone),
                            Err(_) => tz_err(tz_str),
                        },
                        None => tz_err(tz_str),
                    },
                }?;

                if self.is_to {
                    tz_shifted_utc_micros(ts_nanos, local_offset, &to_zone)
                } else {
                    tz_shifted_utc_micros(ts_nanos, &to_zone, local_offset)
                }
                .map_or_else(
                    || exec_err!("`{}`: failed to set local timezone offset", self.name()),
                    |ts| Ok(Some(ts)),
                )
            }
            _ => Ok(None),
        };

        let results = match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(tz_str)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(tz_str)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(tz_str))) => {
                let array = args[0].clone().into_array(1)?;
                _timestamp_to_nanoseconds(array.as_ref(), self.name()).and_then(
                    |(timestamps, _time_unit, _tz_orig)| {
                        timestamps
                            .iter()
                            .map(|ts| from_to_utc_timestamp_func((ts, Some(tz_str.as_str()))))
                            .collect::<Result<Vec<Option<i64>>, DataFusionError>>()
                    },
                )
            }
            ColumnarValue::Array(_) => {
                let arrays = ColumnarValue::values_to_arrays(args)?;
                match arrays[1].as_string_opt::<i32>() {
                    Some(timezones) => _timestamp_to_nanoseconds(&arrays[0], self.name()).and_then(
                        |(timestamps, _time_unit, _tz_orig)| {
                            timestamps
                                .iter()
                                .zip(timezones.iter())
                                .map(from_to_utc_timestamp_func)
                                .collect::<Result<Vec<Option<i64>>, DataFusionError>>()
                        },
                    ),
                    None => exec_err!(
                        "Second argument for `{}` must be string literal or array, received {:?}",
                        self.name(),
                        arrays[1]
                    ),
                }
            }
            default => {
                exec_err!(
                    "Second argument for `{}` must be string literal or array, received {:?}",
                    self.name(),
                    default
                )
            }
        };

        results.map(|values| match args[0] {
            ColumnarValue::Array(_) => ColumnarValue::Array(Arc::new(
                PrimitiveArray::<TimestampMicrosecondType>::from(values)
                    .with_data_type(DataType::Timestamp(TimeUnit::Microsecond, local_offset_arc)),
            )),
            ColumnarValue::Scalar(_) => match values.first() {
                Some(Some(value)) => ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    Some(*value),
                    local_offset_arc,
                )),
                _ => {
                    ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(None, local_offset_arc))
                }
            },
        })
    }
}

fn tz_shifted_utc_micros<T1: TimeZone + Clone, T2: TimeZone + Clone>(
    ts_nanos: i64,
    from_zone: &T1,
    to_zone: &T2,
) -> Option<i64> {
    from_zone
        .timestamp_nanos(ts_nanos)
        .naive_local()
        .and_local_timezone(to_zone.clone())
        .single()
        .map(|ts| ts.to_utc().timestamp_micros())
}

fn _timestamp_to_nanoseconds(
    array: &dyn Array,
    func_name: &str,
) -> Result<(Int64Array, TimeUnit, Option<Arc<str>>)> {
    match array.data_type() {
        DataType::Timestamp(time_unit, tz) => {
            let multiplier = match time_unit {
                TimeUnit::Second => 1_000_000_000,
                TimeUnit::Millisecond => 1_000_000,
                TimeUnit::Microsecond => 1_000,
                TimeUnit::Nanosecond => 1,
            };
            arrow::compute::kernels::cast::cast(array, &DataType::Int64)?
                .as_any()
                .downcast_ref::<Int64Array>().map(|values| {
                    let nanos_values = values
                    .iter()
                    .map(|nanos_opt| nanos_opt.map(|nanos| nanos * multiplier));
                    Ok((PrimitiveArray::from_iter(nanos_values), *time_unit, tz.clone()))
                }).unwrap_or_else(|| exec_err!("`{func_name}`: could not cast timestamp array to int64, this should not be happening"))
        }
        _ => {
            exec_err!(
                "First argument type for `{func_name}` must coerce to timestamp, received {:?}",
                array.data_type()
            )
        }
    }
}
