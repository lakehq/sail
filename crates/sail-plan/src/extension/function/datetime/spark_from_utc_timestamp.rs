use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, PrimitiveArray};
use arrow::datatypes::TimestampMicrosecondType;
use chrono::offset::LocalResult;
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
pub struct SparkFromUtcTimestamp {
    signature: Signature,
    time_unit: TimeUnit,
}

impl SparkFromUtcTimestamp {
    pub fn new(time_unit: TimeUnit) -> Self {
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
        }
    }

    pub fn time_unit(&self) -> &TimeUnit {
        &self.time_unit
    }
}

impl ScalarUDFImpl for SparkFromUtcTimestamp {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_from_utc_timestamp"
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
                "Spark `from_utc_timestamp` function requires 2 arguments, got {}",
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
                "Second argument for `from_utc_timestamp` must be string, received {other:?}"
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
        let error_strs = [
            "[INVALID_TIMEZONE] The timezone:",
            "is invalid. \
            The timezone must be either a region-based zone ID or a zone offset. \
            Region IDs must have the form 'area/city', such as 'America/Los_Angeles'. \
            Zone offsets must be in the format '(+|-)HH', '(+|-)HH:mm’ or '(+|-)HH:mm:ss', \
            e.g '-08' , '+01:00' or '-13:33:33', and must be in the range from -18:00 to +18:00. \
            'Z' and 'UTC' are accepted as synonyms for '+00:00'.",
        ];

        let from_utc_timestamp_func = |inputs: (Option<i64>, Option<&str>)| match inputs {
            (Some(ts_nanos), Some(tz_str)) => match tz_str.parse::<Tz>() {
                Ok(to_zone) => Ok(to_zone),
                Err(_) => match legacy_timezones.get(tz_str).cloned() {
                    Some(tz_str) => match tz_str.parse::<Tz>() {
                        Ok(to_zone) => Ok(to_zone),
                        Err(_) => exec_err!("{:?} {:?} {:?}", error_strs[0], tz_str, error_strs[1]),
                    },
                    None => exec_err!("{:?} {:?} {:?}", error_strs[0], tz_str, error_strs[1]),
                },
            }
            .and_then(|to_zone| {
                let to_dt = to_zone.timestamp_nanos(ts_nanos);
                match to_dt.naive_local().and_local_timezone(*local_offset) {
                    LocalResult::Single(result_ts) => {
                        Ok(Some(result_ts.to_utc().timestamp_micros()))
                    }
                    LocalResult::Ambiguous(_, _) | LocalResult::None => {
                        exec_err!("`from_utc_timestamp`: failed to set local timezone offset")
                    }
                }
            }),
            _ => Ok(None),
        };

        let results = match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(tz_str)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(tz_str)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(tz_str))) => {
                let array = args[0].clone().into_array(1)?;
                _timestamp_to_nanoseconds(array.as_ref()).and_then(
                    |(timestamps, _time_unit, _tz_orig)| {
                        timestamps
                            .iter()
                            .map(|ts| from_utc_timestamp_func((ts, Some(tz_str.as_str()))))
                            .collect::<Result<Vec<Option<i64>>, DataFusionError>>()
                    },
                )
            }
            ColumnarValue::Array(_) => {
                let arrays = ColumnarValue::values_to_arrays(args)?;
                match arrays[1].as_string_opt::<i32>() {
                    Some(timezones) => {
                        _timestamp_to_nanoseconds(&arrays[0]).and_then(
                        |(timestamps, _time_unit, _tz_orig)| {
                                timestamps
                                .iter()
                                .zip(timezones.iter())
                                .map(from_utc_timestamp_func)
                                .collect::<Result<Vec<Option<i64>>, DataFusionError>>()
                            }
                        )
                    },
                    None => exec_err!("Second argument for `from_utc_timestamp` must be string literal or array, received {:?}", arrays[1])
                }
            }
            default => {
                exec_err!(
                    "Second argument for `from_utc_timestamp` must be string literal or array, received {:?}", default
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

fn _timestamp_to_nanoseconds(
    array: &dyn Array,
) -> Result<(Int64Array, TimeUnit, Option<Arc<str>>)> {
    match array.data_type() {
        DataType::Timestamp(time_unit, tz) => {
            let multiplier = match time_unit {
                TimeUnit::Second => 1_000_000_000,
                TimeUnit::Millisecond => 1_000_000,
                TimeUnit::Microsecond => 1_000,
                TimeUnit::Nanosecond => 1,
            };
            match arrow::compute::kernels::cast::cast(array, &DataType::Int64) {
                Ok(casted) => {
                     match casted
                        .as_any()
                        .downcast_ref::<Int64Array>() {
                        Some(values) => {
                            let nanos_values = values
                            .iter()
                            .map(|nanos_opt| nanos_opt.map(|nanos| nanos * multiplier));
                            Ok((PrimitiveArray::from_iter(nanos_values), *time_unit, tz.clone()))
                        },
                        None => exec_err!("`from_utc_timestamp`: could not cast timestamp array to int64, this should not be happening")
                    }
                },
                Err(_) => exec_err!(
                    "`from_utc_timestamp`: could not cast timestamp array to int64, this should not be happening"
                )
            }
        }
        _ => {
            exec_err!(
                "First argument type for `from_utc_timestamp` must coerce to timestamp, received {:?}",
                array.data_type()
            )
        }
    }
}
