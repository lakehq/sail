use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, PrimitiveArray};
use arrow::datatypes::TimestampMicrosecondType;
use chrono::{Local, TimeZone};
use chrono_tz::Tz;
use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
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

        let from_utc_timestamp_func = |inputs: (Option<i64>, Option<&str>)| match inputs {
            (Some(ts_nanos), Some(tz_str)) => {
                let to_zone: Tz = tz_str.parse().unwrap();
                let to_dt = to_zone.timestamp_nanos(ts_nanos);
                let result_ts = to_dt
                    .naive_local()
                    .and_local_timezone(*local_offset)
                    .unwrap()
                    .to_utc();
                Some(result_ts.timestamp_micros())
            }
            _ => None,
        };

        let results = match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(tz_str)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(tz_str)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(tz_str))) => {
                let array = args[0].clone().into_array(1)?;
                let (timestamps, _time_unit, _tz_orig) = _timestamp_to_nanoseconds(array.as_ref());
                Ok(timestamps
                    .unwrap()
                    .iter()
                    .map(|ts| from_utc_timestamp_func((ts, Some(tz_str.as_str()))))
                    .collect::<Vec<_>>())
            }
            ColumnarValue::Array(_) => {
                let arrays = ColumnarValue::values_to_arrays(args)?;
                let (timestamps, _time_unit, _tz_orig) = _timestamp_to_nanoseconds(&arrays[0]);
                let timezones = match arrays[1].as_string_opt::<i32>() {
                    Some(res) => Ok(res),
                    None => exec_err!("Second argument for `from_utc_timestamp` must be string literal or array, received {:?}", arrays[1])
                };
                Ok(timestamps
                    .unwrap()
                    .iter()
                    .zip(timezones.unwrap().iter())
                    .map(from_utc_timestamp_func)
                    .collect::<Vec<_>>())
            }
            default => {
                exec_err!(
                    "Second argument for `from_utc_timestamp` must be string literal or array, received {:?}", default
                )
            }
        };

        match args[0] {
            ColumnarValue::Array(_) => Ok(ColumnarValue::Array(Arc::new(
                PrimitiveArray::<TimestampMicrosecondType>::from(results.unwrap())
                    .with_data_type(DataType::Timestamp(TimeUnit::Microsecond, local_offset_arc)),
            ) as ArrayRef)),
            ColumnarValue::Scalar(_) => match results.unwrap().first().unwrap() {
                Some(value) => Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    Some(*value),
                    local_offset_arc,
                ))),
                None => Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    None,
                    local_offset_arc,
                ))),
            },
        }
    }
}

fn _timestamp_to_nanoseconds(
    array: &dyn Array,
) -> (Option<Int64Array>, Option<TimeUnit>, Option<Arc<str>>) {
    match array.data_type() {
        DataType::Timestamp(time_unit, tz) => {
            let multiplier = match time_unit {
                TimeUnit::Second => 1_000_000_000,
                TimeUnit::Millisecond => 1_000_000,
                TimeUnit::Microsecond => 1_000,
                TimeUnit::Nanosecond => 1,
            };
            let casted = arrow::compute::kernels::cast::cast(array, &DataType::Int64).unwrap();
            let values = casted
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .map(|nanos_opt| nanos_opt.map(|nanos| nanos * multiplier));
            (
                Some(PrimitiveArray::from_iter(values)),
                Some(*time_unit),
                tz.clone(),
            )
        }
        _ => (None, None, None),
    }
}
