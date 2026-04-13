use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::temporal_conversions::timestamp_us_to_datetime;
use datafusion::arrow::array::{ArrayRef, AsArray, PrimitiveArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit, TimestampMicrosecondType};
use datafusion_common::{exec_datafusion_err, exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use super::spark_trunc::trunc_naive_date;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDateTrunc {
    signature: Signature,
}

impl Default for SparkDateTrunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDateTrunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

/// Truncate a microsecond timestamp to the period specified by `format`.
///
/// Fine granularities (microsecond, millisecond, second, minute, hour, day) use
/// simple modular arithmetic.  Coarse granularities (week, month, quarter, year)
/// convert the microsecond value to a [`chrono::NaiveDateTime`] using
/// [`timestamp_us_to_datetime`] — which never multiplies by 1,000 and therefore
/// avoids the nanosecond-overflow present in DataFusion's built-in `date_trunc`
/// for dates outside the range 1677-09-21 … 2262-04-11.
fn trunc_timestamp_micros(micros: i64, granularity: &str) -> Result<i64> {
    const MICROS_PER_MILLIS: i64 = 1_000;
    const MICROS_PER_SECOND: i64 = 1_000_000;
    const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SECOND;
    const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;
    const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;

    match granularity.trim().to_uppercase().as_str() {
        "MICROSECOND" => Ok(micros),
        "MILLISECOND" => Ok(micros - micros.rem_euclid(MICROS_PER_MILLIS)),
        "SECOND" => Ok(micros - micros.rem_euclid(MICROS_PER_SECOND)),
        "MINUTE" => Ok(micros - micros.rem_euclid(MICROS_PER_MINUTE)),
        "HOUR" => Ok(micros - micros.rem_euclid(MICROS_PER_HOUR)),
        "DAY" | "DD" => Ok(micros - micros.rem_euclid(MICROS_PER_DAY)),
        // Coarse granularities: convert to NaiveDateTime, truncate the date, return midnight.
        "WEEK" | "MONTH" | "MON" | "MM" | "QUARTER" | "YEAR" | "YYYY" | "YY" => {
            let dt = timestamp_us_to_datetime(micros).ok_or_else(|| {
                exec_datafusion_err!("date_trunc: unable to convert timestamp {micros} to datetime")
            })?;
            let truncated_date = trunc_naive_date(dt.date(), granularity)?;
            let truncated_micros = truncated_date
                .and_hms_micro_opt(0, 0, 0, 0)
                .ok_or_else(|| {
                    exec_datafusion_err!(
                        "date_trunc: unable to create midnight datetime for {}",
                        truncated_date
                    )
                })?
                .and_utc()
                .timestamp_micros();
            Ok(truncated_micros)
        }
        _ => exec_err!("date_trunc: unsupported granularity '{granularity}'"),
    }
}

fn return_type_for(arg1: &DataType) -> DataType {
    match arg1 {
        DataType::Timestamp(_, tz) => DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
        _ => DataType::Timestamp(TimeUnit::Microsecond, None),
    }
}

impl ScalarUDFImpl for SparkDateTrunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_date_trunc"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(return_type_for(arg_types.get(1).unwrap_or(&DataType::Null)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let [granularity_arg, ts_arg] = args.as_slice() else {
            return exec_err!(
                "Spark `date_trunc` function requires 2 arguments, got {}",
                args.len()
            );
        };

        // Granularity must be a non-null scalar string.
        let granularity = match granularity_arg {
            ColumnarValue::Scalar(
                ScalarValue::Utf8(Some(s))
                | ScalarValue::LargeUtf8(Some(s))
                | ScalarValue::Utf8View(Some(s)),
            ) => s.clone(),
            ColumnarValue::Scalar(
                ScalarValue::Utf8(None)
                | ScalarValue::LargeUtf8(None)
                | ScalarValue::Utf8View(None)
                | ScalarValue::Null,
            ) => {
                let tz = match ts_arg.data_type() {
                    DataType::Timestamp(_, tz) => tz,
                    _ => None,
                };
                return match ts_arg {
                    ColumnarValue::Array(arr) => {
                        let null_arr: Arc<dyn datafusion::arrow::array::Array> = Arc::new(
                            PrimitiveArray::<TimestampMicrosecondType>::new_null(arr.len()),
                        );
                        Ok(ColumnarValue::Array(null_arr))
                    }
                    _ => Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                        None, tz,
                    ))),
                };
            }
            _ => {
                return exec_err!(
                "Spark `date_trunc` function: granularity must be a scalar string, got {granularity_arg:?}"
            )
            }
        };

        match ts_arg {
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(micros_opt, tz)) => {
                let result = if let Some(micros) = micros_opt {
                    Some(trunc_timestamp_micros(*micros, &granularity)?)
                } else {
                    None
                };
                Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    result,
                    tz.clone(),
                )))
            }
            ColumnarValue::Array(array)
                if matches!(
                    array.data_type(),
                    DataType::Timestamp(TimeUnit::Microsecond, _)
                ) =>
            {
                let tz = match array.data_type() {
                    DataType::Timestamp(_, tz) => tz.clone(),
                    _ => None,
                };
                let prim = array.as_primitive::<TimestampMicrosecondType>();
                let result: PrimitiveArray<TimestampMicrosecondType> = prim
                    .iter()
                    .map(|micros_opt| {
                        if let Some(micros) = micros_opt {
                            trunc_timestamp_micros(micros, &granularity).map(Some)
                        } else {
                            Ok(None)
                        }
                    })
                    .collect::<Result<Vec<Option<i64>>>>()?
                    .into_iter()
                    .collect::<PrimitiveArray<TimestampMicrosecondType>>()
                    .with_timezone_opt(tz);
                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }
            other => {
                exec_err!("Unsupported second argument {other:?} for Spark function `date_trunc`")
            }
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return exec_err!(
                "Spark `date_trunc` function requires 2 arguments, got {}",
                arg_types.len()
            );
        }
        // The timestamp argument: preserve timezone if already a Timestamp, but normalise to
        // Microsecond precision.  Date32/Date64 are promoted to Timestamp(Microsecond, None).
        let ts_type = match &arg_types[1] {
            DataType::Timestamp(_, tz) => DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
            _ => DataType::Timestamp(TimeUnit::Microsecond, None),
        };
        Ok(vec![DataType::Utf8, ts_type])
    }
}
