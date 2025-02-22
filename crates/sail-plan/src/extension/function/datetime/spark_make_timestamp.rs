use std::any::Any;
use std::sync::Arc;

use chrono::NaiveDate;
use datafusion::arrow::array::{
    AsArray, Float32Array, Int32Array, PrimitiveArray, PrimitiveBuilder, UInt32Array,
};
use datafusion::arrow::datatypes::{
    DataType, Float32Type, Int32Type, TimeUnit, TimestampMicrosecondType, UInt32Type,
};
use datafusion_common::types::NativeType;
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub struct SparkMakeTimestampNtz {
    signature: Signature,
}

impl Default for SparkMakeTimestampNtz {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMakeTimestampNtz {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkMakeTimestampNtz {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_make_timestamp_ntz"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
    }

    fn invoke_batch(&self, args: &[ColumnarValue], number_rows: usize) -> Result<ColumnarValue> {
        if args.len() != 6 {
            return exec_err!(
                "Spark `make_timestamp_ntz` function requires 6 arguments, got {}",
                args.len()
            );
        }

        let contains_scalar_null = args.iter().any(|arg| {
            matches!(arg, ColumnarValue::Scalar(ScalarValue::Int64(None)))
                || matches!(arg, ColumnarValue::Scalar(ScalarValue::Float32(None)))
        });

        if contains_scalar_null {
            // TODO: If the configuration spark.sql.ansi.enabled is false,
            //  the function returns NULL on invalid inputs. Otherwise, it will throw an error.
            return Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                None, None,
            )));
        }

        let to_int32_array_fn = |col: &ColumnarValue, arg_name: &str| -> Result<Int32Array> {
            match col {
                ColumnarValue::Array(array) => Ok(array.as_primitive::<Int32Type>().to_owned()),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(value))) => {
                    Ok(Int32Array::from_value(*value, number_rows))
                }
                other => {
                    exec_err!(
                    "Unsupported {arg_name} arg {other:?} for Spark function `make_timestamp_ntz`"
                )
                }
            }
        };

        let to_uint32_array_fn = |col: &ColumnarValue, arg_name: &str| -> Result<UInt32Array> {
            match col {
                ColumnarValue::Array(array) => Ok(array.as_primitive::<UInt32Type>().to_owned()),
                ColumnarValue::Scalar(ScalarValue::UInt32(Some(value))) => {
                    Ok(UInt32Array::from_value(*value, number_rows))
                }
                other => {
                    exec_err!(
                    "Unsupported {arg_name} arg {other:?} for Spark function `make_timestamp_ntz`"
                )
                }
            }
        };

        let to_float32_array_fn = |col: &ColumnarValue, arg_name: &str| -> Result<Float32Array> {
            match col {
                ColumnarValue::Array(array) => Ok(array.as_primitive::<Float32Type>().to_owned()),
                ColumnarValue::Scalar(ScalarValue::Float32(Some(value))) => {
                    Ok(Float32Array::from_value(*value, number_rows))
                }
                other => {
                    exec_err!(
                    "Unsupported {arg_name} arg {other:?} for Spark function `make_timestamp_ntz`"
                )
                }
            }
        };

        // TODO: If the configuration spark.sql.ansi.enabled is false,
        //  the function returns NULL on invalid inputs. Otherwise, it will throw an error.
        let (years, months, days, hours, mins, secs) = (
            to_int32_array_fn(&args[0], "years"),
            to_uint32_array_fn(&args[1], "months"),
            to_uint32_array_fn(&args[2], "days"),
            to_uint32_array_fn(&args[3], "hours"),
            to_uint32_array_fn(&args[4], "mins"),
            to_float32_array_fn(&args[5], "secs"),
        );
        let years = match years {
            Ok(years) => years,
            Err(_) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    None, None,
                )))
            }
        };
        let months = match months {
            Ok(months) => months,
            Err(_) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    None, None,
                )))
            }
        };
        let days = match days {
            Ok(days) => days,
            Err(_) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    None, None,
                )))
            }
        };
        let hours = match hours {
            Ok(hours) => hours,
            Err(_) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    None, None,
                )))
            }
        };
        let mins = match mins {
            Ok(mins) => mins,
            Err(_) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    None, None,
                )))
            }
        };
        let secs = match secs {
            Ok(secs) => secs,
            Err(_) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    None, None,
                )))
            }
        };

        let mut builder: PrimitiveBuilder<TimestampMicrosecondType> =
            PrimitiveArray::builder(number_rows);
        for i in 0..number_rows {
            let (year, month, day, hour, min, sec) = (
                years.value(i),
                months.value(i),
                days.value(i),
                hours.value(i),
                mins.value(i),
                secs.value(i),
            );
            match make_timestamp_ntz(year, month, day, hour, min, sec) {
                Some(micros) => builder.append_value(micros),
                None => {
                    // TODO: If the configuration spark.sql.ansi.enabled is false,
                    //  the function returns NULL on invalid inputs. Otherwise, it will throw an error.
                    builder.append_null()
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 6 {
            return exec_err!(
                "Spark `make_timestamp_ntz` function requires 6 arguments, got {}",
                arg_types.len()
            );
        }

        let (years, months, days, hours, mins, secs): (
            NativeType,
            NativeType,
            NativeType,
            NativeType,
            NativeType,
            NativeType,
        ) = (
            (&arg_types[0]).into(),
            (&arg_types[1]).into(),
            (&arg_types[2]).into(),
            (&arg_types[3]).into(),
            (&arg_types[4]).into(),
            (&arg_types[5]).into(),
        );
        if (years.is_integer()
            || matches!(years, NativeType::String)
            || matches!(years, NativeType::Null))
            && (months.is_integer()
                || matches!(months, NativeType::String)
                || matches!(months, NativeType::Null))
            && (days.is_integer()
                || matches!(days, NativeType::String)
                || matches!(days, NativeType::Null))
            && (hours.is_integer()
                || matches!(hours, NativeType::String)
                || matches!(hours, NativeType::Null))
            && (mins.is_integer()
                || matches!(mins, NativeType::String)
                || matches!(mins, NativeType::Null))
            && (secs.is_numeric()
                || matches!(secs, NativeType::String)
                || matches!(secs, NativeType::Null))
        {
            Ok(vec![
                DataType::Int32,
                DataType::UInt32,
                DataType::UInt32,
                DataType::UInt32,
                DataType::UInt32,
                DataType::Float32,
            ])
        } else {
            plan_err!(
                "The arguments of Spark `make_timestamp_ntz` must be integer or string or null"
            )
        }
    }
}

fn make_timestamp_ntz(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    min: u32,
    sec: f32,
) -> Option<i64> {
    if !(1..=9999).contains(&year)
        || !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || !(0..=23).contains(&hour)
        || !(0..=59).contains(&min)
        || !(0.0..=60.0).contains(&sec)
    {
        // TODO: If the configuration spark.sql.ansi.enabled is false,
        //  the function returns NULL on invalid inputs. Otherwise, it will throw an error.
        return None;
    }

    // If the sec argument equals to 60, the seconds field is set to 0 and 1 minute is added to the final timestamp.
    let (min, sec, micro) = if sec == 60.0 {
        (min + 1, 0u32, 0u32)
    } else {
        let sec_int = sec as u32;
        let micro = ((sec - sec_int as f32) * 1_000_000.0) as u32;
        (min, sec_int, micro)
    };

    NaiveDate::from_ymd_opt(year, month, day)
        .and_then(|date| date.and_hms_micro_opt(hour, min, sec, micro))
        .map(|dt| dt.and_utc().timestamp_micros())
}
