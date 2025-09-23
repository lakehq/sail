use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, AsArray, Float64Array, Int32Array, IntervalMonthDayNanoBuilder,
};
use datafusion::arrow::datatypes::IntervalUnit::MonthDayNano;
use datafusion::arrow::datatypes::{DataType, Float64Type, Int32Type, IntervalMonthDayNano};
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::error::invalid_arg_count_exec_err;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMakeInterval {
    signature: Signature,
}

impl Default for SparkMakeInterval {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMakeInterval {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkMakeInterval {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "make_interval"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Interval(MonthDayNano))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;
        if args.is_empty() || args.len() > 7 {
            return Err(invalid_arg_count_exec_err(
                "make_interval",
                (1, 7),
                args.len(),
            ));
        }

        // copy structure from sail/crates/sail-plan/src/extension/function/datetime/spark_make_timestamp.rs
        let mut args = args;
        while args.len() < 7 {
            if args.len() == 6 {
                args.push(ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0))));
            } else {
                args.push(ColumnarValue::Scalar(ScalarValue::Int32(Some(0))));
            }
        }

        let to_int32_array_fn = |col: &ColumnarValue| -> Result<Int32Array> {
            match col {
                ColumnarValue::Array(array) => Ok(array.as_primitive::<Int32Type>().to_owned()),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(value))) => {
                    Ok(Int32Array::from_value(*value, number_rows))
                }
                ColumnarValue::Scalar(ScalarValue::Null)
                | ColumnarValue::Scalar(ScalarValue::Int32(None)) => {
                    Ok(Int32Array::from(vec![None; number_rows]))
                }
                other => {
                    exec_err!("Unsupported arg {other:?} for Spark function `make_interval`")
                }
            }
        };

        let to_float64_array_fn = |col: &ColumnarValue| -> Result<Float64Array> {
            match col {
                ColumnarValue::Array(array) => Ok(array.as_primitive::<Float64Type>().to_owned()),
                ColumnarValue::Scalar(ScalarValue::Float64(Some(value))) => {
                    Ok(Float64Array::from_value(*value, number_rows))
                }
                ColumnarValue::Scalar(ScalarValue::Null)
                | ColumnarValue::Scalar(ScalarValue::Float64(None)) => {
                    Ok(Float64Array::from(vec![None; number_rows]))
                }
                other => {
                    exec_err!("Unsupported arg {other:?} for Spark function `make_interval`")
                }
            }
        };

        let years = to_int32_array_fn(&args[0]);
        let months = to_int32_array_fn(&args[1]);
        let weeks = to_int32_array_fn(&args[2]);
        let days = to_int32_array_fn(&args[3]);
        let hours = to_int32_array_fn(&args[4]);
        let mins = to_int32_array_fn(&args[5]);
        let secs = to_float64_array_fn(&args[6]);
        let years = match years {
            Ok(years) => years,
            Err(_) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(
                    None,
                )))
            }
        };
        let months = match months {
            Ok(months) => months,
            Err(_) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(
                    None,
                )))
            }
        };
        let weeks = match weeks {
            Ok(weeks) => weeks,
            Err(_) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(
                    None,
                )))
            }
        };
        let days = match days {
            Ok(days) => days,
            Err(_) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(
                    None,
                )))
            }
        };
        let hours = match hours {
            Ok(hours) => hours,
            Err(_) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(
                    None,
                )))
            }
        };
        let mins = match mins {
            Ok(mins) => mins,
            Err(_) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(
                    None,
                )))
            }
        };
        let secs = match secs {
            Ok(secs) => secs,
            Err(_) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(
                    None,
                )))
            }
        };
        let mut builder = IntervalMonthDayNanoBuilder::with_capacity(number_rows);
        for i in 0..number_rows {
            if years.is_null(i)
                || months.is_null(i)
                || weeks.is_null(i)
                || days.is_null(i)
                || hours.is_null(i)
                || mins.is_null(i)
                || secs.is_null(i)
            {
                builder.append_null();
                continue;
            }
            match make_interval_month_day_nano(
                years.value(i),
                months.value(i),
                weeks.value(i),
                days.value(i),
                hours.value(i),
                mins.value(i),
                secs.value(i),
            ) {
                Ok(interval) => builder.append_value(interval),
                Err(_) => builder.append_null(),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() || arg_types.len() > 7 {
            return Err(invalid_arg_count_exec_err(
                "make_interval",
                (1, 7),
                arg_types.len(),
            ));
        }

        Ok((0..arg_types.len())
            .map(|i| {
                if i == 6 {
                    DataType::Float64
                } else {
                    DataType::Int32
                }
            })
            .collect())
    }
}

pub fn make_interval_month_day_nano(
    year: i32,
    month: i32,
    week: i32,
    day: i32,
    hour: i32,
    min: i32,
    sec: f64,
) -> Result<IntervalMonthDayNano> {
    use datafusion_common::DataFusionError;

    if !sec.is_finite() {
        return Err(DataFusionError::Execution("seconds is NaN/Inf".into()));
    }

    let months = year
        .checked_mul(12)
        .and_then(|v| v.checked_add(month))
        .ok_or_else(|| DataFusionError::Execution("months overflow".into()))?;

    let total_days = week
        .checked_mul(7)
        .and_then(|v| v.checked_add(day))
        .ok_or_else(|| DataFusionError::Execution("days overflow".into()))?;

    let hours_nanos = (hour as i64)
        .checked_mul(3_600_000_000_000)
        .ok_or_else(|| DataFusionError::Execution("hours to nanos overflow".into()))?;
    let mins_nanos = (min as i64)
        .checked_mul(60_000_000_000)
        .ok_or_else(|| DataFusionError::Execution("minutes to nanos overflow".into()))?;

    let sec_int = sec.trunc() as i64;
    let frac = sec - sec.trunc();
    let mut frac_nanos = (frac * 1_000_000_000.0).round() as i64;

    if frac_nanos.abs() >= 1_000_000_000 {
        if frac_nanos > 0 {
            frac_nanos -= 1_000_000_000;
        } else {
            frac_nanos += 1_000_000_000;
        }
    }

    let secs_nanos = sec_int
        .checked_mul(1_000_000_000)
        .ok_or_else(|| DataFusionError::Execution("seconds to nanos overflow".into()))?;

    let total_nanos = hours_nanos
        .checked_add(mins_nanos)
        .and_then(|v| v.checked_add(secs_nanos))
        .and_then(|v| v.checked_add(frac_nanos))
        .ok_or_else(|| DataFusionError::Execution("sum nanos overflow".into()))?;

    Ok(IntervalMonthDayNano::new(months, total_days, total_nanos))
}
