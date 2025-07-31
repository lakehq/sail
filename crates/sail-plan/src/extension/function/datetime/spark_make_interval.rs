use std::any::Any;
use std::sync::Arc;

use arrow::array::{AsArray, Float32Array, Int32Array, IntervalMonthDayNanoBuilder};
use arrow::datatypes::IntervalUnit::MonthDayNano;
use arrow::datatypes::{Float32Type, Int32Type, IntervalMonthDayNano};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::extension::function::error_utils::invalid_arg_count_exec_err;

#[derive(Debug)]
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
        /*
        from pyspark.sql.functions import make_interval
        df = spark.createDataFrame([(2, 5, 3)], ["year", "month", "day"])
        df.select(make_interval(df.year).alias("interval")).show(truncate=False)

        from pyspark.sql.functions import make_interval
        df = spark.createDataFrame([(2, 5, 3)], ["year", "month", "day"])
        df.select(make_interval(df.year, df.month, None, df.day).alias("interval")).show(truncate=False)
                */
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;
        println!("[make_interval] args received ({}): {:?}", args.len(), args);
        println!("args type {:?}", args[0].data_type());
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
                args.push(ColumnarValue::Scalar(ScalarValue::Float32(Some(0.0))));
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
                other => {
                    exec_err!("Unsupported  arg {other:?} for Spark function `make_timestamp_ntz`")
                }
            }
        };

        let to_float32_array_fn = |col: &ColumnarValue| -> Result<Float32Array> {
            match col {
                ColumnarValue::Array(array) => Ok(array.as_primitive::<Float32Type>().to_owned()),
                ColumnarValue::Scalar(ScalarValue::Float32(Some(value))) => {
                    Ok(Float32Array::from_value(*value, number_rows))
                }
                other => {
                    exec_err!("Unsupported arg {other:?} for Spark function `make_timestamp_ntz`")
                }
            }
        };

        let years = to_int32_array_fn(&args[0]);
        let months = to_int32_array_fn(&args[1]);
        let weeks = to_int32_array_fn(&args[2]);
        let days = to_int32_array_fn(&args[3]);
        let hours = to_int32_array_fn(&args[4]);
        let mins = to_int32_array_fn(&args[5]);
        let secs = to_float32_array_fn(&args[6]);
        let years = match years {
            Ok(years) => years,
            Err(_) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(None)))
            }
        };
        let months = match months {
            Ok(months) => months,
            Err(_) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(None)))
            }
        };
        let weeks = match weeks {
            Ok(weeks) => weeks,
            Err(_) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(None)))
            }
        };
        let days = match days {
            Ok(days) => days,
            Err(_) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(None)))
            }
        };
        let hours = match hours {
            Ok(hours) => hours,
            Err(_) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(None)))
            }
        };
        let mins = match mins {
            Ok(mins) => mins,
            Err(_) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(None)))
            }
        };
        let secs = match secs {
            Ok(secs) => secs,
            Err(_) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(None)))
            }
        };
        let mut builder = IntervalMonthDayNanoBuilder::with_capacity(number_rows);
        for i in 0..number_rows {
            let (year, month, week, day, hour, min, sec) = (
                years.value(i),
                months.value(i),
                weeks.value(i),
                days.value(i),
                hours.value(i),
                mins.value(i),
                secs.value(i),
            );

            let interval: IntervalMonthDayNano =
                make_interval_month_day_nano(year, month, week, day, hour, min, sec as f64);
            builder.append_value(interval);
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
                    DataType::Float32
                } else {
                    DataType::Int32
                }
            })
            .collect())
    }
}

fn make_interval_month_day_nano(
    year: i32,
    month: i32,
    week: i32,
    day: i32,
    hour: i32,
    min: i32,
    sec: f64,
) -> IntervalMonthDayNano {
    let months = year * 12 + month;
    let total_days = week * 7 + day;
    let total_nanos = (hour as i64) * 3_600_000_000_000
        + (min as i64) * 60_000_000_000
        + (sec * 1_000_000_000.0).round() as i64;

    IntervalMonthDayNano::new(months, total_days, total_nanos)
}
