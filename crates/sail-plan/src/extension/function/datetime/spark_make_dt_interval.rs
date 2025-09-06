use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, AsArray, Float64Array, Int32Array, IntervalMonthDayNanoBuilder};
use arrow::datatypes::IntervalUnit::MonthDayNano;
use arrow::datatypes::{Float64Type, Int32Type};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::extension::function::datetime::spark_make_interval::make_interval_month_day_nano;
use crate::extension::function::error_utils::invalid_arg_count_exec_err;

#[derive(Debug)]
pub struct SparkMakeDtInterval {
    signature: Signature,
}

impl Default for SparkMakeDtInterval {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMakeDtInterval {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkMakeDtInterval {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "make_dt_interval"
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
        if args.is_empty() || args.len() > 4 {
            return Err(invalid_arg_count_exec_err(
                "make_dt_interval",
                (1, 4),
                args.len(),
            ));
        }

        let mut args = args;
        while args.len() < 4 {
            if args.len() == 3 {
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
                    exec_err!("Unsupported arg {other:?} for Spark function `make_dt_interval`")
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
                    exec_err!("Unsupported arg {other:?} for Spark function `make_dt_interval`")
                }
            }
        };

        let days = to_int32_array_fn(&args[0]);
        let hours = to_int32_array_fn(&args[1]);
        let mins = to_int32_array_fn(&args[2]);
        let secs = to_float64_array_fn(&args[3]);

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
            if days.is_null(i) || hours.is_null(i) || mins.is_null(i) || secs.is_null(i) {
                builder.append_null();
                continue;
            }
            match make_interval_month_day_nano(
                0,
                0,
                0,
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
        if arg_types.is_empty() || arg_types.len() > 4 {
            return Err(invalid_arg_count_exec_err(
                "make_dt_interval",
                (1, 4),
                arg_types.len(),
            ));
        }

        Ok((0..arg_types.len())
            .map(|i| {
                if i == 3 {
                    DataType::Float64
                } else {
                    DataType::Int32
                }
            })
            .collect())
    }
}
