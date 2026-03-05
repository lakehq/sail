use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::types::TimestampMicrosecondType;
use datafusion::arrow::array::{Array, PrimitiveBuilder};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::scalar::datetime::spark_make_timestamp::{
    make_timestamp_from_date_time, make_timestamp_ntz,
};
use crate::scalar::datetime::utils::{
    to_date32_array, to_float64_array, to_int32_array, to_time64_array, to_uint32_array,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTryMakeTimestampNtz {
    signature: Signature,
}

impl Default for SparkTryMakeTimestampNtz {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTryMakeTimestampNtz {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkTryMakeTimestampNtz {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_try_make_timestamp_ntz"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;

        // Handle 2-arg variant: try_make_timestamp_ntz(date, time)
        if args.len() == 2 {
            return self.invoke_2_arg(&args, number_rows);
        }

        // Handle 6-arg variant: try_make_timestamp_ntz(year, month, day, hour, min, sec)
        if args.len() == 6 {
            return self.invoke_6_arg(&args, number_rows);
        }

        exec_err!(
            "Spark `try_make_timestamp_ntz` function requires 2 or 6 arguments, got {}",
            args.len()
        )
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() == 2 {
            return Ok(vec![
                DataType::Date32,
                DataType::Time64(TimeUnit::Microsecond),
            ]);
        }

        if arg_types.len() == 6 {
            return Ok(vec![
                DataType::Int32,
                DataType::UInt32,
                DataType::UInt32,
                DataType::UInt32,
                DataType::UInt32,
                DataType::Float64,
            ]);
        }

        exec_err!(
            "Spark `try_make_timestamp_ntz` function requires 2 or 6 arguments, got {}",
            arg_types.len()
        )
    }
}

impl SparkTryMakeTimestampNtz {
    fn invoke_2_arg(&self, args: &[ColumnarValue], number_rows: usize) -> Result<ColumnarValue> {
        let contains_scalar_null = args.iter().any(|arg| {
            matches!(arg, ColumnarValue::Scalar(ScalarValue::Date32(None)))
                || matches!(
                    arg,
                    ColumnarValue::Scalar(ScalarValue::Time64Microsecond(None))
                )
        });

        if contains_scalar_null {
            // try_ variant: always returns NULL on error, never throws
            return Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                None, None,
            )));
        }

        let dates = to_date32_array(&args[0], "date", "try_make_timestamp_ntz", number_rows)?;
        let times = to_time64_array(&args[1], "time", "try_make_timestamp_ntz", number_rows)?;

        let mut builder = PrimitiveBuilder::<TimestampMicrosecondType>::with_capacity(number_rows);

        for i in 0..number_rows {
            if dates.is_null(i) || times.is_null(i) {
                builder.append_null();
                continue;
            }

            let date_val = dates.value(i);
            let time_val = times.value(i);

            // try_ variant: always returns NULL on error, never throws
            match make_timestamp_from_date_time(date_val, time_val) {
                Some(micros) => builder.append_value(micros),
                None => builder.append_null(),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }

    fn invoke_6_arg(&self, args: &[ColumnarValue], number_rows: usize) -> Result<ColumnarValue> {
        // Check for scalar nulls
        let contains_scalar_null = args.iter().any(|arg| {
            matches!(arg, ColumnarValue::Scalar(ScalarValue::Int32(None)))
                || matches!(arg, ColumnarValue::Scalar(ScalarValue::UInt32(None)))
                || matches!(arg, ColumnarValue::Scalar(ScalarValue::Float64(None)))
        });

        if contains_scalar_null {
            return Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                None, None,
            )));
        }

        // Convert all arguments to arrays using shared utilities
        let years = to_int32_array(&args[0], "years", "try_make_timestamp_ntz", number_rows)?;
        let months = to_uint32_array(&args[1], "months", "try_make_timestamp_ntz", number_rows)?;
        let days = to_uint32_array(&args[2], "days", "try_make_timestamp_ntz", number_rows)?;
        let hours = to_uint32_array(&args[3], "hours", "try_make_timestamp_ntz", number_rows)?;
        let mins = to_uint32_array(&args[4], "mins", "try_make_timestamp_ntz", number_rows)?;
        let secs = to_float64_array(&args[5], "secs", "try_make_timestamp_ntz", number_rows)?;

        let mut builder = PrimitiveBuilder::<TimestampMicrosecondType>::with_capacity(number_rows);

        for i in 0..number_rows {
            let (year, month, day, hour, min, sec) = (
                years.value(i),
                months.value(i),
                days.value(i),
                hours.value(i),
                mins.value(i),
                secs.value(i),
            );

            // try_ variant: always returns NULL on error, never throws
            match make_timestamp_ntz(year, month, day, hour, min, sec) {
                Some(micros) => builder.append_value(micros),
                None => builder.append_null(),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}
