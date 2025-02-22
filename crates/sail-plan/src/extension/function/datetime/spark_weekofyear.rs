use std::any::Any;
use std::sync::Arc;

use chrono::{DateTime, Datelike};
use datafusion::arrow::array::{AsArray, PrimitiveBuilder};
use datafusion::arrow::datatypes::{DataType, Int64Type, TimeUnit, UInt32Type};
use datafusion::functions::datetime::to_timestamp::ToTimestampNanosFunc;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

/// Returns the week of the year of the given date expressed as the number of days from 1970-01-01.
/// A week is considered to start on a Monday and week 1 is the first week with > 3 days.
#[derive(Debug)]
pub struct SparkWeekOfYear {
    signature: Signature,
    timezone: Arc<str>,
}

impl SparkWeekOfYear {
    pub fn new(timezone: Arc<str>) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            timezone,
        }
    }

    pub fn timezone(&self) -> &str {
        &self.timezone
    }
}

impl ScalarUDFImpl for SparkWeekOfYear {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_weekofyear"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return exec_err!(
                "Spark `weekofyear` function requires 1 argument, got {}",
                arg_types.len()
            );
        }
        Ok(DataType::UInt32)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], number_rows: usize) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!(
                "Spark `weekofyear` function requires 1 argument, got {}",
                args.len()
            );
        }

        let timestamp_nanos = match args[0].data_type() {
            DataType::Int32 | DataType::Int64 => args[0]
                .cast_to(
                    &DataType::Timestamp(TimeUnit::Nanosecond, Some(self.timezone.clone())),
                    None,
                )?
                .cast_to(&DataType::Int64, None),
            DataType::Date64 | DataType::Date32 | DataType::Timestamp(_, None) => args[0]
                .cast_to(
                    &DataType::Timestamp(TimeUnit::Nanosecond, Some(self.timezone.clone())),
                    None,
                )?
                .cast_to(&DataType::Int64, None),
            DataType::Utf8View | DataType::LargeUtf8 | DataType::Utf8 => {
                ToTimestampNanosFunc::new()
                    .invoke_batch(args, number_rows)?
                    .cast_to(
                        &DataType::Timestamp(TimeUnit::Nanosecond, Some(self.timezone.clone())),
                        None,
                    )?
                    .cast_to(&DataType::Int64, None)
            }
            other => {
                exec_err!("Spark `weekofyear` function unsupported data type: {other}")
            }
        }?;

        match timestamp_nanos {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(value))) => {
                let datetime = DateTime::from_timestamp_nanos(value);
                let week_of_year = datetime.iso_week().week();
                Ok(ColumnarValue::Scalar(ScalarValue::UInt32(Some(
                    week_of_year,
                ))))
            }
            ColumnarValue::Array(array) => {
                let array_len = array.len();
                let int64_array = array.as_primitive::<Int64Type>();
                let mut builder = PrimitiveBuilder::<UInt32Type>::with_capacity(array_len);
                for i in 0..array_len {
                    let value = int64_array.value(i);
                    builder.append_value(DateTime::from_timestamp_nanos(value).iso_week().week())
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            other => exec_err!(
                "Spark `weekofyear` function requires a valid timestamp value, got: {other:?}"
            ),
        }
    }
}
