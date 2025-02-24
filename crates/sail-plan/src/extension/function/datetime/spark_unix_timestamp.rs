use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::functions::datetime::to_timestamp::ToTimestampSecondsFunc;
use datafusion_common::{exec_err, Result};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

use crate::extension::function::datetime::datetime_utils::validate_data_types;

#[derive(Debug)]
pub struct SparkUnixTimestamp {
    signature: Signature,
    timezone: Arc<str>,
}

impl SparkUnixTimestamp {
    pub fn new(timezone: Arc<str>) -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            timezone,
        }
    }

    pub fn timezone(&self) -> &str {
        &self.timezone
    }
}

impl ScalarUDFImpl for SparkUnixTimestamp {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_unix_timestamp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], number_rows: usize) -> Result<ColumnarValue> {
        if args.is_empty() {
            return exec_err!("spark_unix_timestamp function requires 1 or more arguments");
        }

        if args.len() > 1 {
            validate_data_types(args, "spark_unix_timestamp", 1)?;
        }

        match args[0].data_type() {
            DataType::Int32 | DataType::Int64 => args[0]
                .cast_to(
                    &DataType::Timestamp(TimeUnit::Second, Some(self.timezone.clone())),
                    None,
                )?
                .cast_to(&DataType::Int64, None),
            DataType::Date64 | DataType::Date32 | DataType::Timestamp(_, None) => args[0]
                .cast_to(
                    &DataType::Timestamp(TimeUnit::Second, Some(self.timezone.clone())),
                    None,
                )?
                .cast_to(&DataType::Int64, None),
            DataType::Utf8View | DataType::LargeUtf8 | DataType::Utf8 => {
                ToTimestampSecondsFunc::new()
                    .invoke_batch(args, number_rows)?
                    .cast_to(
                        &DataType::Timestamp(TimeUnit::Second, Some(self.timezone.clone())),
                        None,
                    )?
                    .cast_to(&DataType::Int64, None)
            }
            other => {
                exec_err!("spark_unix_timestamp function unsupported data type: {other}")
            }
        }
    }
}
