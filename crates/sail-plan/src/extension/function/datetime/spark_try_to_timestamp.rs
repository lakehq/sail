use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::functions::datetime::to_timestamp::ToTimestampMicrosFunc;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub struct SparkTryToTimestamp {
    signature: Signature,
}

impl Default for SparkTryToTimestamp {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTryToTimestamp {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkTryToTimestamp {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_try_to_timestamp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::Timestamp(_, Some(tz)) => Ok(DataType::Timestamp(
                TimeUnit::Microsecond,
                Some(Arc::clone(tz)),
            )),
            _ => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        }
    }

    fn invoke_batch(&self, args: &[ColumnarValue], number_rows: usize) -> Result<ColumnarValue> {
        let data_type = args[0].data_type();
        let result = ToTimestampMicrosFunc::new().invoke_batch(args, number_rows);
        match result {
            Ok(result) => Ok(result),
            Err(_) => match data_type {
                DataType::Timestamp(_, Some(tz)) => Ok(ColumnarValue::Scalar(
                    ScalarValue::TimestampMicrosecond(None, Some(tz)),
                )),
                _ => Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    None, None,
                ))),
            },
        }
    }
}
