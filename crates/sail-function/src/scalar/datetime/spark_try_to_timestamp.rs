use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::functions::datetime::to_timestamp::ToTimestampMicrosFunc;
use datafusion_common::{plan_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug, PartialEq, Eq, Hash)]
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
        let [first, ..] = arg_types else {
            return plan_err!("`try_to_timestamp` function requires at least 1 argument");
        };
        match first {
            DataType::Timestamp(_, Some(tz)) => Ok(DataType::Timestamp(
                TimeUnit::Microsecond,
                Some(Arc::clone(tz)),
            )),
            _ => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [first, ..] = args.args.as_slice() else {
            return plan_err!("`try_to_timestamp` function requires at least 1 argument");
        };
        let data_type = first.data_type();
        let result = ToTimestampMicrosFunc::new_with_config(args.config_options.as_ref())
            .invoke_with_args(args);
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
