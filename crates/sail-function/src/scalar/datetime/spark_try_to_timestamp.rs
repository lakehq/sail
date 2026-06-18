use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::{plan_err, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::scalar::datetime::spark_timestamp::SparkTimestamp;

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
        // Delegate to SparkTimestamp with is_try=true
        // This will return NULL on parse failure instead of raising an error
        let spark_timestamp = SparkTimestamp::try_new(None, true)?;
        spark_timestamp.invoke_with_args(args)
    }
}
