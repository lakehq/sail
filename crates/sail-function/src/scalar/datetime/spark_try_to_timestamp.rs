use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion_common::{Result, plan_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::scalar::datetime::spark_timestamp::SparkTimestamp;
use crate::udf_utils::arg_data_types;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTryToTimestamp {
    timezone: Option<Arc<str>>,
    signature: Signature,
}

impl Default for SparkTryToTimestamp {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTryToTimestamp {
    pub fn new() -> Self {
        Self::try_new(None)
    }

    fn output_type(&self, arg_types: &[DataType]) -> Result<DataType> {
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

    pub fn try_new(timezone: Option<Arc<str>>) -> Self {
        Self {
            timezone,
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }

    pub fn timezone(&self) -> Option<&str> {
        self.timezone.as_deref()
    }
}

impl ScalarUDFImpl for SparkTryToTimestamp {
    fn name(&self) -> &str {
        "spark_try_to_timestamp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    crate::unused_return_type!();

    // `try_*` swallows the failure and yields NULL, so the output is always nullable
    // (TryEval.scala).
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let data_type = self.output_type(&arg_data_types(&args))?;
        Ok(Arc::new(Field::new(self.name(), data_type, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Delegate to SparkTimestamp with is_try=true
        // This will return NULL on parse failure instead of raising an error
        // ansi_mode is set to true for try_to_timestamp (safe parsing)
        let spark_timestamp = SparkTimestamp::try_new(self.timezone.clone(), true, true)?;
        spark_timestamp.invoke_with_args(args)
    }
}
