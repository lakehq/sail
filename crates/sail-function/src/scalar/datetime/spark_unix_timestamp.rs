use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::functions::datetime::to_timestamp::ToTimestampSecondsFunc;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::scalar::datetime::utils::validate_data_types;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkUnixTimestamp {
    signature: Signature,
    timezone: Arc<str>,
    is_try: bool,
}

impl SparkUnixTimestamp {
    pub fn new(timezone: Arc<str>) -> Self {
        Self::new_with_try(timezone, false)
    }

    /// Creates a SparkUnixTimestamp.
    ///
    /// When `is_try` is true, returns NULL on invalid input (ANSI=false behavior).
    /// When `is_try` is false, throws an error on invalid input (ANSI=true behavior).
    pub fn new_with_try(timezone: Arc<str>, is_try: bool) -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            timezone,
            is_try,
        }
    }

    pub fn timezone(&self) -> &str {
        &self.timezone
    }

    pub fn is_try(&self) -> bool {
        self.is_try
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [first, ..] = args.args.as_slice() else {
            return exec_err!("spark_unix_timestamp function requires 1 or more arguments");
        };
        if args.args.len() > 1 {
            validate_data_types(&args.args, "spark_unix_timestamp", 1)?;
        }

        match first.data_type() {
            DataType::Int32 | DataType::Int64 => args.args[0]
                .cast_to(
                    &DataType::Timestamp(TimeUnit::Second, Some(self.timezone.clone())),
                    None,
                )?
                .cast_to(&DataType::Int64, None),
            DataType::Date64 | DataType::Date32 | DataType::Timestamp(_, None) => args.args[0]
                .cast_to(
                    &DataType::Timestamp(TimeUnit::Second, Some(self.timezone.clone())),
                    None,
                )?
                .cast_to(&DataType::Int64, None),
            DataType::Utf8View | DataType::LargeUtf8 | DataType::Utf8 => {
                let result = ToTimestampSecondsFunc::new_with_config(args.config_options.as_ref())
                    .invoke_with_args(args);
                match result {
                    Ok(col) => col
                        .cast_to(
                            &DataType::Timestamp(TimeUnit::Second, Some(self.timezone.clone())),
                            None,
                        )?
                        .cast_to(&DataType::Int64, None),
                    Err(_) if self.is_try => Ok(ColumnarValue::Scalar(ScalarValue::Int64(None))),
                    Err(e) => Err(e),
                }
            }
            other => {
                exec_err!("spark_unix_timestamp function unsupported data type: {other}")
            }
        }
    }
}
