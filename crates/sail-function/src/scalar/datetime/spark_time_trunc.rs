use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, PrimitiveBuilder};
use datafusion::arrow::datatypes::{DataType, Time64MicrosecondType, TimeUnit};
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::scalar::datetime::utils::to_time64_array;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTimeTrunc {
    signature: Signature,
}

impl Default for SparkTimeTrunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTimeTrunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

/// Returns the truncation divisor in microseconds for a given unit string.
/// Returns `None` for unrecognized units.
fn truncation_divisor(unit: &str) -> Option<i64> {
    match unit.to_uppercase().as_str() {
        "MICROSECOND" => Some(1),
        "MILLISECOND" => Some(1_000),
        "SECOND" => Some(1_000_000),
        "MINUTE" => Some(60_000_000),
        "HOUR" => Some(3_600_000_000),
        _ => None,
    }
}

impl ScalarUDFImpl for SparkTimeTrunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_time_trunc"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Time64(TimeUnit::Microsecond))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;

        if args.len() != 2 {
            return exec_err!(
                "Spark `time_trunc` function requires 2 arguments, got {}",
                args.len()
            );
        }

        let contains_scalar_null = args.iter().any(|arg| {
            matches!(arg, ColumnarValue::Scalar(ScalarValue::Utf8(None)))
                || matches!(arg, ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)))
                || matches!(
                    arg,
                    ColumnarValue::Scalar(ScalarValue::Time64Microsecond(None))
                )
                || matches!(arg, ColumnarValue::Scalar(ScalarValue::Null))
        });
        if contains_scalar_null {
            return Ok(ColumnarValue::Scalar(ScalarValue::Time64Microsecond(None)));
        }

        // Extract unit string (must be a scalar/literal)
        let unit_str = match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.clone(),
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => s.clone(),
            _ => {
                return exec_err!("time_trunc: unit must be a string literal");
            }
        };

        let divisor = match truncation_divisor(&unit_str) {
            Some(d) => d,
            None => {
                return exec_err!(
                    "time_trunc: unsupported unit '{}'. Supported: HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND",
                    unit_str
                );
            }
        };

        let times = to_time64_array(&args[1], "time", "time_trunc", number_rows)?;

        let mut builder = PrimitiveBuilder::<Time64MicrosecondType>::with_capacity(number_rows);
        for i in 0..number_rows {
            if times.is_null(i) {
                builder.append_null();
            } else {
                let val = times.value(i);
                builder.append_value(val - (val % divisor));
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return exec_err!(
                "Spark `time_trunc` function requires 2 arguments, got {}",
                arg_types.len()
            );
        }
        Ok(vec![
            DataType::Utf8,
            DataType::Time64(TimeUnit::Microsecond),
        ])
    }
}
