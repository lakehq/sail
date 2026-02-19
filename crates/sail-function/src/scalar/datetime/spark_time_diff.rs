use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Builder};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::scalar::datetime::utils::to_time64_array;

/// Returns the divisor in microseconds for a given time_diff unit string.
/// Returns `None` for unsupported units.
fn unit_divisor(unit: &str) -> Option<i64> {
    match unit.to_uppercase().as_str() {
        "MICROSECOND" => Some(1),
        "MILLISECOND" => Some(1_000),
        "SECOND" => Some(1_000_000),
        "MINUTE" => Some(60_000_000),
        "HOUR" => Some(3_600_000_000),
        _ => None,
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTimeDiff {
    signature: Signature,
}

impl Default for SparkTimeDiff {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTimeDiff {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkTimeDiff {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_time_diff"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;

        if args.len() != 3 {
            return exec_err!(
                "Spark `time_diff` function requires 3 arguments, got {}",
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
            return Ok(ColumnarValue::Scalar(ScalarValue::Int64(None)));
        }

        // Extract unit string (must be a scalar/literal)
        let unit_str = match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.clone(),
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => s.clone(),
            _ => {
                return exec_err!("time_diff: unit must be a string literal");
            }
        };

        let divisor = match unit_divisor(&unit_str) {
            Some(d) => d,
            None => {
                return exec_err!(
                    "time_diff: unsupported unit '{}'. Supported: HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND",
                    unit_str
                );
            }
        };

        let starts = to_time64_array(&args[1], "start", "time_diff", number_rows)?;
        let ends = to_time64_array(&args[2], "end", "time_diff", number_rows)?;

        let mut builder = Int64Builder::with_capacity(number_rows);
        for i in 0..number_rows {
            if starts.is_null(i) || ends.is_null(i) {
                builder.append_null();
            } else {
                let diff = ends.value(i) - starts.value(i);
                // Rust integer division truncates toward zero, matching Spark behavior
                builder.append_value(diff / divisor);
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 3 {
            return exec_err!(
                "Spark `time_diff` function requires 3 arguments, got {}",
                arg_types.len()
            );
        }
        Ok(vec![
            DataType::Utf8,
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Time64(TimeUnit::Microsecond),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unit_divisor() {
        assert_eq!(unit_divisor("HOUR"), Some(3_600_000_000));
        assert_eq!(unit_divisor("hour"), Some(3_600_000_000));
        assert_eq!(unit_divisor("MINUTE"), Some(60_000_000));
        assert_eq!(unit_divisor("SECOND"), Some(1_000_000));
        assert_eq!(unit_divisor("MILLISECOND"), Some(1_000));
        assert_eq!(unit_divisor("MICROSECOND"), Some(1));
        assert_eq!(unit_divisor("DAY"), None);
    }

    #[test]
    fn test_time_diff_truncation() {
        // 20:30:29 and 21:30:28 → diff = 59min 59sec = 3_599_000_000 micros
        // 3_599_000_000 / 3_600_000_000 = 0 (truncated)
        let diff: i64 = 3_599_000_000;
        assert_eq!(diff / 3_600_000_000i64, 0);

        // 20:30:29 and 21:30:29 → diff = 1 hour exactly
        let diff: i64 = 3_600_000_000;
        assert_eq!(diff / 3_600_000_000i64, 1);

        // Negative: 20:30:29 and 12:00:00 → diff = -8h 30min 29sec
        let start: i64 = 20 * 3_600_000_000 + 30 * 60_000_000 + 29 * 1_000_000;
        let end: i64 = 12 * 3_600_000_000;
        let diff = end - start;
        assert_eq!(diff / 3_600_000_000i64, -8);
    }
}
