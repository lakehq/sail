use std::sync::Arc;

use datafusion::arrow::array::{StructArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use datafusion::arrow::error::ArrowError;
use datafusion_common::cast::as_timestamp_microsecond_array;
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

/// The Spark `session_window` of a single row: a `struct<start, end>` where `start` is the time
/// of the row and `end` is the time plus the gap duration in microseconds, which is a literal.
/// A row with a null time has null fields.
///
/// In a grouping, the planner replaces a call to this function with the merged sessions of the
/// rows, so the function itself only runs for a `session_window` outside of a grouping.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSessionWindow {
    signature: Signature,
}

impl Default for SparkSessionWindow {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSessionWindow {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkSessionWindow {
    fn name(&self) -> &str {
        "session_window"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let tz = match arg_types {
            [
                DataType::Timestamp(TimeUnit::Microsecond, tz),
                DataType::Int64,
            ] => tz.clone(),
            other => {
                return plan_err!(
                    "session_window expects a Timestamp(Microsecond, *) and an Int64 argument, got {other:?}"
                );
            }
        };
        let field_type = DataType::Timestamp(TimeUnit::Microsecond, tz);
        Ok(DataType::Struct(Fields::from(vec![
            Field::new("start", field_type.clone(), true),
            Field::new("end", field_type, true),
        ])))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args,
            number_rows,
            return_field,
            ..
        } = args;
        let [time, gap] = args.as_slice() else {
            return exec_err!("session_window requires 2 arguments");
        };
        let ColumnarValue::Scalar(ScalarValue::Int64(Some(gap))) = gap else {
            return exec_err!("the gap duration of session_window must be a literal");
        };
        let DataType::Struct(fields) = return_field.data_type() else {
            return exec_err!("the return type of session_window must be a struct");
        };
        let DataType::Timestamp(TimeUnit::Microsecond, tz) = fields[0].data_type() else {
            return exec_err!("the fields of session_window must be Timestamp(Microsecond, *)");
        };

        let array = time.to_array(number_rows)?;
        let starts = as_timestamp_microsecond_array(&array)?;
        let ends: TimestampMicrosecondArray = starts
            .try_unary(|start| {
                start.checked_add(*gap).ok_or_else(|| {
                    ArrowError::ComputeError(
                        "overflow in the end of the session window".to_string(),
                    )
                })
            })?
            .with_timezone_opt(tz.clone());
        let session = StructArray::new(
            fields.clone(),
            vec![Arc::new(starts.clone()), Arc::new(ends)],
            None,
        );
        Ok(ColumnarValue::Array(Arc::new(session)))
    }
}
