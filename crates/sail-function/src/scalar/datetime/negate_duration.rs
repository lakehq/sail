use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray};
use datafusion::arrow::datatypes::{
    DataType, DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
    DurationSecondType, Field, FieldRef, TimeUnit,
};
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct NegateDuration {
    signature: Signature,
}

impl Default for NegateDuration {
    fn default() -> Self {
        Self::new()
    }
}

impl NegateDuration {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NegateDuration {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "negate_duration"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    /// Unary negation preserves the input's data type *and* its Field metadata
    /// (e.g. the Spark interval qualifier extension metadata on a typed
    /// `DayTimeIntervalType` column). The default `return_field_from_args`
    /// drops the metadata, which causes `-INTERVAL '3' DAY` to render via the
    /// canonical "INTERVAL '0 00:00:00' DAY TO SECOND" formatter instead of
    /// "INTERVAL '-3' DAY".
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [input] = args.arg_fields else {
            return exec_err!(
                "`negate_duration` function requires 1 argument, got {}",
                args.arg_fields.len()
            );
        };
        Ok(Arc::new(
            Field::new(self.name(), input.data_type().clone(), true)
                .with_metadata(input.metadata().clone()),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let [arg] = args.as_slice() else {
            return exec_err!(
                "`negate_duration` function requires 1 argument, got {}",
                args.len()
            );
        };
        match arg {
            ColumnarValue::Scalar(ScalarValue::DurationSecond(val)) => Ok(ColumnarValue::Scalar(
                ScalarValue::DurationSecond(val.map(|x| -x)),
            )),
            ColumnarValue::Scalar(ScalarValue::DurationMillisecond(val)) => Ok(
                ColumnarValue::Scalar(ScalarValue::DurationMillisecond(val.map(|x| -x))),
            ),
            ColumnarValue::Scalar(ScalarValue::DurationMicrosecond(val)) => Ok(
                ColumnarValue::Scalar(ScalarValue::DurationMicrosecond(val.map(|x| -x))),
            ),
            ColumnarValue::Scalar(ScalarValue::DurationNanosecond(val)) => Ok(
                ColumnarValue::Scalar(ScalarValue::DurationNanosecond(val.map(|x| -x))),
            ),
            ColumnarValue::Array(array) => {
                let result: ArrayRef = match array.data_type() {
                    DataType::Duration(TimeUnit::Second) => Arc::new(
                        array
                            .as_primitive::<DurationSecondType>()
                            .unary::<_, DurationSecondType>(|x| -x),
                    ),
                    DataType::Duration(TimeUnit::Millisecond) => Arc::new(
                        array
                            .as_primitive::<DurationMillisecondType>()
                            .unary::<_, DurationMillisecondType>(|x| -x),
                    ),
                    DataType::Duration(TimeUnit::Microsecond) => Arc::new(
                        array
                            .as_primitive::<DurationMicrosecondType>()
                            .unary::<_, DurationMicrosecondType>(|x| -x),
                    ),
                    DataType::Duration(TimeUnit::Nanosecond) => Arc::new(
                        array
                            .as_primitive::<DurationNanosecondType>()
                            .unary::<_, DurationNanosecondType>(|x| -x),
                    ),
                    other => {
                        return exec_err!(
                            "Unsupported data type {other:?} for function negate_duration"
                        )
                    }
                };
                Ok(ColumnarValue::Array(result))
            }
            other => exec_err!("Unsupported arg {other:?} for function negate_duration"),
        }
    }
}
