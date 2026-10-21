use std::sync::Arc;

use datafusion::arrow::array::ArrowNativeTypeOp;
use datafusion::arrow::compute::kernels::numeric::neg;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use sail_common::spec::SAIL_SPARK_INTERVAL_METADATA_KEY;

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
    fn name(&self) -> &str {
        "negate_duration"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let arg = &args.arg_fields[0];
        let mut field = Field::new(self.name(), arg.data_type().clone(), arg.is_nullable());
        // Negation preserves the interval type, not arbitrary column metadata.
        if let Some(interval) = arg.metadata().get(SAIL_SPARK_INTERVAL_METADATA_KEY) {
            field.metadata_mut().insert(
                SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(),
                interval.clone(),
            );
        }
        Ok(Arc::new(field))
    }

    fn is_strict(&self) -> bool {
        true
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
            ColumnarValue::Scalar(ScalarValue::DurationSecond(value)) => Ok(ColumnarValue::Scalar(
                ScalarValue::DurationSecond(value.map(i64::neg_checked).transpose()?),
            )),
            ColumnarValue::Scalar(ScalarValue::DurationMillisecond(value)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::DurationMillisecond(
                    value.map(i64::neg_checked).transpose()?,
                )))
            }
            ColumnarValue::Scalar(ScalarValue::DurationMicrosecond(value)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::DurationMicrosecond(
                    value.map(i64::neg_checked).transpose()?,
                )))
            }
            ColumnarValue::Scalar(ScalarValue::DurationNanosecond(value)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::DurationNanosecond(
                    value.map(i64::neg_checked).transpose()?,
                )))
            }
            ColumnarValue::Array(array) if matches!(array.data_type(), DataType::Duration(_)) => {
                Ok(ColumnarValue::Array(neg(array.as_ref())?))
            }
            _ => exec_err!("Unsupported arg {arg:?} for function negate_duration"),
        }
    }
}
